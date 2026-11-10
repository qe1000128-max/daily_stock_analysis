# -*- coding: utf-8 -*-
# Derived from AlphaSift revision 9f522747caafd3c0b1ddb7e14d5cf44c8580b6cf.
# Licensed under Apache-2.0 and modified for daily_stock_analysis.
"""Optional enrichment layers for the screening pipeline.

This module is deliberately standalone: it adds industry/concepts,
A-share fundamentals and US (EDGAR) fundamentals on top of a snapshot
DataFrame, but it never rewires the pipeline itself. The pipeline can
call :func:`attach_industry` / :func:`enrich_fundamentals` /
:func:`enrich_us_fundamentals` later; until then these helpers are
opt-in and every failure degrades to blank/``None`` values instead of
raising, so enrichment can never fail a screening run.

Layers
------
1. A-share industry (Eastmoney push2 ``clist`` paging). One serial
   paged sweep over the SH/SZ universe plus the Beijing segment,
   ``f100`` carrying the industry name. Eastmoney bans IPs that burst
   (``>5 req/s`` or ``>=200 req/min``), so all Eastmoney requests in
   this module go through one throttled session with a >=1s gap.
2. A-share fundamentals, per-pick only (<=20 codes). There is no
   one-shot whole-market fundamentals cross-section, so this layer is
   for screened picks, not for the snapshot. Source: Eastmoney
   ``RPT_F10_FINANCE_MAINFINADATA`` (latest report period) plus an
   optional Tencent quote for PE-TTM/PB.
3. US fundamentals (SEC EDGAR frames). One request returns a whole
   metric for the entire market; it has no prices/volume, so it is an
   optional screening layer, never a snapshot source. The SEC requires
   a ``User-Agent`` with a real name and email; it is read from the
   ``EDGAR_USER_AGENT`` environment variable only, never hardcoded.

Tunables live as module constants / environment variables read inside
this module only (no new ``config.py`` items). See
``docs/screening-enrichment.md``.
"""

from __future__ import annotations

import json
import os
import random
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.services.screening.normalize import (
    normalize_code as _normalize_code,
    safe_float as _safe_float,
    safe_text as _safe_text,
)

_PROJECT_ROOT = Path(__file__).resolve().parents[3]

# ---------------------------------------------------------------------------
# Tunables (module-level constants; env overrides read inside this module)
# ---------------------------------------------------------------------------

EASTMONEY_CLIST_URL = "https://push2.eastmoney.com/api/qt/clist/get"
EASTMONEY_SLIST_URL = "https://push2.eastmoney.com/api/qt/slist/get"
EASTMONEY_MAINFINADATA_URL = "https://datacenter.eastmoney.com/securities/api/data/get"
TENCENT_QUOTE_URL_TEMPLATE = "https://qt.gtimg.cn/q={prefix}{code}"
EDGAR_FRAMES_URL_TEMPLATE = "https://data.sec.gov/api/xbrl/frames/{taxonomy}/{metric}/{unit}/{period}.json"
EDGAR_TICKER_CIK_URL = "https://www.sec.gov/files/company_tickers.json"

# push2 clist: SH main + STAR + SZ main + ChiNext (akshare
# stock_zh_a_spot_em universe), plus the Beijing segment. The Beijing
# segment ("m:0+t:81+s:2048") also returns non-stock rows (81xxxx);
# those are filtered out in _parse_clist_rows.
CLIST_FS_SEGMENTS = (
    "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23",
    "m:0+t:81+s:2048",
)
CLIST_FIELDS = "f12,f13,f14,f100"
CLIST_CACHE_SCHEMA = "v1"
CLIST_CACHE_FILENAME = "eastmoney_clist_map_v1.json"
EDGAR_TICKER_CACHE_FILENAME = "edgar_company_tickers_v1.json"

DEFAULT_EASTMONEY_MIN_INTERVAL_SEC = 1.0
DEFAULT_EASTMONEY_JITTER_SEC = 0.3
DEFAULT_CLIST_PAGE_SIZE = 100
DEFAULT_CLIST_CACHE_TTL_HOURS = 24.0
DEFAULT_EDGAR_MIN_INTERVAL_SEC = 0.2
DEFAULT_EDGAR_TIMEOUT_SEC = 30.0
DEFAULT_TICKER_CIK_CACHE_TTL_HOURS = 24.0 * 7
MAX_FUNDAMENTAL_CODES = 20

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)

# Fundamental fields exposed by enrich_fundamentals, mapped from
# RPT_F10_FINANCE_MAINFINADATA columns (latest report period).
# MAINFINADATA carries no PE/PB/dividend: PE-TTM/PB come from the
# optional Tencent quote instead; dividend yield is not provided by
# either source in this layer.
FUNDAMENTAL_FIELD_COLUMNS: dict[str, str] = {
    "eps": "EPSJB",
    "bps": "BPS",
    "roe": "ROEJQ",
    "roic": "ROIC",
    "gross_margin": "XSMLL",
    "net_margin": "XSJLL",
    "revenue": "TOTALOPERATEREVE",
    "net_profit": "PARENTNETPROFIT",
    "operating_cash_per_share": "MGJYXJJE",
    "asset_turnover": "ZCFZL",
}
DEFAULT_FUNDAMENTAL_FIELDS: tuple[str, ...] = tuple(FUNDAMENTAL_FIELD_COLUMNS)

# ---------------------------------------------------------------------------
# Throttled shared sessions (serial, >= min_interval between requests)
# ---------------------------------------------------------------------------

_EM_LOCK = threading.Lock()
_EM_LAST_CALL = 0.0
_EM_SESSION: requests.Session | None = None
_EDGAR_LOCK = threading.Lock()
_EDGAR_LAST_CALL = 0.0
_EDGAR_SESSION: requests.Session | None = None


def _float_env(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return float(default)
    try:
        return float(raw)
    except ValueError:
        return float(default)


def _eastmoney_min_interval_seconds() -> float:
    return max(
        0.0,
        _float_env(
            "SCREENING_ENRICHMENT_EASTMONEY_MIN_INTERVAL_SEC",
            DEFAULT_EASTMONEY_MIN_INTERVAL_SEC,
        ),
    )


def _eastmoney_jitter_seconds() -> float:
    return max(
        0.0,
        _float_env(
            "SCREENING_ENRICHMENT_EASTMONEY_JITTER_SEC",
            DEFAULT_EASTMONEY_JITTER_SEC,
        ),
    )


def _edgar_min_interval_seconds() -> float:
    return max(
        0.0,
        _float_env(
            "SCREENING_ENRICHMENT_EDGAR_MIN_INTERVAL_SEC",
            DEFAULT_EDGAR_MIN_INTERVAL_SEC,
        ),
    )


def _build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=2,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _throttled_get(
    session: requests.Session | None,
    url: str,
    *,
    params: dict[str, str] | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = 15.0,
    min_interval: float | None = None,
    jitter: float | None = None,
    lock: threading.Lock = _EM_LOCK,
    session_kind: str = "eastmoney",
) -> requests.Response:
    """GET through a shared session, enforcing a >= min_interval gap.

    ``session`` may be injected (tests); otherwise the shared session
    for ``session_kind`` ("eastmoney" or "edgar") is used. Retries on
    429/5xx are handled by the session adapter; a 403 is an Eastmoney
    wind-control signal and is not retried.
    """
    if session_kind == "edgar":
        interval = (
            _edgar_min_interval_seconds()
            if min_interval is None
            else max(0.0, float(min_interval))
        )
        jitter_seconds = 0.0 if jitter is None else max(0.0, float(jitter))
        last_attr, session_attr = "_EDGAR_LAST_CALL", "_EDGAR_SESSION"
    else:
        interval = (
            _eastmoney_min_interval_seconds()
            if min_interval is None
            else max(0.0, float(min_interval))
        )
        jitter_seconds = (
            _eastmoney_jitter_seconds()
            if jitter is None
            else max(0.0, float(jitter))
        )
        last_attr, session_attr = "_EM_LAST_CALL", "_EM_SESSION"
    with lock:
        last = globals()[last_attr]
        wait = interval - (time.monotonic() - last)
        if wait > 0:
            time.sleep(wait + random.uniform(0.0, jitter_seconds))
        active_session = session
        if active_session is None:
            active_session = globals()[session_attr]
            if active_session is None:
                active_session = _build_session()
                globals()[session_attr] = active_session
        response = active_session.get(url, params=params, headers=headers, timeout=timeout)
        globals()[last_attr] = time.monotonic()
    response.raise_for_status()
    return response


def _eastmoney_get(
    url: str,
    *,
    params: dict[str, str] | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = 15.0,
    min_interval: float | None = None,
    jitter: float | None = None,
    session: requests.Session | None = None,
) -> requests.Response:
    merged = {"User-Agent": _USER_AGENT}
    merged.update(headers or {})
    return _throttled_get(
        session,
        url,
        params=params,
        headers=merged,
        timeout=timeout,
        min_interval=min_interval,
        jitter=jitter,
        lock=_EM_LOCK,
        session_kind="eastmoney",
    )


# ---------------------------------------------------------------------------
# Cache helpers (reuse the industry_provider_cache_dir / TTL convention)
# ---------------------------------------------------------------------------


def default_cache_dir() -> Path:
    """Resolve the enrichment cache dir (same convention as industry.py)."""
    explicit = (
        os.getenv("SCREENING_INDUSTRY_PROVIDER_CACHE_DIR", "").strip()
        or os.getenv("INDUSTRY_PROVIDER_CACHE_DIR", "").strip()
    )
    if explicit:
        return Path(explicit)
    data_dir = Path(os.getenv("SCREENING_DATA_DIR", str(_PROJECT_ROOT / "data")))
    return data_dir / "industry_provider_cache"


def _cache_ttl_hours() -> float:
    raw = (
        os.getenv("SCREENING_INDUSTRY_PROVIDER_CACHE_TTL_HOURS", "").strip()
        or os.getenv("INDUSTRY_PROVIDER_CACHE_TTL_HOURS", "").strip()
        or str(DEFAULT_CLIST_CACHE_TTL_HOURS)
    )
    try:
        return max(0.0, float(raw))
    except ValueError:
        return float(DEFAULT_CLIST_CACHE_TTL_HOURS)


def _resolve_cache_dir(cache_dir: str | Path | None) -> Path | None:
    if cache_dir is None:
        return default_cache_dir()
    return Path(cache_dir)


def _today_str() -> str:
    return datetime.now().date().isoformat()


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> str:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_name(f".{path.name}.{time.time_ns()}.tmp")
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp_path.replace(path)
        return f"cache saved: {path}"
    except Exception as exc:  # noqa: BLE001 - cache must never break enrichment
        return f"cache skipped: {path} error={exc}"


# ---------------------------------------------------------------------------
# Layer 1: A-share industry via Eastmoney push2 clist paging
# ---------------------------------------------------------------------------


def _secucode(code: str) -> str:
    """Return an Eastmoney SECUCODE such as ``600519.SH`` for a 6-digit code."""
    normalized = _normalize_code(code)
    if not normalized:
        return ""
    if normalized.startswith(("60", "68", "9")):
        return f"{normalized}.SH"
    if normalized.startswith("92"):
        return f"{normalized}.BJ"
    return f"{normalized}.SZ"


def _parse_clist_rows(
    rows: list[dict[str, Any]],
    *,
    beijing_segment: bool = False,
) -> tuple[dict[str, dict[str, str]], int]:
    """Parse one clist page: code -> {"industry", "name"}.

    ``f100`` is the industry name on clist (verified live: 600519-adjacent
    rows carry e.g. "半导体", "电力", "医疗器械"). The Beijing segment
    also returns non-stock rows (81xxxx); only 92xxxx codes are kept
    from it and the rest are counted as dropped.
    """
    mapping: dict[str, dict[str, str]] = {}
    dropped = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        code = _normalize_code(row.get("f12"))
        if not code or code == "000000":
            continue
        if beijing_segment and not code.startswith("92"):
            dropped += 1
            continue
        industry = _safe_text(row.get("f100"))
        if not industry:
            continue
        mapping[code] = {
            "industry": industry,
            "name": _safe_text(row.get("f14")),
        }
    return mapping, dropped


def fetch_eastmoney_clist_map(
    *,
    page_size: int = DEFAULT_CLIST_PAGE_SIZE,
    max_pages: int | None = None,
    cache_dir: str | Path | None = None,
    cache_ttl_hours: float | None = None,
    force_refresh: bool = False,
    min_interval: float | None = None,
    jitter: float | None = None,
    session: requests.Session | None = None,
) -> tuple[dict[str, dict[str, str]], list[str]]:
    """Fetch the whole-market code -> industry map via push2 clist paging.

    Serial paging with a >=1s gap between requests (Eastmoney bans
    bursty IPs). Results are cached per calendar day under the
    industry-provider cache dir; a fresh cache hit returns without any
    HTTP request. On any failure an empty mapping is returned with the
    error recorded in ``notes`` -- never raises.
    """
    notes: list[str] = []
    resolved_dir = _resolve_cache_dir(cache_dir)
    ttl_seconds = (DEFAULT_CLIST_CACHE_TTL_HOURS if cache_ttl_hours is None else float(cache_ttl_hours)) * 3600
    cache_path = resolved_dir / CLIST_CACHE_FILENAME if resolved_dir is not None else None
    today = _today_str()
    if cache_path is not None and not force_refresh:
        cached, cache_note = _read_clist_cache(cache_path, ttl_seconds=ttl_seconds, today=today)
        if cache_note:
            notes.append(cache_note)
        if cached is not None:
            return cached, notes

    page_size = max(1, min(int(page_size), 200))
    mapping: dict[str, dict[str, str]] = {}
    total_dropped = 0
    pages_fetched = 0
    try:
        for segment_index, fs in enumerate(CLIST_FS_SEGMENTS):
            page = 1
            total: int | None = None
            while True:
                if max_pages is not None and pages_fetched >= max_pages:
                    notes.append(f"clist paging stopped at max_pages={max_pages}")
                    break
                response = _eastmoney_get(
                    EASTMONEY_CLIST_URL,
                    params={
                        "pn": str(page),
                        "pz": str(page_size),
                        "po": "1",
                        "np": "1",
                        "fltt": "2",
                        "invt": "2",
                        "fid": "f12",
                        "fs": fs,
                        "fields": CLIST_FIELDS,
                    },
                    timeout=15,
                    min_interval=min_interval,
                    jitter=jitter,
                    session=session,
                )
                payload = response.json()
                data = payload.get("data") if isinstance(payload, dict) else None
                if not isinstance(data, dict) or payload.get("rc") != 0:
                    raise RuntimeError(f"clist returned unexpected payload: {str(payload)[:120]}")
                page_total = data.get("total")
                if not isinstance(page_total, int) or isinstance(page_total, bool) or page_total < 0:
                    raise RuntimeError(f"clist missing total: {str(payload)[:120]}")
                rows = data.get("diff")
                if rows is None:
                    rows = []
                if not isinstance(rows, list) or not all(isinstance(r, dict) for r in rows):
                    raise RuntimeError("clist diff is not a list of objects")
                if total is None:
                    total = page_total
                elif page_total != total:
                    raise RuntimeError(
                        f"clist total changed mid-paging {total} -> {page_total}; retry later"
                    )
                page_mapping, dropped = _parse_clist_rows(
                    rows, beijing_segment=(segment_index == 1)
                )
                mapping.update(page_mapping)
                total_dropped += dropped
                pages_fetched += 1
                if not rows or page * page_size >= total:
                    break
                page += 1
    except Exception as exc:  # noqa: BLE001 - enrichment must never raise
        notes.append(f"clist fetch failed after {pages_fetched} pages: {exc}")
        return {}, notes

    notes.append(
        f"clist map fetched: rows={len(mapping)} pages={pages_fetched} "
        f"dropped_non_stock_bj={total_dropped}"
    )
    if cache_path is not None and mapping:
        cache_note = _write_json_atomic(
            cache_path,
            {
                "schema": CLIST_CACHE_SCHEMA,
                "provider": "eastmoney_clist",
                "trade_date": today,
                "created_at": datetime.now().isoformat(),
                "mapping": mapping,
            },
        )
        notes.append(cache_note)
    return mapping, notes


def _read_clist_cache(
    path: Path, *, ttl_seconds: float, today: str
) -> tuple[dict[str, dict[str, str]] | None, str]:
    try:
        stat = path.stat()
    except FileNotFoundError:
        return None, ""
    if ttl_seconds <= 0:
        return None, f"clist cache expired: {path}"
    if time.time() - stat.st_mtime > ttl_seconds:
        return None, f"clist cache expired: {path}"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001 - corrupt cache is a miss, not an error
        return None, f"clist cache skipped: {path} error={exc}"
    if not isinstance(payload, dict) or payload.get("schema") != CLIST_CACHE_SCHEMA:
        return None, f"clist cache skipped: {path} schema mismatch"
    if payload.get("trade_date") != today:
        return None, f"clist cache stale (trade_date={payload.get('trade_date')} != {today}): {path}"
    raw_mapping = payload.get("mapping")
    if not isinstance(raw_mapping, dict):
        return None, f"clist cache skipped: {path} invalid mapping"
    mapping: dict[str, dict[str, str]] = {}
    for code, item in raw_mapping.items():
        normalized = _normalize_code(code)
        if not normalized or not isinstance(item, dict):
            continue
        industry = _safe_text(item.get("industry"))
        if industry:
            mapping[normalized] = {
                "industry": industry,
                "name": _safe_text(item.get("name")),
            }
    return mapping, f"clist cache hit: {path} rows={len(mapping)}"


def attach_industry(
    df: pd.DataFrame,
    *,
    mapping: dict[str, dict[str, str]] | None = None,
    cache_dir: str | Path | None = None,
    cache_ttl_hours: float | None = None,
    force_refresh: bool = False,
    min_interval: float | None = None,
    jitter: float | None = None,
    session: requests.Session | None = None,
) -> tuple[pd.DataFrame, list[str]]:
    """Fill the ``industry`` column of a snapshot DataFrame from clist.

    Only blank ``industry`` cells are filled; existing values (e.g.
    from ``industry.py`` map files or the snapshot source itself) are
    never overwritten. Rows with no industry in the map stay blank.
    Never raises: on fetch/cache failure the frame is returned
    unchanged (with the ``industry`` column ensured) and the error is
    recorded in ``notes``.
    """
    result = df.copy()
    notes: list[str] = []
    if result.empty or "code" not in result.columns:
        return result, notes
    if "industry" not in result.columns:
        result["industry"] = ""
    if mapping is None:
        try:
            mapping, fetch_notes = fetch_eastmoney_clist_map(
                cache_dir=cache_dir,
                cache_ttl_hours=cache_ttl_hours,
                force_refresh=force_refresh,
                min_interval=min_interval,
                jitter=jitter,
                session=session,
            )
        except Exception as exc:  # noqa: BLE001 - enrichment must never raise
            notes.append(f"industry enrichment failed: {exc}")
            return result, notes
        notes.extend(fetch_notes)
    if not mapping:
        return result, notes
    current = result["industry"].map(_safe_text)
    incoming = result["code"].map(_normalize_code).map(
        lambda code: mapping.get(code, {}).get("industry", "")
    )
    mask = current.eq("") & incoming.ne("")
    if mask.any():
        result.loc[mask, "industry"] = incoming[mask].to_numpy()
    notes.append(f"industry attached: filled={int(mask.sum())} of {len(result)} rows")
    return result, notes


def fetch_stock_boards(
    code: str,
    *,
    min_interval: float | None = None,
    jitter: float | None = None,
    session: requests.Session | None = None,
) -> tuple[dict[str, Any], list[str]]:
    """Fetch one stock's board membership (industry/concept/region).

    Eastmoney push2 ``slist`` with ``spt=3`` returns the industry,
    concept and region boards a stock belongs to, plus each board's
    daily change and lead stock. This is one request per stock, so it
    is a per-pick helper, not a whole-market sweep. Returns
    ``{"total", "boards": [{name, code, change_pct, lead_stock}],
    "concept_tags": [board names]}``.
    """
    notes: list[str] = []
    normalized = _normalize_code(code)
    if not normalized:
        return {"total": 0, "boards": [], "concept_tags": []}, ["stock boards skipped: bad code"]
    market_code = 1 if normalized.startswith(("6", "9")) else 0
    try:
        response = _eastmoney_get(
            EASTMONEY_SLIST_URL,
            params={
                "fltt": "2",
                "invt": "2",
                "secid": f"{market_code}.{normalized}",
                "spt": "3",
                "pi": "0",
                "pz": "200",
                "po": "1",
                "fields": "f12,f14,f3,f128",
            },
            headers={"Referer": "https://quote.eastmoney.com/"},
            timeout=15,
            min_interval=min_interval,
            jitter=jitter,
            session=session,
        )
        payload = response.json()
    except Exception as exc:  # noqa: BLE001 - per-stock enrichment must not raise
        return {"total": 0, "boards": [], "concept_tags": []}, [
            f"stock boards failed for {normalized}: {exc}"
        ]
    diff = (payload.get("data") or {}).get("diff") if isinstance(payload, dict) else None
    items = diff.values() if isinstance(diff, dict) else (diff or [])
    boards = [
        {
            "name": _safe_text(item.get("f14")),
            "code": _safe_text(item.get("f12")),
            "change_pct": _safe_float(item.get("f3")),
            "lead_stock": _safe_text(item.get("f128")),
        }
        for item in items
        if isinstance(item, dict) and _safe_text(item.get("f14"))
    ]
    return {
        "total": len(boards),
        "boards": boards,
        "concept_tags": [board["name"] for board in boards],
    }, notes


# ---------------------------------------------------------------------------
# Layer 2: A-share fundamentals, per-pick only
# ---------------------------------------------------------------------------


def _tencent_prefix(code: str) -> str:
    normalized = _normalize_code(code)
    if normalized.startswith(("92", "8")):
        return "bj"
    if normalized.startswith(("6", "9")):
        return "sh"
    return "sz"


def _fetch_tencent_quote(
    code: str,
    *,
    timeout: float = 30.0,
    min_interval: float | None = None,
    jitter: float | None = None,
    session: requests.Session | None = None,
) -> dict[str, float | None]:
    """Best-effort Tencent quote: PE-TTM (field 39) and PB (field 46).

    Tencent is not Eastmoney, but the same serial throttle is applied
    because the quote is fetched once per pick inside this layer.
    Returns ``{"pe_ttm": ..., "pb": ...}`` with ``None`` on any failure.
    """
    normalized = _normalize_code(code)
    result: dict[str, float | None] = {"pe_ttm": None, "pb": None}
    if not normalized:
        return result
    try:
        response = _eastmoney_get(
            TENCENT_QUOTE_URL_TEMPLATE.format(prefix=_tencent_prefix(normalized), code=normalized),
            timeout=timeout,
            min_interval=min_interval,
            jitter=jitter,
            session=session,
        )
        text = response.text
        payload = text.split('"')[1].split("~")
        if len(payload) > 46:
            result["pe_ttm"] = _safe_float(payload[39])
            result["pb"] = _safe_float(payload[46])
    except Exception:  # noqa: BLE001 - quote fallback must not raise
        return result
    return result


def enrich_fundamentals(
    codes: list[str],
    *,
    fields: list[str] | None = None,
    include_quote: bool = True,
    timeout: float = 30.0,
    min_interval: float | None = None,
    jitter: float | None = None,
    session: requests.Session | None = None,
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    """Enrich a SMALL list of screened picks with fundamentals.

    Per code (serial, throttled): the latest
    ``RPT_F10_FINANCE_MAINFINADATA`` row (report period, EPS, BPS,
    ROE, ROIC, margins, revenue, net profit, operating cash per
    share, asset turnover) plus, when ``include_quote`` is true, the
    Tencent PE-TTM/PB. Missing fields are ``None``; a per-code failure
    yields an empty dict for that code and a note -- never raises
    (except for misuse: more than :data:`MAX_FUNDAMENTAL_CODES`
    codes, which is rejected upfront because this layer must not
    become a whole-market sweep).
    """
    normalized_codes: list[str] = []
    for code in codes or []:
        normalized = _normalize_code(code)
        if normalized and normalized not in normalized_codes:
            normalized_codes.append(normalized)
    if len(normalized_codes) > MAX_FUNDAMENTAL_CODES:
        raise ValueError(
            f"enrich_fundamentals takes at most {MAX_FUNDAMENTAL_CODES} codes "
            f"(per-pick enrichment only); got {len(normalized_codes)}"
        )
    wanted = list(fields) if fields is not None else list(DEFAULT_FUNDAMENTAL_FIELDS)
    unknown = [field for field in wanted if field not in FUNDAMENTAL_FIELD_COLUMNS]
    if unknown:
        raise ValueError(f"unknown fundamental fields: {unknown}")
    notes: list[str] = []
    results: dict[str, dict[str, Any]] = {}
    for code in normalized_codes:
        entry: dict[str, Any] = {}
        try:
            response = _eastmoney_get(
                EASTMONEY_MAINFINADATA_URL,
                params={
                    "type": "RPT_F10_FINANCE_MAINFINADATA",
                    "sty": "APP_F10_MAINFINADATA",
                    "quoteColumns": "",
                    "filter": f'(SECUCODE="{_secucode(code)}")',
                    "p": "1",
                    "ps": "1",
                    "sr": "-1",
                    "st": "REPORT_DATE",
                    "source": "HSF10",
                    "client": "PC",
                },
                timeout=timeout,
                min_interval=min_interval,
                jitter=jitter,
                session=session,
            )
            payload = response.json()
            rows = (payload.get("result") or {}).get("data") if isinstance(payload, dict) else None
            if payload.get("code") != 0 or not rows:
                raise RuntimeError(
                    f"MAINFINADATA returned code={payload.get('code')} "
                    f"message={payload.get('message')}"
                )
            row = rows[0]
            entry["report_date"] = _safe_text(str(row.get("REPORT_DATE", ""))[:10])
            entry["report_name"] = _safe_text(row.get("REPORT_DATE_NAME"))
            for field in wanted:
                entry[field] = _safe_float(row.get(FUNDAMENTAL_FIELD_COLUMNS[field]))
        except Exception as exc:  # noqa: BLE001 - per-code failure degrades, never raises
            notes.append(f"fundamentals failed for {code}: {exc}")
            results[code] = {}
            continue
        if include_quote:
            quote = _fetch_tencent_quote(
                code, timeout=timeout, min_interval=min_interval, jitter=jitter, session=session
            )
            entry["pe_ttm"] = quote["pe_ttm"]
            entry["pb"] = quote["pb"]
            if quote["pe_ttm"] is None and quote["pb"] is None:
                notes.append(f"Tencent quote unavailable for {code}: pe_ttm/pb left blank")
        results[code] = entry
    notes.append(f"fundamentals enriched: {len(results)} codes")
    return results, notes


# ---------------------------------------------------------------------------
# Layer 3: US fundamentals via SEC EDGAR frames
# ---------------------------------------------------------------------------


def _edgar_user_agent(user_agent: str | None) -> str:
    """Resolve the SEC User-Agent; it must name a real person + email."""
    candidate = (user_agent or os.getenv("EDGAR_USER_AGENT", "")).strip()
    if not candidate:
        raise ValueError(
            "EDGAR_USER_AGENT is required (SEC fair-access rules: "
            "User-Agent must declare a real name and email, e.g. "
            "'Research Name name@example.com'). Set the EDGAR_USER_AGENT "
            "environment variable or pass user_agent= explicitly. No "
            "default identity is used."
        )
    if "@" not in candidate:
        raise ValueError(
            "EDGAR_USER_AGENT must include an email address "
            "(SEC fair-access rules); got a value without '@'."
        )
    return candidate


def fetch_edgar_frames(
    metric: str,
    unit: str,
    period: str,
    *,
    taxonomy: str = "us-gaap",
    user_agent: str | None = None,
    min_interval: float | None = None,
    session: requests.Session | None = None,
    timeout: float = DEFAULT_EDGAR_TIMEOUT_SEC,
) -> tuple[dict[int, float], list[str]]:
    """Fetch one EDGAR frames metric for the whole market.

    ``GET data.sec.gov/api/xbrl/frames/{taxonomy}/{metric}/{unit}/{period}.json``
    returns every company's value for that metric/period in one
    response (e.g. NetIncomeLoss/USD/CY2025Q1 covers ~5,300
    companies). Returns ``{cik: value}``; when a CIK appears more
    than once (restatements) the last row wins and the duplicate count
    is noted. Raises ``ValueError`` for bad arguments or a missing
    ``EDGAR_USER_AGENT``; raises ``RuntimeError`` for HTTP/parse
    failures (callers that must not fail should catch it).
    """
    metric = (metric or "").strip()
    unit = (unit or "").strip()
    period = (period or "").strip()
    taxonomy = (taxonomy or "us-gaap").strip()
    if not metric or not unit or not period:
        raise ValueError("fetch_edgar_frames requires metric, unit and period")
    agent = _edgar_user_agent(user_agent)
    url = EDGAR_FRAMES_URL_TEMPLATE.format(
        taxonomy=taxonomy, metric=metric, unit=unit, period=period
    )
    response = _throttled_get(
        session,
        url,
        headers={
            "User-Agent": agent,
            "Accept-Encoding": "gzip, deflate",
            "Host": "data.sec.gov",
        },
        timeout=timeout,
        min_interval=min_interval,
        jitter=0.0,
        lock=_EDGAR_LOCK,
        session_kind="edgar",
    )
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(f"EDGAR frames returned non-JSON for {metric}/{unit}/{period}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"EDGAR frames returned unexpected payload for {metric}/{unit}/{period}")
    rows = payload.get("data")
    if not isinstance(rows, list):
        raise RuntimeError(
            f"EDGAR frames payload for {metric}/{unit}/{period} has no 'data' list"
        )
    mapping: dict[int, float] = {}
    duplicates = 0
    skipped = 0
    for row in rows:
        if not isinstance(row, dict):
            skipped += 1
            continue
        cik = row.get("cik")
        value = _safe_float(row.get("val"))
        if not isinstance(cik, int) or isinstance(cik, bool) or value is None:
            skipped += 1
            continue
        if cik in mapping:
            duplicates += 1
        mapping[cik] = value
    label = _safe_text(payload.get("label"))
    notes = [
        f"EDGAR frames {taxonomy}/{metric}/{unit}/{period}: "
        f"label={label or '-'} companies={len(mapping)} "
        f"rows={len(rows)} duplicates={duplicates} skipped={skipped}"
    ]
    return mapping, notes


def fetch_ticker_cik_map(
    *,
    cache_dir: str | Path | None = None,
    cache_ttl_hours: float | None = None,
    force_refresh: bool = False,
    user_agent: str | None = None,
    session: requests.Session | None = None,
    timeout: float = DEFAULT_EDGAR_TIMEOUT_SEC,
) -> tuple[dict[str, int], list[str]]:
    """Download (once, cached) the SEC ticker -> CIK table.

    Source: ``https://www.sec.gov/files/company_tickers.json``.
    Cached for 7 days by default under the enrichment cache dir.
    Requires ``EDGAR_USER_AGENT`` (same rule as frames). On failure
    an empty mapping is returned with the error in ``notes``.
    """
    notes: list[str] = []
    resolved_dir = _resolve_cache_dir(cache_dir)
    ttl_seconds = (
        DEFAULT_TICKER_CIK_CACHE_TTL_HOURS
        if cache_ttl_hours is None
        else float(cache_ttl_hours)
    ) * 3600
    cache_path = resolved_dir / EDGAR_TICKER_CACHE_FILENAME if resolved_dir is not None else None
    if cache_path is not None and not force_refresh:
        try:
            stat = cache_path.stat()
            if ttl_seconds > 0 and time.time() - stat.st_mtime <= ttl_seconds:
                payload = json.loads(cache_path.read_text(encoding="utf-8"))
                mapping = _parse_ticker_cik_payload(payload)
                if mapping:
                    return mapping, [f"ticker-CIK cache hit: {cache_path} rows={len(mapping)}"]
                notes.append(f"ticker-CIK cache skipped: {cache_path} invalid payload")
        except FileNotFoundError:
            pass
        except Exception as exc:  # noqa: BLE001 - corrupt cache is a miss
            notes.append(f"ticker-CIK cache skipped: {cache_path} error={exc}")
    try:
        agent = _edgar_user_agent(user_agent)
        response = _throttled_get(
            session,
            EDGAR_TICKER_CIK_URL,
            headers={"User-Agent": agent, "Accept-Encoding": "gzip, deflate"},
            timeout=timeout,
            min_interval=None,
            jitter=0.0,
            lock=_EDGAR_LOCK,
            session_kind="edgar",
        )
        payload = response.json()
    except Exception as exc:  # noqa: BLE001 - ticker map failure degrades, never raises
        notes.append(f"ticker-CIK download failed: {exc}")
        return {}, notes
    mapping = _parse_ticker_cik_payload(payload)
    if not mapping:
        notes.append("ticker-CIK download returned no rows")
        return {}, notes
    notes.append(f"ticker-CIK downloaded: rows={len(mapping)}")
    if cache_path is not None:
        cache_note = _write_json_atomic(
            cache_path,
            {
                "schema": CLIST_CACHE_SCHEMA,
                "provider": "edgar_ticker_cik",
                "fetched_at": datetime.now().isoformat(),
                "mapping": {ticker: cik for ticker, cik in sorted(mapping.items())},
            },
        )
        notes.append(cache_note)
    return mapping, notes


def _parse_ticker_cik_payload(payload: Any) -> dict[str, int]:
    """Parse company_tickers.json (or its cached {"mapping": ...} form)."""
    mapping: dict[str, int] = {}
    if isinstance(payload, dict) and isinstance(payload.get("mapping"), dict):
        raw_items = payload["mapping"].items()
        for ticker, cik in raw_items:
            text = _safe_text(ticker).upper()
            parsed = _safe_float(cik)
            if text and parsed is not None:
                mapping[text] = int(parsed)
        return mapping
    if isinstance(payload, dict):
        for item in payload.values():
            if not isinstance(item, dict):
                continue
            ticker = _safe_text(item.get("ticker")).upper()
            cik = _safe_float(item.get("cik_str"))
            if ticker and cik is not None:
                mapping[ticker] = int(cik)
    return mapping


def enrich_us_fundamentals(
    tickers: list[str],
    metric: str,
    unit: str,
    period: str,
    *,
    taxonomy: str = "us-gaap",
    cache_dir: str | Path | None = None,
    cache_ttl_hours: float | None = None,
    user_agent: str | None = None,
    min_interval: float | None = None,
    session: requests.Session | None = None,
    timeout: float = DEFAULT_EDGAR_TIMEOUT_SEC,
) -> tuple[dict[str, float], list[str]]:
    """Attach one EDGAR frames metric to a list of US tickers.

    One frames request (whole market) + the cached ticker-CIK table;
    tickers with no CIK or no value in the frame are absent from the
    result and noted. Never raises for missing tickers/values; bad
    arguments or a missing ``EDGAR_USER_AGENT`` raise ``ValueError``.
    """
    normalized = sorted({_safe_text(ticker).upper() for ticker in tickers or [] if _safe_text(ticker)})
    notes: list[str] = []
    cik_map, cik_notes = fetch_ticker_cik_map(
        cache_dir=cache_dir,
        cache_ttl_hours=cache_ttl_hours,
        user_agent=user_agent,
        session=session,
        timeout=timeout,
    )
    notes.extend(cik_notes)
    if not cik_map:
        return {}, notes
    frames, frame_notes = fetch_edgar_frames(
        metric,
        unit,
        period,
        taxonomy=taxonomy,
        user_agent=user_agent,
        min_interval=min_interval,
        session=session,
        timeout=timeout,
    )
    notes.extend(frame_notes)
    results: dict[str, float] = {}
    missing: list[str] = []
    for ticker in normalized:
        cik = cik_map.get(ticker)
        if cik is None or cik not in frames:
            missing.append(ticker)
            continue
        results[ticker] = frames[cik]
    if missing:
        notes.append(f"tickers without a value in this frame: {','.join(missing)}")
    notes.append(f"US fundamentals enriched: {len(results)} of {len(normalized)} tickers")
    return results, notes
