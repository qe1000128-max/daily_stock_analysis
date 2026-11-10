# -*- coding: utf-8 -*-
# Derived from AlphaSift revision 9f522747caafd3c0b1ddb7e14d5cf44c8580b6cf.
# Licensed under Apache-2.0 and modified for daily_stock_analysis.
"""US equity snapshot via Tencent / Sina / yfinance.

US snapshot provider for the screening L1 pipeline. Fetches a configurable
equity universe and returns the standard snapshot DataFrame schema.

Source chain, first success wins (see ``DEFAULT_US_SNAPSHOT_SOURCE_PRIORITY``
in ``src.services.screening.config``):

    tencent  — Tencent US batch quotes (``qt.gtimg.cn/q=usXXX``, no key).
               Supplies price, change, amount, market caps, PE, PB and
               turnover natively; ``volume_ratio`` is enriched from Sina
               US daily K-lines (see :func:`enrich_volume_ratio_us`).
    sina_gb  — Sina ``gb_`` batch quotes (``hq.sinajs.cn/list=gb_xxx``,
               no key). Partial: price, change, volume, 52-week high/low,
               market cap, EPS and PE only — no amount, PB, turnover or
               share counts, so it cannot serve strategies that require
               those columns (required-column gating skips it then).
    yfinance — yfinance download + fast_info (last resort; fragile).

HK is not supported yet: there is no HK universe source or ticker
configuration path, so ``market="hk"`` is rejected at the pipeline level
rather than silently screening the US pool.
"""

import json
import logging
import os
import re
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests

from src.core.trading_calendar import MARKET_TIMEZONE
from src.services.screening.config import (
    DEFAULT_US_SNAPSHOT_SOURCE_PRIORITY,
    resolve_us_snapshot_source_priority,
)
from src.services.screening.source_guard import (
    call_with_timeout,
    parse_source_timeout_seconds,
)

logger = logging.getLogger(__name__)

_SP500_WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

_DEFAULT_US_UNIVERSE = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "BRK-B",
    "AVGO", "JPM", "LLY", "V", "MA", "UNH", "XOM", "COST", "HD", "PG",
    "JNJ", "ABBV", "WMT", "NFLX", "BAC", "KO", "CRM", "CVX", "MRK",
    "PEP", "AMD", "TMO", "LIN", "ACN", "CSCO", "MCD", "ABT", "ADBE",
    "WFC", "GE", "DHR", "TXN", "PM", "ISRG", "MS", "NEE", "INTU",
    "DIS", "QCOM", "CAT", "NOW",
]

_BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)
_SINA_REFERER = "https://finance.sina.com.cn/"

# Tencent US batch quotes (global-stock-data Layer 1.1; a-stock-data style
# chunking reused from the CN Tencent path: 500 codes/chunk, 3 workers max —
# qt.gtimg.cn throttles bursts from one egress IP).
_TENCENT_US_QUOTE_URL = "https://qt.gtimg.cn/q="
_TENCENT_US_CHUNK_SIZE = 500
_TENCENT_US_MAX_WORKERS = 3
_TENCENT_US_TIMEOUT_SECONDS = 20
_TENCENT_US_CHUNK_RETRIES = 3

# Sina US batch quotes (global-stock-data Layer 1.1). The endpoint 403s
# without a browser User-Agent + Referer.
_SINA_US_QUOTE_URL = "https://hq.sinajs.cn/list="
_SINA_US_CHUNK_SIZE = 500
_SINA_US_MAX_WORKERS = 3
_SINA_US_TIMEOUT_SECONDS = 15
_SINA_US_CHUNK_RETRIES = 3

# Sina US daily K-lines (global-stock-data Layer 2.1). The ``num`` param is
# ignored: the endpoint always returns full history since 1984 (AAPL
# ~10,071 bars), so callers truncate client-side to the last 21 bars.
# The response carries a leading ``/*<script>...</script>*/`` prefix that
# must be stripped before JSON parsing.
_SINA_US_DAILY_K_URL = (
    "https://stock.finance.sina.com.cn/usstock/api/jsonp.php/"
    "var/US_MinKService.getDailyK"
)
_SINA_US_DAILY_K_TIMEOUT_SECONDS = 15
_SINA_US_DAILY_K_RETRIES = 2
# Bars kept per ticker for volume_ratio: today + the prior 20 sessions.
_VOLUME_RATIO_LOOKBACK_BARS = 21

_SNAPSHOT_COLUMNS = [
    "code", "name", "price", "change_pct", "amount", "total_mv", "circ_mv",
    "pe_ratio", "pb_ratio", "volume_ratio", "turnover_rate", "industry",
]

_US_SNAPSHOT_CALL_TIMEOUT_SECONDS = 180.0


def fetch_us_universe(source: str = "auto") -> list[str]:
    """Return a list of US equity tickers.

    Sources:
        sp500   — scrape S&P 500 from Wikipedia
        env     — read SCREENING_US_TICKERS (comma-separated)
        default — hardcoded top-50 US large-caps
        auto    — try sp500 → env → default
    """
    src = source.lower()
    if src == "auto":
        for s in ("sp500", "env", "default"):
            try:
                tickers = fetch_us_universe(s)
                if tickers:
                    logger.info("US universe from %s: %d tickers", s, len(tickers))
                    return tickers
            except Exception as e:
                logger.debug("US universe source %s failed: %s", s, e)
        return list(_DEFAULT_US_UNIVERSE)

    if src == "sp500":
        return _fetch_sp500_tickers()
    elif src == "env":
        raw = os.getenv("SCREENING_US_TICKERS", "").strip()
        if not raw:
            raise ValueError("SCREENING_US_TICKERS not set")
        return [t.strip() for t in raw.split(",") if t.strip()]
    elif src == "default":
        return list(_DEFAULT_US_UNIVERSE)
    else:
        raise ValueError(f"Unknown US universe source: {source}")


def _fetch_sp500_tickers() -> list[str]:
    tables = pd.read_html(_SP500_WIKI_URL)
    for tbl in tables:
        if "Symbol" in tbl.columns:
            return sorted(tbl["Symbol"].dropna().str.strip().str.replace(".", "-", regex=False).tolist())
    raise RuntimeError("Could not find Symbol column in S&P 500 Wikipedia table")


def fetch_us_snapshot(
    tickers: list[str] | None = None,
    *,
    universe_source: str = "auto",
    max_workers: int = 8,
    sources: list[str] | None = None,
) -> pd.DataFrame:
    """Fetch a US equity snapshot in the screening schema.

    Tries the configured US source chain in order (default
    ``["tencent", "sina_gb", "yfinance"]``, overridable with
    ``SCREENING_US_SNAPSHOT_SOURCE_PRIORITY``) and returns the first
    source that yields a non-empty frame. Returns a DataFrame matching
    the standard snapshot columns: code, name, price, change_pct, amount,
    total_mv, circ_mv, pe_ratio, pb_ratio, volume_ratio, turnover_rate,
    industry. The winning source is recorded in ``df.attrs["snapshot_source"]``
    and failed attempts in ``df.attrs["source_errors"]``.

    Raises RuntimeError if every source fails.
    """
    if tickers is None:
        tickers = fetch_us_universe(universe_source)

    priority = list(sources) if sources is not None else resolve_us_snapshot_source_priority()
    if not priority:
        priority = list(DEFAULT_US_SNAPSHOT_SOURCE_PRIORITY)

    logger.info(
        "Fetching US snapshot for %d tickers from %s",
        len(tickers), ",".join(priority),
    )
    errors: list[str] = []
    for source in priority:
        try:
            df = call_with_timeout(
                fetch_us_snapshot_source,
                source,
                list(tickers),
                max_workers=max_workers,
                timeout_sec=_us_snapshot_call_timeout_seconds(),
                label=f"US snapshot source {source}",
            )
        except Exception as e:
            errors.append(f"{source}: {e}")
            logger.warning("US snapshot source %s failed: %s", source, e)
            continue
        if df is None or df.empty:
            errors.append(f"{source}: returned empty data")
            logger.warning("US snapshot source %s returned empty data", source)
            continue
        df.attrs["snapshot_source"] = source
        df.attrs["source_errors"] = list(errors)
        logger.info("US snapshot: %d rows from %s", len(df), source)
        return df
    raise RuntimeError(f"All US snapshot sources failed: {'; '.join(errors)}")


def fetch_us_snapshot_source(
    source: str,
    tickers: list[str] | None = None,
    *,
    universe_source: str = "auto",
    max_workers: int = 8,
) -> pd.DataFrame:
    """Fetch a US snapshot from one named source (no chain, no fallback).

    ``source`` is one of ``"tencent"``, ``"sina_gb"`` or ``"yfinance"``.
    Raises ValueError for an unknown source and RuntimeError when the
    source yields no usable rows.
    """
    if source == "tencent":
        return fetch_us_snapshot_tencent(
            tickers, universe_source=universe_source, max_workers=max_workers,
        )
    if source == "sina_gb":
        return fetch_us_snapshot_sina_gb(
            tickers, universe_source=universe_source, max_workers=max_workers,
        )
    if source == "yfinance":
        return fetch_us_snapshot_yfinance(
            tickers, universe_source=universe_source, max_workers=max_workers,
        )
    raise ValueError(f"Unknown US snapshot source: {source}")


def fetch_us_snapshot_tencent(
    tickers: list[str] | None = None,
    *,
    universe_source: str = "auto",
    max_workers: int = 8,
    enrich_volume_ratio: bool = True,
    volume_ratio_tickers: list[str] | None = None,
    volume_ratio_max_tickers: int | None = None,
) -> pd.DataFrame:
    """Fetch a US snapshot via Tencent US batch quotes.

    Field indices follow the verified 71-field ``usXXX`` layout
    (global-stock-data Layer 1.1, calibrated 2026-07-26; re-verified
    against the live 2026-11-10 layout): [1] name, [3] price,
    [6] volume, [32] change_pct, [37] amount (USD), [39] PE, [44]
    circulating market value (100M USD), [45] total market value
    (100M USD), [51] PB, [62] total shares, [63] circulating shares.
    turnover_rate = volume / total shares * 100. There is no
    volume_ratio or industry in the quote; volume_ratio is enriched
    from Sina US daily K-lines unless ``enrich_volume_ratio`` is False.
    """
    if tickers is None:
        tickers = fetch_us_universe(universe_source)

    rows, dropped_stale = _tencent_us_quotes(tickers)
    if not rows:
        raise RuntimeError("tencent returned empty data")
    logger.info(
        "tencent US: %d rows from %d tickers (%d stale quotes dropped)",
        len(rows), len(tickers), dropped_stale,
    )
    df = _normalize_us_rows(rows, source="tencent")
    if enrich_volume_ratio:
        df = enrich_volume_ratio_us(
            df,
            tickers=volume_ratio_tickers,
            max_tickers=volume_ratio_max_tickers,
            max_workers=max_workers,
        )
    return df


def fetch_us_snapshot_sina_gb(
    tickers: list[str] | None = None,
    *,
    universe_source: str = "auto",
    max_workers: int = 8,
    enrich_volume_ratio: bool = True,
    volume_ratio_tickers: list[str] | None = None,
    volume_ratio_max_tickers: int | None = None,
) -> pd.DataFrame:
    """Fetch a US snapshot via Sina ``gb_`` batch quotes.

    Field indices follow the verified 36-field ``gb_`` layout
    (global-stock-data Layer 1.1): [0] name, [1] price, [2] change_pct,
    [3] timestamp, [5] open, [6] high, [7] low, [8] 52-week high,
    [9] 52-week low, [10] volume, [12] market cap (USD),
    [13] EPS, [14] PE, [26] previous close. The endpoint carries no
    amount, PB, turnover rate or share counts, so those columns stay
    empty; volume_ratio is enriched from Sina US daily K-lines unless
    ``enrich_volume_ratio`` is False.
    """
    if tickers is None:
        tickers = fetch_us_universe(universe_source)

    rows = _sina_gb_us_quotes(tickers)
    if not rows:
        raise RuntimeError("sina_gb returned empty data")
    logger.info("sina_gb US: %d rows from %d tickers", len(rows), len(tickers))
    df = _normalize_us_rows(rows, source="sina_gb")
    if enrich_volume_ratio:
        df = enrich_volume_ratio_us(
            df,
            tickers=volume_ratio_tickers,
            max_tickers=volume_ratio_max_tickers,
            max_workers=max_workers,
        )
    return df


def fetch_us_snapshot_yfinance(
    tickers: list[str] | None = None,
    *,
    universe_source: str = "auto",
    max_workers: int = 8,
) -> pd.DataFrame:
    """Fetch a US equity snapshot via yfinance (last-resort source).

    Uses yfinance to fetch current data for each ticker. Returns a
    DataFrame matching the standard snapshot columns: code, name, price,
    change_pct, amount, total_mv, circ_mv, pe_ratio, pb_ratio,
    volume_ratio, turnover_rate, industry.
    """
    import yfinance as yf

    if tickers is None:
        tickers = fetch_us_universe(universe_source)

    logger.info("Fetching US snapshot for %d tickers via yfinance", len(tickers))

    hist_end = pd.Timestamp.now().normalize()
    hist_start = hist_end - pd.Timedelta(days=30)
    data = yf.download(
        tickers,
        start=hist_start.strftime("%Y-%m-%d"),
        end=hist_end.strftime("%Y-%m-%d"),
        group_by="ticker",
        auto_adjust=True,
        progress=False,
        threads=True,
    )

    rows = []

    def _process_ticker(ticker: str) -> dict | None:
        try:
            if len(tickers) == 1:
                hist = data.copy()
                if isinstance(hist.columns, pd.MultiIndex):
                    hist.columns = hist.columns.droplevel("Ticker")
            else:
                if ticker not in data.columns.get_level_values(0):
                    return None
                hist = data[ticker].copy()
            if hist.empty:
                return None

            hist = hist[hist["Close"].notna()]
            if len(hist) < 2:
                return None

            latest = hist.iloc[-1]
            prev = hist.iloc[-2]
            price = float(latest["Close"])
            prev_close = float(prev["Close"])
            volume = float(latest["Volume"])
            change_pct = ((price - prev_close) / prev_close * 100) if prev_close > 0 else 0.0

            vol_20d = float(hist["Volume"].tail(20).mean())
            volume_ratio = (volume / vol_20d) if vol_20d > 0 else 1.0

            info = yf.Ticker(ticker).fast_info
            market_cap = getattr(info, "market_cap", None) or 0
            shares = getattr(info, "shares", None) or 0
            turnover_rate = (volume / shares * 100) if shares > 0 else 0.0

            return {
                "code": ticker,
                "name": ticker,
                "price": price,
                "change_pct": round(change_pct, 2),
                "amount": round(volume * price, 0),
                "total_mv": market_cap,
                "circ_mv": market_cap,
                "pe_ratio": None,
                "pb_ratio": None,
                "volume_ratio": round(volume_ratio, 2),
                "turnover_rate": round(turnover_rate, 4),
                "industry": "",
            }
        except Exception as e:
            logger.debug("Failed to process %s: %s", ticker, e)
            return None

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_process_ticker, t): t for t in tickers}
        for future in as_completed(futures):
            result = future.result()
            if result:
                rows.append(result)

    if not rows:
        raise RuntimeError("yfinance returned no valid data for any ticker")

    df = _normalize_us_rows(rows, source="yfinance")
    _enrich_info_fields(df)
    logger.info("US snapshot: %d rows from yfinance", len(df))
    return df


def _enrich_info_fields(df: pd.DataFrame) -> None:
    """Best-effort enrichment of pe_ratio, pb_ratio, industry from yfinance info."""
    import yfinance as yf

    needs_pe = df["pe_ratio"].isna().sum() > len(df) * 0.5
    if not needs_pe:
        return

    for idx in df.index:
        ticker = df.at[idx, "code"]
        try:
            info = yf.Ticker(ticker).info
            if pd.isna(df.at[idx, "pe_ratio"]) or df.at[idx, "pe_ratio"] == 0:
                df.at[idx, "pe_ratio"] = info.get("trailingPE")
            if pd.isna(df.at[idx, "pb_ratio"]) or df.at[idx, "pb_ratio"] == 0:
                df.at[idx, "pb_ratio"] = info.get("priceToBook")
            if not df.at[idx, "industry"]:
                df.at[idx, "industry"] = info.get("industry", "")
            if not df.at[idx, "name"] or df.at[idx, "name"] == ticker:
                df.at[idx, "name"] = info.get("shortName", ticker)
        except Exception:
            pass


def fetch_daily_history_yfinance(
    ticker: str,
    *,
    lookback_days: int = 120,
) -> pd.DataFrame:
    """Fetch daily OHLCV history for a US ticker via yfinance.

    Returns a DataFrame with columns: date, open, high, low, close, volume
    matching the schema expected by the daily enrichment logic.
    """
    import yfinance as yf

    today = pd.Timestamp.now(tz=MARKET_TIMEZONE["us"]).normalize()
    # yfinance excludes end; include today's US session regardless of host TZ.
    end = today + pd.DateOffset(days=1)
    start = today - pd.Timedelta(days=max(lookback_days * 2, 180))
    hist = yf.download(
        ticker,
        start=start.strftime("%Y-%m-%d"),
        end=end.strftime("%Y-%m-%d"),
        auto_adjust=True,
        progress=False,
    )
    if hist is None or hist.empty:
        raise RuntimeError(f"yfinance daily history empty for {ticker}")

    if isinstance(hist.columns, pd.MultiIndex):
        hist.columns = hist.columns.droplevel("Ticker")

    hist = hist.tail(max(lookback_days, 30)).copy()
    hist = hist.rename(columns={
        "Open": "开盘", "High": "最高", "Low": "最低",
        "Close": "收盘", "Volume": "成交量",
    })
    hist.index.name = "日期"
    hist = hist.reset_index()
    return hist


# ---------------------------------------------------------------------------
# Tencent US batch quotes
# ---------------------------------------------------------------------------

def _tencent_us_quotes(tickers: list[str]) -> tuple[list[dict], int]:
    """Fetch Tencent US quotes for tickers, in parallel chunks."""
    prefixed = [_tencent_us_code(ticker) for ticker in tickers]
    chunks = [
        prefixed[i:i + _TENCENT_US_CHUNK_SIZE]
        for i in range(0, len(prefixed), _TENCENT_US_CHUNK_SIZE)
    ]
    rows: list[dict] = []
    dropped_stale = 0
    with ThreadPoolExecutor(max_workers=_TENCENT_US_MAX_WORKERS) as pool:
        for chunk_rows, chunk_stale in pool.map(_tencent_us_quote_chunk, chunks):
            rows.extend(chunk_rows)
            dropped_stale += chunk_stale
    return rows, dropped_stale


def _tencent_us_code(ticker: str) -> str:
    """Map a screening ticker to its Tencent ``usXXX`` query code.

    Dots are the Tencent-side separator for share classes (``BRK-B`` ->
    ``usBRK.B``); the mapping is best-effort and unverified live, so an
    unmapped ticker is simply dropped from the Tencent frame.
    """
    return "us" + ticker.strip().upper().replace("-", ".")


def _tencent_us_quote_chunk(prefixed: list[str]) -> tuple[list[dict], int]:
    """Fetch one Tencent US quote chunk, retrying transient failures."""
    last_exc: Exception | None = None
    for attempt in range(_TENCENT_US_CHUNK_RETRIES):
        try:
            return _tencent_us_quote_chunk_once(prefixed)
        except Exception as exc:  # noqa: BLE001 - retry, then fail the source.
            last_exc = exc
            logger.warning(
                "tencent US chunk attempt %d/%d failed (%d codes): %s",
                attempt + 1, _TENCENT_US_CHUNK_RETRIES, len(prefixed), exc,
            )
            time.sleep(1.0 * (attempt + 1))
    raise RuntimeError(f"tencent US chunk failed after {_TENCENT_US_CHUNK_RETRIES} attempts: {last_exc}")


def _tencent_us_quote_chunk_once(prefixed: list[str]) -> tuple[list[dict], int]:
    url = _TENCENT_US_QUOTE_URL + ",".join(prefixed)
    req = urllib.request.Request(url, headers={"User-Agent": _BROWSER_USER_AGENT})
    with urllib.request.urlopen(req, timeout=_TENCENT_US_TIMEOUT_SECONDS) as resp:
        text = resp.read().decode("gbk")
    rows: list[dict] = []
    stale = 0
    for line in text.strip().split(";"):
        parsed = _parse_tencent_us_quote_line(line)
        if parsed is None:
            continue
        row, is_stale = parsed
        if is_stale:
            stale += 1
            continue
        rows.append(row)
    return rows, stale


def _parse_tencent_us_quote_line(line: str) -> tuple[dict | None, bool] | None:
    """Parse one ``v_usXXX="..."`` Tencent US quote line.

    Returns ``(row, is_stale)`` or None when the line is unusable.
    Field indices follow the verified 71-field layout (see
    :func:`fetch_us_snapshot_tencent`): market caps are in 100M USD.
    """
    line = line.strip()
    if not line or "=" not in line or '"' not in line:
        return None
    key = line.split("=", 1)[0].split("_")[-1]
    vals = line.split('"')[1].split("~")
    if len(vals) < 64:
        return None
    ticker = key[2:].upper() if key.lower().startswith("us") else key.upper()
    if not ticker:
        return None
    try:
        price = float(vals[3] or 0)
        last_close = float(vals[4] or 0)
        volume = float(vals[6] or 0)
        amount = float(vals[37] or 0)
        total_shares = float(vals[62] or 0)
    except ValueError:
        return None
    # Stale-quote guard, mirroring the CN Tencent path: a zero-amount
    # quote frozen at last close is a suspended stock or zombie data.
    if amount == 0 and price == last_close and price > 0:
        return None, True
    try:
        row = {
            "code": ticker,
            "name": vals[1] or ticker,
            "price": price,
            "change_pct": float(vals[32] or 0),
            "amount": amount,
            "total_mv": float(vals[45] or 0) * 1e8,
            "circ_mv": float(vals[44] or 0) * 1e8,
            "pe_ratio": float(vals[39] or 0) or None,
            "pb_ratio": float(vals[51] or 0) or None,
            "volume_ratio": None,
            "turnover_rate": (volume / total_shares * 100) if total_shares > 0 else 0.0,
            "industry": "",
        }
    except ValueError:
        return None
    return row, False


# ---------------------------------------------------------------------------
# Sina gb_ batch quotes
# ---------------------------------------------------------------------------

def _sina_gb_us_quotes(tickers: list[str]) -> list[dict]:
    """Fetch Sina gb_ quotes for tickers, in parallel chunks."""
    prefixed = ["gb_" + ticker.strip().lower() for ticker in tickers]
    chunks = [
        prefixed[i:i + _SINA_US_CHUNK_SIZE]
        for i in range(0, len(prefixed), _SINA_US_CHUNK_SIZE)
    ]
    rows: list[dict] = []
    with ThreadPoolExecutor(max_workers=_SINA_US_MAX_WORKERS) as pool:
        for chunk_rows in pool.map(_sina_gb_us_quote_chunk, chunks):
            rows.extend(chunk_rows)
    return rows


def _sina_gb_us_quote_chunk(prefixed: list[str]) -> list[dict]:
    """Fetch one Sina gb_ quote chunk, retrying transient failures."""
    last_exc: Exception | None = None
    for attempt in range(_SINA_US_CHUNK_RETRIES):
        try:
            return _sina_gb_us_quote_chunk_once(prefixed)
        except Exception as exc:  # noqa: BLE001 - retry, then fail the source.
            last_exc = exc
            logger.warning(
                "sina_gb US chunk attempt %d/%d failed (%d codes): %s",
                attempt + 1, _SINA_US_CHUNK_RETRIES, len(prefixed), exc,
            )
            time.sleep(1.0 * (attempt + 1))
    raise RuntimeError(f"sina_gb US chunk failed after {_SINA_US_CHUNK_RETRIES} attempts: {last_exc}")


def _sina_gb_us_quote_chunk_once(prefixed: list[str]) -> list[dict]:
    url = _SINA_US_QUOTE_URL + ",".join(prefixed)
    resp = requests.get(
        url,
        headers={"User-Agent": _BROWSER_USER_AGENT, "Referer": _SINA_REFERER},
        timeout=_SINA_US_TIMEOUT_SECONDS,
    )
    resp.raise_for_status()
    resp.encoding = "gbk"
    rows: list[dict] = []
    for line in resp.text.strip().split(";"):
        row = _parse_sina_gb_us_quote_line(line)
        if row is not None:
            rows.append(row)
    return rows


def _parse_sina_gb_us_quote_line(line: str) -> dict | None:
    """Parse one ``var hq_str_gb_xxx="..."`` Sina US quote line.

    Returns a snapshot row or None when the line is unusable. Field
    indices follow the verified 36-field layout (see
    :func:`fetch_us_snapshot_sina_gb`); market cap is in USD.
    """
    line = line.strip()
    if not line or "=" not in line or '"' not in line:
        return None
    key = line.split("=", 1)[0].split("_")[-1]
    fields = line.split('"')[1].split(",")
    if len(fields) < 27:
        return None
    ticker = key.upper()
    if not ticker:
        return None
    try:
        price = float(fields[1] or 0)
    except ValueError:
        return None
    if price <= 0:
        return None
    try:
        return {
            "code": ticker,
            "name": fields[0] or ticker,
            "price": price,
            "change_pct": float(fields[2] or 0),
            "amount": None,
            "total_mv": float(fields[12] or 0) or None,
            "circ_mv": None,
            "pe_ratio": float(fields[14] or 0) or None,
            "pb_ratio": None,
            "volume_ratio": None,
            "turnover_rate": None,
            "industry": "",
        }
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Sina US daily K-lines → volume_ratio
# ---------------------------------------------------------------------------

def enrich_volume_ratio_us(
    df: pd.DataFrame,
    tickers: list[str] | None = None,
    *,
    max_tickers: int | None = None,
    max_workers: int = 4,
) -> pd.DataFrame:
    """Attach ``volume_ratio`` to a US snapshot frame via Sina daily K.

    Fetches one daily-K series per ticker (``num`` is ignored server-side:
    full history since 1984 is returned and truncated client-side to the
    last 21 bars) and sets ``volume_ratio = today volume / mean of the
    prior 20 bars' volume``. Only tickers that need the ratio are fetched:
    ``tickers`` selects them explicitly (e.g. tickers that passed the
    quote gates), otherwise every row with a valid price is used.

    ``max_tickers`` caps how many tickers are enriched (default from
    ``SCREENING_US_VOLUME_RATIO_MAX_TICKERS``, 500 — enough for the full
    S&P 500; each full-history response is ~1 MB, so 500 tickers is
    ~125 MB of traffic). Tickers beyond the cap keep ``volume_ratio``
    empty and are dropped by any ``volume_ratio_min`` hard filter.
    Per-ticker failures leave that row's ratio empty instead of failing
    the whole snapshot.
    """
    result = df.copy()
    if "volume_ratio" not in result.columns:
        result["volume_ratio"] = None
    if tickers is None:
        prices = pd.to_numeric(result["price"], errors="coerce") if "price" in result.columns else pd.Series(dtype=float)
        tickers = [
            str(code) for code, price in zip(result["code"], prices)
            if pd.notna(price) and float(price) > 0
        ]
    cap = _volume_ratio_max_tickers(max_tickers)
    if len(tickers) > cap:
        logger.warning(
            "volume_ratio enrichment capped at %d of %d tickers; "
            "the rest keep an empty volume_ratio",
            cap, len(tickers),
        )
        tickers = tickers[:cap]
    if not tickers:
        return result

    ratios: dict[str, float] = {}
    with ThreadPoolExecutor(max_workers=max(1, max_workers)) as pool:
        futures = {pool.submit(_volume_ratio_for_ticker, t): t for t in tickers}
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                ratio = future.result()
            except Exception as e:
                logger.debug("volume_ratio fetch failed for %s: %s", ticker, e)
                continue
            if ratio is not None:
                ratios[ticker] = ratio
    if ratios:
        for idx in result.index:
            code = str(result.at[idx, "code"])
            if code in ratios:
                result.at[idx, "volume_ratio"] = round(ratios[code], 2)
    logger.info(
        "volume_ratio enriched for %d of %d tickers", len(ratios), len(tickers),
    )
    return result


def _volume_ratio_for_ticker(ticker: str) -> float | None:
    bars = _fetch_us_daily_k_sina(ticker)
    return _volume_ratio_from_bars(bars)


def _fetch_us_daily_k_sina(ticker: str) -> list[dict]:
    """Fetch Sina US daily K-lines for one ticker, truncated to 21 bars.

    The endpoint ignores ``num`` and returns full history since 1984,
    so the newest 21 bars are kept client-side.
    """
    last_exc: Exception | None = None
    for attempt in range(_SINA_US_DAILY_K_RETRIES):
        try:
            resp = requests.get(
                _SINA_US_DAILY_K_URL,
                params={"symbol": ticker.upper(), "num": _VOLUME_RATIO_LOOKBACK_BARS},
                headers={"Referer": _SINA_REFERER},
                timeout=_SINA_US_DAILY_K_TIMEOUT_SECONDS,
            )
            resp.raise_for_status()
            bars = _parse_sina_us_daily_k(resp.text)
            if not bars:
                raise RuntimeError(f"sina daily K returned no bars for {ticker}")
            return bars[-_VOLUME_RATIO_LOOKBACK_BARS:]
        except Exception as exc:  # noqa: BLE001 - retry, then fail the ticker.
            last_exc = exc
            logger.debug(
                "sina daily K attempt %d/%d failed for %s: %s",
                attempt + 1, _SINA_US_DAILY_K_RETRIES, ticker, exc,
            )
            time.sleep(0.5 * (attempt + 1))
    raise RuntimeError(f"sina daily K failed for {ticker}: {last_exc}")


def _parse_sina_us_daily_k(text: str) -> list[dict]:
    """Parse a Sina US daily-K JSONP response into OHLCV bars.

    Strips the leading ``/*<script>...</script>*/`` prefix, then extracts
    the JSON array from ``var([...]);``. Each bar is
    ``{date, open, high, low, close, volume}`` in chronological order.
    Returns [] when the payload carries no parseable bars.
    """
    if not text:
        return []
    # Drop the leading /*<script>location.href='//sina.com';</script>*/ guard.
    cleaned = re.sub(r"^\s*/\*.*?\*/\s*", "", text, count=1, flags=re.DOTALL)
    match = re.search(r"\((\[.*\])\)", cleaned, flags=re.DOTALL)
    if not match:
        return []
    try:
        items = json.loads(match.group(1))
    except ValueError:
        return []
    bars: list[dict] = []
    for item in items:
        try:
            bars.append({
                "date": str(item.get("d", "")),
                "open": float(item.get("o", 0) or 0),
                "high": float(item.get("h", 0) or 0),
                "low": float(item.get("l", 0) or 0),
                "close": float(item.get("c", 0) or 0),
                "volume": int(float(item.get("v", 0) or 0)),
            })
        except (TypeError, ValueError):
            continue
    return bars


def _volume_ratio_from_bars(bars: list[dict]) -> float | None:
    """Return today volume / mean prior-20-bar volume, or None.

    Needs at least 2 bars; with fewer than 21 bars (recent IPO) the
    mean is taken over however many prior bars exist.
    """
    if len(bars) < 2:
        return None
    volumes = [float(bar.get("volume") or 0) for bar in bars]
    today = volumes[-1]
    prior_mean = sum(volumes[:-1]) / len(volumes[:-1])
    if prior_mean <= 0:
        return None
    return today / prior_mean


def _volume_ratio_max_tickers(explicit: int | None) -> int:
    if explicit is not None:
        return max(0, int(explicit))
    raw = os.getenv("SCREENING_US_VOLUME_RATIO_MAX_TICKERS", "").strip()
    if raw:
        try:
            return max(0, int(raw))
        except ValueError:
            logger.warning(
                "Ignoring invalid SCREENING_US_VOLUME_RATIO_MAX_TICKERS=%r", raw,
            )
    return 500


def _us_snapshot_call_timeout_seconds() -> float | None:
    return parse_source_timeout_seconds(
        "SCREENING_US_SNAPSHOT_CALL_TIMEOUT_SEC",
        default=_US_SNAPSHOT_CALL_TIMEOUT_SECONDS,
    )


def _normalize_us_rows(rows: list[dict], *, source: str) -> pd.DataFrame:
    """Normalize US snapshot rows to the standard schema."""
    df = pd.DataFrame(rows)
    for col in _SNAPSHOT_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[_SNAPSHOT_COLUMNS]
    numeric_cols = [
        "price", "change_pct", "amount", "total_mv", "circ_mv",
        "pe_ratio", "pb_ratio", "volume_ratio", "turnover_rate",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["price"])
    df = df[df["price"] > 0]
    df.attrs["snapshot_source"] = source
    return df
