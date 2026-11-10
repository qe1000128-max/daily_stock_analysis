# -*- coding: utf-8 -*-
"""US snapshot sources: Tencent / Sina gb_ / Sina daily K / yfinance chain.

Tencent paths cannot be live-tested on this VM (qt.gtimg.cn is rate-limited
here), so they run against mocked responses built from the verified 71-field
layout. Sina paths are live-tested behind the ``network`` marker and mocked
elsewhere; the mocked gb_ payload below is a verbatim live response captured
2026-11-10 (AAPL 350.69, +2.43%).
"""

import json
from contextlib import contextmanager
from types import SimpleNamespace

import pandas as pd
import pytest

from src.services.screening import snapshot as screening_snapshot
from src.services.screening import snapshot_us
from src.services.screening.config import resolve_us_snapshot_source_priority

# --- Tencent fixtures -------------------------------------------------------

_TENCENT_AAPL_FIELDS = [""] * 71
_TENCENT_AAPL_FIELDS[1] = "苹果"
_TENCENT_AAPL_FIELDS[2] = "AAPL.OQ"
_TENCENT_AAPL_FIELDS[3] = "349.98"
_TENCENT_AAPL_FIELDS[4] = "342.37"
_TENCENT_AAPL_FIELDS[5] = "345.00"
_TENCENT_AAPL_FIELDS[6] = "48000000"
_TENCENT_AAPL_FIELDS[32] = "2.22"
_TENCENT_AAPL_FIELDS[33] = "352.33"
_TENCENT_AAPL_FIELDS[34] = "344.78"
_TENCENT_AAPL_FIELDS[37] = "16800000000"
_TENCENT_AAPL_FIELDS[39] = "42.25"
_TENCENT_AAPL_FIELDS[44] = "48881.62"
_TENCENT_AAPL_FIELDS[45] = "48911.83"
_TENCENT_AAPL_FIELDS[46] = "Apple Inc."
_TENCENT_AAPL_FIELDS[51] = "45.93"
_TENCENT_AAPL_FIELDS[62] = "14532258793"
_TENCENT_AAPL_FIELDS[63] = "14500000000"


def _tencent_line(ticker: str, fields: list[str]) -> str:
    return f'v_us{ticker}="' + "~".join(fields) + '";'


def _fake_urlopen(text: str):
    payload = text.encode("gbk")

    @contextmanager
    def _open(request, timeout=None):
        yield SimpleNamespace(read=lambda: payload)

    return _open


# --- Sina fixtures ----------------------------------------------------------
# Verbatim live gb_aapl response, 2026-11-10 09:12:45 CST (36 fields).

_SINA_GB_AAPL = (
    "苹果,350.6900,2.43,2026-11-10 09:12:45,8.3200,344.9950,352.3300,"
    "344.7800,355.3800,242.8900,48753031,56680400,5096317836117,8.30,"
    "42.250000,0.00,0.68,0.00,0.00,14532258793,63,350.5000,-0.05,-0.19,"
    "Nov 09 07:59PM EST,Nov 09 04:00PM EST,342.3700,8566944,1,2026,"
    "17077621972.0000,352.0200,330.3559,3004103205.0576,350.5000,342.3700"
)


def _sina_gb_line(ticker: str, payload: str) -> str:
    return f'var hq_str_gb_{ticker.lower()}="{payload}";'


def _fake_requests_get(text: str):
    def _get(url, **kwargs):
        return SimpleNamespace(text=text, raise_for_status=lambda: None)

    return _get


def _daily_k_text(volumes: list[int]) -> str:
    bars = [
        {
            "d": f"2026-10-{i + 1:02d}" if i < 30 else "2026-11-09",
            "o": "100.00", "h": "101.00", "l": "99.00", "c": "100.50",
            "v": str(volume), "a": "0",
        }
        for i, volume in enumerate(volumes)
    ]
    return (
        "/*<script>location.href='//sina.com';</script>*/\n"
        "var(" + json.dumps(bars) + ");"
    )


def _good_df(source: str = "tencent") -> pd.DataFrame:
    df = pd.DataFrame([{
        "code": "AAPL", "name": "Apple", "price": 350.69, "change_pct": 2.43,
        "amount": 17000000000.0, "total_mv": 5096317836117.0,
        "circ_mv": 5096317836117.0, "pe_ratio": 42.25, "pb_ratio": 45.93,
        "volume_ratio": 1.0, "turnover_rate": 0.33, "industry": "",
    }])
    df.attrs["snapshot_source"] = source
    return df


# --- Tencent US quotes ------------------------------------------------------

def test_tencent_us_quote_field_mapping(monkeypatch) -> None:
    text = _tencent_line("AAPL", _TENCENT_AAPL_FIELDS)
    monkeypatch.setattr("urllib.request.urlopen", _fake_urlopen(text))

    df = snapshot_us.fetch_us_snapshot_tencent(
        ["AAPL"], enrich_volume_ratio=False,
    )

    assert len(df) == 1
    row = df.iloc[0]
    assert row["code"] == "AAPL"
    assert row["name"] == "苹果"
    assert row["price"] == pytest.approx(349.98)
    assert row["change_pct"] == pytest.approx(2.22)
    assert row["amount"] == pytest.approx(16800000000.0)
    # Market caps are in 100M USD in the quote, normalized to USD.
    assert row["total_mv"] == pytest.approx(48911.83 * 1e8)
    assert row["circ_mv"] == pytest.approx(48881.62 * 1e8)
    assert row["pe_ratio"] == pytest.approx(42.25)
    assert row["pb_ratio"] == pytest.approx(45.93)
    # turnover_rate = volume / total shares * 100.
    assert row["turnover_rate"] == pytest.approx(48000000 / 14532258793 * 100)
    assert pd.isna(row["volume_ratio"])
    assert df.attrs["snapshot_source"] == "tencent"


def test_tencent_us_stale_quote_is_dropped(monkeypatch) -> None:
    fields = list(_TENCENT_AAPL_FIELDS)
    fields[37] = "0"  # zero amount, frozen at last close
    fields[3] = fields[4]
    monkeypatch.setattr(
        "urllib.request.urlopen",
        _fake_urlopen(_tencent_line("AAPL", fields)),
    )

    with pytest.raises(RuntimeError, match="tencent returned empty data"):
        snapshot_us.fetch_us_snapshot_tencent(["AAPL"], enrich_volume_ratio=False)


def test_tencent_us_empty_response_fails(monkeypatch) -> None:
    monkeypatch.setattr("urllib.request.urlopen", _fake_urlopen(""))

    with pytest.raises(RuntimeError, match="tencent returned empty data"):
        snapshot_us.fetch_us_snapshot_tencent(["AAPL"], enrich_volume_ratio=False)


def test_tencent_us_name_falls_back_to_ticker(monkeypatch) -> None:
    fields = list(_TENCENT_AAPL_FIELDS)
    fields[1] = ""
    monkeypatch.setattr(
        "urllib.request.urlopen",
        _fake_urlopen(_tencent_line("AAPL", fields)),
    )

    df = snapshot_us.fetch_us_snapshot_tencent(
        ["AAPL"], enrich_volume_ratio=False,
    )
    assert df.iloc[0]["name"] == "AAPL"


# --- Sina gb_ quotes --------------------------------------------------------

def test_sina_gb_us_quote_field_mapping(monkeypatch) -> None:
    text = _sina_gb_line("AAPL", _SINA_GB_AAPL)
    monkeypatch.setattr(snapshot_us.requests, "get", _fake_requests_get(text))

    df = snapshot_us.fetch_us_snapshot_sina_gb(
        ["AAPL"], enrich_volume_ratio=False,
    )

    assert len(df) == 1
    row = df.iloc[0]
    assert row["code"] == "AAPL"
    assert row["name"] == "苹果"
    assert row["price"] == pytest.approx(350.69)
    assert row["change_pct"] == pytest.approx(2.43)
    # Market cap is in USD directly (live: ~$5.10T for AAPL).
    assert row["total_mv"] == pytest.approx(5096317836117.0)
    assert row["pe_ratio"] == pytest.approx(42.25)
    # gb_ carries no amount / PB / turnover / shares / volume_ratio.
    assert pd.isna(row["amount"])
    assert pd.isna(row["pb_ratio"])
    assert pd.isna(row["turnover_rate"])
    assert pd.isna(row["volume_ratio"])
    assert df.attrs["snapshot_source"] == "sina_gb"


def test_sina_gb_us_empty_response_fails(monkeypatch) -> None:
    monkeypatch.setattr(snapshot_us.requests, "get", _fake_requests_get(""))

    with pytest.raises(RuntimeError, match="sina_gb returned empty data"):
        snapshot_us.fetch_us_snapshot_sina_gb(["AAPL"], enrich_volume_ratio=False)


# --- Sina daily K parsing + volume_ratio ------------------------------------

def test_parse_sina_us_daily_k_strips_script_prefix() -> None:
    volumes = [100] * 20 + [300]
    bars = snapshot_us._parse_sina_us_daily_k(_daily_k_text(volumes))

    assert len(bars) == 21
    assert bars[0]["date"] == "2026-10-01"
    assert bars[-1]["volume"] == 300
    assert bars[-1]["close"] == pytest.approx(100.50)


def test_parse_sina_us_daily_k_empty_payload() -> None:
    assert snapshot_us._parse_sina_us_daily_k("") == []
    assert snapshot_us._parse_sina_us_daily_k("var([]);") == []
    assert snapshot_us._parse_sina_us_daily_k("not json at all") == []


def test_volume_ratio_from_bars_math() -> None:
    bars = [{"volume": v} for v in [100] * 20 + [300]]
    assert snapshot_us._volume_ratio_from_bars(bars) == pytest.approx(3.0)

    bars = [{"volume": v} for v in [50, 150]]
    assert snapshot_us._volume_ratio_from_bars(bars) == pytest.approx(3.0)

    assert snapshot_us._volume_ratio_from_bars([{"volume": 10}]) is None
    assert snapshot_us._volume_ratio_from_bars([]) is None
    assert snapshot_us._volume_ratio_from_bars(
        [{"volume": 0}] * 20 + [{"volume": 10}]
    ) is None


def test_fetch_us_daily_k_truncates_to_last_21_bars(monkeypatch) -> None:
    volumes = [100] * 29 + [2100]  # 30 bars; last bar is the spike
    monkeypatch.setattr(
        snapshot_us.requests, "get", _fake_requests_get(_daily_k_text(volumes)),
    )

    bars = snapshot_us._fetch_us_daily_k_sina("AAPL")

    assert len(bars) == 21
    assert bars[-1]["volume"] == 2100
    assert snapshot_us._volume_ratio_from_bars(bars) == pytest.approx(2100 / 100)


def test_enrich_volume_ratio_us_attaches_ratio(monkeypatch) -> None:
    volumes = [100] * 20 + [250]
    monkeypatch.setattr(
        snapshot_us, "_fetch_us_daily_k_sina",
        lambda ticker: snapshot_us._parse_sina_us_daily_k(_daily_k_text(volumes)),
    )
    df = pd.DataFrame([{
        "code": "AAPL", "name": "Apple", "price": 350.69, "change_pct": 2.43,
        "amount": 17000000000.0, "total_mv": 5096317836117.0,
        "circ_mv": 5096317836117.0, "pe_ratio": 42.25, "pb_ratio": 45.93,
        "volume_ratio": None, "turnover_rate": 0.33, "industry": "",
    }])

    enriched = snapshot_us.enrich_volume_ratio_us(df)

    assert enriched.iloc[0]["volume_ratio"] == pytest.approx(2.5)


def test_enrich_volume_ratio_us_respects_cap(monkeypatch) -> None:
    volumes = [100] * 20 + [200]
    monkeypatch.setattr(
        snapshot_us, "_fetch_us_daily_k_sina",
        lambda ticker: snapshot_us._parse_sina_us_daily_k(_daily_k_text(volumes)),
    )
    df = pd.DataFrame([{
        "code": code, "name": code, "price": 100.0, "change_pct": 1.0,
        "amount": 1e9, "total_mv": 1e11, "circ_mv": 1e11, "pe_ratio": 20.0,
        "pb_ratio": 2.0, "volume_ratio": None, "turnover_rate": 1.0,
        "industry": "",
    } for code in ("AAPL", "MSFT", "NVDA")])

    enriched = snapshot_us.enrich_volume_ratio_us(df, max_tickers=2)

    assert enriched.iloc[0]["volume_ratio"] == pytest.approx(2.0)
    assert enriched.iloc[1]["volume_ratio"] == pytest.approx(2.0)
    assert pd.isna(enriched.iloc[2]["volume_ratio"])


def test_enrich_volume_ratio_us_per_ticker_failure_is_isolated(monkeypatch) -> None:
    def _fake_k(ticker: str):
        if ticker == "MSFT":
            raise RuntimeError("boom")
        return snapshot_us._parse_sina_us_daily_k(
            _daily_k_text([100] * 20 + [200]),
        )

    monkeypatch.setattr(snapshot_us, "_fetch_us_daily_k_sina", _fake_k)
    df = pd.DataFrame([{
        "code": code, "name": code, "price": 100.0, "change_pct": 1.0,
        "amount": 1e9, "total_mv": 1e11, "circ_mv": 1e11, "pe_ratio": 20.0,
        "pb_ratio": 2.0, "volume_ratio": None, "turnover_rate": 1.0,
        "industry": "",
    } for code in ("AAPL", "MSFT")])

    enriched = snapshot_us.enrich_volume_ratio_us(df)

    assert enriched.iloc[0]["volume_ratio"] == pytest.approx(2.0)
    assert pd.isna(enriched.iloc[1]["volume_ratio"])


# --- Chain + priority ---------------------------------------------------------

def test_us_source_priority_env_override(monkeypatch) -> None:
    monkeypatch.setenv("SCREENING_US_SNAPSHOT_SOURCE_PRIORITY", "yfinance")
    assert resolve_us_snapshot_source_priority() == ["yfinance"]

    monkeypatch.delenv("SCREENING_US_SNAPSHOT_SOURCE_PRIORITY")
    assert resolve_us_snapshot_source_priority() == [
        "tencent", "sina_gb", "yfinance",
    ]


def test_fetch_us_snapshot_source_unknown() -> None:
    with pytest.raises(ValueError, match="Unknown US snapshot source"):
        snapshot_us.fetch_us_snapshot_source("nope", ["AAPL"])


def test_fetch_us_snapshot_chain_falls_through(monkeypatch) -> None:
    calls: list[str] = []

    def _fake_source(source: str, tickers=None, **kwargs):
        calls.append(source)
        if source == "tencent":
            raise RuntimeError("tencent down")
        if source == "sina_gb":
            return _good_df("sina_gb")
        raise AssertionError(f"unexpected source {source}")

    monkeypatch.setattr(snapshot_us, "fetch_us_snapshot_source", _fake_source)

    df = snapshot_us.fetch_us_snapshot(
        ["AAPL"], sources=["tencent", "sina_gb", "yfinance"],
    )

    assert calls == ["tencent", "sina_gb"]
    assert df.attrs["snapshot_source"] == "sina_gb"
    assert "tencent: tencent down" in df.attrs["source_errors"][0]


def test_fetch_us_snapshot_chain_reaches_yfinance(monkeypatch) -> None:
    calls: list[str] = []

    def _fake_source(source: str, tickers=None, **kwargs):
        calls.append(source)
        if source == "yfinance":
            return _good_df("yfinance")
        raise RuntimeError(f"{source} down")

    monkeypatch.setattr(snapshot_us, "fetch_us_snapshot_source", _fake_source)

    df = snapshot_us.fetch_us_snapshot(
        ["AAPL"], sources=["tencent", "sina_gb", "yfinance"],
    )

    assert calls == ["tencent", "sina_gb", "yfinance"]
    assert df.attrs["snapshot_source"] == "yfinance"


def test_fetch_us_snapshot_chain_all_fail(monkeypatch) -> None:
    monkeypatch.setattr(
        snapshot_us, "fetch_us_snapshot_source",
        lambda source, tickers=None, **kwargs: (_ for _ in ()).throw(
            RuntimeError(f"{source} down"),
        ),
    )

    with pytest.raises(RuntimeError, match="All US snapshot sources failed"):
        snapshot_us.fetch_us_snapshot(
            ["AAPL"], sources=["tencent", "sina_gb", "yfinance"],
        )


# --- snapshot.py fallback wiring ----------------------------------------------

def test_us_fallback_skips_source_missing_required_columns(monkeypatch) -> None:
    monkeypatch.setattr(screening_snapshot, "_SOURCE_HEALTH", {})
    bad = _good_df("tencent")
    bad["volume_ratio"] = None  # all-NaN: fails required-column gating

    def _fake_source(source: str, tickers=None, **kwargs):
        if source == "tencent":
            return bad
        return _good_df("sina_gb")

    monkeypatch.setattr(snapshot_us, "fetch_us_snapshot_source", _fake_source)

    result = screening_snapshot.fetch_snapshot_with_fallback(
        ["tencent", "sina_gb"],
        required_columns=["volume_ratio"],
        market="us",
    )

    assert result.attrs["snapshot_source"] == "sina_gb"
    assert result.attrs["fallback_used"] is False
    assert "missing required columns volume_ratio" in result.attrs["source_errors"][0]
    health = screening_snapshot.snapshot_source_health_snapshot(["us_tencent"])
    assert health["us_tencent"]["failures"] == 1
    # US guard keys are namespaced: the CN "tencent" guard is untouched.
    assert screening_snapshot.snapshot_source_health_snapshot(
        ["tencent"])["tencent"]["failures"] == 0


def test_us_fallback_uses_last_good_cache(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(screening_snapshot, "_SOURCE_HEALTH", {})
    cached = _good_df("tencent")
    cache_path = tmp_path / "snapshot_us.last_good.json"
    screening_snapshot._write_last_good_snapshot(cache_path, cached)

    monkeypatch.setattr(
        snapshot_us, "fetch_us_snapshot_source",
        lambda source, tickers=None, **kwargs: (_ for _ in ()).throw(
            RuntimeError(f"{source} down"),
        ),
    )

    result = screening_snapshot._fetch_us_snapshot_with_fallback(
        ["volume_ratio"],
        sources=["tencent", "sina_gb", "yfinance"],
        fallback_snapshot_path=cache_path,
    )

    assert result.attrs["fallback_used"] is True
    assert result.attrs["snapshot_source"] == "last_good_cache"
    assert result.iloc[0]["code"] == "AAPL"


def test_us_fallback_all_fail_without_cache(monkeypatch) -> None:
    monkeypatch.setattr(screening_snapshot, "_SOURCE_HEALTH", {})
    monkeypatch.setattr(
        snapshot_us, "fetch_us_snapshot_source",
        lambda source, tickers=None, **kwargs: (_ for _ in ()).throw(
            RuntimeError(f"{source} down"),
        ),
    )

    with pytest.raises(RuntimeError, match="All US snapshot sources failed"):
        screening_snapshot._fetch_us_snapshot_with_fallback(
            ["volume_ratio"],
            sources=["tencent", "sina_gb"],
            fallback_snapshot_path="/nonexistent-dir/snapshot_us.json",
        )


def test_us_source_guard_cooldown_skips_source(monkeypatch) -> None:
    monkeypatch.setattr(screening_snapshot, "_SOURCE_HEALTH", {})
    calls: list[str] = []

    def _fake_source(source: str, tickers=None, **kwargs):
        calls.append(source)
        if source == "tencent":
            raise RuntimeError("tencent down")
        return _good_df("sina_gb")

    monkeypatch.setattr(snapshot_us, "fetch_us_snapshot_source", _fake_source)

    for _ in range(3):
        result = screening_snapshot.fetch_snapshot_with_fallback(
            ["tencent", "sina_gb"], market="us",
        )
        assert result.attrs["snapshot_source"] == "sina_gb"

    health = screening_snapshot.snapshot_source_health_snapshot(["us_tencent"])
    assert health["us_tencent"]["failures"] == 3
    assert health["us_tencent"]["disabled"] is True

    calls.clear()
    result = screening_snapshot.fetch_snapshot_with_fallback(
        ["tencent", "sina_gb"], market="us",
    )
    assert calls == ["sina_gb"]
    assert "temporarily disabled" in result.attrs["source_errors"][0]


# --- Live Sina checks (network marker; skipped by the offline gate) ----------

@pytest.mark.network
def test_live_sina_gb_quote() -> None:
    df = snapshot_us.fetch_us_snapshot_sina_gb(
        ["AAPL", "MSFT", "NVDA"], enrich_volume_ratio=False,
    )
    assert len(df) == 3
    assert (df["price"] > 0).all()
    assert (df["total_mv"] > 1e12).all()
    aapl = df.set_index("code").loc["AAPL"]
    assert aapl["price"] == pytest.approx(350.69, rel=0.2)


@pytest.mark.network
def test_live_sina_daily_k_volume_ratio() -> None:
    import requests

    # The endpoint ignores `num`: a raw call returns full history (~10k bars).
    raw = requests.get(
        snapshot_us._SINA_US_DAILY_K_URL,
        params={"symbol": "AAPL", "num": 120},
        headers={"Referer": "https://finance.sina.com.cn/"},
        timeout=20,
    )
    raw.raise_for_status()
    assert raw.text.startswith("/*<script>")
    all_bars = snapshot_us._parse_sina_us_daily_k(raw.text)
    assert len(all_bars) > 5000
    assert all_bars[0]["date"].startswith("1984")

    bars = snapshot_us._fetch_us_daily_k_sina("AAPL")
    assert len(bars) == 21
    ratio = snapshot_us._volume_ratio_from_bars(bars)
    assert ratio is not None and 0.1 < ratio < 10.0
