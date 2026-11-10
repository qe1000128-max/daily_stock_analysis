# -*- coding: utf-8 -*-
"""Tests for the screening enrichment layers (src/services/screening/enrichment.py).

All network access is mocked: no test hits Eastmoney, Tencent or the
SEC, and no test writes outside ``tmp_path``.
"""

from __future__ import annotations

from unittest import mock

import pandas as pd
import pytest

from src.services.screening import enrichment


class FakeResponse:
    def __init__(self, json_data=None, text=""):
        self._json_data = json_data
        self.text = text
        self.status_code = 200

    def json(self):
        if self._json_data is None:
            raise ValueError("no JSON")
        return self._json_data

    def raise_for_status(self):
        return None


class FakeSession:
    """A requests-like session dispatching on URL substrings."""

    def __init__(self, handler):
        self.handler = handler
        self.calls: list[dict] = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append({"url": url, "params": dict(params or {}), "headers": dict(headers or {})})
        return self.handler(url, params or {}, headers or {})


@pytest.fixture(autouse=True)
def _reset_throttle_state():
    enrichment._EM_LAST_CALL = 0.0
    enrichment._EDGAR_LAST_CALL = 0.0
    yield
    enrichment._EM_LAST_CALL = 0.0
    enrichment._EDGAR_LAST_CALL = 0.0


def _clist_page(rows, total):
    return {"rc": 0, "data": {"total": total, "diff": rows}}


def _make_clist_session(pages_by_segment):
    """pages_by_segment: {segment_index: [(total, [rows]), ...]} in pn order."""

    def handler(url, params, headers):
        fs = params.get("fs", "")
        segment = 1 if "t:81" in fs else 0
        pn = int(params.get("pn", "1"))
        pages = pages_by_segment[segment]
        total, rows = pages[pn - 1]
        return FakeResponse(_clist_page(rows, total))

    return FakeSession(handler)


# ---------------------------------------------------------------------------
# Layer 1: clist paging / cache / attach_industry
# ---------------------------------------------------------------------------


def test_clist_paging_is_serial_and_fills_industry(tmp_path):
    session = _make_clist_session(
        {
            0: [
                (250, [{"f12": "600519", "f13": 1, "f14": "贵州茅台", "f100": "白酒"}]),
                (250, [{"f12": "000001", "f13": 0, "f14": "平安银行", "f100": "银行"}]),
                (250, [{"f12": "002697", "f13": 0, "f14": "红旗连锁", "f100": "一般零售"}]),
            ],
            1: [(0, [])],
        }
    )
    mapping, notes = enrichment.fetch_eastmoney_clist_map(
        page_size=100, cache_dir=tmp_path, min_interval=0.0, jitter=0.0, session=session
    )
    assert mapping["600519"]["industry"] == "白酒"
    assert mapping["000001"]["industry"] == "银行"
    assert mapping["002697"]["industry"] == "一般零售"
    # Serial order: pn 1,2,3 for segment 0, then pn 1 for segment 1.
    assert [call["params"]["pn"] for call in session.calls] == ["1", "2", "3", "1"]
    assert all(call["params"]["pz"] == "100" for call in session.calls)
    assert any("rows=3" in note for note in notes)
    # Cache file written under the given dir.
    assert (tmp_path / enrichment.CLIST_CACHE_FILENAME).is_file()


def test_clist_cache_hit_makes_no_requests(tmp_path):
    session = _make_clist_session(
        {
            0: [(1, [{"f12": "600519", "f13": 1, "f14": "贵州茅台", "f100": "白酒"}])],
            1: [(0, [])],
        }
    )
    enrichment.fetch_eastmoney_clist_map(
        cache_dir=tmp_path, min_interval=0.0, jitter=0.0, session=session
    )
    assert len(session.calls) == 2
    session.calls.clear()
    mapping, notes = enrichment.fetch_eastmoney_clist_map(
        cache_dir=tmp_path, min_interval=0.0, jitter=0.0, session=session
    )
    assert session.calls == []
    assert mapping["600519"]["industry"] == "白酒"
    assert any("cache hit" in note for note in notes)


def test_clist_total_change_mid_paging_is_a_failure_not_partial_data(tmp_path):
    def handler(url, params, headers):
        pn = int(params.get("pn", "1"))
        total = 200 if pn == 1 else 201
        return FakeResponse(_clist_page([{"f12": "600519", "f13": 1, "f14": "贵州茅台", "f100": "白酒"}], total))

    session = FakeSession(handler)
    mapping, notes = enrichment.fetch_eastmoney_clist_map(
        cache_dir=tmp_path, min_interval=0.0, jitter=0.0, session=session
    )
    assert mapping == {}
    assert any("total changed" in note for note in notes)


def test_eastmoney_gap_enforced_between_requests():
    session = FakeSession(lambda url, params, headers: FakeResponse({"rc": 0}))
    sleeps: list[float] = []
    # Call 1 at t=100.0 (no wait: last=0.0), call 2 at t=100.5 -> wait 0.5s.
    monotonic_values = iter([100.0, 100.0, 100.5, 100.5])
    with mock.patch("time.monotonic", side_effect=lambda: next(monotonic_values)), mock.patch(
        "time.sleep", side_effect=lambda seconds: sleeps.append(seconds)
    ):
        enrichment._eastmoney_get(
            "https://push2.eastmoney.com/api/qt/clist/get",
            min_interval=1.0,
            jitter=0.0,
            session=session,
        )
        enrichment._eastmoney_get(
            "https://push2.eastmoney.com/api/qt/clist/get",
            min_interval=1.0,
            jitter=0.0,
            session=session,
        )
    assert sleeps == [pytest.approx(0.5)]
    assert len(session.calls) == 2


def test_parse_clist_rows_drops_non_stock_beijing_rows():
    mapping, dropped = enrichment._parse_clist_rows(
        [
            {"f12": "920992", "f13": 0, "f14": "中科美菱", "f100": "医疗器械"},
            {"f12": "810014", "f13": 0, "f14": "北证50", "f100": "指数"},
            {"f12": "600519", "f13": 1, "f14": "贵州茅台", "f100": "白酒"},
        ],
        beijing_segment=True,
    )
    assert mapping == {
        "920992": {"industry": "医疗器械", "name": "中科美菱"},
    }
    # 810014 (non-stock BJ row) and 600519 (a SH code, which cannot
    # appear in the Beijing segment) are both dropped.
    assert dropped == 2


def test_attach_industry_fills_only_known_blank_rows():
    df = pd.DataFrame(
        {
            "code": ["600519", "000001", "999999"],
            "industry": ["", "银行", ""],
        }
    )
    mapping = {
        "600519": {"industry": "白酒", "name": "贵州茅台"},
        "000001": {"industry": "银行Ⅱ", "name": "平安银行"},
    }
    result, notes = enrichment.attach_industry(df, mapping=mapping)
    assert result["industry"].tolist() == ["白酒", "银行", ""]
    assert any("filled=1 of 3" in note for note in notes)


def test_attach_industry_never_raises_on_fetch_failure(tmp_path):
    def handler(url, params, headers):
        raise ConnectionError("boom")

    session = FakeSession(handler)
    df = pd.DataFrame({"code": ["600519"], "industry": [""]})
    result, notes = enrichment.attach_industry(
        df, cache_dir=tmp_path, min_interval=0.0, jitter=0.0, session=session
    )
    assert result["industry"].tolist() == [""]
    assert any("failed" in note for note in notes)


def test_fetch_stock_boards_returns_concept_tags():
    def handler(url, params, headers):
        assert params["spt"] == "3"
        assert params["secid"] == "1.600519"
        return FakeResponse(
            {
                "data": {
                    "diff": {
                        "0": {"f12": "BK001", "f14": "食品饮料", "f3": 1.2, "f128": "贵州茅台"},
                        "1": {"f12": "BK002", "f14": "酿酒概念", "f3": 0.5, "f128": "五粮液"},
                    }
                }
            }
        )

    session = FakeSession(handler)
    result, _notes = enrichment.fetch_stock_boards(
        "600519", min_interval=0.0, jitter=0.0, session=session
    )
    assert result["total"] == 2
    assert result["concept_tags"] == ["食品饮料", "酿酒概念"]
    assert result["boards"][0]["lead_stock"] == "贵州茅台"


# ---------------------------------------------------------------------------
# Layer 2: per-pick fundamentals
# ---------------------------------------------------------------------------

_MAINFINADATA_ROW = {
    "SECUCODE": "600519.SH",
    "SECURITY_NAME_ABBR": "贵州茅台",
    "REPORT_DATE": "2026-09-30 00:00:00",
    "REPORT_DATE_NAME": "2026三季报",
    "EPSJB": 51.53,
    "BPS": 216.95,
    "ROEJQ": 24.47,
    "ROIC": 23.27,
    "XSMLL": 89.58,
    "XSJLL": 50.94,
    "TOTALOPERATEREVE": 133283652508.95,
    "PARENTNETPROFIT": 64467425981.95,
    "MGJYXJJE": 67.24,
    "ZCFZL": 11.90,
}


def _tencent_text(pe="21.91", pb="6.64"):
    fields = [""] * 47
    fields[1] = "贵州茅台"
    fields[3] = "1440.00"
    fields[39] = pe
    fields[45] = "18001.18"
    fields[46] = pb
    return 'v_sh600519="' + "~".join(fields) + '";'


def _fundamentals_session(*, fail_codes=(), tencent_ok=True):
    def handler(url, params, headers):
        if "MAINFINADATA" in str(params.get("type", "")):
            secucode = params.get("filter", "")
            code = secucode.split('"')[1].split(".")[0] if '"' in secucode else ""
            if code in fail_codes:
                return FakeResponse({"code": 9501, "message": "报表配置不存在", "result": None})
            return FakeResponse({"code": 0, "result": {"count": 1, "data": [_MAINFINADATA_ROW]}})
        if "qt.gtimg.cn" in url:
            if not tencent_ok:
                raise ConnectionError("tencent timeout")
            return FakeResponse(None, text=_tencent_text())
        raise AssertionError(f"unexpected url {url}")

    return FakeSession(handler)


def test_enrich_fundamentals_maps_mainfinadata_and_tencent():
    session = _fundamentals_session()
    results, notes = enrichment.enrich_fundamentals(
        ["600519"], min_interval=0.0, jitter=0.0, session=session
    )
    row = results["600519"]
    assert row["report_date"] == "2026-09-30"
    assert row["report_name"] == "2026三季报"
    assert row["eps"] == pytest.approx(51.53)
    assert row["bps"] == pytest.approx(216.95)
    assert row["roe"] == pytest.approx(24.47)
    assert row["roic"] == pytest.approx(23.27)
    assert row["gross_margin"] == pytest.approx(89.58)
    assert row["net_margin"] == pytest.approx(50.94)
    assert row["revenue"] == pytest.approx(133283652508.95)
    assert row["net_profit"] == pytest.approx(64467425981.95)
    assert row["pe_ttm"] == pytest.approx(21.91)
    assert row["pb"] == pytest.approx(6.64)
    assert any("1 codes" in note for note in notes)


def test_enrich_fundamentals_partial_failure_tolerated():
    session = _fundamentals_session(fail_codes={"000001"})
    results, notes = enrichment.enrich_fundamentals(
        ["600519", "000001"], include_quote=False, min_interval=0.0, jitter=0.0, session=session
    )
    assert results["600519"]["roe"] == pytest.approx(24.47)
    assert results["000001"] == {}
    assert any("000001" in note for note in notes)


def test_enrich_fundamentals_tencent_failure_leaves_none():
    session = _fundamentals_session(tencent_ok=False)
    results, _notes = enrichment.enrich_fundamentals(
        ["600519"], min_interval=0.0, jitter=0.0, session=session
    )
    assert results["600519"]["pe_ttm"] is None
    assert results["600519"]["pb"] is None
    assert results["600519"]["roe"] == pytest.approx(24.47)


def test_enrich_fundamentals_rejects_whole_market_use():
    with pytest.raises(ValueError, match="at most 20 codes"):
        enrichment.enrich_fundamentals([f"{i:06d}" for i in range(21)])
    with pytest.raises(ValueError, match="unknown fundamental fields"):
        enrichment.enrich_fundamentals(["600519"], fields=["dividend_yield"])


# ---------------------------------------------------------------------------
# Layer 3: EDGAR frames
# ---------------------------------------------------------------------------

_FRAMES_PAYLOAD = {
    "taxonomy": "us-gaap",
    "tag": "NetIncomeLoss",
    "label": "Net Income (Loss) Attributable to Parent",
    "pts": 4,
    "data": [
        {"accn": "a1", "cik": 320193, "entityName": "Apple Inc.", "val": 1000},
        {"accn": "a2", "cik": 789019, "entityName": "Microsoft Corp", "val": 2000},
        {"accn": "a3", "cik": 320193, "entityName": "Apple Inc.", "val": 1100},
        {"accn": "a4", "cik": "not-a-cik", "entityName": "Bad", "val": 5},
    ],
}

_TICKER_PAYLOAD = {
    "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
    "1": {"cik_str": 789019, "ticker": "MSFT", "title": "MICROSOFT CORP"},
}


@pytest.fixture
def edgar_ua(monkeypatch):
    monkeypatch.setenv("EDGAR_USER_AGENT", "Test Person test@example.com")


def test_fetch_edgar_frames_parses_cik_mapping(edgar_ua):
    session = FakeSession(lambda url, params, headers: FakeResponse(_FRAMES_PAYLOAD))
    mapping, notes = enrichment.fetch_edgar_frames(
        "NetIncomeLoss", "USD", "CY2025Q1", min_interval=0.0, session=session
    )
    # Duplicate CIK: last row wins.
    assert mapping == {320193: 1100.0, 789019: 2000.0}
    assert any("companies=2" in note for note in notes)
    assert any("duplicates=1" in note for note in notes)
    call = session.calls[0]
    assert call["url"].endswith("/us-gaap/NetIncomeLoss/USD/CY2025Q1.json")
    assert call["headers"]["User-Agent"] == "Test Person test@example.com"


def test_fetch_edgar_frames_missing_user_agent_errors(monkeypatch):
    monkeypatch.delenv("EDGAR_USER_AGENT", raising=False)
    session = FakeSession(lambda url, params, headers: FakeResponse(_FRAMES_PAYLOAD))
    with pytest.raises(ValueError, match="EDGAR_USER_AGENT is required"):
        enrichment.fetch_edgar_frames("NetIncomeLoss", "USD", "CY2025Q1", session=session)
    assert session.calls == []


def test_fetch_edgar_frames_user_agent_without_email_errors(monkeypatch):
    monkeypatch.setenv("EDGAR_USER_AGENT", "Test Person")
    session = FakeSession(lambda url, params, headers: FakeResponse(_FRAMES_PAYLOAD))
    with pytest.raises(ValueError, match="must include an email"):
        enrichment.fetch_edgar_frames("NetIncomeLoss", "USD", "CY2025Q1", session=session)


def test_fetch_ticker_cik_map_downloads_and_caches(edgar_ua, tmp_path):
    session = FakeSession(lambda url, params, headers: FakeResponse(_TICKER_PAYLOAD))
    mapping, notes = enrichment.fetch_ticker_cik_map(cache_dir=tmp_path, session=session)
    assert mapping == {"AAPL": 320193, "MSFT": 789019}
    assert (tmp_path / enrichment.EDGAR_TICKER_CACHE_FILENAME).is_file()
    session.calls.clear()
    mapping2, notes2 = enrichment.fetch_ticker_cik_map(cache_dir=tmp_path, session=session)
    assert mapping2 == mapping
    assert session.calls == []
    assert any("cache hit" in note for note in notes2)


def test_enrich_us_fundamentals_joins_ticker_to_cik_to_frame(edgar_ua, tmp_path):
    def handler(url, params, headers):
        if "company_tickers" in url:
            return FakeResponse(_TICKER_PAYLOAD)
        if "frames" in url:
            return FakeResponse(_FRAMES_PAYLOAD)
        raise AssertionError(f"unexpected url {url}")

    session = FakeSession(handler)
    results, notes = enrichment.enrich_us_fundamentals(
        ["aapl", "MSFT", "NOPE"],
        "NetIncomeLoss",
        "USD",
        "CY2025Q1",
        cache_dir=tmp_path,
        min_interval=0.0,
        session=session,
    )
    assert results == {"AAPL": 1100.0, "MSFT": 2000.0}
    assert any("NOPE" in note for note in notes)
