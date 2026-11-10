# Screening enrichment layers

Optional enrichment for the screening pipeline, implemented in
`src/services/screening/enrichment.py`. All three layers are opt-in
and degrade gracefully: a failed enrichment leaves blank/`None`
values and a note, it never fails a screening run. Nothing in this
document wires the layers into the pipeline — the proposed call
sites are at the end, for the coordinator to decide.

## Layer 1 — A-share industry / concepts (Eastmoney push2 clist)

**What it does:** one serial paged sweep over the whole A-share
market, filling the `industry` column of a snapshot DataFrame.

- Endpoint: `GET https://push2.eastmoney.com/api/qt/clist/get`
- Params per page: `pn`, `pz` (100), `po=1`, `np=1`, `fltt=2`,
  `invt=2`, `fid=f12`, `fields=f12,f13,f14,f100`, plus `fs` per
  segment:
  - SH/SZ: `fs=m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23` (same universe as
    akshare `stock_zh_a_spot_em`; ~5,563 rows)
  - Beijing: `fs=m:0+t:81+s:2048` (~365 rows, non-stock `81xxxx`
    rows filtered out, only `92xxxx` kept)
- Field codes are from simonlin1212/a-stock-data `SKILL.md` v3.10.4
  (`_em_clist_all` / `st_stock_list`, `board_fund_flow`): the clist
  field carrying the industry name is **`f100`** (e.g. `半导体`,
  `电力`, `医疗器械`). Note `f127` is *not* the industry on clist
  (it is numeric there); on the single-stock `qt/stock/get`
  endpoint the industry is `f127` instead — do not mix the two.
- **Rate limit (hard):** requests are serial with a >=1s gap
  (+ up to 0.3s jitter), enforced by a process-wide throttle in the
  module. Eastmoney bans IPs that burst (>5 req/s or >=200 req/min);
  a full sweep is ~60 pages and takes ~60s at the enforced pace
  (141.5s measured end-to-end from this VM, where the egress proxy
  adds retries/slowness — see live verification). Never parallelise
  this pager, and never lower the interval below 1s.
- **Cache:** one JSON file per calendar day,
  `eastmoney_clist_map_v1.json`, under the industry-provider cache
  dir (same convention as `industry.py`: `SCREENING_DATA_DIR` /
  `data/industry_provider_cache`, overridable with
  `SCREENING_INDUSTRY_PROVIDER_CACHE_DIR` /
  `INDUSTRY_PROVIDER_CACHE_DIR`; TTL 24h via
  `SCREENING_INDUSTRY_PROVIDER_CACHE_TTL_HOURS` /
  `INDUSTRY_PROVIDER_CACHE_TTL_HOURS`). A fresh cache hit makes no
  HTTP request. The file records `trade_date`; a file from another
  day is treated as stale.
- **Entry points:**
  - `fetch_eastmoney_clist_map(...) -> (mapping, notes)` — the sweep;
    returns `{code: {"industry", "name"}}`.
  - `attach_industry(df, ...) -> (df, notes)` — fills only blank
    `industry` cells; existing values (from map files, the akshare
    provider, or the snapshot source) are never overwritten; unknown
    codes stay blank.
  - `fetch_stock_boards(code, ...) -> (result, notes)` — per-stock
    board membership via push2 `slist` (`spt=3`,
    `fields=f12,f14,f3,f128`): industry + concept + region boards
    with each board's daily change and lead stock. One request per
    stock, so this is a per-pick helper, not a market sweep.

**Relation to `industry.py`:** the existing
`enrich_industry_concepts()` supports map files and the `akshare`
board provider only; there is no clist provider there, so this
module does not reuse it — it only reuses the cache-dir/TTL env
convention and the `normalize` helpers. A future coordinator step
could add a `provider="eastmoney_clist"` branch to `industry.py`
that calls `fetch_eastmoney_clist_map()`.

### Live verification (2026-11-10, this VM)

- clist p1: `rc=0`, `total=5563`, 100 rows; `f100` values e.g.
  `689009 九号公司-WD → 摩托车及其他`, `688981 中芯国际 → 半导体`.
- Beijing segment: 4 pages, 365 rows, all with non-blank `f100`.
- Single-stock check: `600519 → 白酒Ⅱ`, `002697 → 一般零售`,
  `000001 → 银行Ⅱ` (via `qt/stock/get`, field `f127`).
- `slist spt=3` for `600519`: 32 boards, incl. `食品饮料`,
  `白酒Ⅱ`, `白酒Ⅲ`, `贵州板块`, `酿酒概念`, `HS300_`.
- Full sweep through the new module (`fetch_eastmoney_clist_map`,
  cache in `/tmp`): 60 pages in 141.5s, **5,922 rows**,
  `dropped_non_stock_bj=6`; spot values `600519 → 白酒Ⅱ`,
  `000001 → 银行Ⅱ`, `002697 → 一般零售`, `920992 → 医疗器械`;
  `attach_industry` on a 5-row frame filled 3, kept a preset
  `银行` value untouched, left unknown code `999999` blank.
- One transient failure seen during probing: a first `push2`
  request failed with proxy `502 Bad Gateway` (and once with
  `RemoteDisconnected`); the session retry adapter recovered on
  the next attempt. The module retries 429/5xx (not 403).

## Layer 2 — A-share fundamentals (per-pick only, <=20 codes)

**What it does:** `enrich_fundamentals(codes, fields=None,
include_quote=True, ...) -> (results, notes)` fetches the latest
report-period fundamentals for a small list of screened picks.
More than 20 codes raises `ValueError` — there is no one-shot
whole-market fundamentals cross-section, and this layer must not
become one.

- Source 1 — Eastmoney financial indicators (verified):
  `GET https://datacenter.eastmoney.com/securities/api/data/get`
  with `type=RPT_F10_FINANCE_MAINFINADATA`,
  `sty=APP_F10_MAINFINADATA`, `filter=(SECUCODE="600519.SH")`,
  `p=1`, `ps=1`, `sr=-1`, `st=REPORT_DATE`, `source=HSF10`,
  `client=PC`. (Parameter style taken from the installed akshare
  `stock_financial_analysis_indicator_em`; the datacenter-web
  `reportName=RPT_F10_FINANCE_INDICATOR` variant was tested and
  **rejected**: it returns `code=9501, message=报表配置不存在`.)
- Fields (MAINFINADATA columns → exposed names): `EPSJB → eps`,
  `BPS → bps`, `ROEJQ → roe`, `ROIC → roic`, `XSMLL →
  gross_margin`, `XSJLL → net_margin`, `TOTALOPERATEREVE →
  revenue`, `PARENTNETPROFIT → net_profit`, `MGJYXJJE →
  operating_cash_per_share`, `ZCFZL → asset_turnover`. Plus
  `report_date` / `report_name` on every row.
- Source 2 — Tencent quote for PE-TTM/PB (optional, best effort):
  `GET https://qt.gtimg.cn/q=sh600519`, `~`-separated fields,
  index 39 = PE-TTM, index 46 = PB. Same serial throttle applies.
- Missing field → `None`; a per-code failure → `{}` for that code
  plus a note; the other codes still return.
- **Not provided:** dividend yield. Neither MAINFINADATA nor the
  Tencent quote carries it; a dividend-history source is a
  follow-up, not part of this layer.

### Live verification (2026-11-10, this VM)

- `600519.SH`, latest `2026三季报` (2026-09-30): `EPSJB=51.53`,
  `BPS=216.95`, `ROEJQ=24.47`, `ROIC=23.27`, `XSMLL=89.58`,
  `XSJLL=50.94`, revenue `133,283,652,508.95`,
  net profit `64,467,425,981.95`, `MGJYXJJE=67.24`, count=104 rows.
- `000001.SZ`, latest `2026三季报`: `ROEJQ=8.07`, count=123 rows.
- Endpoint mechanics validated separately with
  `RPT_DAILYBILLBOARD_DETAILSNEW` (`code=0`, `count=146`).
- Tencent quote via curl: `600519` price `1440.00`, PE-TTM `21.91`,
  PB `6.64`, market cap `18001.18` 亿 — but it took **22s** through
  this VM's egress proxy, and `requests` read-timed-out twice at
  10s/20s. The module uses a 30s timeout and treats a Tencent
  failure as `pe_ttm/pb=None` with a note; do not lower the timeout.

## Layer 3 — US fundamentals (SEC EDGAR frames)

**What it does:** one request returns a whole metric for the entire
US market. It has no prices/volume, so it is an optional screening
layer, never a snapshot source.

- Endpoint:
  `GET https://data.sec.gov/api/xbrl/frames/{taxonomy}/{metric}/{unit}/{period}.json`
  (e.g. `us-gaap/NetIncomeLoss/USD/CY2025Q1`).
- Response shape: top-level `data` list of
  `{accn, cik, entityName, loc, start, end, val}` plus `pts`
  (company count) and `label`. Duplicate CIKs (restatements) keep
  the last row; the duplicate count is noted.
- **User-Agent (hard SEC rule):** the header must carry a real name
  + email. It is read from the `EDGAR_USER_AGENT` environment
  variable (or the `user_agent=` argument) **only** — there is no
  hardcoded default identity anywhere in the code. Missing env →
  `ValueError` with instructions; a value without `@` → `ValueError`.
- Rate limit: official 10 req/s; the module enforces >=0.2s
  between requests (env `SCREENING_ENRICHMENT_EDGAR_MIN_INTERVAL_SEC`).
- **Entry points:**
  - `fetch_edgar_frames(metric, unit, period, ...) ->
    ({cik: value}, notes)`.
  - `fetch_ticker_cik_map(...) -> ({ticker: cik}, notes)` — one-time
    download of `https://www.sec.gov/files/company_tickers.json`,
    cached 7 days as `edgar_company_tickers_v1.json` in the same
    cache dir (same `EDGAR_USER_AGENT` requirement).
  - `enrich_us_fundamentals(tickers, metric, unit, period, ...) ->
    ({ticker: value}, notes)` — joins ticker → CIK → frame value;
    tickers with no CIK/value are absent and noted.

### Live verification (2026-11-10, this VM)

- `NetIncomeLoss/USD/CY2025Q1`: HTTP 200, 824,137 bytes,
  `pts=5376` companies (task brief cited 5,309 — the count has
  since grown; sample: AAR CORP cik 1750 val −8,900,000; Abbott
  Laboratories cik 1800 val 1,325,000,000). curl took 1.2s.
- Through the new module (`fetch_edgar_frames`): 5,376 companies
  in 1.3s; AAPL (cik 320193) NetIncomeLoss CY2025Q1 =
  24,780,000,000.

## Environment variables (read inside `enrichment.py` only)

| Variable | Default | Meaning |
| --- | --- | --- |
| `EDGAR_USER_AGENT` | — (required) | SEC User-Agent, e.g. `Research Name name@example.com` |
| `SCREENING_ENRICHMENT_EASTMONEY_MIN_INTERVAL_SEC` | `1.0` | Min gap between Eastmoney requests; never set below `1.0` |
| `SCREENING_ENRICHMENT_EASTMONEY_JITTER_SEC` | `0.3` | Extra random delay added after the gap |
| `SCREENING_ENRICHMENT_EDGAR_MIN_INTERVAL_SEC` | `0.2` | Min gap between EDGAR requests |
| `SCREENING_INDUSTRY_PROVIDER_CACHE_DIR` / `INDUSTRY_PROVIDER_CACHE_DIR` | `data/industry_provider_cache` | Cache dir (shared with `industry.py`) |
| `SCREENING_INDUSTRY_PROVIDER_CACHE_TTL_HOURS` / `INDUSTRY_PROVIDER_CACHE_TTL_HOURS` | `24` | clist cache TTL (hours) |

No new `config.py` items were added and no `.env.example` entries
were touched — the coordinator adds those if the tunables are
promoted to config.

## Proposed pipeline call sites (not wired)

- **Industry:** in `pipeline.py`, right after the existing
  `enrich_industry_concepts(...)` call (or as a fallback when it
  yields no mapping): `snapshot_df, notes =
  attach_industry(snapshot_df, cache_dir=config.industry_provider_cache_dir,
  cache_ttl_hours=config.industry_provider_cache_ttl_hours)` and
  extend `degradation` with the notes. Cost on a cache miss is a
  ~60-page serial sweep (~60s at the enforced pace, 141.5s measured
  from this VM); on a cache hit it is free, so
  the daily run should warm the cache or accept the first-run cost.
- **A-share fundamentals:** after hard filtering, on the shortlist
  (same place `enrich_daily_features` runs):
  `results, notes = enrich_fundamentals(shortlist_codes)` — at most
  20 codes, 1–2 throttled requests per code.
- **US fundamentals:** for US-market strategies only, after the
  shortlist: `enrich_us_fundamentals(tickers, "NetIncomeLoss",
  "USD", "CY2025Q1")` (metric/period chosen by the strategy).
  Requires `EDGAR_USER_AGENT` in the runtime environment.

## Risks

- **Eastmoney IP ban:** the >=1s serial gap is the only thing
  standing between the sweep and a 20h+ ban. Any future caller
  must not parallelise `fetch_eastmoney_clist_map`, lower the
  interval, or call it in a tight loop; the day cache exists
  precisely so the sweep runs once per day.
- **Cache staleness:** the clist cache is keyed by calendar day
  and TTL 24h; industry changes mid-day (rare) are picked up the
  next day. `force_refresh=True` bypasses it for a manual refresh.
- **EDGAR UA:** a missing or fake `EDGAR_USER_AGENT` either raises
  (by design) or risks SEC throttling; set a real name + email in
  the deployment environment.
- **Tencent slowness:** PE/PB via `qt.gtimg.cn` took 22s from this
  VM; the layer degrades to `None` rather than blocking, but a
  20-code enrich with quotes can take minutes on a slow egress.
- **Data freshness:** MAINFINADATA returns the latest *reported*
  period (e.g. 2026三季报 in November); EDGAR frames are point-in-
  time snapshots per period, not a live feed.

## Rollback

Delete `src/services/screening/enrichment.py` (plus its test and
this doc) and the layers are gone: nothing else imports it, the
pipeline is unwired, and screening falls back to its existing
industry/files behaviour. Cache files under
`data/industry_provider_cache/` (`eastmoney_clist_map_v1.json`,
`edgar_company_tickers_v1.json`) are inert JSON and can be deleted
at any time.
