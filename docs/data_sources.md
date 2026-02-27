# Data sources

Source-of-truth for raw inputs used by the pipeline. Data lives under `data/` (see [config.py](../config.py)).

## Sharadar tables

| Table | Role in pipeline |
|-------|------------------|
| **TICKERS** | Baseline metadata: firstpricedate, lastpricedate, sector, famaindustry. Universe construction (01). |
| **ACTIONS** | Event log: action type, date. Determines active universe (delist/bankruptcy/voluntary/regulatory/mergerfrom) and forward event flags (fwd_delisted_30d, fwd_delisted_90d, fwd_acquired_90d, fwd_spinoff_60d). Must include full history including delisted companies. |
| **SF1** | Fundamentals. Use **ARQ, ART, ARY only** (as-reported, no restatements). **Do not use MRQ/MRT/MRY** for PIT — they introduce lookahead. Use `datekey` (= SEC filing date) as availability boundary, not calendardate/reportperiod. Consumed by 02_fundamentals. |
| **SF2** | Insider transactions. Use `filingdate` as PIT boundary. Transaction codes P (purchase), S (sale); require transactionshares not null and <> 0 (no securityadcode filter). Consumed by 06_insider_institutional. |
| **SF3 / SF3A** | Institutional holdings (13F). Quarterly with up to 45d lag; use calendardate and filing lag. shrholders, shrunits, putunits, cllunits. Consumed by 06_insider_institutional. |
| **SEP** | Equity prices (not SFP = funds). Use `closeadj` for returns; `close` for level features (e.g. MA distance). Consumed by 01 (activity lookback), 03_price_features. |
| **DAILY** | Price-updated valuation: marketcap, pe, pb, ps, evebitda, evebit. PIT join on ticker + date. Consumed by 02 (valuation), universe (marketcap_daily). |
| **METRICS** | Precomputed: beta1y, volumeavg3m, 52w high/low, ma50d, ma200d. Use most recent row where date <= D for PIT. Consumed by 03_price_features. |
| **SP500** | Historical index membership. Use for backtesting on index composition; avoids reconstitution bias. |

## Dimension rules (SF1)

- **ARQ**: As-reported quarterly, excluding restatements. `datekey` = filing date = availability.
- **ART**: As-reported TTM. Same logic.
- **ARY**: As-reported annual. For CAGR / R² over 10y.
- **MRQ/MRT/MRY**: Do not use for PIT backtesting.

## FRED series

Download and cache as parquet under `data/macro/` (or path in config). For **point-in-time (PIT) backtesting**, use real-time vintages for revised series (CPI, NFCI, Fed Funds); config options `FRED_USE_PIT` and `FRED_PIT_SERIES` (e.g. `cpi`, `nfci`, `fed_funds`) control vintage fetch in `00_fetch_fred.py`. Single-vintage (current) fetch is fine for daily, rarely revised series (VIX, yields, spreads). **Rate limiting:** FRED API allows up to 2 requests/second; the fetch script throttles (e.g. 0.55 s after each request) and retries on HTTP 429 with backoff.

| FRED code | Name / use |
|-----------|------------|
| DGS10 | 10y Treasury yield |
| DGS2 | 2y Treasury yield → yield_curve = DGS10 - DGS2 |
| BAMLH0A0HYM2 | HY credit spread |
| BAMLC0A0CM | IG credit spread |
| VIXCLS | VIX (or verify VIX proxy in SFP) |
| NFCI | Chicago Fed financial conditions |
| FEDFUNDS | Fed funds rate |
| CPIAUCSL | CPI → cpi_yoy, real_rate |
| UNRATE | Unemployment (use FRED-MD vintage for strict PIT if needed) |

## Benchmark tickers (SEP / SFP)

Verify in your data before wiring pipeline:

- **SPY**: S&P 500 ETF (macro regime: spy_regime_ma, spy_ret_12m)
- **QQQ**, **IWM**, **VTI**: Optional.
- **VIX**: Often `^VIX` or VIXY ETF in SFP; otherwise use FRED VIXCLS.

Run a quick `SELECT DISTINCT ticker FROM sep WHERE ticker IN ('SPY','QQQ')` (and similar for SFP) to confirm symbols.
