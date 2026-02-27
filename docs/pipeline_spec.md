# Pipeline spec

Script contract and order. Each script produces one parquet; downstream scripts consume prior outputs only. Nothing is recomputed downstream. If you change universe logic, re-run from 01 and everything rebuilds in order.

Run full chain: `./run_pipeline.sh` (or run scripts 01–07 in order). Timestamps can be logged to a file so you know when the feature table was last built.

## Order and dependencies

1. **01_universe.py** → outputs/universe/daily_universe.parquet  
2. **02_fundamentals.py** → outputs/features/fundamental_pit.parquet (reads universe + raw)  
3. **03_price_features.py** → outputs/features/price_features.parquet (reads universe + raw)  
4. **04_macro_features.py** → outputs/features/macro_features.parquet (reads raw FRED + SEP/SFP)  
5. **05_sector_relative.py** → outputs/features/sector_relative.parquet (reads universe + fundamentals + price)  
6. **06_insider_institutional.py** → outputs/features/insider_institutional.parquet (reads raw SF2/SF3 + universe)  
7. **07_merge.py** → outputs/master/master_features.parquet (reads universe + all feature parquets)

## Script contracts

### 01_universe.py

- **Inputs:** config paths; raw TICKERS, ACTIONS, SEP (and optionally DAILY) under data/.
- **Output:** `outputs/universe/daily_universe.parquet`
- **Logic:** For each date in config range, compute active universe from TICKERS + ACTIONS (firstpricedate/lastpricedate; exclude delist, bankruptcy, voluntary/regulatory delist, mergerfrom). Optional SEP lookback for activity. Compute forward event flags (fwd_delisted_30d, fwd_delisted_90d, fwd_acquired_90d, fwd_spinoff_60d). Attach sector, famaindustry, marketcap_daily (from DAILY when available), and marketcap_rank_annual (year-end rank for Dreman top-1500 filter).

### 02_fundamentals.py

- **Inputs:** daily_universe.parquet (grid); raw SF1, SEP (for current price). No DAILY for valuation.
- **Output:** `outputs/features/fundamental_pit.parquet`
- **Logic:** Separate ARQ and ART paths (no single ASOF over ARQ/ART/ARY). **ARQ path:** ARQ PIT view (one row per ticker, reportperiod, latest datekey); aggregate to annual by fiscal year; Python pass (scipy linregress) computes ncfo_r2_10y, ncfo_cagr_10y, fcf_cagr_5y, roic_level, roic_slope_3y, grossmargin_slope, net_debt_trend, dilution_rate per (ticker, datekey) vintage; grid ASOF-joins quality_metrics. **ART path:** Most recent ART row per (ticker, date) for TTM snapshot; join SEP for closeadj; compute accrual_ratio, fcf_conversion, sbc_pct_revenue, capex_intensity from ART; compute pe_pit, pb_pit, ps_pit, evebitda_pit from current price (closeadj × shareswa) and ART TTM. Staleness (days_since_filing, quarters_stale) from ART datekey.

### 03_price_features.py

- **Inputs:** daily_universe.parquet (for ticker-date grid); raw SEP, METRICS.
- **Output:** `outputs/features/price_features.parquet`
- **Logic:** Rolling returns (ret_1m–ret_12m), vol_20d/60d, vol_ratio, volume_ratio, pct_52w_range, atr_14d, ma50_cross, ma200_cross, momentum_skew_60d from SEP (and METRICS where available). Align to universe dates/tickers.

### 04_macro_features.py

- **Inputs:** config date range; FRED parquet under data/macro/; SEP/SFP for SPY.
- **Output:** `outputs/features/macro_features.parquet`
- **Logic:** One row per date. Yield curve, hy_spread, ig_spread, vix, vix_change_20d, nfci, real_rate, fed_funds, cpi_yoy; SPY regime (spy_regime_ma, spy_ret_12m). FRED: use release date for PIT where applicable.

### 05_sector_relative.py

- **Inputs:** daily_universe.parquet; fundamental_pit.parquet; price_features.parquet (or raw).
- **Output:** `outputs/features/sector_relative.parquet`
- **Logic:** Sector medians per date; company minus sector median for pe, pb, roic; rev_growth_rank_sector; ret_3m_vs_sector; vol_vs_sector.

### 06_insider_institutional.py

- **Inputs:** daily_universe.parquet (ticker-date grid); raw SF2, SF3/SF3A.
- **Output:** `outputs/features/insider_institutional.parquet`
- **Logic:** SF2: insider buys/sells by filingdate (90d/180d windows); SF3A: shrholders, shrunits, putunits, cllunits; QoQ changes; put_call_ratio. Align to ticker-date.

### 07_merge.py

- **Inputs:** daily_universe.parquet; all five feature parquets.
- **Output:** `outputs/master/master_features.parquet`
- **Logic:** Join all on (ticker, date); filter to in_universe = True. One row per ticker per trading day. Output schema = master table (see artifact_schema.md). Missing values: NaN or forward-fill per policy; do not drop rows for missing features where policy says keep.
