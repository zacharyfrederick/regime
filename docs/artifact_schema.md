# Artifact schema

Contract for every parquet output. Pipeline scripts must produce these schemas.

## outputs/universe/daily_universe.parquet

| Column | Type | Semantics |
|--------|------|-----------|
| ticker | VARCHAR | Ticker symbol |
| date | DATE | Trading date |
| in_universe | BOOLEAN | True if ticker was active, tradeable, and had recent price on this date |
| days_listed | INTEGER | Days since firstpricedate |
| fwd_delisted_30d | BOOLEAN | Delisted within 30 days after date |
| fwd_delisted_90d | BOOLEAN | Delisted within 90 days |
| fwd_acquired_90d | BOOLEAN | Acquired within 90 days |
| fwd_spinoff_60d | BOOLEAN | Spinoff within 60 days |
| sector | VARCHAR | TICKERS.sector (GICS-approximate) |
| famaindustry | VARCHAR | Fama-French 48 industry |
| marketcap_daily | BIGINT | From DAILY.marketcap (PIT) when available |
| marketcap_rank_annual | BIGINT | Year-end rank by marketcap_daily (1 = largest); NULL when no rank |
| scalemarketcap | INTEGER | TICKERS scale 1–6 (e.g. 1=Nano … 6=Mega) or NULL when DAILY absent |

## outputs/features/fundamental_pit.parquet

Valuation ratios (pe_pit, pb_pit, ps_pit, pcf_pit, pfcf_pit, evebitda_pit) use **shareswa** (TTM weighted average shares). Basic shares as of filing (sharesbas) could be used for a more current share count; left for future refinement. When both ART and quality_metrics are missing for a row, datekey and days_since_filing can be NULL — downstream should treat NULL staleness as unknown, not fresh.

| Column | Type | Semantics |
|--------|------|-----------|
| ticker | VARCHAR | |
| date | DATE | |
| art_datekey | DATE | Most recent ART (TTM) filing date |
| arq_datekey | DATE | Datekey of quality_metrics row (ARQ vintage) |
| datekey | DATE | GREATEST(art_datekey, arq_datekey); use for staleness |
| days_since_filing | INTEGER | date - datekey |
| quarters_stale | DOUBLE | days_since_filing / 91 |
| ncfo_r2_10y | DOUBLE | OLS R² of log(ncfo) ~ time over 10y ARY |
| ncfo_cagr_10y | DOUBLE | CAGR of ncfo (ARY) |
| fcf_cagr_5y | DOUBLE | CAGR of FCF (ARY) |
| roic_level | DOUBLE | ROIC (ARQ/ART) |
| roic_slope_3y | DOUBLE | Slope of ROIC over 3y |
| grossmargin_slope | DOUBLE | Slope over 8 quarters (ARQ) |
| fcf_conversion | DOUBLE | fcf / netinccmn (ART) |
| accrual_ratio | DOUBLE | (netinccmn - fcf) / assets (ART) |
| sbc_pct_revenue | DOUBLE | sbcomp / revenueusd (ART) |
| capex_intensity | DOUBLE | capex / revenueusd (ART) |
| net_debt_trend | DOUBLE | Trend in (debt - cashnequsd) (ARQ) |
| dilution_rate | DOUBLE | Delta sharesbas / sharesbas (ARY) |
| pe_pit | DOUBLE | Computed from SEP closeadj × ART shareswa / ART netinccmn (current price, TTM); not DAILY |
| pb_pit | DOUBLE | marketcap / ART equity (SEP + ART) |
| ps_pit | DOUBLE | marketcap / ART revenue (SEP + ART) |
| evebitda_pit | DOUBLE | (marketcap + debt − cash) / ART ebitda (SEP + ART). EV omits preferred stock unless SF1 has preferredstock. |
| fcf_recon_ttm | DOUBLE | ncfo_ttm − capex_ttm (reconstructed FCF) |
| pcf_pit | DOUBLE | price / ncfo_ttm (Dreman PCF) |
| pfcf_pit | DOUBLE | price / fcf_recon_ttm |
| ncfo_to_revenue | DOUBLE | ncfo_ttm / revenue_ttm |
| fcf_to_revenue | DOUBLE | fcf_recon_ttm / revenue_ttm |
| ncfo_pct_positive | DOUBLE | Fraction of years with positive NCFO (10y window) |
| ncfo_r2_adjusted | DOUBLE | ncfo_r2_10y × ncfo_pct_positive |
| fcf_r2_10y | DOUBLE | R² of log(fcf_recon) over 5y |
| fcf_pct_positive | DOUBLE | Fraction of years positive FCF |
| fcf_r2_adjusted | DOUBLE | fcf_r2 × fcf_pct_positive |
| fcf_ncfo_r2_delta | DOUBLE | ncfo_r2_adjusted − fcf_r2_adjusted |

(Additional ARQ/ART/ARY columns as needed; merge script selects final master set.)

## outputs/features/price_features.parquet

| Column | Type | Semantics |
|--------|------|-----------|
| ticker | VARCHAR | |
| date | DATE | |
| ret_1m, ret_3m, ret_6m, ret_12m | DOUBLE | closeadj returns |
| vol_20d, vol_60d | DOUBLE | Annualized vol |
| vol_ratio | DOUBLE | vol_20d / vol_60d |
| volume_ratio_1m | DOUBLE | volume / avg(volume, 20d) |
| pct_52w_range | DOUBLE | (price - low52w) / (high52w - low52w) |
| atr_14d | DOUBLE | Avg high-low 14d; atr_14d_normalized in master |
| ma50_cross, ma200_cross | DOUBLE | close/ma - 1 |
| momentum_skew_60d | DOUBLE | Skewness of 60d returns |

## outputs/features/macro_features.parquet

| Column | Type | Semantics |
|--------|------|-----------|
| date | DATE | One row per date |
| yield_curve | DOUBLE | DGS10 - DGS2 |
| hy_spread | DOUBLE | BAMLH0A0HYM2 |
| ig_spread | DOUBLE | BAMLC0A0CM |
| vix | DOUBLE | VIXCLS or SFP proxy |
| vix_change_20d | DOUBLE | 20d change in VIX |
| nfci | DOUBLE | NFCI |
| real_rate | DOUBLE | DGS10 - trailing 12m CPI change |
| fed_funds | DOUBLE | FEDFUNDS |
| cpi_yoy | DOUBLE | YoY CPI |
| spy_regime_ma | DOUBLE | SPY vs 200d MA (optional) |
| spy_ret_12m | DOUBLE | SPY 12m return |

## outputs/features/sector_relative.parquet

| Column | Type | Semantics |
|--------|------|-----------|
| ticker | VARCHAR | |
| date | DATE | |
| pe_vs_sector | DOUBLE | Company PE vs sector median (ratio or difference per pipeline) |
| pb_vs_sector | DOUBLE | Same for PB |
| ps_vs_sector | DOUBLE | Same for PS |
| pcf_vs_sector | DOUBLE | Same for PCF |
| evebitda_vs_sector | DOUBLE | Same for EV/EBITDA |
| roic_vs_sector | DOUBLE | Company ROIC − sector median |
| ret_3m_vs_sector | DOUBLE | 3m return vs sector |
| vol_vs_sector | DOUBLE | Vol vs sector |
| ret_3m_rank_sector | DOUBLE | Percent rank of ret_3m within sector-date |

## outputs/features/insider_institutional.parquet

| Column | Type | Semantics |
|--------|------|-----------|
| ticker | VARCHAR | |
| date | DATE | |
| insider_buy_count_90d | DOUBLE | Count of buy transactions (SF2, filingdate, 90d window) |
| insider_sell_count_90d | DOUBLE | Count of sell transactions |
| insider_net_shares_90d | DOUBLE | Net shares (buys − sells) in 90d window |
| insider_net_ratio_90d | DOUBLE | Net buy/sell ratio (e.g. denominator = avg shares owned in window) |
| insider_officer_buy_90d | DOUBLE | Officer buy count or flag in 90d window |
| inst_shrholders | DOUBLE | 13F shrholders (SF3A) |
| inst_shrunits | DOUBLE | 13F shrunits |
| inst_shrvalue | DOUBLE | 13F shrvalue |
| inst_put_call_ratio | DOUBLE | putunits / cllunits (SF3A), capped at 100 in pipeline |
| inst_shrholders_chg_qoq | DOUBLE | QoQ change in shrholders |
| inst_shrunits_chg_qoq | DOUBLE | QoQ change in shrunits |

## outputs/master/master_features.parquet

One row per (ticker, date) with in_universe = True. All upstream tables joined on (ticker, date); macro on date. Missing values: NaN or documented policy (e.g. forward-fill within ticker where appropriate). Schema matches [pipeline/07_merge.py](../pipeline/07_merge.py) output.

**Identity:** ticker, date, sector, famaindustry  
**Universe metadata:** days_listed, scalemarketcap, fwd_spinoff_60d, fwd_delisted_30d, fwd_delisted_90d, fwd_acquired_90d  
**Staleness:** days_since_filing, quarters_stale, days_since_art, days_since_arq, days_since_filing_max  
**Price:** ret_1m, ret_3m, ret_6m, ret_12m, vol_20d, vol_60d, vol_ratio, volume_ratio_1m, pct_52w_range, ma50_cross, ma200_cross, atr_14d_normalized, momentum_skew_60d  
**Fundamental quality:** ncfo_r2_10y, ncfo_cagr_10y, ncfo_pct_positive, ncfo_r2_adjusted, fcf_cagr_5y, fcf_r2_10y, fcf_pct_positive, fcf_r2_adjusted, fcf_ncfo_r2_delta, roic_level, roic_slope_3y, grossmargin_slope, fcf_conversion, accrual_ratio, sbc_pct_revenue, capex_intensity, net_debt_trend, dilution_rate  
**Valuation / cash flow:** fcf_recon_ttm, pcf_pit, pfcf_pit, ncfo_to_revenue, fcf_to_revenue, pe_pit, pb_pit, ps_pit, evebitda_pit  
**Sector relative:** pe_vs_sector, pb_vs_sector, ps_vs_sector, pcf_vs_sector, evebitda_vs_sector, roic_vs_sector, ret_3m_vs_sector, vol_vs_sector, ret_3m_rank_sector  
**Macro:** yield_curve, hy_spread, vix, vix_change_20d, nfci, real_rate, spy_regime_ma, spy_ret_12m  
**Insider / institutional:** insider_buy_count_90d, insider_sell_count_90d, insider_net_shares_90d, insider_net_ratio_90d, insider_officer_buy_90d, inst_shrholders, inst_shrunits, inst_shrvalue, inst_put_call_ratio, inst_shrholders_chg_qoq, inst_shrunits_chg_qoq  

**Size estimate:** ~15–20M rows after liquidity filter; ~2–4 GB parquet (snappy). Fits in DuckDB in-process. First full run ~1.5–3 hours; single-stage re-runs ~5–40 minutes depending on stage.
