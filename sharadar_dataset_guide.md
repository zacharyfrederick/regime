**Sharadar Dataset Construction**

*Walk-Forward Point-in-Time Pipeline for Regime Classification*

# **1. Core Philosophy: Walk-Forward Snapshot Architecture**

Your intuition to walk forward day-by-day rather than running giant SQL queries is correct and important. The goal is to reconstruct the exact information set available to a real investor on each simulation date — no more, no less. Every source of forward-looking contamination degrades the model's real-world validity, sometimes silently and catastrophically.

Think of each simulation date as a 'knowledge horizon.' You know everything filed, reported, or published on or before that date. You know nothing after it. The pipeline enforces this mechanically so it never depends on human discipline to avoid peeking.

# **2. Universe Construction Per Day**

## **2.1 Active Universe from TICKERS + ACTIONS**

The TICKERS table gives you baseline metadata. The ACTIONS table gives you the event log. Together they let you reconstruct which tickers were valid, tradeable, and covered on any given date.

For each simulation date D, a ticker is in the active universe if all of the following hold:

- tickers.firstpricedate <= D (the company was already trading)

- tickers.lastpricedate >= D (or is null, meaning still active)

- No 'delisted', 'bankruptcyliquidation', 'voluntarydelisting', or 'regulatorydelisting' action in ACTIONS with date <= D

- At least one price row exists in SEP within a reasonable lookback window (e.g., 5 trading days) to confirm actual trading activity

| Key: The ACTIONS table action types that remove a ticker from the universe are: delisted, bankruptcyliquidation, voluntarydelisting, regulatorydelisting, mergerfrom. Use actions.date as the boundary — on or after that date the ticker leaves the universe. |
| --- |

## **2.2 Forward-Looking Event Windows**

For each active ticker on date D, pre-compute forward event flags by scanning ACTIONS for events that occur within defined windows after D. These become target variables or exclusion flags depending on your use case.

| Event | ACTIONS field | Use |
| --- | --- | --- |
| Delisted in 30d | action IN ('delisted','bankruptcyliquidation','voluntarydelisting','regulatorydelisting') AND date BETWEEN D AND D+30 | Flag: forward_delisted_30d |
| Delisted in 90d | same, date BETWEEN D AND D+90 | Flag: forward_delisted_90d |
| Acquired in 90d | action IN ('acquisitionby','mergerfrom') AND date BETWEEN D AND D+90 | Flag: forward_acquired_90d |
| Spinoff in 60d | action = 'spinoff' AND date BETWEEN D AND D+60 | Flag: forward_spinoff_60d |
| Ticker change | action = 'tickerchangeto' AND date BETWEEN D AND D+30 | Note: continuity only |

For forward return calculations (e.g., 1-year return), if forward_delisted_90d or forward_acquired_90d is TRUE, you need a decision: either compute the return up to the delist/acquisition date and mark it as a partial return, or exclude the row from supervised return prediction. For regime classification this matters less, but for any target variable involving price appreciation you must handle it explicitly or your label distribution will be biased toward survivorship.

# **3. Point-in-Time Fundamentals from SF1**

## **3.1 Dimension Selection and Restatement Strategy**

SF1 has six dimension codes. For a PIT backtest the correct choices are:

| Dimension | Meaning and Use |
| --- | --- |
| ARQ | As-reported quarterly, excluding restatements. Use datekey (= SEC filing date) as your availability boundary. This is the correct PIT dimension for most uses. |
| ART | As-reported trailing twelve months, excluding restatements. Useful for TTM metrics like FCF, EBITDA margin. Same filing date logic. |
| ARY | As-reported annual, excluding restatements. For annual ratios and CAGR computations. |
| MRQ/MRT/MRY | Include restatements. DO NOT use for PIT backtesting. These reflect information that was not available on the original filing date. |

| Restatement Strategy: Use ARQ/ART/ARY exclusively. The 'M' dimensions (MRQ, MRT, MRY) contain restated values whose datekey reflects when the restatement was filed, but earlier rows in your backtest will still show the original incorrect values. Mixing dimensions creates subtle lookahead. Stick to AR dimensions and accept that your model trains on what was actually known at the time — which is the point. |
| --- |

## **3.2 PIT Join Logic with DuckDB**

For each ticker on simulation date D, retrieve the most recent SF1 row where datekey <= D. In DuckDB on Parquet this is efficient:

-- Most recent ARQ row available on date D for each ticker

SELECT DISTINCT ON (ticker)

ticker, calendardate, datekey, revenue, ncfo, fcf,

capex, ebitda, equity, debt, cashnequsd, marketcap,

grossmargin, netmargin, roic, fcfps, pe, pb, ps, evebitda

FROM sf1

WHERE dimension = 'ARQ'

AND datekey <= $sim_date

AND ticker = ANY($universe_tickers)

ORDER BY ticker, datekey DESC

The DAILY table is valuable here — it provides price-updated valuation ratios (marketcap, pe, pb, ps, evebitda, evebit) computed against the most recently filed fundamentals but using the daily price. This gives you valuation metrics that update every trading day even when fundamentals only update quarterly. Join DAILY on ticker + date for these.

## **3.3 Staleness Flag**

Always compute a staleness indicator: the number of calendar days between the simulation date D and the most recent filing's calendardate. Flag rows where staleness > 120 days (two missed quarters) as potentially unreliable. This is especially important for small caps that file late.

days_since_filing = D - most_recent_ARQ.datekey

quarters_stale = days_since_filing / 91

# **4. Price Features from SEP**

## **4.1 Core Price Data**

Use SEP (not SFP which is funds) for equity prices. Use closeadj for all return calculations — it adjusts for splits, dividends, and spinoffs. Use close for level-based features like distance from moving averages. closeunadj is rarely needed in modeling.

## **4.2 Price-Derived Features**

Compute these on a rolling basis anchored to date D:

| Feature | Computation | Notes |
| --- | --- | --- |
| Return 1m/3m/6m/12m | closeadj[D] / closeadj[D-N] - 1 | Use closeadj; skip 1m for momentum (Jegadeesh skip-month) |
| Volatility 20d/60d | std(daily_returns, window) | Annualize x sqrt(252) |
| Volume ratio | volume[D] / avg(volume, 20d) | Relative volume; detects institutional activity |
| 52w high/low % | (price - low52w) / (high52w - low52w) | Available in METRICS table directly |
| ATR 14d | avg(high-low, 14d) | Range compression signal |
| MA cross | close / ma50d - 1, close / ma200d - 1 | Available precomputed in METRICS |
| Beta 1y | beta1y from METRICS | Precomputed weekly; pull PIT |
| Volume avg ratio | volume / volumeavg3m from METRICS | Precomputed; use directly |
| Price momentum skew | skewness of 60d returns | Captures return distribution shape |
| Realized vol ratio | vol_20d / vol_60d | Detects vol regime compression |

# **5. Fundamental Features from SF1**

## **5.1 Your Existing Framework**

Your OLS R-squared on log(ncfo) over 10 years and the CAGR of ncfo are strong quality signals. Here is how to extend that framework using specific SF1 indicators:

## **5.2 Quality and Consistency Features**

| Feature | SF1 Indicator(s) | Description |
| --- | --- | --- |
| NCFO R2 (10yr) | ncfo, ARY dimension | OLS of log(ncfo) ~ time; R2 measures cash flow consistency. Your core quality signal. |
| NCFO CAGR (10yr) | ncfo, ARY | Compound growth rate of operating cash flow. |
| FCF consistency | fcf = ncfo - capex, ARY | Same R2/CAGR on free cash flow. FCF is harder to manage than reported earnings. |
| Revenue CAGR (5yr/10yr) | revenueusd, ARY | Top-line growth quality. Use USD for cross-company comparability. |
| Gross margin trend | grossmargin, ARQ | Slope of gross margin over 8 quarters. Expanding margin = pricing power. |
| EBITDA margin trend | ebitdamargin, ARQ | Operating leverage signal. |
| ROIC level + trend | roic, ARQ/ARY | Level and 3yr/5yr slope. High and stable ROIC is the strongest moat signal. |
| FCF conversion | fcf / netinccmn, ART | FCF / net income. >1.0 consistently = high earnings quality. |
| SBC as % revenue | sbcomp / revenueusd, ART | High SBC dilutes real FCF. Common quality trap. |
| Capex intensity | capex / revenueusd, ART | Asset-light businesses sustain returns more easily. |
| Working capital efficiency | workingcapital / revenueusd, ARQ | Negative WC businesses (collect before spending) are high quality. |
| Cash conversion cycle | receivables, inventory, payables, ARQ | Days receivables + days inventory - days payable. |
| Debt/EBITDA | debt / ebitdausd, ART | Leverage. Use USD versions for cross-company comparability. |
| Net debt trend | (debt - cashnequsd), ARQ | Is the company accumulating or burning net debt? |
| Interest coverage | ebit / intexp, ART | Debt service safety margin. |
| Earnings quality (accruals) | (netinccmn - fcf) / assets, ART | Sloan accrual ratio. High accruals predict earnings reversals. |
| Buyback yield | ncfcommon / marketcap, ART | Negative ncfcommon = buybacks. Capital return signal. |
| Dilution rate | delta(sharesbas) / sharesbas, ARY | Annual dilution. Negative = buybacks. Positive erodes per-share value. |

# **6. Market Context and Macro Features**

## **6.1 Benchmark Indices**

The SP500 table gives you historical index membership. For index-level price data you need the SEP or SFP tables. Common tickers in the Sharadar dataset:

| Index / Instrument | Ticker in SEP/SFP |
| --- | --- |
| S&P 500 ETF | SPY |
| NASDAQ 100 ETF | QQQ |
| Russell 2000 ETF | IWM |
| Total Market ETF | VTI |
| VIX (volatility index) | ^VIX — verify in your data; may be VIXY as ETF proxy |
| Sector ETFs (XLK, XLF, XLE, etc.) | These are typically in SFP as fund prices |
| 10yr Treasury ETF | IEF or TLT as proxy |

| Verify: Run a quick SELECT DISTINCT ticker FROM sfp WHERE ticker LIKE '%VIX%' and similar queries to confirm exact ticker names in your local parquet files before building the pipeline around them. |
| --- |

## **6.2 FRED Macro Data**

FRED does provide point-in-time data in the sense that each series has a vintage date — FRED-MD and FRED-QD specifically are designed for real-time vintage tracking. For most macro series the difference between real-time and final revised data is small, but for GDP and employment it can be meaningful.

Recommended FRED series and their PIT considerations:

| Series | FRED Code | PIT Notes |
| --- | --- | --- |
| Fed Funds Rate | FEDFUNDS | Daily/monthly; essentially no revision. Safe to use as-is. |
| 10yr Treasury Yield | DGS10 | Daily; no revision. Use directly. |
| 2yr Treasury Yield | DGS2 | Daily; no revision. Yield curve = DGS10 - DGS2. |
| Credit Spread (HY) | BAMLH0A0HYM2 | Daily; no revision. Strong risk-on/off signal. |
| IG Credit Spread | BAMLC0A0CM | Daily; complements HY spread. |
| VIX | VIXCLS | Daily; no revision. Use FRED or pull from SFP. |
| CPI YoY | CPIAUCSL | Monthly; released with ~2 week lag. Use release date not period end. |
| Unemployment Rate | UNRATE | Monthly; revised. Use FRED-MD vintage for strict PIT. |
| ISM Manufacturing PMI | MANEMP proxy / ISM | Monthly; minimal revision. |
| Conference Board LEI | USSLIND | Monthly; 3-4 week lag. Useful regime signal. |
| Chicago Fed NFCI | NFCI | Weekly financial conditions index; minimal revision. |
| TED Spread | TEDRATE | Daily; no revision. Systemic stress indicator. |

Use pandas-datareader or the fredapi Python library. The key PIT principle for FRED: use the actual release date, not the reference period end date. For example, November CPI (covering October prices) is typically released in mid-November — a backtest date of November 1st should not have access to it.

# **7. Insider and Institutional Features from SF2/SF3**

## **7.1 SF2 Insider Transactions**

Insider buying is one of the most reliable signals in academic literature. Use transaction codes P (open market purchase) and S (open market sale) from SF2; require transactionshares not null and <> 0. (Do not filter on securityadcode — in Sharadar, P transactions are mostly NULL/DA, not 'NA'.) Compute:

- Net insider buying $ in last 90 days: sum(transactionpricepershare * transactionshares) for P minus S

- Insider buy/sell ratio: count of P vs S transactions in 180 days

- CEO/CFO specific buying: filter isofficer = 'Y' and officertitle contains 'Chief'

- Use filingdate (not transactiondate) as your PIT boundary — the transaction may have occurred earlier but you only knew about it on filing

## **7.2 SF3/SF3A Institutional Holdings**

13F filings are quarterly with up to 45 days delay after quarter end. A filing for Q3 (ending Sept 30) may not appear until November 14. Use calendardate as the quarter reference but account for the filing lag when setting your PIT boundary. Useful features from SF3A:

- shrholders: number of institutional shareholders — breadth of ownership

- shrunits: total institutional shares held — concentration signal

- putunits vs cllunits: put/call ratio from institutional options positions — sophisticated hedging signal

- Quarter-over-quarter change in shrunits: accumulation vs distribution signal

# **8. Feature Engineering Strategy: Overcalculate Then Reduce**

## **8.1 The Case for Overcalculation**

Your instinct to compute everything and then reduce is the right approach. Financial features have complex multicollinearity (ROIC, ROE, and ROA are all related; gross margin, EBITDA margin, and net margin move together). Letting a data-driven method find the relevant subspace is more robust than pre-selecting based on intuition.

## **8.2 Recommended Reduction Approaches**

| Method | When to Use and How |
| --- | --- |
| PCA | Use on normalized fundamental ratio groups (e.g., all margin metrics together, all leverage metrics together). Interpret components as latent factors. Works well for reducing within-cluster redundancy. |
| SHAP feature importance | Train a gradient boosted model (XGBoost/LightGBM) on your regime labels, extract SHAP values, drop features with near-zero importance. Most practical for your use case. |
| Correlation clustering | Cluster features by absolute correlation (e.g., hierarchical clustering on the correlation matrix), then keep one representative from each cluster. Simple and interpretable. |
| Variance inflation factor (VIF) | Iteratively drop features with VIF > 10 to remove multicollinearity before feeding to linear models or HMM emission distributions. |
| Mutual information | Use sklearn's mutual_info_classif against your regime labels to score each feature. Captures non-linear relationships that correlation misses. |

| Recommendation: Start with SHAP on a tree model as your primary reduction tool. It gives you both importance and direction of effect, which is useful for the 'why' narrative you want to provide to customers. Follow with correlation clustering to remove near-duplicate features that survived the SHAP cut. |
| --- |

# **9. Backtesting Biases to Guard Against**

## **9.1 The Big Three**

- **Lookahead bias (most dangerous):**

- Using MRQ/MRT/MRY dimensions instead of ARQ/ART/ARY

- Joining on calendardate or reportperiod instead of datekey

- Using DAILY table metrics without confirming they use filed-date fundamentals

- Computing forward returns using prices before D+1 open

- Using FRED data by period end date rather than release date

- **Survivorship bias (second most dangerous):**

- Training only on currently active tickers — the tickers table isdelisted flag is your friend

- Your Sharadar dataset includes delisted companies; confirm they're in your universe construction

- Companies that were acquired at premium prices create positive bias if excluded

- Bankruptcies create negative bias if excluded — include both

- **Selection/universe bias:**

- Liquidity filters that inadvertently select quality: filtering on avg volume > X removes microcaps that went bankrupt, biasing upward

- Market cap filters: use the marketcap from DAILY (PIT) not current marketcap

- The SP500 membership table (SP500) lets you backtest on the actual historical index composition, avoiding the bias of using today's members

## **9.2 Less Obvious Biases**

| Bias | Description and Mitigation |
| --- | --- |
| Earnings announcement timing | Fundamentals become public on datekey (filing date), but the stock often gaps on earnings call date which precedes filing by days. For regime labels based on price reactions, use the 8-K event date from EVENTS (eventcode 22 = 'Results of Operations') as a more precise availability date. |
| Index reconstitution bias | SP500 additions and removals create artificial price patterns. If benchmarking against the index, use historical composition from the SP500 table. |
| Multiple hypothesis testing | Regime models often try many configurations (N states, feature sets) on the same dataset. Use a held-out OOS test set that you touch only once at the end. |
| Short history bias for derived features | Your 10-year NCFO CAGR requires 10 years of annual data. Early in a company's history this feature is unavailable — handle missing values explicitly rather than dropping rows, as missingness itself may be informative (young company). |
| Fiscal year offset | Different companies have different fiscal year ends. Sharadar's calendardate normalizes this but be careful when computing same-quarter comparisons. |
| Price adjustment consistency | closeadj in SEP adjusts for cash dividends and spinoffs. For momentum signals, some practitioners prefer split-adjusted-only (close in SEP) to avoid spurious return signals from dividend ex-dates. Be consistent. |
| Micro-structure inflation | For small caps, bid-ask spread and market impact mean the closeadj price is not achievable. Apply a liquidity filter and consider realistic transaction cost assumptions in any forward return calculation. |
| Point-in-time for beta/vol | beta1y in METRICS is recalculated weekly. For strict PIT use the most recent beta1y where the METRICS date <= D. |

# **10. Pipeline Architecture with DuckDB + Parquet**

## **10.1 Recommended Schema for Pre-Computed Feature Tables**

The separation of concerns that will save you the most time: do the PIT integrity work once into pre-computed feature tables, then run modeling experiments against those clean tables. Never re-solve the join logic in modeling code.

-- Pre-computed PIT feature table schema (conceptual)

CREATE TABLE pit_features AS (

sim_date        DATE,          -- simulation date

ticker          VARCHAR,

-- Universe flags

in_universe     BOOLEAN,

days_listed     INTEGER,

-- Forward event flags

fwd_delisted_30d  BOOLEAN,

fwd_delisted_90d  BOOLEAN,

fwd_acquired_90d  BOOLEAN,

-- Price features

ret_1m, ret_3m, ret_6m, ret_12m  DOUBLE,

vol_20d, vol_60d                  DOUBLE,

vol_ratio                         DOUBLE,   -- vol_20d/vol_60d

volume_ratio_1m                   DOUBLE,

pct_52w_range                     DOUBLE,

-- Fundamental features (PIT from ARQ/ART/ARY)

days_since_filing  INTEGER,

ncfo_r2_10y        DOUBLE,

ncfo_cagr_10y      DOUBLE,

fcf_cagr_5y        DOUBLE,

roic_level         DOUBLE,

roic_slope_3y      DOUBLE,

grossmargin_slope  DOUBLE,

fcf_conversion     DOUBLE,

accrual_ratio      DOUBLE,

-- Macro context (joined from fred_features table)

yield_curve        DOUBLE,   -- DGS10 - DGS2

hy_spread          DOUBLE,

vix                DOUBLE,

nfci               DOUBLE,

-- Relative features (vs sector/market)

rel_strength_sp500_3m  DOUBLE,

rel_pe_vs_sector       DOUBLE,

)

## **10.2 Efficient DuckDB Walk-Forward Strategy**

Rather than iterating day by day in Python (which would be slow), use DuckDB's window functions and ASOF joins to compute the full PIT feature table in batch. The key insight: you only need daily granularity for price features. Fundamental features can be computed quarterly and then forward-filled.

-- DuckDB ASOF join for PIT fundamentals

-- Gets most recent ARQ row available on each sim_date

SELECT

d.date AS sim_date,

d.ticker,

f.datekey AS last_filing_date,

f.ncfo, f.fcf, f.roic, f.grossmargin, ...

FROM sep d

ASOF JOIN sf1 f

ON f.ticker = d.ticker

AND f.dimension = 'ART'

AND f.datekey <= d.date

ORDER BY d.ticker, d.date

| DuckDB ASOF JOIN: DuckDB's ASOF JOIN is purpose-built for this pattern — it finds the most recent row satisfying the inequality condition. This replaces the expensive self-join pattern and runs entirely in-process on your Parquet files without loading into memory. |
| --- |

## **10.3 Computing Your OLS R2 and CAGR Features**

These require window computations over annual history. Best done in Python with pandas after the initial DuckDB extraction, or using DuckDB's list_aggregate and linregr functions:

-- DuckDB linregr for NCFO R2 (requires annual data pivoted to arrays)

-- Easier approach: extract to Python, compute with scipy.stats.linregress

-- on log(ncfo) where ncfo > 0 for the 10 available ARY rows

from scipy import stats

import numpy as np

def ncfo_r2_cagr(ncfo_series):

ncfo = ncfo_series[ncfo_series > 0].dropna()

if len(ncfo) < 5: return np.nan, np.nan

t = np.arange(len(ncfo))

slope, _, r, _, _ = stats.linregress(t, np.log(ncfo))

cagr = np.exp(slope) - 1

return r**2, cagr

# **11. FRED Integration and Macro Features**

## **11.1 Download and Cache**

Download FRED series once and store as Parquet alongside your Sharadar data. Use pandas-datareader or the fredapi library. For PIT accuracy, download using observation_start and observation_end to get full vintage history where available (FRED-MD dataset specifically).

import pandas_datareader as pdr

fred_series = {

'DGS10': 'treasury_10y',

'DGS2': 'treasury_2y',

'BAMLH0A0HYM2': 'hy_spread',

'BAMLC0A0CM': 'ig_spread',

'VIXCLS': 'vix',

'NFCI': 'nfci',

'FEDFUNDS': 'fed_funds',

'CPIAUCSL': 'cpi',

'UNRATE': 'unemployment',

}

for fred_code, name in fred_series.items():

df = pdr.get_data_fred(fred_code, start='1990-01-01')

df.to_parquet(f'macro/{name}.parquet')

## **11.2 Macro Feature Engineering**

Beyond raw series values, derive:

- Yield curve slope: DGS10 - DGS2 (positive = normal, negative = inverted = recession risk)

- Credit spread change: 20-day change in BAMLH0A0HYM2 (widening = risk-off regime)

- VIX regime: VIX level (>20 stress, >30 crisis) and 20-day VIX change

- Financial conditions percentile: NFCI ranked in historical distribution (tighter = higher percentile = more restrictive)

- Real rate: DGS10 minus trailing 12m CPI change (negative real rates = accommodative)

- SPY regime context: 200-day MA cross, 12m return of SPY as market regime input

# **12. Sector Relative Features**

## **12.1 Using TICKERS for Sector Assignment**

TICKERS.sector provides GICS-approximate sector classification. TICKERS.famaindustry provides Fama-French 48 industry classification, which is more granular and commonly used in academic factor research. Use both: sector for broad macro context, famaindustry for peer comparison.

## **12.2 Relative Valuation Features**

For each ticker on each simulation date, compute its valuation metrics relative to its sector peers. This removes the macro valuation level and captures company-specific mispricing:

- pe_vs_sector_median: company PE minus median PE of sector peers (in-universe on date D)

- pb_vs_sector_median: same for price-to-book

- roic_vs_sector: company ROIC minus sector median

- revenue_growth_vs_sector: company revenue CAGR rank within sector

- These relative features tend to be more stable cross-regime than absolute metrics

# **13. Implementation Checklist**

## **Phase 1: Data Infrastructure**

- Verify all Sharadar parquet files load cleanly in DuckDB

- Confirm ACTIONS table covers full history including delisted companies

- Validate SF1 has both AR and MR dimensions; confirm datekey vs calendardate behavior

- Download and cache all FRED series as parquet

- Identify correct ticker symbols for SPY, QQQ, IWM, sector ETFs in your SEP/SFP files

## **Phase 2: Universe and Events Table**

- Build daily universe snapshot table using TICKERS + ACTIONS

- Compute forward event flags (delisted_30d, acquired_90d, etc.) for every ticker-date

- Validate: confirm known bankruptcies (e.g., Lehman, Hertz) appear correctly

## **Phase 3: Feature Table Construction**

- Use DuckDB ASOF JOIN to build PIT fundamental feature table from ARQ/ART/ARY

- Compute price features from SEP using rolling windows

- Compute NCFO R2 and CAGR features in Python; join back to feature table

- Join FRED macro features using forward-fill on date

- Compute sector-relative valuation features

- Add staleness flag for fundamental data

## **Phase 4: Validation**

- Spot-check 10 specific ticker-date pairs against raw data manually

- Verify no future information for 5 known events (earnings surprises, acquisitions)

- Confirm delisted companies appear in history then disappear from universe correctly

- Check feature distributions for obvious outliers suggesting data errors

## **Phase 5: Modeling**

- Start with HMM baseline (hmmlearn) on price + vol features only

- Add fundamental features and measure regime quality improvement

- Add macro features and sector-relative features

- Apply SHAP-based feature reduction

- Walk-forward validation with vectorbt

*This document reflects best practices for PIT backtesting with the Sharadar dataset as of the dataset schema provided. Verify indicator names against your local parquet file schemas before implementation.*

