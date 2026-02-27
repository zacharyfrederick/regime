# DEBUG run validation
Generated: 2026-02-27 13:34:33
Date range: 2000-01-01 to 2024-12-31
DEBUG_TICKERS: ('AAPL', 'MSFT', 'JPM', 'XOM', 'JNJ', 'HTZ', 'TWX', 'MON', 'TIF', 'DNKN', 'ETFC')


============================================================
  1. Contract tests
============================================================

  master: EXISTS
  universe: EXISTS
  fp: EXISTS
  macro: EXISTS
  price: EXISTS
  sector_relative: EXISTS
  insider: EXISTS
  Master schema: required columns present
  Universe schema: required columns present
  Duplicate (ticker, date): 0 PASS
  Date range: [2000-01-03, 2024-12-31] OK
  Master row count: 50236
  Universe (in_universe=True) rows: 50236
  Cross-join row count: PASS (50236)
  Cross-join EXCEPT: PASS (0 both directions)

============================================================
  2. Known-output spot checks
============================================================

  AAPL ret_12m null for first 252 rows: FAIL (expected all null, got 77/252 null)
  AAPL ret_12m on 2021-06-15: PASS (expected ~0.522129, got 0.522129)
  AAPL pe_pit on 2020-12-31: PASS (expected ~39.01, got 39.01)
  JPM sector: PASS (Financial Services)
  XOM famaindustry: PASS (Petroleum and Natural Gas)
  Macro yield curve Aug-Oct 2019: PASS (min=-0.040)
  Macro VIX Mar 2020 spike: PASS (57.7 > 2*15.4)
  Macro CPI 2022 elevated: PASS (10.8 > 2*3.0)

============================================================
  3. Sanity tests
============================================================

  PIT integrity: PASS (no datekey > date)
  pe_pit outside [0,500]: 0/46521 (0.0%)
  pb_pit outside [0,50]: 358/48526 (0.7%)
  pcf_pit outside [0,200]: 76/45563 (0.2%)
  evebitda_pit outside [0,100]: 64/47196 (0.1%)
  vol_20d median: PASS (0.234)
  atr_14d_normalized median: WARN (0.221, expect < 0.10)
  days_since_filing (non-null): median=44, max=237 PASS
  ncfo_r2_adjusted null rate: PASS (18.4%)
  Macro vs universe date count: PASS (6289)
  Macro date EXCEPT: PASS (0 both directions)
  Macro weekend dates: PASS (0)

============================================================
  4. Coverage matrix (non-null % by ticker)
============================================================

  ticker     ret_12m      pe_pit     pcf_pitncfo_r2_adjusted     vol_20d yield_curvedays_since_filing
  AAPL           98.8%       97.0%      100.0%       97.7%      100.0%      100.0%      100.0%
  DNKN           89.3%       93.8%       93.8%       54.4%       99.9%      100.0%      100.0%
  ETFC           98.5%       68.5%       90.1%       67.0%      100.0%      100.0%      100.0%
  HTZ            68.1%       71.9%       82.9%        0.0%       99.7%      100.0%      100.0%
  JNJ            98.8%      100.0%       92.2%       99.0%      100.0%      100.0%      100.0%
  JPM            98.8%      100.0%       54.0%       48.6%      100.0%      100.0%      100.0%
  MON            48.6%       37.8%        0.0%        0.0%       99.6%      100.0%      100.0%
  MSFT           98.8%      100.0%      100.0%       99.6%      100.0%      100.0%      100.0%
  TIF            98.5%      100.0%      100.0%       82.5%      100.0%      100.0%      100.0%
  TWX            98.3%       79.3%      100.0%       84.5%      100.0%      100.0%      100.0%
  XOM            98.8%       96.0%      100.0%       99.1%      100.0%      100.0%      100.0%
  FLAG: pcf_pit 0% for ['MON'] (others >50%)
  FLAG: ncfo_r2_adjusted 0% for ['HTZ', 'MON'] (others >50%)

============================================================
  5. Forward label sanity
============================================================

  AAPL fwd_delisted: PASS (all 0)
  MSFT fwd_delisted: PASS (all 0)
  HTZ after 2020-05-22: skip (HTZ in dataset is post-bankruptcy only; no rows in delist window)
  HTZ in_universe after delist: skip (post-bankruptcy only)

============================================================
  6. Temporal continuity
============================================================

  AAPL ret_12m null gaps: PASS (0)
  pe_pit vs DAILY.pe (AAPL 2020-2022): correlation=0.999, median ratio=0.984

============================================================
  SUMMARY
============================================================

Critical failures: coverage_matrix
