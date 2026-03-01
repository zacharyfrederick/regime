# Validation Report

Generated: 2026-02-28 19:53:11
Master: /Users/zacharyfrederick/regime/outputs/master/master_features.parquet
Date range: 2000-01-01 to 2024-12-31

---

## Output

```

============================================================
  1. PIT Integrity
============================================================
NULL datekey rate: 0.0%
PIT datekey check: PASS (no datekey > date)

days_since_filing distribution (p25/p50/p75/p99, pct_stale_1y):
    p25   p50   p75    p99  pct_stale_1y      n
0  21.0  44.0  69.0  116.0           0.0  50236

============================================================
  2. Survivorship Bias
============================================================
Delisted LEH: ticker not in universe (skip).
Delisted HTZ (2020-05-22): skip (HTZ in dataset is post-bankruptcy only; no rows in delist window).
  DIAGNOSTIC D: HTZ (bankruptcy 2020-05-22) — labels from master:
  HTZ universe date range (diagnostic: window empty): min=2021-11-09 00:00:00 max=2024-12-31 00:00:00 total rows=790

fwd_delisted_63td True: 0.6% (expect ~2-5%)

Ticker date ranges: 11 tickers

============================================================
  3. Duplicate Rows
============================================================
Duplicate (ticker, date) rows: 0
Duplicate check: PASS

============================================================
  4. Distribution Sanity
============================================================
Master shape:     rows  tickers  dates
0  50236       11   6289
pe_pit: 0 values outside [0, 500] (0.0%)
pb_pit: 358 values outside [0, 50] (0.7%)
pcf_pit: 76 values outside [0, 200] (0.2%)
evebitda_pit: 64 values outside [0, 100] (0.1%)

ret_12m distribution:
        min       p01       p25       p50       p75       p99       max
0 -0.904297 -0.711624 -0.066783  0.103795  0.314276  1.327905  2.946269

vol_20d distribution:
        min       p01       p50       p99       max  high_vol
0  0.004139  0.055533  0.234375  1.213815  3.083856       0.0

ncfo_r2_adjusted distribution:
        min      mean       max
0  0.000002  0.459785  0.983061

============================================================
  5. Sector Relative Sanity
============================================================
Sector relative (median for valuation, mean for diff):
   pe_vs_sector  pb_vs_sector  pcf_vs_sector  roic_vs_sector  ret_3m_vs_sector
0           NaN           NaN            NaN             NaN               NaN
  (expect pe/pb/pcf median ~1.0, roic/ret_3m mean ~0.0)

============================================================
  6. Macro Features Temporal Sanity
============================================================
macro_features.parquet not found; skip.

============================================================
  7. Insider Signal Sanity
============================================================
Insider buy activity: 13.7%  OK
Insider buy rate by scalemarketcap:
   scalemarketcap   buy_pct   rows
0               1  0.137431  50236
scalemarketcap distinct values: 1 (expect ≤6 categorical tiers)

============================================================
  8. Cross-Feature Consistency
============================================================
pcf_pit vs pe_pit correlation: 0.309 (expect 0.3-0.7)
pe_pit vs DAILY.pe (global) correlation: 0.977, median ratio: 0.823 (informational)
pe_pit vs DAILY.pe (AAPL 2020-2022): correlation=0.999, median ratio=0.984 (PASS if corr>=0.95 and ratio in [0.9,1.1])
  DIAGNOSTIC B: pe_pit vs DAILY.pe (AAPL 2020-2022):
         date     pe_pit  daily_pe     ratio
0  2020-01-02  24.204350      24.2  1.000180
1  2020-01-03  23.969012      23.9  1.002888
2  2020-01-06  24.160224      24.1  1.002499
3  2020-01-07  24.046567      24.0  1.001940
4  2020-01-08  24.433336      24.4  1.001366
5  2020-01-09  24.952149      24.9  1.002094
6  2020-01-10  25.008643      25.0  1.000346
7  2020-01-13  25.543167      25.5  1.001693
8  2020-01-14  25.198184      25.1  1.003912
9  2020-01-15  25.090209      25.0  1.003608
10 2020-01-16  25.404438      25.3  1.004128
11 2020-01-17  25.685573      25.6  1.003343
12 2020-01-21  25.511744      25.5  1.000461
13 2020-01-22  25.602670      25.5  1.004026
14 2020-01-23  25.726022      25.7  1.001013
15 2020-01-24  25.651810      25.6  1.002024
16 2020-01-27  24.897660      24.8  1.003938
17 2020-01-28  25.602002      25.5  1.004000
18 2020-01-29  24.003475      24.7  0.971801
19 2020-01-30  23.968478      24.6  0.974328
20 2020-01-31  22.905989      23.5  0.974723
21 2020-02-03  22.843056      23.5  0.972045
22 2020-02-04  23.597022      24.3  0.971071
23 2020-02-05  23.789504      24.4  0.974980
24 2020-02-06  24.067636      24.7  0.974398
25 2020-02-07  23.741306      24.3  0.977008
26 2020-02-10  23.854278      24.5  0.973644
27 2020-02-11  23.710301      24.3  0.975733
28 2020-02-12  24.273318      24.9  0.974832
29 2020-02-13  24.100484      24.7  0.975728

============================================================
  9. Temporal Consistency
============================================================
AAPL vol_20d spikes >200% overnight: 0

AAPL pcf_pit jumps >50% overnight: 6
        date    pcf_pit  days_since_filing
0 2000-09-29   7.930096                 60
1 2001-02-12  12.659201                  0
2 2001-08-13  82.144415                  0
3 2001-12-21  32.954667                  0
4 2002-12-19  47.580926                  0
5 2006-05-05  36.698237                  0
# (Can be acceptable when days_since_filing=0: new filing date.)

============================================================
  10. Null Rate Audit
============================================================
Null rates (key columns):
                     0
ret_1m_null   0.001254
pcf_null      0.093021
macro_null    1.000000
quality_null  0.184031
inst_null     0.555598
pe_null       0.073951
vol_null      0.000119
ret_12m_null  0.027311

Features with >80% null (investigate): ['macro_null']

============================================================
  DIAGNOSTIC A: Quality Metrics in fundamental_pit
============================================================
   total_rows  ncfo_r2_non_null  roic_non_null  gm_slope_non_null  dilution_non_null
0       50236           40991.0            0.0            49018.0            48619.0

============================================================
  11. Null Rate by Year
============================================================
Null rates by year (inst, quality, pe, yield_curve):
    year  inst_null  quality_null   pe_null  yc_null
0   2000   1.000000      0.644345  0.107639      1.0
1   2001   1.000000      0.500000  0.100806      1.0
2   2002   1.000000      0.484127  0.263393      1.0
3   2003   1.000000      0.334821  0.242560      1.0
4   2004   1.000000      0.250000  0.024306      1.0
5   2005   1.000000      0.250000  0.000000      1.0
6   2006   1.000000      0.231574  0.000000      1.0
7   2007   1.000000      0.125000  0.000000      1.0
8   2008   1.000000      0.125000  0.105731      1.0
9   2009   1.000000      0.125000  0.233631      1.0
10  2010   1.000000      0.125000  0.140873      1.0
11  2011   1.000000      0.170273  0.091251      1.0
12  2012   1.000000      0.206222  0.016000      1.0
13  2013   0.615079      0.111111  0.094797      1.0
14  2014   0.000000      0.111111  0.015873      1.0
15  2015   0.000000      0.093474  0.000000      1.0
16  2016   0.000000      0.000000  0.000000      1.0
17  2017   0.000000      0.000000  0.000000      1.0
18  2018   0.000000      0.000000  0.000000      1.0
19  2019   0.000000      0.000000  0.000000      1.0
20  2020   0.000000      0.000000  0.000000      1.0
21  2021   0.002592      0.182113  0.322748      1.0
22  2022   0.000000      0.283676  0.099886      1.0
23  2023   0.000000      0.166667  0.000000      1.0
24  2024   0.000000      0.166667  0.070106      1.0
# inst_shrunits ~100% null before 2013; ncfo_r2 high null before 2003; yield_curve 0% null

============================================================
  12. Universe Composition Over Time
============================================================
Universe size by year:
    year  tickers
0   2000        8
1   2001        8
2   2002        8
3   2003        8
4   2004        8
5   2005        8
6   2006        8
7   2007        8
8   2008        8
9   2009        8
10  2010        8
11  2011        9
12  2012        9
13  2013        9
14  2014        9
15  2015        9
16  2016        9
17  2017        9
18  2018        9
19  2019        8
20  2020        8
21  2021        8
22  2022        7
23  2023        6
24  2024        6
# Should grow from ~3000 in 2000 to ~5000+ in 2020s; big drops = bug; flat = wrong filter

============================================================
  13. Restatement Coverage (Raw SF1)
============================================================
GE: 2 periods with multiple filings
      datekey reportperiod          ncfo       revenue     netinccmn
0  2006-10-31   2006-09-30  9.491000e+09  4.085600e+10  4.964000e+09
1  2007-01-19   2006-09-30  9.491000e+09  4.069300e+10  4.867000e+09
      datekey reportperiod          ncfo       revenue     netinccmn
0  2016-11-02   2016-09-30  1.040000e+09  2.926600e+10  1.994000e+09
1  2016-11-09   2016-09-30  1.040000e+09  2.926600e+10  1.994000e+09
UAA: 1 periods with multiple filings
      datekey reportperiod         ncfo      revenue   netinccmn
0  2013-02-25   2012-12-31  205773000.0  505863000.0  50132000.0
1  2013-02-26   2012-12-31  205773000.0  505863000.0  50132000.0
NFLX: 2 periods with multiple filings
      datekey reportperiod         ncfo       revenue    netinccmn
0  2018-01-29   2017-12-31 -487957000.0  3.285755e+09  185517000.0
1  2018-02-05   2017-12-31 -487957000.0  3.285755e+09  185517000.0
      datekey reportperiod        ncfo      revenue   netinccmn
0  2011-10-27   2011-09-30  49531000.0  821839000.0  62460000.0
1  2011-11-07   2011-09-30  49531000.0  821839000.0  62460000.0
TSLA: 1 periods with multiple filings
      datekey reportperiod        ncfo     revenue   netinccmn
0  2011-05-13   2011-03-31 -43297000.0  49030000.0 -48941000.0
1  2011-06-02   2011-03-31 -43297000.0  49030000.0 -48941000.0

============================================================
  SUMMARY
============================================================
  ✓ PIT integrity: PASS
  ✓ HTZ survivorship: PASS
  ✓ Quality metrics present: PASS
  ✓ pe_pit vs DAILY.pe: PASS
  ? Yield curve 2019: UNKNOWN
  ✓ scalemarketcap tiers: PASS
  ✓ Insider buy activity: PASS
  ✓ Duplicate rows: PASS
  ✗ Macro null rate: FAIL

Total: 7/9 PASS
Failures: Macro null rate
Unknown (section skipped): Yield curve 2019

```
