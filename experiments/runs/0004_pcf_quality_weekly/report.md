# Descriptive backtest report

**Run:** `0004_pcf_quality_weekly`  
**Description:** PCF decile + quality filter (fcf_r2>0.5, fcf_pct>=0.5), VIX 13-35, top 1500, ex-Fin/RE, weekly  
**Date range:** 2009-05-04 00:00:00 to 2024-11-04 00:00:00  
**Buckets:** 10 (Q1 = cheapest, Q10 = most expensive)

---

## 1. Per-bucket summary

CAGR, annualized volatility, Sharpe, Sortino, max drawdown, and win rate (% of months with positive return).

| bucket | CAGR | ann_vol | Sharpe | Sortino | max_dd | win_rate_pct |
| --- | --- | --- | --- | --- | --- | --- |
| Q1 | 6.39% | 8.00% | 0.22 | 0.20 | -34.55% | 20.2% |
| Q2 | 11.06% | 7.86% | 0.35 | 0.34 | -32.62% | 22.2% |
| Q3 | 8.00% | 6.45% | 0.31 | 0.28 | -32.98% | 21.0% |
| Q4 | 9.11% | 6.23% | 0.35 | 0.32 | -27.94% | 22.3% |
| Q5 | 6.39% | 6.24% | 0.26 | 0.24 | -27.18% | 22.3% |
| Q6 | 9.00% | 6.08% | 0.36 | 0.34 | -25.65% | 22.3% |
| Q7 | 7.39% | 6.24% | 0.29 | 0.27 | -28.41% | 22.2% |
| Q8 | 6.31% | 6.49% | 0.25 | 0.23 | -31.74% | 21.2% |
| Q9 | 8.25% | 6.84% | 0.30 | 0.28 | -29.69% | 22.0% |
| Q10 | 6.78% | 8.20% | 0.22 | 0.20 | -39.11% | 22.1% |

---

## 2. Cumulative equity curves (log scale)

Whether outperformance is steady or regime-dependent.

![cumulative_returns_log.png](figures/cumulative_returns_log.png)

---

## 3. Annual returns heatmap

Bucket × year. Are mid-buckets consistently better or lumpy?

![annual_returns_heatmap.png](figures/annual_returns_heatmap.png)

---

## 4. Monthly return distributions

KDE/histogram per bucket. Quality filter compressing the left tail in mid-buckets vs Q1 would show here. Skewness and kurtosis below.

![return_distribution.png](figures/return_distribution.png)

**Skewness & kurtosis (monthly returns):**

| bucket | skewness | kurtosis | n_months |
| --- | --- | --- | --- |
| Q1 | 0.560 | 11.428 | 810 |
| Q2 | 0.282 | 6.054 | 810 |
| Q3 | -0.062 | 6.940 | 810 |
| Q4 | 0.039 | 5.789 | 810 |
| Q5 | 0.009 | 6.005 | 810 |
| Q6 | 0.074 | 5.993 | 810 |
| Q7 | -0.219 | 5.860 | 810 |
| Q8 | -0.230 | 6.467 | 810 |
| Q9 | 0.121 | 7.484 | 810 |
| Q10 | -0.159 | 7.879 | 810 |

---

## 5. Drawdown curves

Peak-to-trough and recovery by bucket.

![drawdown_curves.png](figures/drawdown_curves.png)

---

## 6. Holdings count per bucket over time

Check that no bucket is thin in certain periods (spurious results).

![holdings_count.png](figures/holdings_count.png)

---

## 7. Sector composition per bucket

Stacked area: are certain buckets sector bets in disguise?

![sector_composition.png](figures/sector_composition.png)

---

## 8. Rolling Sharpe (36-month window)

Signal stability over time rather than a single full-period number.

![rolling_sharpe.png](figures/rolling_sharpe.png)

**Rolling Sharpe summary:**

| bucket | rolling_sharpe_mean | rolling_sharpe_min | rolling_sharpe_max | rolling_sharpe_std |
| --- | --- | --- | --- | --- |
| Q1 | 0.259 | -0.905 | 1.602 | 0.504 |
| Q2 | 0.435 | -1.183 | 2.280 | 0.558 |
| Q3 | 0.441 | -1.322 | 1.745 | 0.545 |
| Q4 | 0.487 | -1.255 | 1.593 | 0.548 |
| Q5 | 0.405 | -1.198 | 1.954 | 0.590 |
| Q6 | 0.468 | -1.094 | 1.998 | 0.541 |
| Q7 | 0.422 | -1.076 | 2.049 | 0.541 |
| Q8 | 0.359 | -0.955 | 1.759 | 0.517 |
| Q9 | 0.387 | -0.912 | 1.592 | 0.548 |
| Q10 | 0.293 | -0.938 | 1.923 | 0.557 |

---

## 9. Split-sample (all deciles × multiple periods)

Sharpe by decile in each of 4 equal-length sub-periods over the full data range.

![split_sample_heatmap.png](figures/split_sample_heatmap.png)

**Sharpe by decile and period (markdown table):**

| decile | 2009-05 to 2013-03 | 2013-03 to 2017-01 | 2017-01 to 2020-12 | 2020-12 to 2024-11 |
| --- | --- | --- | --- | --- |
| Q1 | -0.004 | 0.369 | -0.059 | 0.545 |
| Q2 | 0.219 | 0.480 | 0.053 | 0.730 |
| Q3 | 0.286 | 0.491 | 0.036 | 0.471 |
| Q4 | 0.258 | 0.668 | -0.012 | 0.616 |
| Q5 | 0.220 | 0.375 | 0.039 | 0.433 |
| Q6 | 0.297 | 0.631 | 0.119 | 0.469 |
| Q7 | 0.202 | 0.496 | 0.230 | 0.365 |
| Q8 | 0.191 | 0.353 | 0.161 | 0.356 |
| Q9 | 0.109 | 0.465 | 0.305 | 0.439 |
| Q10 | 0.089 | 0.203 | 0.351 | 0.306 |

**Plain text (Sharpe by decile × period):**

```
        2009-05 to 2013-03  2013-03 to 2017-01  2017-01 to 2020-12  2020-12 to 2024-11
decile                                                                                
Q1                  -0.004               0.369              -0.059               0.545
Q2                   0.219               0.480               0.053               0.730
Q3                   0.286               0.491               0.036               0.471
Q4                   0.258               0.668              -0.012               0.616
Q5                   0.220               0.375               0.039               0.433
Q6                   0.297               0.631               0.119               0.469
Q7                   0.202               0.496               0.230               0.365
Q8                   0.191               0.353               0.161               0.356
Q9                   0.109               0.465               0.305               0.439
Q10                  0.089               0.203               0.351               0.306
```
