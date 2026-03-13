# Descriptive backtest report

**Run:** `pe_weekly`  
**Description:** Dreman-style P/CF quintiles only, top 1500, ex-Fin/RE, weekly rebalance  
**Date range:** 2010-01-04 00:00:00 to 2024-12-30 00:00:00  
**Buckets:** 10 (Q1 = cheapest, Q10 = most expensive)

---

## 1. Per-bucket summary

CAGR, annualized volatility, Sharpe, Sortino, max drawdown, and win rate (% of months with positive return).

| bucket | CAGR | ann_vol | Sharpe | Sortino | max_dd | win_rate_pct |
| --- | --- | --- | --- | --- | --- | --- |
| Q1 | 9.70% | 7.15% | 0.33 | 0.35 | -28.40% | 32.1% |
| Q2 | 7.84% | 6.58% | 0.30 | 0.29 | -29.67% | 33.0% |
| Q3 | 6.72% | 6.27% | 0.27 | 0.28 | -29.75% | 31.5% |
| Q4 | 8.68% | 6.34% | 0.33 | 0.34 | -26.42% | 32.6% |
| Q5 | 7.56% | 6.21% | 0.30 | 0.30 | -26.49% | 32.2% |
| Q6 | 7.93% | 6.27% | 0.31 | 0.31 | -25.02% | 33.1% |
| Q7 | 7.69% | 6.19% | 0.31 | 0.31 | -26.97% | 32.6% |
| Q8 | 7.36% | 6.41% | 0.29 | 0.29 | -28.18% | 32.7% |
| Q9 | 8.91% | 6.88% | 0.32 | 0.33 | -30.05% | 31.8% |
| Q10 | 10.00% | 7.44% | 0.33 | 0.34 | -29.25% | 32.7% |

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
| Q1 | -0.373 | 4.537 | 783 |
| Q2 | -0.612 | 4.999 | 783 |
| Q3 | -0.506 | 4.361 | 783 |
| Q4 | -0.417 | 4.195 | 783 |
| Q5 | -0.635 | 4.482 | 783 |
| Q6 | -0.405 | 4.720 | 783 |
| Q7 | -0.586 | 4.775 | 783 |
| Q8 | -0.620 | 4.130 | 783 |
| Q9 | -0.319 | 4.130 | 783 |
| Q10 | -0.321 | 4.740 | 783 |

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
| Q1 | 0.494 | -0.985 | 2.085 | 0.620 |
| Q2 | 0.414 | -0.903 | 2.108 | 0.613 |
| Q3 | 0.424 | -1.052 | 2.256 | 0.633 |
| Q4 | 0.472 | -1.001 | 1.871 | 0.577 |
| Q5 | 0.434 | -1.109 | 1.878 | 0.593 |
| Q6 | 0.453 | -0.972 | 2.132 | 0.599 |
| Q7 | 0.426 | -1.010 | 2.180 | 0.586 |
| Q8 | 0.412 | -0.840 | 2.021 | 0.565 |
| Q9 | 0.422 | -0.857 | 1.984 | 0.568 |
| Q10 | 0.442 | -0.769 | 1.977 | 0.515 |

---

## 9. Split-sample (all deciles × multiple periods)

Sharpe by decile in each of 4 equal-length sub-periods over the full data range.

![split_sample_heatmap.png](figures/split_sample_heatmap.png)

**Sharpe by decile and period (markdown table):**

| decile | 2010-01 to 2013-09 | 2013-09 to 2017-06 | 2017-06 to 2021-03 | 2021-03 to 2024-12 |
| --- | --- | --- | --- | --- |
| Q1 | 0.154 | 0.716 | 0.180 | 0.446 |
| Q2 | 0.220 | 0.716 | 0.084 | 0.318 |
| Q3 | 0.153 | 0.657 | 0.027 | 0.355 |
| Q4 | 0.159 | 0.632 | 0.155 | 0.505 |
| Q5 | 0.214 | 0.673 | 0.122 | 0.335 |
| Q6 | 0.238 | 0.805 | 0.124 | 0.267 |
| Q7 | 0.177 | 0.641 | 0.110 | 0.425 |
| Q8 | 0.133 | 0.681 | 0.129 | 0.354 |
| Q9 | 0.118 | 0.626 | 0.302 | 0.388 |
| Q10 | 0.209 | 0.689 | 0.263 | 0.309 |

**Plain text (Sharpe by decile × period):**

```
        2010-01 to 2013-09  2013-09 to 2017-06  2017-06 to 2021-03  2021-03 to 2024-12
decile                                                                                
Q1                   0.154               0.716               0.180               0.446
Q2                   0.220               0.716               0.084               0.318
Q3                   0.153               0.657               0.027               0.355
Q4                   0.159               0.632               0.155               0.505
Q5                   0.214               0.673               0.122               0.335
Q6                   0.238               0.805               0.124               0.267
Q7                   0.177               0.641               0.110               0.425
Q8                   0.133               0.681               0.129               0.354
Q9                   0.118               0.626               0.302               0.388
Q10                  0.209               0.689               0.263               0.309
```
