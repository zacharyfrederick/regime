# Sector rotation backtest update roadmap

This roadmap maps every change from [sector.md](../sector.md) to the notebook [experiments/sector_rotation_backtest.ipynb](../experiments/sector_rotation_backtest.ipynb), and corrects the misleading text at the end of sector.md.

---

## Correction: text at the bottom of sector.md

**The closing paragraph of sector.md (lines 1515–1534) is not correct.**

- It says "If you want, I can show you a very powerful diagnostic notebook…" — implying a separate notebook or future deliverable. **Correct:** All diagnostics are to be implemented in **this** notebook (`sector_rotation_backtest.ipynb`); there is no separate "diagnostic notebook."
- The "forward return vs signal rank" plot is described as "the most revealing" and "reveals if the model is learning nonlinear effects." **Correct:** It is one of several diagnostics to add here (along with IC time series, IC histogram, IC decay, quantile return bars, top-minus-bottom spread). The spec's own "clean final diagnostic pipeline" (sector.md ~1484–1514) lists filtering, `compute_ic`, `compute_quantile_returns`, spread by date/quantile, then "plot: IC time series, IC histogram, quantile return bars, top-bottom cumulative return" — with no separate 40-line notebook. The "forward return vs signal rank" plot can be added as an additional diagnostic in the same notebook.

---

## 1. Data and label fixes (notebook: load cell)

| Spec item | Where | Action |
|-----------|--------|--------|
| **Week definition** | `weekly["week"] = ...` | Use `dt.to_period("W-FRI")` so the "last trading day" is Friday-consistent (sector.md ~614–622). |
| **Forward return horizon** | DuckDB + `raw`/`weekly` | Add `fwd_ret_20d` and optionally 10d/15d for IC decay via extra `LEAD(open, ...)` in SQL. |
| **IC decay forward returns** | If implementing IC decay | Add `fwd_ret_10d`, `fwd_ret_15d`, `fwd_ret_20d`. |

---

## 2. Feature and target fixes (notebook: load/feature cell)

| Spec item | Where | Action |
|-----------|--------|--------|
| **Ranks normalized** | `ret_1w_rank`, `ret_1m_rank` | Use `.rank(pct=True, method="first")` (sector.md ~361–372, ~483–493). |
| **Add ret_3m_rank** | For baselines / features | Compute and use in momentum/combo baselines and FEATURE_COLS. |
| **Reduce collinearity** | `FEATURE_COLS` | Keep relative momentum (vs SPY) + ret_6m/ret_12m; drop raw ret_1w/ret_1m/ret_3m (sector.md ~374–388, ~399–413). |

---

## 3. Rolling retraining fix (notebook: rolling retrain cell)

| Spec item | Where | Action |
|-----------|--------|--------|
| **Embargo in rolling** | `train_dates = ... i - 1` | Change to `i - EMBARGO_WEEKS` so the last training week does not overlap the prediction week (sector.md ~338–356). |

---

## 4. Model and universe checks

| Spec item | Where | Action |
|-----------|--------|--------|
| **Full-universe check before ranking** | Rolling cell | Skip prediction when `pred_df["ticker"].nunique() != len(TICKERS)` (sector.md ~462–472). |
| **XGBoost hyperparameters (optional)** | `XGBRegressor(...)` | Spec suggests e.g. n_estimators=500, max_depth=4, subsample=0.8, colsample_bytree=0.8 (sector.md ~415–427). |

---

## 5. Baselines

| Spec item | Action |
|-----------|--------|
| **Mean reversion** | `naive_pred = -ret_1w`; evaluate with `evaluate_ls`. |
| **Momentum** | `pred = ret_3m`; evaluate (sector.md ~430–432). |
| **Combo** | `pred = ret_3m - ret_1w`; evaluate (sector.md ~433–441, 735–739). |

---

## 6. Diagnostics (in this notebook)

Use `rolling_preds_df` filtered to full universe. Implement:

- **IC:** cross-sectional Spearman(pred, fwd_ret_5td) by date; series + histogram + t-stat.
- **Quantile returns:** n_quantiles=3, pd.qcut(..., duplicates="drop"); mean return by (date, quantile); bar plot.
- **Top-minus-bottom spread:** by date, then cumulative plot.
- **IC decay:** if fwd_ret_10d/15d/20d exist, mean IC per horizon; plot IC vs days.
- **Forward return vs signal rank:** scatter or binned plot (pred rank vs fwd_ret_5td).
- **Sanity filter:** filter to weeks with enough assets; optional clip of pred to (0.01, 0.99) quantiles.

---

## 7. Optional / minor

- Staggered PnL alignment (sector.md ~624–632).
- SPY join: LEFT JOIN and verify alignment (sector.md ~518–525).
- Transaction costs: document or add simple drag (sector.md ~634–642).

---

## Implementation status

The above items have been implemented in `experiments/sector_rotation_backtest.ipynb` per this roadmap. The previous paragraph in sector.md about a separate diagnostic notebook is superseded by this document.
