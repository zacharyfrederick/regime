# Walk-forward validation monthly — spec review

Comparison of `experiments/walk_forward_validation_monthly.ipynb` to [walkforward.md](../walkforward.md).

---

## Aligned with spec

### Design and constants
- **Same universe, dates, folds, portfolio construction, evaluation**: One `load_fold`, one fold generator, one `evaluate_fold` / `evaluate_equal_weight`, TOP_N=100, fwd_ret_21td. Only the ranking rule (scorer) changes. Matches “What to hold constant.”
- **Single fold runner**: `run_strategy_over_folds(folds, conn, strategy_name, score_fn, ..., use_equal_weight)` with `score_fn(is_df, oos_df) -> (is_scores, oos_scores)`. Matches “best implementation pattern is a single fold runner.”

### Labels and targets
- **Primary target**: Cross-sectional excess return `target_xs = fwd_ret_21td - groupby("date").transform(mean)`. Implemented in `add_target_xs`. Matches “use cross-sectional excess return regression as the primary target.”
- **Secondary target**: Top-quintile (top 20%) via `add_top_bucket_label` (rank-based, top_frac=0.2), used as `label_top` for logistic/XGB classifier. Matches “secondary benchmark target: top-quintile monthly classification.”
- **Fixed >2% label retired**: No LABEL_THRESHOLD; EXCLUDE includes `label_top`, `target_xs`. Matches “retire the fixed > 2% label.”

### Evaluators
- **Screen-only**: Dedicated `evaluate_equal_weight(df)` (equal-weight all names per month), not top-N. Matches “screen-only needs its own equal-weight evaluator.”
- **Top-N**: `evaluate_fold(df, preds, top_n)` returns sharpe, hit_rate, monthly_rets, and **selected_by_date** for turnover. Matches “evaluate_fold” and “selected_by_date for turnover.”
- **Turnover**: `compute_turnover(selected_by_date)` with `1 - |prev ∩ curr| / |prev ∪ curr|` per month. Logged per strategy. Matches “turnover needs to be logged.”

### Common panel
- **Same OOS panel**: For each fold, `eval_oos = oos_df.dropna(subset=["fwd_ret_21td"] + feature_cols_for_panel)`. All rank-based strategies use the same eval_is/eval_oos. Matches “use the same OOS panel for fair comparisons.”
- Screen-only uses the same eval_is/eval_oos (so same row set as rankers). Strict apples-to-apples.

### Composite score
- **build_composite_score**: Cross-sectional z-score per date, weighted sum, **min_count=1** in `sum(axis=1, min_count=1)`, inf/nan handling. Missing features get 0 weight. Matches “Better version” with min_count=1 and missing-feature handling.

### Ladder tiers (order and content)
- **Tier 0**: screen_only (equal weight). Matches “Tier 0: pure universe/screen.”
- **Tier 1**: ret_12m, ret_6m, ncfo_r2_10y, pe_vs_sector, vol_vs_sector. Matches “Tier 1: direct rankers” (spec also lists vol_vs_sector).
- **Tier 2**: momentum_quality_composite (ret_12m, ret_6m, ncfo_r2_10y, ncfo_cagr_5y, pe_vs_sector). Matches “hand-built composite” and “momentum_quality_components” mini-ladder.
- **Tier 3**: ridge_core (Ridge on target_xs), logistic_core (Logistic on label_top). Matches “Tier 3: simple models” (logistic, ridge on 5–8 core features).
- **Tier 4**: xgb_reg_core, xgb_clf_core, xgb_reg_top25. Matches “Tier 4: nonlinear” (XGBoost same features + top-25).

### Models and targets
- **Ridge**: On target_xs with SimpleImputer + StandardScaler + Ridge(alpha=1.0). Matches “Ridge on target_xs.”
- **Logistic**: On label_top (top-quintile), median impute. Matches “train logistic/XGBoost classifier on that as the comparison.”
- **XGB**: Regression on target_xs and classification on label_top; early stopping on last 20% of IS dates. Same feature subset and fold structure. Matches “same fold structure, same validation split logic within IS, same feature subset, same label construction.”

### Metrics logged
- **Per strategy in ladder_df**: OOS Sharpe (mean + full stitched), OOS hit rate, turnover, max drawdown, CAGR. Matches “OOS Sharpe, OOS hit rate, CAGR, max drawdown, turnover.”
- **Per-fold in summary**: is_sharpe, oos_sharpe, is_hit_rate, oos_hit_rate, turnover, avg_universe_size, avg_selected_count. Matches “average monthly universe size, average selected count.”

### Bucket monotonicity
- **quantile_forward_returns(df, score_col, n_quantiles=5)**: Quintiles per month, equal-weight bucket return, mean across months. Run for ret_12m, ncfo_r2_10y, momentum_quality composite on one fold’s OOS. Matches “bucket monotonicity is a key pre-model diagnostic.”

### Baselines (Step 0 / Step 1)
- **Market (SPY)**: SPY monthly returns from SFP (merge_asof to rebalance dates), OOS calendar only; Sharpe, CAGR, max drawdown appended to ladder. Matches “Step 0 — Market baseline: Buy SPY every month.”
- **Universe random**: Random TOP_N names per month from same eval_oos panel, 5 seeds; mean full OOS Sharpe (and std in full_oos_sharpe_std), mean CAGR, mean max_dd. Matches “Step 1 — Universe baseline: randomly pick … from universe, equal weight.”

### Final test and persistence
- Final test: one “fold” (train to embargo, test last FINAL_TEST_MONTHS) with ridge_core; same target_xs and feature_cols_core. Model and encoder persisted for inference. Matches “final test … same label/target and feature set.”
- Current inference cell supports regression (predict) vs classification (predict_proba[:, 1]) via saved model_type.

---

## Gaps / deviations

### 1. Tier 2: only one composite in the ladder
- **Spec**: “Tier 2: hand composites — Momentum composite, Quality composite, Quality + momentum + valuation composite.”
- **Notebook**: Only **momentum_quality_composite** (momentum + quality + valuation via pe_vs_sector). No separate “quality only” or “quality + momentum + valuation” with different weights.
- **Impact**: Minor. The spec’s “mini-ladder” only requires momentum_quality_composite vs ret_12m, ncfo_r2_10y, logistic, xgboost. Adding quality-only and a second composite would make Tier 2 match the letter of the spec.

### 2. avg_universe_size and avg_selected_count not in ladder_df
- **Spec**: “average monthly universe size, average selected count” in “What I would log for each strategy.”
- **Notebook**: Runner collects `avg_universe_size` and `avg_selected_count` in per-fold rows but they are **not** appended to each `ladder_results` entry (only mean_oos_sharpe, full_oos_sharpe, oos_hit_rate, turnover, max_drawdown, cagr).
- **Impact**: You can still inspect them from the per-strategy summary DataFrames returned by `run_strategy_over_folds`; they’re just not in the main comparison table.

### 3. Optional: rank IC and top-minus-bottom spread
- **Spec**: “If possible, also compute: monthly rank IC, top-minus-bottom spread by month.”
- **Notebook**: Not implemented.
- **Impact**: Optional diagnostic; not required for the main conclusion.

### 4. “Raw feature ranker vs model-on-one-feature” distinction
- **Spec**: “Raw feature ranker (score = ret_12m) vs Logistic model with one feature P(label=1 | ret_12m) … distinct baselines.”
- **Notebook**: Only raw feature rankers (Tier 1). No “logistic with single feature” or “ridge with single feature” diagnostic.
- **Impact**: Nice-to-have diagnostic; spec says “I should have included” but the main ladder does not require it.

### 5. Universe baseline: N = 10 vs TOP_N
- **Spec (simplified section)**: “randomly pick **10** stocks from universe.”
- **Notebook**: Uses **TOP_N (100)** for the random universe baseline so it’s comparable to other strategies (same N).
- **Assessment**: Using TOP_N is consistent with “same portfolio construction”; the spec’s “10” was for a simpler illustration. No change needed unless you explicitly want a “random 10” benchmark.

---

## Summary

| Spec requirement | Status |
|------------------|--------|
| Same universe, folds, TOP_N, evaluation | Done |
| Single fold runner, scorer abstraction | Done |
| Primary target: target_xs (cross-sectional excess) | Done |
| Secondary target: label_top (top-quintile) | Done |
| Screen-only: dedicated equal-weight evaluator | Done |
| Common OOS panel (dropna same cols) | Done |
| Turnover logged (selected_by_date, compute_turnover) | Done |
| build_composite_score with min_count=1 | Done |
| Tier 0–4 progression (screen, single-feature, composite, linear, XGB) | Done |
| Ridge on target_xs, Logistic on label_top | Done |
| XGB reg + clf, same features/folds | Done |
| Bucket monotonicity diagnostic | Done |
| Market (SPY) and universe (random) baselines | Done (SPY from SFP) |
| CAGR, max drawdown in ladder | Done |
| Per-fold avg_universe_size, avg_selected_count | Collected but not in ladder_df |
| Tier 2: three composites (momentum, quality, momentum+quality+valuation) | Only one composite in ladder |
| Rank IC / top-minus-bottom spread | Not implemented |
| Logistic/ridge on one feature (diagnostic) | Not implemented |

Overall the notebook matches the main decisions and structure of walkforward.md. Remaining gaps are: (1) adding avg_universe_size and avg_selected_count to the ladder summary table if you want them in the comparison view, (2) optionally adding the two extra Tier 2 composites and the “model-on-one-feature” diagnostic, and (3) optionally adding rank IC and top-minus-bottom spread.
