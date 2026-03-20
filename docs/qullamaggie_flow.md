# Qullamaggie data flow (screener → model → trigger)

This doc maps the trading mental model to [experiments/qullamaggie.py](../experiments/qullamaggie.py).

## End of day T (signal bar)

| Layer | Role | In code |
|-------|------|---------|
| **Screener** | Who is in play (liquidity + momentum) | Optional `--use-screener`: grid from `daily_screener_candidates.parquet` ⨝ `daily_universe` |
| **Setup features** | Pullback, range, vol, SMA distance, etc. | `qm_features_full` → joined on grid; causal through **T** close |
| **Trigger** | Confirm move started (e.g. close > **prior** 20d high) | `breakout_close_10d`, `breakout_close_20d` on panel (prior window **excludes** bar T) |
| **Model** | Rank setup quality for labeled path | `fit_predict_fold`: `score_pred` from `mfe_atr` / `mae_atr` heads |

## Day T+1

| Layer | Role | In code |
|-------|------|---------|
| **Execution** | Enter at next open | `entries`: `entry_open` = next SEP bar open; `fwd_ret_entry_open_H` for PnL |

Labels (MFE/MAE) are defined from **that** next-open entry, not from T’s close.

## What the backtest simulates today

- **Training:** unchanged; model learns setup quality under the existing label definition.
- **Evaluation PnL** (`--eval-ret`, walk-forward only; features/model/screener unchanged):
  - **`horizon`** (default): portfolio mean of `fwd_ret_entry_open_H` (H-day / terminal exit vs next open).
  - **`tp_sl`**: portfolio mean of `fwd_ret_tp_sl_atr` — long path from `entry_open` (T+1 open): each bar, if `low` hits **stop** first (`entry − k_stop × ATR$` on **T**, ATR$ = `atr_14_pct × closeadj` on signal bar) → stop return; else if `high` hits **target** (`entry + k_target × ATR$`) → target return; else after the hold window use the same **label** `exit_price` as the horizon column. **Same daily bar:** if both stop and target could print, the sim assumes **stop is hit first** (conservative for longs). Rebuild panel after changing k’s: `--tp-sl-k-stop`, `--tp-sl-k-target` (panel build only). Panel also stores path tags: `tp_sl_tp_hit_first`, `tp_sl_sl_hit_first`, `tp_sl_neither_hit`, `tp_sl_bars_to_tp`, `tp_sl_bars_to_sl` (rebuild `--force-panel` if missing).
  - **`pred_proxy`**: **diagnostic only** — uses `score_pred` (same as `mfe_pred − ALPHA × |mae_pred|` in code) **as if** it were the per-name return when aggregating the portfolio. Not tradable; checks whether ranking lines up with that synthetic payoff.
- **Selection** (configurable via CLI): walk-forward ranks by **`_rank_score`** from predicted heads (`--rank-score`: default matches `score_pred`; other modes re-weight |MAE| or use RR **without retraining**).
  - **`baseline`:** top fraction (or fixed `--top-k`) by that rank score each date — **no** trigger (legacy).
  - **`policy_a`:** top fraction on **all** names, then require breakout (can yield few/zero names).
  - **`policy_b`:** breakout first, then top fraction **among breakouts only** (recommended default for trigger tests).

Optional **`score_threshold`** variant: among rows passing breakout, select where `score_pred >= threshold` (threshold from `--eval-score-threshold` or IS quantile via `--eval-score-quantile`).

## Trigger definition (causal)

**Invalid:** `close[T] > max(close[T-20:T])` — today’s close is inside the max.

**Valid:** `close[T] > max(close[T-20:T-1])` — implemented as `MAX(closeadj) OVER (ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING)` per ticker.

Daily bars imply you see the breakout **after** T’s close; next-open entry is intentional lag, not a bug.

## Rebuild after feature changes

Panel must be rebuilt (`--force-panel`) after adding breakout columns or the `fwd_ret_tp_sl_atr` column / TP–SL multipliers. Screener parquet unchanged.

## TP/SL diagnostic matrix (candidate vs score vs sim)

With **`--eval-ret tp_sl`** and **`--experiment-matrix`** (baseline policy only), stitched OOS prints four equal-K portfolios:

| Row | Rule |
|-----|------|
| **A_random** | `K` random names per date (seed `--random-seed`) |
| **B_screener** | Top `K` by `n_screen_horizons` then max `sc_ret_rank_pct_*` (needs **`--use-screener`** panel) |
| **C_model_rank** | Top `K` by current `--rank-score` of `mfe_pred` / `mae_pred` |
| **D_oracle_tp_sl** | Top `K` by realized **`fwd_ret_tp_sl_atr`** (upper bound for this sim only) |

Use **`--experiment-top-k`** (default 1) for K. Interpretation: D bad → universe thin; D good & C bad → score; B ≈ C → model adds little; C ≫ B → model helps.

**Rank-score sweep:** `--rank-score-sweep` prints stitched Sharpe for `default`, `m05`, `m10`, `m15`, `rr` under the same selection rules.

**Path vs decile:** With fresh panel, stitched output includes a table: TP-first rate, SL-first rate, neither rate, mean bars, mean `fwd_ret_tp_sl_atr` by cross-sectional decile of `_rank_score`.

## CLI (train / both)

| Flag | Purpose |
|------|---------|
| `--selection-policy` | `baseline` (default), `policy_a`, `policy_b` |
| `--breakout-col` | e.g. `breakout_close_20d`, `breakout_close_10d` |
| `--eval-variant` | `top_frac` (default) or `score_threshold` |
| `--eval-score-threshold` | Fixed cutoff on `score_pred` when using `score_threshold` |
| `--eval-score-quantile` | Per-fold IS quantile for threshold when fixed threshold not set |
| `--top-frac` | Fraction for top-* ranking (default 0.1) |
| `--top-k` | If set, take this many names per date (baseline only); overrides top-frac count |
| `--rank-score` | `default`, `m05`, `m10`, `m15`, `rr` — re-rank OOS without retraining |
| `--rank-score-sweep` | Print all rank modes on stitched OOS |
| `--experiment-matrix` | A/B/C/D baselines (`--eval-ret tp_sl`, baseline policy) |
| `--experiment-top-k`, `--random-seed` | Matrix size and RNG seed |
| `--eval-ret` | `horizon`, `tp_sl`, or `pred_proxy` (see above) |
| `--tp-sl-k-stop`, `--tp-sl-k-target` | ATR multiples for `tp_sl` column at panel build |

Example (Policy B, 20d breakout, top 10% among breakouts):

`python experiments/qullamaggie.py train --selection-policy policy_b`

Example (TP/SL eval, top 1/day, full diagnostic matrix + rank sweep; screener grid):

`python experiments/qullamaggie.py train --use-screener --eval-ret tp_sl --top-k 1 --experiment-matrix --rank-score-sweep`

Stitched summary with `--eval-score-quantile` uses the same quantile on **pooled OOS** scores as a diagnostic (not a true IS pool).

## Walk-forward: few panel dates

`--wf-freq day` defaults to long IS/OOS windows (`DEFAULT_DAILY_FOLD_PERIODS`: 504 + embargo + min OOS trading-day steps). If the panel has only ~hundreds of distinct dates, **`generate_folds` returns 0 folds** and training prints nothing.

**Fix:** the script **auto-switches** to compact daily windows when that happens; you can force them with **`--wf-compact`**. Alternatively use **`--wf-freq month`** or **`week`**, or rebuild the panel with more `(ticker, date)` history (wider `DATE_END` / universe ∩ screener overlap).

A panel with very few rows (e.g. hundreds vs thousands of screener-days) usually means **sparse overlap** between `daily_universe`, screener candidates, and rows that survive label filters — worth validating in DuckDB, not only WF settings.

**Training row floor:** `fit_predict_fold` requires at least **`MIN_IS_TRAIN_ROWS` (30)** in-sample rows with valid targets and features; below **100** you get a **sparse-train warning** (estimates are noisy). The old hard cap of 100 caused all folds to skip on thin panels (~1 row per rebal date).
