# Label upgrade roadmap

Concise implementation guide for the calendar-anchored forward labels upgrade. Full design, code snippets, and Q&A are in [label_upgrade.md](../label_upgrade.md).

---

## Summary

Current labels use **naive trading-day offsets** (e.g. rn + 21 for “monthly”), which causes entry/exit drift (e.g. a Wednesday entry sells the following Wednesday, not month-end). There is **no T-1 feature lag** (features are observable after close, so the earliest trade is next day’s close), and labels are computed for **every (ticker, date)** in the grid even though rebalancing happens only on fixed period starts. The upgrade moves to **calendar-anchored periods** (weekly/monthly/quarterly/annual), **rebalance-only entry dates** per horizon, and a **feature_date** column set to T-1 so downstream can join features correctly.

---

## Decisions (locked)

| Decision | Choice |
|----------|--------|
| Horizons | **5, 21, 63, 252** only — drop 10td and 126td |
| Output shape | **Wide sparse (Option A)** — one parquet, NULL where a (ticker, date) is not a rebalance entry for that horizon |
| New columns | **fwd_exit_date_{N}td**, **fwd_feature_date_{N}td** per horizon (N = 5, 21, 63, 252) |
| Terminal logic | **Unchanged** — existing terminal event resolution and delist handling stay as-is |
| fwd_holding_days | **Actual length** = (exit_rn − entry_rn), not hardcoded N |

---

## Schema change

| Aspect | Before | After |
|--------|--------|--------|
| Exit date | rn + N (drifts with entry) | Calendar period end (last trading day of week/month/quarter/year) |
| Entry dates | Every grid date | Only rebalance entry dates per horizon (e.g. first trading day of month for monthly) |
| Feature ref | Same as grid date | **feature_date** = T-1 (trading day before entry_date) |
| Horizons | 5, 10, 21, 63, 126, 252 | 5, 21, 63, 252 |
| New columns | — | **fwd_exit_date_{N}td**, **fwd_feature_date_{N}td** per horizon |

**Per-horizon column set (4 horizons × 6 columns):**  
`fwd_ret_{N}td`, `fwd_holding_days_{N}td`, `fwd_delisted_{N}td`, `fwd_delist_type_{N}td`, `fwd_exit_date_{N}td`, `fwd_feature_date_{N}td` for N ∈ {5, 21, 63, 252}.

---

## Period keys (no mixing)

Calendar periods use **distinct key sets**; do not mix ISO week with calendar year/month/quarter:

| Horizon | Rebalance table | Period key | Notes |
|---------|-----------------|------------|--------|
| Weekly (5td) | rebalance_weekly | **(isoyear, iso_week)** | ISO week only; do not use (year, month) for weekly |
| Monthly (21td) | rebalance_monthly | **(year, month)** | Calendar month |
| Quarterly (63td) | rebalance_quarterly | **(year, quarter)** | Calendar quarter |
| Annual (252td) | rebalance_annual | **(year)** | Calendar year |

**Cursor / implementation:** Weekly uses `iso_year` and `iso_week` from the trading calendar; monthly uses `year` and `month`; quarterly uses `year` and `quarter`; annual uses `year` only. No mixing of ISO-week with year/month/quarter in the same table.

---

## 07_labels.py roadmap

- After **sep_ranked** is created, build **trading_calendar** from distinct dates in SEP: columns **date**, **td_index**, **iso_year**, **iso_week** (for weekly only), **year**, **month**, **quarter** (for monthly/quarterly/annual). Weekly period boundaries use (isoyear, iso_week); all other horizons use (year, month), (year, quarter), or (year) only.
- Build **rebalance_weekly** with `GROUP BY iso_year, iso_week`; **rebalance_monthly** with `GROUP BY year, month`; **rebalance_quarterly** with `GROUP BY year, quarter`; **rebalance_annual** with `GROUP BY year`. Each has entry_date = first trading day of period, exit_date = last trading day of period. No mixing of period keys across tables.
- Replace **HORIZONS_TD** with **HORIZON_CONFIG** mapping N → rebalance table: 5 → rebalance_weekly, 21 → rebalance_monthly, 63 → rebalance_quarterly, 252 → rebalance_annual.
- In the horizon loop: filter grid with **INNER JOIN** rebal_table **rb ON g.date = rb.entry_date**; get exit price via **LEFT JOIN sep_ranked f ON f.ticker = g.ticker AND f.date = rb.exit_date** (not rn + N). Keep terminal_row / last_day / labels_N assembly unchanged.
- Compute **fwd_holding_days** as (exit_rn − entry_rn); add **fwd_exit_date** (= rb.exit_date) and **fwd_feature_date** (T-1 via trading_calendar: td_index − 1 relative to entry_date).
- Final wide join: 4 horizons × 6 columns; write single **forward_labels.parquet** (sparse wide). Rows are still (ticker, date); date is entry date; columns for horizons where this date is not a rebalance entry are NULL.

---

## 08_merge.py roadmap

- **Drop** all 10td and 126td columns from the master SELECT, from the empty-schema fallback, and from any validation that references horizon columns.
- **Add** **fwd_exit_date_{N}td** and **fwd_feature_date_{N}td** (DATE) for N in (5, 21, 63, 252) to the SELECT and to the empty forward_labels schema.
- **Add** a validation/log line: report **non-null rate per horizon** (e.g. percentage of rows with non-NULL fwd_ret_5td, fwd_ret_21td, fwd_ret_63td, fwd_ret_252td) so that sparse NULLs are expected and not mistaken for bugs.

---

## Downstream / join

Join key remains **(ticker, date)**; date is the **entry date** (rebalance date). For a given horizon, experiments can join features with **features.date = labels.fwd_feature_date_{N}td** when using that horizon, or continue joining on **date** and **guard against NULL labels** (filter to rows where the chosen horizon’s fwd_ret is NOT NULL).

---

## Reference

Full design rationale, SQL/code examples, and Q&A: **[label_upgrade.md](../label_upgrade.md)**.
