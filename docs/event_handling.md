# Event handling: assumptions and event-study learnings

This document is the single source of truth for how terminal corporate events (delistings, acquisitions, bankruptcies) are handled in the pipeline so that **returns reflect actual economic outcomes** and labels stay aligned with trading-day horizons. It summarizes findings from the delist event study (see `events.md` and `pipeline/000_event_study.py`) and the design decisions for `01_universe.py` and `09_labels.py`.

---

## Event-study findings

- **Acquisitions:** 0-day gap between last SEP price and event date. Last price reflects deal price (~7,400 distinct tickers). No price adjustment needed; return series is already correct. Final day often has volume = 1 or a spike (last cross).
- **Bankruptcies:** 0-day gap. Last SEP price captures where the stock actually traded at termination (~3,300 distinct tickers). Median 252-day return before delist ~-85.9%. Do not blindly append closeadj = 0 — some cases (e.g. structured wind-down) trade at non-zero until the end.
- **Regulatory/voluntary delists:** 0-day gap, clean data.
- **mergerfrom:** Date semantics are broken for terminal event detection (ticker recycling: ACTIONS date is original merger date but ticker may have continued under a new name). **Do not use mergerfrom for removal or for fwd_delisted / terminal-price logic.** Use `delisted` paired with `acquisitionby` (or other specific action) instead. `mergerfrom` is included in the resolved terminal event table only for **reporting and audit**; it is excluded from fwd_delisted flag logic and from forward return terminal price adjustments.
- **Ticker renames:** Non-issue. Sharadar retroactively updates SEP and SF1 to use the current ticker. Permaticker exists but is not needed for internal pipeline joins. SEP/SF1 ticker mismatches (e.g. many rows in SEP with no SF1) are mostly secondary share classes and ADRs, not rename artifacts.
- **ACTIONS semantics:** The `delisted` action is a **companion row** — every acquisition, bankruptcy, regulatory/voluntary delist has both `delisted` and a specific action (`acquisitionby`, `bankruptcyliquidation`, etc.) on the same date. Build one row per event by joining `delisted` to companion rows; do not use `delisted` alone for counting or for inferring type. ~7,000 delisted tickers have only `delisted` with no companion → treat as `unknown` or exclude via rename check (tickerchangefrom within ±5 days).

---

## Assumptions

- **Terminal price:** Use **last SEP closeadj** for all event types (acquisition, bankruptcy, regulatory, voluntary). No synthetic $0 row. The event study showed gap = 0 for all event types except the broken mergerfrom; last traded price is the defensible, auditable exit price. (Some institutional datasets use bankruptcy recovery estimates; we document this as a possible future revisit.)
- **Forward labels:** Use **trading-day** horizons (e.g. 21td, 63td, 126td, 252td) and keep labels in a **separate** artifact (`outputs/labels/forward_labels.parquet`). Master contains features only; labels are joined at modeling time to avoid leakage.
- **Renames:** Exclude from terminal events when `tickerchangefrom` exists for that ticker within ±5 days of the `delisted` date — treat as rename, not a terminal delist.
- **mergerfrom:** Included as a possible `delist_type` in the resolved table for reporting/audit. Excluded from (a) fwd_delisted flag logic and (b) forward return terminal price adjustments.

---

## Caveats and limitations

- **HTZ-style survivorship:** Companies that go bankrupt may stop filing (e.g. ARQ) before the delist date. They can drop out of the universe before the terminal event. Document as a known limitation; run the bankruptcy-coverage query (see below) to quantify how many bankruptcies are missing from the universe in the 90 days before the event.
- **ACTIONS.value:** NaN on generic `delisted` rows; populated on companion rows. Do not rely on `value` from the `delisted` row.
- **Terminal day volume:** Can be 1 or very small (administrative print). Do **not** filter on volume when identifying the terminal price; use last SEP closeadj regardless of volume.
- **contraticker:** Often blank for acquisitions (e.g. DNKN). Use `IS NOT NULL AND TRIM(contraticker) != ''` if needed; contraticker is informational only, not required for return calculation.
- **Recycled tickers:** Some symbols (e.g. MON) are reused; the current MON in data may be a different entity than the original. Label generation is ticker-agnostic; the universe filter determines which labels matter for modeling.

---

## Bankruptcy coverage query

To verify how many bankruptcies are missing from the universe in the 90 days before the event:

```sql
WITH bankruptcies AS (
    SELECT ticker, CAST(date AS DATE) AS event_date
    FROM actions
    WHERE action = 'bankruptcyliquidation'
),
universe_coverage AS (
    SELECT b.ticker, b.event_date,
           COUNT(u.date) AS universe_days_in_window,
           MIN(CAST(u.date AS DATE)) AS first_universe_date,
           MAX(CAST(u.date AS DATE)) AS last_universe_date
    FROM bankruptcies b
    LEFT JOIN universe u
        ON u.ticker = b.ticker
        AND CAST(u.date AS DATE) BETWEEN b.event_date - INTERVAL '90 days'
                                      AND b.event_date
    GROUP BY b.ticker, b.event_date
)
SELECT
    COUNT(*) AS total_bankruptcies,
    SUM(CASE WHEN universe_days_in_window > 0 THEN 1 ELSE 0 END) AS in_universe,
    SUM(CASE WHEN universe_days_in_window = 0 THEN 1 ELSE 0 END) AS missing_from_universe,
    SUM(CASE WHEN DATEDIFF('day', last_universe_date, event_date) <= 5 THEN 1 ELSE 0 END) AS coverage_to_event,
    SUM(CASE WHEN universe_days_in_window > 0
             AND DATEDIFF('day', last_universe_date, event_date) > 30 THEN 1 ELSE 0 END) AS dropped_early
FROM universe_coverage;
```

Run per-ticker detail for `dropped_early` cases to inspect survivorship bias.

---

## Pipeline usage

- **01_universe.py:** Builds resolved terminal event table (companion join, rename exclusion); removal_per_ticker does **not** use mergerfrom; fwd_delisted_30d / fwd_delisted_90d (calendar) unchanged for backward compatibility.
- **09_labels.py:** Consumes universe (all ticker-date grid), SEP, and ACTIONS; produces `forward_labels.parquet` with fwd_ret_*td, fwd_holding_days_*td, fwd_delisted_*td, fwd_delist_type. Grid = all (ticker, date) in universe parquet (not filtered by in_universe) so different universe definitions can be applied at modeling time without rerunning labels.

See the plan in `.cursor/plans/` and `events.md` for full implementation details.
