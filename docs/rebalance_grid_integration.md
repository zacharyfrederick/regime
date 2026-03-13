# Rebalance-grid integration: using calendar dates earlier in the pipeline

This report describes how to thin the pipeline to **rebalance entry dates only** (weekly/monthly/quarterly/annual), so we store and join fewer rows while **keeping feature calculations unchanged**. The principle: **use full daily series for computation; use a rebalance-only grid for output.**

---

## 1. Goal and principle

**Goal:** Only persist (ticker, date) rows where `date` is a rebalance entry date (first trading day of an ISO week, month, quarter, or year). That reduces artifact size and downstream join cost.

**Principle (no disruption to feature math):**

- **Rolling / windowed features** (e.g. ret_1m, vol_20d) must be computed over the **full daily series** so that values on rebalance dates are correct (e.g. ret_1m on the first trading day of February still needs ~21 prior trading days).
- **Output** is then restricted to the rebalance grid: we **join** the fully computed feature series to a grid that contains only rebalance dates, and write only those rows.
- **PIT / ASOF features** (fundamentals, sector-relative, insider) are evaluated per (ticker, date); restricting the set of dates to rebalance dates does not change the value at those dates, as long as the underlying data (SF1, SEP, etc.) is unchanged.

So we introduce a **rebalance grid** artifact and use it everywhere we currently use the full daily universe for **which rows to output**. We do **not** thin the inputs (SEP, SF1, etc.) or the internal date range used for window/LAG logic.

---

## 2. Current flow (where the grid is used)

| Script | Grid/universe source | How it is used |
|--------|----------------------|----------------|
| [01_universe.py](pipeline/01_universe.py) | Writes `daily_universe.parquet` (full daily) | Final view `daily_universe` → COPY to [config.DAILY_UNIVERSE_PATH](config.py) |
| [02_fundamentals.py](pipeline/02_fundamentals.py) | `grid` = read_parquet(DAILY_UNIVERSE_PATH) | ASOF joins `FROM grid g`; output one row per grid row ([line 126](pipeline/02_fundamentals.py), [216](pipeline/02_fundamentals.py), [294](pipeline/02_fundamentals.py)) |
| [03_price_features.py](pipeline/03_price_features.py) | `grid` = read_parquet(DAILY_UNIVERSE_PATH) | `sep_filtered` = SEP filtered by grid tickers + date range; `price_features_full` over full daily; final `FROM grid g LEFT JOIN price_features_full p` ([49](pipeline/03_price_features.py), [76–79](pipeline/03_price_features.py), [136–144](pipeline/03_price_features.py)) |
| [04_macro_features.py](pipeline/04_macro_features.py) | `dates` = DISTINCT date FROM daily_universe | Macro series built over `dates`; LAG/windows are over this date series ([66–70](pipeline/04_macro_features.py)) |
| [05_sector_relative.py](pipeline/05_sector_relative.py) | `universe` = read_parquet(DAILY_UNIVERSE_PATH) | `combined` from universe; sector medians per date; output one row per universe row ([60](pipeline/05_sector_relative.py), [123–161](pipeline/05_sector_relative.py)) |
| [06_insider_institutional.py](pipeline/06_insider_institutional.py) | `grid` from DAILY_UNIVERSE_PATH | `FROM grid g` for final view; output one row per grid row ([82–90](pipeline/06_insider_institutional.py), [266](pipeline/06_insider_institutional.py)) |
| [07_labels.py](pipeline/07_labels.py) | `grid` = read_parquet(DAILY_UNIVERSE_PATH) | Grid drives which (ticker, date) get labels; final wide table `FROM grid g LEFT JOIN ...` ([66–67](pipeline/07_labels.py), [357](pipeline/07_labels.py), [394](pipeline/07_labels.py)) |
| [08_merge.py](pipeline/08_merge.py) | `universe` = read_parquet(DAILY_UNIVERSE_PATH) | Master = universe LEFT JOIN all feature/label parquets; one row per universe row ([99](pipeline/08_merge.py)) |

---

## 3. Proposed artifacts and config

- **Keep:** `daily_universe.parquet` — full daily (ticker, date) from 01. Used for (a) defining the **full trading date range** and ticker set for feature computation, and (b) macro date grid if we keep macro over all trading dates.
- **Add:** `rebalance_universe.parquet` (or `rebalance_grid.parquet`) — subset of daily_universe where `date` is a **rebalance entry date**: union of first trading day of each ISO week, month, quarter, and year (same period keys as [docs/label_upgrade_roadmap.md](label_upgrade_roadmap.md): weekly = (isoyear, iso_week), monthly = (year, month), quarterly = (year, quarter), annual = (year)).

**Config change:** In [config.py](config.py), add for example:

```python
REBALANCE_UNIVERSE_PATH = UNIVERSE_DIR / "rebalance_universe.parquet"
```

Downstream scripts that today use `DAILY_UNIVERSE_PATH` for **output row set** would instead use `REBALANCE_UNIVERSE_PATH` (see below). Scripts that need the **full date range** (03, 04) would still read from `DAILY_UNIVERSE_PATH` where needed.

---

## 4. Where to make changes (by script)

### 4.1 Producing the rebalance grid

**Option A — New script (e.g. `01b_rebalance_universe.py`):**  
Read `daily_universe.parquet` and a trading calendar (distinct dates from SEP or from daily_universe), build rebalance_weekly/monthly/quarterly/annual (same logic as [07_labels.py lines 195–250](pipeline/07_labels.py)), take union of all `entry_date`s, then restrict daily_universe to rows with `date IN (rebalance_entry_dates)` and write `rebalance_universe.parquet`. Depends on 01 (daily_universe) and optionally SEP (if calendar is from SEP).

**Option B — Inside 01_universe.py:**  
After writing `daily_universe.parquet`, in the same run (or a second pass), build trading_calendar from the dates just written, compute rebalance entry dates, filter daily_universe to those dates, and write `rebalance_universe.parquet`. No new script; 01 must have access to the list of trading dates (it already does via its own logic).

**Suggested:** Option B keeps one place that knows “universe”; rebalance is just a filtered view of it. Period keys must match [label_upgrade_roadmap.md](label_upgrade_roadmap.md): weekly = (isoyear, iso_week), no mixing with (year, month).

---

### 4.2 [02_fundamentals.py](pipeline/02_fundamentals.py)

- **Change:** Load grid from `REBALANCE_UNIVERSE_PATH` instead of `DAILY_UNIVERSE_PATH` (e.g. [line 126](pipeline/02_fundamentals.py)).
- **Why safe:** ART/ARQ ASOF joins are “as of g.date”; they do not depend on other grid dates. Evaluating only for rebalance dates yields the same PIT values on those dates. Ticker set still from grid (same as now).

---

### 4.3 [03_price_features.py](pipeline/03_price_features.py) — critical for “no disruption”

- **Grid for output:** Load `grid` from `REBALANCE_UNIVERSE_PATH` ([line 49](pipeline/03_price_features.py)) so that the final `price_features` view and COPY have one row per rebalance date only.
- **Full series for computation:** Keep `price_features_full` computed over **all trading days** in range for grid tickers. So:
  - **Do not** restrict `sep_filtered` to rebalance dates. Restrict only by ticker and by **date range** (as today). For the date range, use the same bounds as now (e.g. DATE_START to DATE_END with lookback). Easiest: build a view `trading_dates` from `SELECT DISTINCT date FROM read_parquet(DAILY_UNIVERSE_PATH)` (or from SEP) and use it only to define the range for `sep_filtered`; do **not** use it to filter rows of `sep_filtered` to rebalance-only.
  - Example pattern (conceptual):

```text
-- grid = rebalance_universe (for output)
-- trading_dates = SELECT DISTINCT date FROM read_parquet(DAILY_UNIVERSE_PATH) WHERE date BETWEEN ... (full range)
-- sep_filtered = sep WHERE ticker IN (SELECT ticker FROM grid) AND date BETWEEN (DATE_START - lookback) AND DATE_END
-- price_features_full = unchanged (CTEs over sep_filtered)
-- price_features = SELECT g.ticker, g.date, p.* FROM grid g LEFT JOIN price_features_full p ON ...
```

- **Result:** ret_1m, vol_20d, etc. on a rebalance date are identical to current behavior; we simply do not write rows for non-rebalance dates.

---

### 4.4 [04_macro_features.py](pipeline/04_macro_features.py)

- **Option (recommended):** Keep macro computed over **all trading dates**. So keep `dates` = `SELECT DISTINCT date FROM read_parquet(DAILY_UNIVERSE_PATH)` ([66–70](pipeline/04_macro_features.py)). Write `macro_features.parquet` with one row per trading date (unchanged). In 08_merge, the master join is `universe u LEFT JOIN macro_features m ON u.date = m.date`; once `universe` is rebalance_only, master will only contain macro for rebalance dates. So LAG(20) etc. remain “20 trading days” and macro values on rebalance dates are unchanged.
- **Alternative:** If we ever wanted macro parquet to contain only rebalance dates, we would need to compute macro series on a **full** trading-date series first (to preserve LAG semantics), then filter the result to rebalance dates before writing. The report recommends the first option to avoid touching 04.

---

### 4.5 [05_sector_relative.py](pipeline/05_sector_relative.py)

- **Change:** Load `universe` from `REBALANCE_UNIVERSE_PATH` instead of `DAILY_UNIVERSE_PATH` ([line 60](pipeline/05_sector_relative.py)).
- **Why safe:** `combined` and `sector_medians` are built from `universe`; we only have rebalance dates, so sector medians are computed per rebalance date over the cross-section that exists on that date. Values are the same as if we had computed for the full grid and then selected those dates. Validation that reads `DAILY_UNIVERSE_PATH` ([181](pipeline/05_sector_relative.py)) should be updated to use the same universe as input (rebalance) or to a separate “full” path if you still want a full-universe sanity check.

---

### 4.6 [06_insider_institutional.py](pipeline/06_insider_institutional.py)

- **Change:** Build `grid` from `REBALANCE_UNIVERSE_PATH` instead of `DAILY_UNIVERSE_PATH` ([82–90](pipeline/06_insider_institutional.py)). Final view remains `FROM grid g LEFT JOIN ...` ([266](pipeline/06_insider_institutional.py)).
- **Why safe:** Insider/inst features are per (ticker, date) with lookbacks (e.g. 90d); the value on a rebalance date is unchanged whether that date is the only one we output or one of many.

---

### 4.7 [07_labels.py](pipeline/07_labels.py)

- **Change:** Load `grid` from `REBALANCE_UNIVERSE_PATH` instead of `DAILY_UNIVERSE_PATH` ([66–67](pipeline/07_labels.py)). All logic (trading_calendar, rebalance tables, horizon loop, final wide join) is unchanged; we simply output one row per (ticker, rebalance_date) and every such row will have at least one horizon with non-NULL labels (the date is by construction a rebalance entry for some horizon).
- **Note:** 07 already builds trading_calendar and rebalance_* from SEP; the rebalance_universe.parquet from 01 must use the same period-key convention (weekly = isoyear, iso_week; no mixing).

---

### 4.8 [08_merge.py](pipeline/08_merge.py)

- **Change:** Load `universe` from `REBALANCE_UNIVERSE_PATH` instead of `DAILY_UNIVERSE_PATH` ([99](pipeline/08_merge.py)). Master then has one row per (ticker, rebalance_date). All LEFT JOINs (features, labels) remain on (ticker, date); they will only match rebalance dates, and feature parquets will now only contain those dates, so row counts align.

---

## 5. Summary: avoiding disruption to feature calculations

| Concern | How we avoid disruption |
|--------|--------------------------|
| **Rolling/window features (03)** | `price_features_full` is still computed over the **full** daily series (same `sep_filtered` date range and tickers). Only the **output** is thinned by joining to the rebalance grid. So ret_1m, vol_20d, etc. on any rebalance date are identical to current. |
| **PIT/ASOF (02)** | We only evaluate at rebalance dates; the ASOF logic and underlying SF1/SEP are unchanged, so values at those dates are correct. |
| **Sector medians (05)** | Medians are computed per date over the cross-section present on that date; using only rebalance dates does not change the median on a rebalance date. |
| **Macro (04)** | Keep building macro over **all** trading dates (from daily_universe); merge uses rebalance universe so master only gets macro for rebalance dates. LAG semantics stay in trading days. |
| **Insider/inst (06)** | Per-(ticker, date) with lookbacks; rebalance-date values unchanged. |
| **Labels (07)** | Already defined only on rebalance semantics; grid = rebalance just removes NULL-only rows from the output. |

---

## 6. Order and dependencies

- 01_universe: produces `daily_universe.parquet` (unchanged) and **new** `rebalance_universe.parquet` (filter daily_universe to rebalance entry dates using trading_calendar + rebalance_weekly/monthly/quarterly/annual).
- 02–08: use `REBALANCE_UNIVERSE_PATH` for the grid/universe that defines **which rows to output**; 03 (and optionally 04) still use `DAILY_UNIVERSE_PATH` only where a **full trading date range** is required for correct window/LAG behavior.

No change to artifact schemas (column sets); only row counts drop to rebalance-date subsets. Downstream (e.g. experiments) that already filter to rebalance dates or to non-NULL labels will see the same values on the same (ticker, date) keys; they may see fewer rows if they previously joined to the full grid.
