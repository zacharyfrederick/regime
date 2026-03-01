#!/usr/bin/env python3
"""
Delist & merger event study.

Explores what happens to prices around terminal corporate events (bankruptcy,
acquisition, merger, regulatory/voluntary delist). Uses ACTIONS, SEP, and
optionally the universe to understand:

1. What event types exist and their frequency
2. For each event type, what does the price series look like near the end?
3. How many trading days gap between last SEP price and ACTIONS date?
4. What terminal value does ACTIONS report vs last traded price?

Run from repo root:
    python pipeline/000_event_study.py

Output: prints to stdout + writes outputs/event_study_report.md
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from config import DATA_DIR, OUTPUTS_DIR

import duckdb

REPORT_PATH = OUTPUTS_DIR / "event_study_report.md"
REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)

# Case study tickers: known events
# HTZ: bankruptcy 2020, LEH: bankruptcy 2008 (may not exist), 
# TWX: acquired by AT&T 2018, MON: acquired by Bayer 2018
# Add any others you want to study
CASE_STUDY_TICKERS = ["HTZ", "LEH", "TWX", "MON", "ETFC", "TIF", "CIT", "DNKN"]

def _find(name: str) -> Path:
    p = DATA_DIR / f"{name}.parquet"
    return p if p.exists() else DATA_DIR / f"{name.lower()}.parquet"

def _ps(p: Path) -> str:
    return repr(str(p.resolve()))


def main():
    con = duckdb.connect()
    lines: list[str] = []

    def out(s: str = ""):
        print(s)
        lines.append(s)

    out("# Delist & Merger Event Study")
    out("")

    # ── Load tables ──────────────────────────────────────────────
    actions_path = _find("ACTIONS")
    sep_path = _find("SEP")
    daily_universe_path = OUTPUTS_DIR / "universe" / "daily_universe.parquet"

    if not actions_path.exists():
        out("ERROR: ACTIONS.parquet not found in data/")
        return
    if not sep_path.exists():
        out("ERROR: SEP.parquet not found in data/")
        return

    con.execute(f"CREATE VIEW actions AS SELECT * FROM read_parquet({_ps(actions_path)})")
    # SEP: closeunadj may be missing in some exports; fall back to closeadj
    try:
        con.execute(f"""
            CREATE VIEW sep AS
            SELECT ticker, date, closeadj, closeunadj, volume
            FROM read_parquet({_ps(sep_path)})
        """)
    except Exception:
        con.execute(f"""
            CREATE VIEW sep AS
            SELECT ticker, date, closeadj, closeadj AS closeunadj, volume
            FROM read_parquet({_ps(sep_path)})
        """)
    if daily_universe_path.exists():
        con.execute(f"CREATE VIEW universe AS SELECT * FROM read_parquet({_ps(daily_universe_path)})")
        has_universe = True
    else:
        has_universe = False
        out("NOTE: daily_universe.parquet not found; skipping universe checks")

    # Detect optional ACTIONS columns (Sharadar-specific)
    try:
        described = con.execute("DESCRIBE actions").fetchall()
        # DESCRIBE returns (column_name, column_type, ...); first column is name
        actions_columns = [row[0].lower() if row[0] else "" for row in described]
    except Exception:
        actions_columns = []
    has_value = "value" in actions_columns
    has_contraticker = "contraticker" in actions_columns
    has_name = "name" in actions_columns
    out("")

    # ══════════════════════════════════════════════════════════════
    # SECTION 1: ACTIONS overview — what event types exist?
    # ══════════════════════════════════════════════════════════════
    out("## 1. ACTIONS event type overview")
    out("")
    df = con.execute("""
        SELECT action, COUNT(*) AS n,
               MIN(CAST(date AS DATE)) AS earliest,
               MAX(CAST(date AS DATE)) AS latest
        FROM actions
        GROUP BY action
        ORDER BY n DESC
    """).df()
    out("| Action | Count | Earliest | Latest |")
    out("|--------|-------|----------|--------|")
    for _, r in df.iterrows():
        out(f"| {r['action']} | {r['n']:,} | {r['earliest']} | {r['latest']} |")
    out("")

    # Terminal events specifically
    TERMINAL_ACTIONS = [
        "delisted", "bankruptcyliquidation", "acquisitionby",
        "mergerfrom", "regulatorydelisting", "voluntarydelisting",
    ]
    out("### Terminal events (cause ticker to stop trading)")
    out("")
    df_term = con.execute(f"""
        SELECT action, COUNT(DISTINCT ticker) AS tickers, COUNT(*) AS events,
               MIN(CAST(date AS DATE)) AS earliest,
               MAX(CAST(date AS DATE)) AS latest
        FROM actions
        WHERE LOWER(TRIM(action)) IN ({','.join(f"'{a}'" for a in TERMINAL_ACTIONS)})
        GROUP BY action
        ORDER BY tickers DESC
    """).df()
    out("| Action | Distinct Tickers | Events | Earliest | Latest |")
    out("|--------|-----------------|--------|----------|--------|")
    for _, r in df_term.iterrows():
        out(f"| {r['action']} | {r['tickers']:,} | {r['events']:,} | {r['earliest']} | {r['latest']} |")
    out("")

    # ══════════════════════════════════════════════════════════════
    # SECTION 2: What does ACTIONS store for terminal events?
    # ══════════════════════════════════════════════════════════════
    out("## 2. ACTIONS fields for terminal events (sample)")
    out("")
    extra_cols = []
    if has_value:
        extra_cols.append("value")
    if has_contraticker:
        extra_cols.append("contraticker")
    if has_name:
        extra_cols.append("name")
    out("Checking: ticker, date, action" + (", " + ", ".join(extra_cols) if extra_cols else ""))
    out("")
    select_cols = "ticker, CAST(date AS DATE) AS date, action" + (", " + ", ".join(extra_cols) if extra_cols else "")
    for action in TERMINAL_ACTIONS:
        sample = con.execute(f"""
            SELECT {select_cols}
            FROM actions
            WHERE LOWER(TRIM(action)) = '{action}'
            ORDER BY date DESC
            LIMIT 5
        """).df()
        if sample.empty:
            continue
        out(f"### {action} (5 most recent)")
        out("")
        out(sample.to_markdown(index=False))
        out("")

    # ══════════════════════════════════════════════════════════════
    # SECTION 3: Case studies — price series around terminal events
    # ══════════════════════════════════════════════════════════════
    out("## 3. Case studies: price behavior around terminal events")
    out("")

    # Find which case study tickers actually have terminal events
    case_extra = []
    if has_value:
        case_extra.append("value")
    if has_contraticker:
        case_extra.append("contraticker")
    case_select = "ticker, CAST(date AS DATE) AS event_date, action" + (", " + ", ".join(case_extra) if case_extra else "")

    case_events = con.execute(f"""
        SELECT {case_select}
        FROM actions
        WHERE LOWER(TRIM(action)) IN ({','.join(f"'{a}'" for a in TERMINAL_ACTIONS)})
          AND ticker IN ({','.join(f"'{t}'" for t in CASE_STUDY_TICKERS)})
        ORDER BY ticker, date
    """).df()

    if case_events.empty:
        out("No case study tickers found in terminal events. Trying all terminal events for sample tickers...")
        case_events = con.execute(f"""
            SELECT {case_select}
            FROM actions
            WHERE LOWER(TRIM(action)) IN ({','.join(f"'{a}'" for a in TERMINAL_ACTIONS)})
            ORDER BY date DESC
            LIMIT 20
        """).df()

    out(f"Found {len(case_events)} terminal events for study tickers:")
    out("")
    out(case_events.to_markdown(index=False))
    out("")

    # For each event, look at the price series
    for _, event in case_events.iterrows():
        ticker = event["ticker"]
        event_date = event["event_date"]
        action = event["action"]
        actions_value = event.get("value")  # final mktcap in $M per ACTIONS docs (optional)

        out(f"### {ticker} — {action} on {event_date}")
        out("")

        # Last 30 trading days + any after
        price_around = con.execute(f"""
            WITH prices AS (
                SELECT CAST(date AS DATE) AS date, closeadj, closeunadj, volume,
                       ROW_NUMBER() OVER (ORDER BY CAST(date AS DATE) DESC) AS rn_desc
                FROM sep
                WHERE ticker = '{ticker}'
            ),
            last_price AS (
                SELECT MIN(date) AS first_date, MAX(date) AS last_date,
                       COUNT(*) AS total_days
                FROM prices
            )
            SELECT p.date, p.closeadj, p.closeunadj, p.volume
            FROM prices p, last_price lp
            WHERE p.date >= lp.last_date - INTERVAL '60 days'
            ORDER BY p.date
        """).df()

        if price_around.empty:
            out(f"  No SEP data for {ticker}")
            out("")
            continue

        last_sep_date = price_around["date"].max()
        last_closeadj = price_around.loc[price_around["date"] == last_sep_date, "closeadj"].iloc[0]
        last_closeunadj = price_around.loc[price_around["date"] == last_sep_date, "closeunadj"].iloc[0]
        first_date_in_window = price_around["date"].min()
        first_closeadj = price_around.loc[price_around["date"] == first_date_in_window, "closeadj"].iloc[0]

        # Gap between last price and event date
        gap_days = (event_date - last_sep_date).days if event_date is not None else None

        out(f"  - **Last SEP date**: {last_sep_date}")
        out(f"  - **Event date (ACTIONS)**: {event_date}")
        out(f"  - **Gap (calendar days)**: {gap_days}")
        out(f"  - **Last closeadj**: ${last_closeadj:.2f}")
        out(f"  - **Last closeunadj**: ${last_closeunadj:.2f}")
        if actions_value is not None:
            out(f"  - **ACTIONS value (final mktcap $M)**: {actions_value}")
        if first_closeadj and first_closeadj > 0:
            ret_final_60d = (last_closeadj / first_closeadj) - 1
            out(f"  - **Return over last ~60 cal days of trading**: {ret_final_60d:.1%}")
        out("")

        # Show last 10 prices
        tail = price_around.tail(10).copy()
        tail["closeadj"] = tail["closeadj"].map(lambda x: f"${x:.2f}" if x else "")
        tail["closeunadj"] = tail["closeunadj"].map(lambda x: f"${x:.2f}" if x else "")
        tail["volume"] = tail["volume"].map(lambda x: f"{x:,.0f}" if x else "")
        out("  Last 10 trading days:")
        out("")
        out(tail.to_markdown(index=False))
        out("")

        # If acquisition/merger, check contraticker price around same date
        if event.get("contraticker") and str(action).lower() in ("acquisitionby", "mergerfrom"):
            contra = event["contraticker"]
            out(f"  Acquirer/survivor: {contra}")
            contra_price = con.execute(f"""
                SELECT CAST(date AS DATE) AS date, closeadj
                FROM sep
                WHERE ticker = '{contra}'
                  AND CAST(date AS DATE) BETWEEN '{event_date}'::DATE - INTERVAL '5 days'
                                              AND '{event_date}'::DATE + INTERVAL '5 days'
                ORDER BY date
            """).df()
            if not contra_price.empty:
                out(f"  {contra} price around event:")
                out(contra_price.to_markdown(index=False))
            else:
                out(f"  No SEP data for {contra} around event date")
            out("")

    # ══════════════════════════════════════════════════════════════
    # SECTION 4: Systematic analysis — gap between last price and event
    # ══════════════════════════════════════════════════════════════
    out("## 4. Systematic: gap between last SEP price and terminal event date")
    out("")

    gap_df = con.execute(f"""
        WITH terminal AS (
            SELECT ticker, CAST(date AS DATE) AS event_date, action
            FROM actions
            WHERE LOWER(TRIM(action)) IN ({','.join(f"'{a}'" for a in TERMINAL_ACTIONS)})
        ),
        last_prices AS (
            SELECT ticker, MAX(CAST(date AS DATE)) AS last_sep_date
            FROM sep
            GROUP BY ticker
        )
        SELECT t.action,
               COUNT(*) AS n,
               AVG(DATEDIFF('day', lp.last_sep_date, t.event_date)) AS avg_gap_days,
               MEDIAN(DATEDIFF('day', lp.last_sep_date, t.event_date)) AS median_gap_days,
               MIN(DATEDIFF('day', lp.last_sep_date, t.event_date)) AS min_gap_days,
               MAX(DATEDIFF('day', lp.last_sep_date, t.event_date)) AS max_gap_days,
               SUM(CASE WHEN DATEDIFF('day', lp.last_sep_date, t.event_date) > 5 THEN 1 ELSE 0 END) AS gap_gt_5d,
               SUM(CASE WHEN DATEDIFF('day', lp.last_sep_date, t.event_date) < 0 THEN 1 ELSE 0 END) AS event_before_last_price
        FROM terminal t
        LEFT JOIN last_prices lp ON lp.ticker = t.ticker
        WHERE lp.last_sep_date IS NOT NULL
        GROUP BY t.action
        ORDER BY n DESC
    """).df()
    out(gap_df.to_markdown(index=False))
    out("")

    # ══════════════════════════════════════════════════════════════
    # SECTION 5: Terminal return analysis
    # ══════════════════════════════════════════════════════════════
    out("## 5. Terminal returns: last 30/60/90 trading day returns before delist")
    out("")

    terminal_returns = con.execute(f"""
        WITH terminal AS (
            SELECT ticker, CAST(date AS DATE) AS event_date, action
            FROM actions
            WHERE LOWER(TRIM(action)) IN ({','.join(f"'{a}'" for a in TERMINAL_ACTIONS)})
        ),
        price_ranked AS (
            SELECT ticker, CAST(date AS DATE) AS date, closeadj,
                   ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY CAST(date AS DATE) DESC) AS rn
            FROM sep
        ),
        joined AS (
            SELECT t.ticker, t.action,
                   MAX(CASE WHEN rn = 1 THEN closeadj END) AS price_last,
                   MAX(CASE WHEN rn = 21 THEN closeadj END) AS price_21d_ago,
                   MAX(CASE WHEN rn = 63 THEN closeadj END) AS price_63d_ago,
                   MAX(CASE WHEN rn = 126 THEN closeadj END) AS price_126d_ago,
                   MAX(CASE WHEN rn = 252 THEN closeadj END) AS price_252d_ago
            FROM terminal t
            JOIN price_ranked pr ON pr.ticker = t.ticker
            WHERE pr.rn <= 252
            GROUP BY t.ticker, t.action
        )
        SELECT action,
               COUNT(*) AS n,
               -- Last 21 trading days return
               AVG(CASE WHEN price_21d_ago > 0 THEN (price_last / price_21d_ago - 1) END) AS avg_ret_21d,
               MEDIAN(CASE WHEN price_21d_ago > 0 THEN (price_last / price_21d_ago - 1) END) AS med_ret_21d,
               -- Last 63 trading days return
               AVG(CASE WHEN price_63d_ago > 0 THEN (price_last / price_63d_ago - 1) END) AS avg_ret_63d,
               MEDIAN(CASE WHEN price_63d_ago > 0 THEN (price_last / price_63d_ago - 1) END) AS med_ret_63d,
               -- Last 252 trading days return
               AVG(CASE WHEN price_252d_ago > 0 THEN (price_last / price_252d_ago - 1) END) AS avg_ret_252d,
               MEDIAN(CASE WHEN price_252d_ago > 0 THEN (price_last / price_252d_ago - 1) END) AS med_ret_252d
        FROM joined
        GROUP BY action
        ORDER BY n DESC
    """).df()
    out(terminal_returns.to_markdown(index=False))
    out("")
    out("Interpretation: bankruptcies should show large negative returns; acquisitions near 0 or positive (premium).")
    out("")

    # ══════════════════════════════════════════════════════════════
    # SECTION 6: What fraction of terminal value is captured by last SEP price?
    # ══════════════════════════════════════════════════════════════
    out("## 6. ACTIONS value vs last SEP price (acquisitions/mergers)")
    out("")
    if not has_value:
        out("Skipped: ACTIONS has no 'value' column (Sharadar-specific).")
        out("")
    else:
        out("ACTIONS.value = final market cap in $M. Compare to last closeadj * sharesbas if available.")
        out("")

        # For acquisitions, the ACTIONS value field should approximate last mktcap
        acq_check = con.execute(f"""
            WITH terminal AS (
                SELECT ticker, CAST(date AS DATE) AS event_date, action, value AS actions_mktcap_m
                FROM actions
                WHERE LOWER(TRIM(action)) IN ('acquisitionby', 'mergerfrom')
                  AND value IS NOT NULL AND value > 0
            ),
            last_price AS (
                SELECT ticker,
                       MAX(CAST(date AS DATE)) AS last_date,
                       (SELECT closeadj FROM sep s2
                        WHERE s2.ticker = sep.ticker
                        ORDER BY CAST(s2.date AS DATE) DESC LIMIT 1) AS last_closeadj
                FROM sep
                GROUP BY ticker
            )
            SELECT t.ticker, t.action, t.event_date,
                   t.actions_mktcap_m,
                   lp.last_date,
                   lp.last_closeadj
            FROM terminal t
            JOIN last_price lp ON lp.ticker = t.ticker
            ORDER BY t.event_date DESC
            LIMIT 20
        """).df()
        if not acq_check.empty:
            out(acq_check.to_markdown(index=False))
        else:
            out("No acquisition/merger events with value data found.")
        out("")

    # ══════════════════════════════════════════════════════════════
    # SECTION 7: Universe delist flags vs ACTIONS
    # ══════════════════════════════════════════════════════════════
    if has_universe:
        out("## 7. Universe delist flags vs ACTIONS dates")
        out("")
        try:
            universe_cols = [r[0] for r in con.execute("DESCRIBE universe").fetchall()]
        except Exception:
            universe_cols = []
        if "fwd_delisted_30d" not in universe_cols or "fwd_delisted_90d" not in universe_cols:
            out("Universe no longer carries calendar-day delist flags (fwd_delisted_30d/90d).")
            out("Delist timing and forward-return labels use trading-day horizons from")
            out("**outputs/labels/forward_labels.parquet** and the master table (fwd_delisted_21td, fwd_delisted_63td, etc.).")
            out("")
        else:
            out("Check: does fwd_delisted_30d flip to 1 at the right time?")
            out("")

            sample_tickers = con.execute(f"""
                SELECT DISTINCT ticker FROM actions
                WHERE LOWER(TRIM(action)) IN ({','.join(f"'{a}'" for a in TERMINAL_ACTIONS)})
                ORDER BY RANDOM()
                LIMIT 10
            """).df()["ticker"].tolist()

            for ticker in sample_tickers:
                event_row = con.execute(f"""
                    SELECT CAST(date AS DATE) AS event_date, action
                    FROM actions
                    WHERE ticker = '{ticker}'
                      AND LOWER(TRIM(action)) IN ({','.join(f"'{a}'" for a in TERMINAL_ACTIONS)})
                    ORDER BY date
                    LIMIT 1
                """).fetchone()
                if not event_row:
                    continue

                event_date, action = event_row

                flag_check = con.execute(f"""
                    SELECT CAST(date AS DATE) AS date,
                           fwd_delisted_30d,
                           fwd_delisted_90d
                    FROM universe
                    WHERE ticker = '{ticker}'
                      AND CAST(date AS DATE) BETWEEN '{event_date}'::DATE - INTERVAL '100 days'
                                                  AND '{event_date}'::DATE + INTERVAL '10 days'
                    ORDER BY date
                """).df()

                if flag_check.empty:
                    continue

                first_flag_30 = flag_check[flag_check["fwd_delisted_30d"] == True]
                first_flag_90 = flag_check[flag_check["fwd_delisted_90d"] == True]

                out(f"**{ticker}** ({action} on {event_date}):")
                if not first_flag_30.empty:
                    out(f"  - fwd_delisted_30d first TRUE: {first_flag_30['date'].iloc[0]}")
                    days_before = (event_date - first_flag_30["date"].iloc[0]).days
                    out(f"  - Days before event: {days_before} (expect ~30)")
                else:
                    out(f"  - fwd_delisted_30d: never TRUE in window")
                if not first_flag_90.empty:
                    out(f"  - fwd_delisted_90d first TRUE: {first_flag_90['date'].iloc[0]}")
                out("")

    # ══════════════════════════════════════════════════════════════
    # SECTION 8: Recommendations
    # ══════════════════════════════════════════════════════════════
    out("## 8. Recommendations for price adjustment")
    out("")
    out("Based on the data above, consider:")
    out("")
    out("1. **Add `delist_type` to universe** from ACTIONS (bankruptcyliquidation, acquisitionby, etc.)")
    out("2. **Bankruptcy terminal price**: If last SEP closeadj > $1 but action = bankruptcyliquidation,")
    out("   append a synthetic row with closeadj = 0 on the event date (or a recovery fraction).")
    out("3. **Acquisition terminal price**: Last SEP price usually reflects the deal price.")
    out("   Verify the gap is small; if > 5 days, interpolate or use ACTIONS value / sharesbas.")
    out("4. **Forward return labels**: Split fwd_delisted into fwd_bankruptcy and fwd_acquired.")
    out("   Models should treat these very differently.")
    out("5. **Gap handling**: For tickers where last SEP date is > 5 days before event date,")
    out("   decide whether to forward-fill the last price or mark those days as missing.")
    out("")

    # Write report
    con.close()
    with open(REPORT_PATH, "w") as f:
        f.write("\n".join(lines))
        f.write("\n")
    print(f"\nReport written to {REPORT_PATH}")


if __name__ == "__main__":
    main()