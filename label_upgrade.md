See **docs/label_upgrade_roadmap.md** for the concise implementation roadmap.

We are using duckdb with python for working with historical fundamentals. We are trying to make labels for backtesting.

We have an event database where we have the terminal event for each ticker across each time horizon. So if we are looking at monthly returns we need to know if something caused that stock to be delisted which reduces survivorship bias. We have that in flags 

Our signals compute daily so we will have to factor in on a given date the signal date which is last known pit fundamentals and the price date. The price date is the reference because that is what connects us to current time. 

We are going to want to look at weeks months quarters and years in trading days. We should be using close to close. So if it’s a weekly strategy buy at Monday close and sell at Friday close. We won’t have pricing data for days where the market is closed. But I’m worried that if the trading window is 5 for weekly we don’t want do the buying on a Tuesday and then sell on next Monday. Even if it the market opens late we close the same time. So we want to ignore basic offsets. Is there a performant way to do this?
1:59 PM

You're raising a really important point about alignment. Let me think through this clearly.

The core problem: if you use a naive "offset by N trading days" approach, your entry day drifts. A Monday holiday means your Tuesday entry shifts your entire window, and now you're selling on Monday instead of Friday. You want calendar-anchored windows where the entry point is always the same day of the week/month/quarter/year, and the exit is always at the close of the last trading day in that window.

Here's a clean, performant approach in DuckDB:
python

import duckdb

con = duckdb.connect("your_db.duckdb")

# Step 1: Build a trading calendar with calendar-anchored period labels
# This is the key insight - label each trading day with its canonical period,
# then your entry/exit are just first/last trading day within each period.

con.execute("""
CREATE OR REPLACE TABLE trading_calendar AS
WITH prices AS (
    SELECT DISTINCT date 
    FROM daily_prices 
    ORDER BY date
),
labeled AS (
    SELECT
        date,
        -- Weekly: ISO week (Mon-Fri naturally)
        date_part('isoyear', date) AS iso_year,
        date_part('week', date) AS iso_week,
        
        -- Monthly
        date_part('year', date) AS year,
        date_part('month', date) AS month,
        
        -- Quarterly
        date_part('quarter', date) AS quarter,
        
        -- Yearly
        -- (year already captured)
        
        -- Trading day index (useful for custom windows)
        row_number() OVER (ORDER BY date) AS td_index
    FROM prices
)
SELECT
    date,
    iso_year, iso_week, year, month, quarter, td_index,
    
    -- For each period, mark entry (first trading day) and exit (last trading day)
    FIRST_VALUE(date) OVER (PARTITION BY iso_year, iso_week ORDER BY date) AS week_entry,
    LAST_VALUE(date) OVER (
        PARTITION BY iso_year, iso_week 
        ORDER BY date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS week_exit,
    
    FIRST_VALUE(date) OVER (PARTITION BY year, month ORDER BY date) AS month_entry,
    LAST_VALUE(date) OVER (
        PARTITION BY year, month 
        ORDER BY date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS month_exit,
    
    FIRST_VALUE(date) OVER (PARTITION BY year, quarter ORDER BY date) AS quarter_entry,
    LAST_VALUE(date) OVER (
        PARTITION BY year, quarter 
        ORDER BY date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS quarter_exit,
    
    FIRST_VALUE(date) OVER (PARTITION BY year ORDER BY date) AS year_entry,
    LAST_VALUE(date) OVER (
        PARTITION BY year 
        ORDER BY date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS year_exit
    
FROM labeled
""")

Now building the return labels becomes a straightforward join:
python

con.execute("""
CREATE OR REPLACE TABLE return_labels AS
WITH signal_dates AS (
    -- Your signal fires on some date, we need to find which period it enters
    SELECT
        s.ticker,
        s.signal_date,
        s.price_date,  -- the anchor to real time
        tc.week_entry, tc.week_exit,
        tc.month_entry, tc.month_exit,
        tc.quarter_entry, tc.quarter_exit,
        tc.year_entry, tc.year_exit,
        tc.iso_year, tc.iso_week, tc.year, tc.month, tc.quarter
    FROM signals s
    JOIN trading_calendar tc ON s.price_date = tc.date
),
-- For forward-looking: we want the NEXT period's entry/exit
-- e.g., signal fires mid-week -> enter at next week's open (Monday close)
next_periods AS (
    SELECT
        sd.*,
        
        -- Next week: find the first trading day of the next ISO week
        (SELECT MIN(tc2.date) FROM trading_calendar tc2 
         WHERE tc2.date > sd.week_exit) AS next_week_entry,
        (SELECT MIN(tc2.week_exit) FROM trading_calendar tc2 
         WHERE tc2.date > sd.week_exit) AS next_week_exit,
         
        -- Next month
        (SELECT MIN(tc2.date) FROM trading_calendar tc2 
         WHERE tc2.date > sd.month_exit) AS next_month_entry,
        (SELECT MIN(tc2.month_exit) FROM trading_calendar tc2 
         WHERE tc2.date > sd.month_exit) AS next_month_exit,
         
        -- Next quarter
        (SELECT MIN(tc2.date) FROM trading_calendar tc2 
         WHERE tc2.date > sd.quarter_exit) AS next_quarter_entry,
        (SELECT MIN(tc2.quarter_exit) FROM trading_calendar tc2 
         WHERE tc2.date > sd.quarter_exit) AS next_quarter_exit,
         
        -- Next year
        (SELECT MIN(tc2.date) FROM trading_calendar tc2 
         WHERE tc2.date > sd.year_exit) AS next_year_entry,
        (SELECT MIN(tc2.year_exit) FROM trading_calendar tc2 
         WHERE tc2.date > sd.year_exit) AS next_year_exit
         
    FROM signal_dates sd
)
SELECT
    np.ticker,
    np.signal_date,
    np.price_date,
    
    -- Weekly return label
    np.next_week_entry AS week_buy_date,
    np.next_week_exit AS week_sell_date,
    p_buy_w.close AS week_buy_price,
    -- Use event DB for terminal handling
    CASE 
        WHEN ev_w.terminal_event IS NOT NULL 
        THEN ev_w.terminal_price  -- delisting price, acquisition price, etc.
        ELSE p_sell_w.close 
    END AS week_sell_price,
    ev_w.terminal_event AS week_terminal_flag,
    
    -- Monthly return label
    np.next_month_entry AS month_buy_date,
    np.next_month_exit AS month_sell_date,
    p_buy_m.close AS month_buy_price,
    CASE 
        WHEN ev_m.terminal_event IS NOT NULL 
        THEN ev_m.terminal_price
        ELSE p_sell_m.close 
    END AS month_sell_price,
    ev_m.terminal_event AS month_terminal_flag,
    
    -- Quarter and year follow same pattern...
    np.next_quarter_entry AS quarter_buy_date,
    np.next_quarter_exit AS quarter_sell_date,
    np.next_year_entry AS year_buy_date,
    np.next_year_exit AS year_sell_date

FROM next_periods np

-- Weekly price joins
LEFT JOIN daily_prices p_buy_w 
    ON np.ticker = p_buy_w.ticker AND np.next_week_entry = p_buy_w.date
LEFT JOIN daily_prices p_sell_w 
    ON np.ticker = p_sell_w.ticker AND np.next_week_exit = p_sell_w.date

-- Weekly terminal event check  
LEFT JOIN events ev_w 
    ON np.ticker = ev_w.ticker 
    AND ev_w.event_date BETWEEN np.next_week_entry AND np.next_week_exit
    AND ev_w.terminal_event IS NOT NULL

-- Monthly price joins
LEFT JOIN daily_prices p_buy_m 
    ON np.ticker = p_buy_m.ticker AND np.next_month_entry = p_buy_m.date
LEFT JOIN daily_prices p_sell_m 
    ON np.ticker = p_sell_m.ticker AND np.next_month_exit = p_sell_m.date

LEFT JOIN events ev_m 
    ON np.ticker = ev_m.ticker 
    AND ev_m.event_date BETWEEN np.next_month_entry AND np.next_month_exit
    AND ev_m.terminal_event IS NOT NULL

-- ... quarter and year joins follow same pattern
""")

A few design notes:

Why this works for your drift problem: ISO weeks are always Monday-Sunday. Even if Monday is a holiday, the first trading day that week is Tuesday, and the last is Friday. Your window is still "that week." You're never accidentally spanning into the next calendar week.

Performance: The trading calendar is tiny (maybe 7,000 rows for 30 years) and the window functions compute once. The expensive part is the price joins, but those are all equi-joins on (ticker, date) which DuckDB handles extremely well with its columnar engine. Make sure daily_prices has an index or is sorted by (ticker, date).

Terminal event handling: When a stock gets delisted mid-period, you use the terminal price from your event DB instead of the period-end close. If the stock has no close on the sell date and no terminal event, that's a data gap you'd want to flag.

Signal timing nuance: I assumed you want to trade the next full period after the signal fires (to avoid look-ahead). If the signal fires on a Wednesday, you enter at next Monday's close, not this Friday's. Adjust if your logic is different.

Want me to refine any of the horizon logic or the terminal event integration?

So we are calculating fundamentals by taking the dates from prices as a reference and for each vintage day calculating the point in time fundamentals for the market. That data is published after close of market. So if I’m buying today I can only know yesterday’s signal. Instead of signal I mean features
2:03 PM

Right, so there's a one-day lag between when you observe the features and when you can act. If features are computed from yesterday's close (and published after market close), then the earliest you can trade on them is today's close. Let me reframe the timing:

Day T-1 close: prices finalize → fundamentals computed overnight
Day T: you observe the signal → buy at Day T close
Day T+N: sell at close

So the price_date on the features is T-1, but the entry price is T close. This is your look-ahead protection.

Here's how I'd structure this cleanly:
python

con.execute("""
CREATE OR REPLACE TABLE features_with_timing AS
SELECT
    f.ticker,
    f.price_date,                          -- T-1: the date the features describe
    tc_signal.date AS signal_observable_date, -- T: next trading day after price_date
    -- This is when you KNOW the signal and CAN act
    
    -- For each horizon, find the entry and exit
    -- Entry: signal_observable_date's close (you buy at T close)
    -- Exit: last trading day of the holding period
    
FROM features f
JOIN trading_calendar tc_feat ON f.price_date = tc_feat.date
-- T is the next trading day after T-1
JOIN trading_calendar tc_signal 
    ON tc_signal.td_index = tc_feat.td_index + 1
""")

The td_index + 1 is the clean way to get "next trading day" without worrying about weekends or holidays. Since your trading calendar only has actual trading days, index + 1 always lands on the next open market day.

Now for the holding periods, building on the calendar-anchored approach:
python

con.execute("""
CREATE OR REPLACE TABLE holding_periods AS
WITH entry_points AS (
    SELECT
        f.ticker,
        f.price_date,
        tc.date AS entry_date,        -- T: buy at this day's close
        tc.td_index AS entry_td_index,
        tc.iso_year AS entry_iso_year,
        tc.iso_week AS entry_iso_week,
        tc.year AS entry_year,
        tc.month AS entry_month,
        tc.quarter AS entry_quarter
    FROM features f
    JOIN trading_calendar tc_feat ON f.price_date = tc_feat.date
    JOIN trading_calendar tc ON tc.td_index = tc_feat.td_index + 1
)
SELECT
    ep.*,
    
    ------------------------------------------------------------------
    -- WEEKLY: buy at T close, sell at that week's Friday close
    -- If T is already Friday, sell same day (0-day hold, probably skip)
    -- If T is Monday, sell Friday = 4 trading days later
    ------------------------------------------------------------------
    ep.entry_date AS week_buy_date,
    (SELECT MAX(tc2.date) 
     FROM trading_calendar tc2 
     WHERE tc2.iso_year = ep.entry_iso_year 
       AND tc2.iso_week = ep.entry_iso_week
    ) AS week_sell_date,
    
    ------------------------------------------------------------------
    -- MONTHLY: buy at T close, sell at month-end close
    -- Same idea: sell on last trading day of entry month
    ------------------------------------------------------------------
    ep.entry_date AS month_buy_date,
    (SELECT MAX(tc2.date) 
     FROM trading_calendar tc2 
     WHERE tc2.year = ep.entry_year 
       AND tc2.month = ep.entry_month
    ) AS month_sell_date,
    
    ------------------------------------------------------------------
    -- QUARTERLY: sell last trading day of entry quarter
    ------------------------------------------------------------------
    ep.entry_date AS quarter_buy_date,
    (SELECT MAX(tc2.date) 
     FROM trading_calendar tc2 
     WHERE tc2.year = ep.entry_year 
       AND tc2.quarter = ep.entry_quarter
    ) AS quarter_sell_date,
    
    ------------------------------------------------------------------
    -- YEARLY: sell last trading day of entry year
    ------------------------------------------------------------------
    ep.entry_date AS year_buy_date,
    (SELECT MAX(tc2.date) 
     FROM trading_calendar tc2 
     WHERE tc2.year = ep.entry_year
    ) AS year_sell_date

FROM entry_points ep
""")

Wait — there's a subtlety I want to flag. Two different rebalancing philosophies:

Option A — "Remainder of current period": Signal fires on Wednesday, you hold until that Friday. Short and variable holding period. This is what the query above does.

Option B — "Next full period": Signal fires on Wednesday, you enter at next Monday's close and sell next Friday's close. Always a full 5-day (or however many trading days that week has) holding period. This is what I had in the first message.

These produce very different backtest characteristics. Option A has variable hold lengths within a horizon (enter Thursday = 1-day hold for "weekly"). Option B is more consistent but has a variable lag between signal and entry.

Most systematic strategies I've seen use Option B or a hybrid — rebalance on a fixed schedule regardless of when signals update. Here's that version:
python

con.execute("""
CREATE OR REPLACE TABLE holding_periods_next_full AS
WITH entry_points AS (
    SELECT
        f.ticker,
        f.price_date,
        tc.date AS signal_date,       -- T: when you observe the signal
        tc.td_index,
        tc.iso_year, tc.iso_week,
        tc.year, tc.month, tc.quarter
    FROM features f
    JOIN trading_calendar tc_feat ON f.price_date = tc_feat.date
    JOIN trading_calendar tc ON tc.td_index = tc_feat.td_index + 1
)
SELECT
    ep.*,
    
    ------------------------------------------------------------------
    -- WEEKLY (next full week)
    -- Find first trading day of next ISO week = entry
    -- Last trading day of that same week = exit
    ------------------------------------------------------------------
    nw_entry.date AS week_buy_date,
    (SELECT MAX(tc2.date) 
     FROM trading_calendar tc2 
     WHERE tc2.iso_year = nw_entry.iso_year 
       AND tc2.iso_week = nw_entry.iso_week
    ) AS week_sell_date,
    
    ------------------------------------------------------------------
    -- MONTHLY (next full month)
    ------------------------------------------------------------------
    nm_entry.date AS month_buy_date,
    (SELECT MAX(tc2.date) 
     FROM trading_calendar tc2 
     WHERE tc2.year = nm_entry.year 
       AND tc2.month = nm_entry.month
    ) AS month_sell_date,
    
    ------------------------------------------------------------------
    -- QUARTERLY (next full quarter) 
    ------------------------------------------------------------------
    nq_entry.date AS quarter_buy_date,
    (SELECT MAX(tc2.date) 
     FROM trading_calendar tc2 
     WHERE tc2.year = nq_entry.year 
       AND tc2.quarter = nq_entry.quarter
    ) AS quarter_sell_date,
    
    ------------------------------------------------------------------
    -- YEARLY (next full year)
    ------------------------------------------------------------------
    ny_entry.date AS year_buy_date,
    (SELECT MAX(tc2.date) 
     FROM trading_calendar tc2 
     WHERE tc2.year = ny_entry.year
    ) AS year_sell_date

FROM entry_points ep

-- Next week's first trading day
JOIN trading_calendar nw_entry ON nw_entry.date = (
    SELECT MIN(tc2.date) FROM trading_calendar tc2
    WHERE (tc2.iso_year > ep.iso_year) 
       OR (tc2.iso_year = ep.iso_year AND tc2.iso_week > ep.iso_week)
)

-- Next month's first trading day  
JOIN trading_calendar nm_entry ON nm_entry.date = (
    SELECT MIN(tc2.date) FROM trading_calendar tc2
    WHERE (tc2.year > ep.year)
       OR (tc2.year = ep.year AND tc2.month > ep.month)
)

-- Next quarter's first trading day
JOIN trading_calendar nq_entry ON nq_entry.date = (
    SELECT MIN(tc2.date) FROM trading_calendar tc2
    WHERE (tc2.year > ep.year)
       OR (tc2.year = ep.year AND tc2.quarter > ep.quarter)
)

-- Next year's first trading day
JOIN trading_calendar ny_entry ON ny_entry.date = (
    SELECT MIN(tc2.date) FROM trading_calendar tc2
    WHERE tc2.year > ep.year
)
""")

Then the final return labels with terminal event handling:
python

con.execute("""
CREATE OR REPLACE TABLE return_labels AS
SELECT
    hp.ticker,
    hp.price_date,
    hp.signal_date,
    
    -- Weekly
    hp.week_buy_date,
    hp.week_sell_date,
    p_wb.close AS week_buy_price,
    COALESCE(ev_w.terminal_price, p_ws.close) AS week_sell_price,
    ev_w.terminal_event AS week_terminal_flag,
    (COALESCE(ev_w.terminal_price, p_ws.close) / p_wb.close) - 1.0 AS week_return,
    
    -- Monthly
    hp.month_buy_date,
    hp.month_sell_date,
    p_mb.close AS month_buy_price,
    COALESCE(ev_m.terminal_price, p_ms.close) AS month_sell_price,
    ev_m.terminal_event AS month_terminal_flag,
    (COALESCE(ev_m.terminal_price, p_ms.close) / p_mb.close) - 1.0 AS month_return,
    
    -- Quarter / Year same pattern...

FROM holding_periods_next_full hp

-- Price joins
LEFT JOIN daily_prices p_wb ON hp.ticker = p_wb.ticker AND hp.week_buy_date = p_wb.date
LEFT JOIN daily_prices p_ws ON hp.ticker = p_ws.ticker AND hp.week_sell_date = p_ws.date
LEFT JOIN daily_prices p_mb ON hp.ticker = p_mb.ticker AND hp.month_buy_date = p_mb.date
LEFT JOIN daily_prices p_ms ON hp.ticker = p_ms.ticker AND hp.month_sell_date = p_ms.date

-- Terminal events: did something happen during the holding window?
LEFT JOIN events ev_w 
    ON hp.ticker = ev_w.ticker 
    AND ev_w.event_date BETWEEN hp.week_buy_date AND hp.week_sell_date
    AND ev_w.terminal_event IS NOT NULL
LEFT JOIN events ev_m 
    ON hp.ticker = ev_m.ticker 
    AND ev_m.event_date BETWEEN hp.month_buy_date AND hp.month_sell_date
    AND ev_m.terminal_event IS NOT NULL
""")

The correlated subqueries in the "next full period" joins could be slow at scale. If that's an issue, you can pre-materialize a lookup table that maps each trading day to its next-period-entry date for each horizon — that turns all the correlated subqueries into simple equi-joins and should be very fast in DuckDB.

Our fundamentals are calculated daily but the return horizon will be linked to the rebalancing. So only consider signals on the rebalancing dates
2:05 PM

Good clarification. That simplifies things a lot. You only care about features computed on rebalancing dates, not every day. So the flow is:

Rebalance date R (e.g., first trading day of the month)
Features use price_date = R-1 (last trading day before R)
Buy at R close
Sell at period end close (or terminal event)

Let's pre-build the rebalance schedule from the trading calendar and then everything joins cleanly:
python

con.execute("""
-- Rebalance schedule: first and last trading day per period
CREATE OR REPLACE TABLE rebalance_schedule AS
SELECT
    iso_year, iso_week, year, month, quarter,
    
    -- Weekly
    MIN(date) FILTER (WHERE true) AS week_start,
    MAX(date) FILTER (WHERE true) AS week_end,
    
    -- Monthly  
    MIN(date) FILTER (WHERE true) AS month_start,
    MAX(date) FILTER (WHERE true) AS month_end,
    
    -- Quarterly
    MIN(date) FILTER (WHERE true) AS quarter_start,
    MAX(date) FILTER (WHERE true) AS quarter_end,
    
    -- Yearly
    MIN(date) FILTER (WHERE true) AS year_start,
    MAX(date) FILTER (WHERE true) AS year_end
    
FROM trading_calendar
GROUP BY iso_year, iso_week, year, month, quarter
""")

Actually, that's a bit awkward since the GROUP BY mixes horizons. Let me do it cleaner — one table per horizon, then union or keep separate:
python

con.execute("""
CREATE OR REPLACE TABLE rebalance_weeks AS
SELECT
    iso_year, iso_week,
    MIN(date) AS entry_date,
    MAX(date) AS exit_date
FROM trading_calendar
GROUP BY iso_year, iso_week;

CREATE OR REPLACE TABLE rebalance_months AS
SELECT
    year, month,
    MIN(date) AS entry_date,
    MAX(date) AS exit_date
FROM trading_calendar
GROUP BY year, month;

CREATE OR REPLACE TABLE rebalance_quarters AS
SELECT
    year, quarter,
    MIN(date) AS entry_date,
    MAX(date) AS exit_date
FROM trading_calendar
GROUP BY year, quarter;

CREATE OR REPLACE TABLE rebalance_years AS
SELECT
    year,
    MIN(date) AS entry_date,
    MAX(date) AS exit_date
FROM trading_calendar
GROUP BY year;
""")

Now features are only relevant on entry dates, and we need the prior trading day for the feature lookup:
python

con.execute("""
-- Weekly return labels
CREATE OR REPLACE TABLE weekly_labels AS
SELECT
    f.ticker,
    f.price_date,                -- T-1: feature reference date
    rw.entry_date AS buy_date,   -- T: buy at close
    rw.exit_date AS sell_date,   -- end of week: sell at close
    
    p_buy.close AS buy_price,
    COALESCE(ev.terminal_price, p_sell.close) AS sell_price,
    ev.terminal_event,
    
    (COALESCE(ev.terminal_price, p_sell.close) / p_buy.close) - 1.0 AS forward_return

FROM rebalance_weeks rw

-- Feature lookup: price_date is the trading day before entry
JOIN trading_calendar tc_entry ON rw.entry_date = tc_entry.date
JOIN trading_calendar tc_prev ON tc_prev.td_index = tc_entry.td_index - 1

-- Features keyed on price_date (the T-1 date)
JOIN features f ON f.price_date = tc_prev.date

-- Entry and exit prices
LEFT JOIN daily_prices p_buy 
    ON f.ticker = p_buy.ticker AND rw.entry_date = p_buy.date
LEFT JOIN daily_prices p_sell 
    ON f.ticker = p_sell.ticker AND rw.exit_date = p_sell.date

-- Terminal event during holding window
LEFT JOIN events ev 
    ON f.ticker = ev.ticker
    AND ev.event_date BETWEEN rw.entry_date AND rw.exit_date
    AND ev.terminal_event IS NOT NULL
);

-- Monthly return labels
CREATE OR REPLACE TABLE monthly_labels AS
SELECT
    f.ticker,
    f.price_date,
    rm.entry_date AS buy_date,
    rm.exit_date AS sell_date,
    
    p_buy.close AS buy_price,
    COALESCE(ev.terminal_price, p_sell.close) AS sell_price,
    ev.terminal_event,
    
    (COALESCE(ev.terminal_price, p_sell.close) / p_buy.close) - 1.0 AS forward_return

FROM rebalance_months rm

JOIN trading_calendar tc_entry ON rm.entry_date = tc_entry.date
JOIN trading_calendar tc_prev ON tc_prev.td_index = tc_entry.td_index - 1

JOIN features f ON f.price_date = tc_prev.date

LEFT JOIN daily_prices p_buy 
    ON f.ticker = p_buy.ticker AND rm.entry_date = p_buy.date
LEFT JOIN daily_prices p_sell 
    ON f.ticker = p_sell.ticker AND rm.exit_date = p_sell.date

LEFT JOIN events ev 
    ON f.ticker = ev.ticker
    AND ev.event_date BETWEEN rm.entry_date AND rm.exit_date
    AND ev.terminal_event IS NOT NULL
);

-- Quarterly and yearly follow identical pattern with rebalance_quarters / rebalance_years
""")

This is nice because:

    No wasted computation — you only join features on rebalance dates, not every trading day
    Calendar-anchored — entry is always first trading day of the period, exit is always last, no drift
    The T-1 lookup is exact — td_index - 1 handles holidays/weekends correctly since the calendar only contains trading days
    Terminal events slot in naturally with the COALESCE pattern

One thing to watch: if your features table has every ticker × every trading day, that JOIN features f ON f.price_date = tc_prev.date will pull the full cross-section for each rebalance date, which is what you want. But if a ticker is missing features on that specific date (data gap), it silently drops out. You might want a LEFT JOIN from a universe table instead if you want to flag missing coverage.

here is the code as it stands. I havent factored in anything from our conversation. how far are we from what we want. assume the events handling is good
2:07 PM

Looking at your code, the core issue is straightforward: you're using naive trading-day offsets (rn + N) instead of calendar-anchored periods, and there's no T-1 lag for feature observability.

Let me break down what needs to change:

Problem 1: Trading day offset drift

Your current approach:

rn_t + 5  → "weekly"
rn_t + 21 → "monthly"

This means if the grid date is a Wednesday, your "weekly" exit is the following Wednesday. That's not a week — it's a rolling 5-day window that drifts with the entry point. Same issue compounds for monthly/quarterly/yearly.

Problem 2: No rebalancing alignment

You compute labels for every (ticker, date) in the grid. But you said features rebalance on fixed dates (first trading day of the week/month/quarter/year). Most of these grid rows will never be used as entry points for a given horizon. You're doing a lot of unnecessary work and the labels themselves are conceptually wrong for a calendar-aligned strategy.

Problem 3: No T-1 lag

Features are computed from yesterday's close but you're using the same date as both signal and entry. You need to buy at T close using T-1 features.

What's fine as-is:

    Terminal event resolution logic — solid
    The sep_ranked approach with rn for trading-day arithmetic — keep this, it's useful for the T-1 lookup
    Temp parquet per horizon to limit memory — good pattern
    The delist flag / delist type handling

The fix is fairly contained. You need to:

    Build a rebalance calendar (first/last trading day per period) — small table
    For each horizon, only generate labels for rows where grid.date equals that horizon's entry date
    Entry price = entry_date close, exit price = exit_date close (last trading day of that period)
    Features join on T-1 (the trading day before entry_date via rn - 1)
    Terminal logic stays the same but the "did it terminate within the window" check uses the calendar-anchored exit date instead of rn + N

Want me to rewrite the horizon loop with these changes? The structure stays almost identical — the main diff is swapping rn_t + N for a join against the rebalance calendar to get the exit date, and filtering the grid to rebalance-only dates per horizon.

show me a roadmap of what that would look like. or some example code and instructions i can show to cursor
2:08 PM

Here's a clear roadmap with code examples:
Roadmap: Calendar-Anchored Forward Labels
Step 1: Build Trading Calendar with Period Labels

Add this right after sep_ranked is created. This only needs to run once.
python

# Build from the distinct trading dates already in sep_ranked
con.execute("""
    CREATE OR REPLACE TABLE trading_calendar AS
    SELECT
        date,
        ROW_NUMBER() OVER (ORDER BY date) AS td_index,
        date_part('isoyear', date) AS iso_year,
        date_part('week', date) AS iso_week,
        date_part('year', date) AS year,
        date_part('month', date) AS month,
        date_part('quarter', date) AS quarter
    FROM (SELECT DISTINCT date FROM sep_ranked)
    ORDER BY date
""")

Step 2: Build Rebalance Schedules

One table per horizon. Entry = first trading day of period, exit = last trading day.
python

HORIZON_DEFS = {
    5:   ('iso_year, iso_week', 'weekly'),
    10:  ('iso_year, iso_week', 'biweekly'),   # see note below
    21:  ('year, month', 'monthly'),
    63:  ('year, quarter', 'quarterly'),
    126: ('year, quarter', 'semi_annual'),      # see note below
    252: ('year', 'annual'),
}

Decision needed for 10td and 126td: These don't map cleanly to a single calendar period. Options:

    10td (biweekly): Use every-other-week rebalancing (pick odd ISO weeks), or just use 2-week rolling from month start. Simplest: keep as rn + 10 offset but only on weekly entry dates.
    126td (semi-annual): Rebalance on Q1/Q3 start (Jan, Jul), hold 6 months. Or rebalance quarterly and hold 2 quarters.

For the clean calendar periods (5, 21, 63, 252):
python

con.execute("""
    CREATE OR REPLACE TABLE rebalance_weekly AS
    SELECT
        iso_year, iso_week,
        MIN(date) AS entry_date,
        MAX(date) AS exit_date
    FROM trading_calendar
    GROUP BY iso_year, iso_week
""")

con.execute("""
    CREATE OR REPLACE TABLE rebalance_monthly AS
    SELECT
        year, month,
        MIN(date) AS entry_date,
        MAX(date) AS exit_date
    FROM trading_calendar
    GROUP BY year, month
""")

con.execute("""
    CREATE OR REPLACE TABLE rebalance_quarterly AS
    SELECT
        year, quarter,
        MIN(date) AS entry_date,
        MAX(date) AS exit_date
    FROM trading_calendar
    GROUP BY year, quarter
""")

con.execute("""
    CREATE OR REPLACE TABLE rebalance_annual AS
    SELECT
        year,
        MIN(date) AS entry_date,
        MAX(date) AS exit_date
    FROM trading_calendar
    GROUP BY year
""")

Step 3: Refactor the Horizon Loop

Replace the current for N in HORIZONS_TD loop. The key change: instead of rn_t + N, join against the rebalance table to get exit_date.
python

# Map horizon to rebalance table and whether to use offset fallback
HORIZON_CONFIG = {
    5:   {'table': 'rebalance_weekly',    'offset_fallback': False},
    10:  {'table': 'rebalance_weekly',    'offset_fallback': True, 'offset': 10},
    21:  {'table': 'rebalance_monthly',   'offset_fallback': False},
    63:  {'table': 'rebalance_quarterly', 'offset_fallback': False},
    126: {'table': 'rebalance_quarterly', 'offset_fallback': True, 'offset': 126},
    252: {'table': 'rebalance_annual',    'offset_fallback': False},
}

for N, cfg in HORIZON_CONFIG.items():
    rebal_table = cfg['table']

    if cfg.get('offset_fallback'):
        # Hybrid: use calendar entry dates but offset-based exit
        # Enter on rebalance entry_date, exit rn + offset
        offset = cfg['offset']
        con.execute(f"""
            CREATE OR REPLACE VIEW grid_cur AS
            SELECT
                g.ticker, g.date,
                s.closeadj AS price_t,
                s.rn AS rn_t,
                rb.entry_date,
                rb.exit_date AS rebal_exit  -- not used for exit, just for filtering
            FROM grid g
            INNER JOIN sep_ranked s ON s.ticker = g.ticker AND s.date = g.date
            INNER JOIN {rebal_table} rb ON g.date = rb.entry_date
            -- ^^^ This is the key filter: only grid rows that ARE rebalance entry dates
        """)

        con.execute(f"""
            CREATE OR REPLACE VIEW fwd_N AS
            SELECT
                g.ticker, g.date,
                g.price_t, g.rn_t,
                f.date AS date_n,
                f.closeadj AS price_n,
                f.rn AS rn_n
            FROM grid_cur g
            LEFT JOIN sep_ranked f ON f.ticker = g.ticker AND f.rn = g.rn_t + {offset}
        """)
    else:
        # Pure calendar-anchored: enter on entry_date, exit on exit_date
        con.execute(f"""
            CREATE OR REPLACE VIEW grid_cur AS
            SELECT
                g.ticker, g.date,
                s.closeadj AS price_t,
                s.rn AS rn_t,
                rb.exit_date
            FROM grid g
            INNER JOIN sep_ranked s ON s.ticker = g.ticker AND s.date = g.date
            INNER JOIN {rebal_table} rb ON g.date = rb.entry_date
        """)

        # Exit price: join sep_ranked on (ticker, exit_date) instead of rn + N
        con.execute(f"""
            CREATE OR REPLACE VIEW fwd_N AS
            SELECT
                g.ticker, g.date,
                g.price_t, g.rn_t,
                f.date AS date_n,
                f.closeadj AS price_n,
                f.rn AS rn_n
            FROM grid_cur g
            LEFT JOIN sep_ranked f ON f.ticker = g.ticker AND f.date = g.exit_date
        """)

    # From here, terminal_row / last_day / labels_N logic stays IDENTICAL
    # ... (existing terminal_row, last_day, labels_N views unchanged)

Step 4: Add T-1 Feature Lag Column

Add a feature_date column to the output so downstream knows which date's features to use.
python

# After grid_cur, add:
con.execute("""
    CREATE OR REPLACE VIEW grid_with_lag AS
    SELECT
        gc.*,
        tc_prev.date AS feature_date  -- T-1: the date whose features are observable at T
    FROM grid_cur gc
    JOIN trading_calendar tc ON tc.date = gc.date
    JOIN trading_calendar tc_prev ON tc_prev.td_index = tc.td_index - 1
""")

Include feature_date in the final labels output so when you join features later:
sql

-- In the training pipeline:
SELECT f.*, l.*
FROM features f
JOIN forward_labels l ON f.ticker = l.ticker AND f.price_date = l.feature_date

Step 5: Output Schema Change

The output parquet changes shape. Before: one row per (ticker, date) in grid with all horizons. After: rows only exist for rebalance entry dates, and different horizons have different entry dates.

Two options:

Option A — Keep wide, sparse: Same schema but most cells are NULL (a monthly entry date won't have weekly labels unless it happens to also be a Monday). This is simplest and compatible with the current temp-parquet-join pattern.

Option B — Separate parquets per horizon: weekly_labels.parquet, monthly_labels.parquet, etc. Cleaner, no sparsity, but changes downstream consumers.

I'd go with Option A to minimize downstream breakage. The INNER JOIN {rebal_table} rb ON g.date = rb.entry_date inside each horizon naturally produces NULLs for non-rebalance dates when you LEFT JOIN back to the full grid in the final assembly step.
Summary of Changes
What	Before	After
Exit date	rn + N (drifts)	Calendar period end date
Entry dates	Every grid date	Only rebalance dates per horizon
Feature ref	Same as grid date	feature_date = T-1 (new column)
Terminal logic	Unchanged	Unchanged
10td / 126td	rn + N	Hybrid: calendar entry, offset exit
Output shape	Dense wide	Sparse wide (Option A)

The terminal_row / last_day / labels_N assembly after fwd_N is created doesn't change at all — that's the part you said is good, and it works regardless of how fwd_N gets its exit date.

ignore 10td and 126td. keep weekly monthly quarterly. 

why does sep_ranked give us what we need

what prices are you using? closeadj to closeadj? 

how performant will this be.
2:11 PM

Good questions. Let me address each.
Why sep_ranked works

sep_ranked is your trading calendar per ticker. It only contains dates where that ticker actually traded (had a valid closeadj), and the rn column gives you a dense integer index over those dates. This is what makes td_index - 1 give you "previous trading day" correctly — no weekends, no holidays, no gaps. It also means the terminal logic works: when a stock stops trading, its max rn is the last real price, and you can detect "no forward data" cleanly.

The global trading_calendar I'm adding is built from the distinct union of all trading dates in SEP. It gives you the market-level calendar for period boundaries (when does this ISO week start/end). Then sep_ranked handles the per-ticker question of "does this ticker have a price on that date."

You need both: the global calendar for rebalance scheduling, and the per-ticker ranked series for price lookups and terminal detection.
Prices: closeadj to closeadj

Yes, the current code uses closeadj throughout — entry price is closeadj on the entry date, exit price is closeadj on the exit date (or terminal date). That's close-to-close adjusted returns, which is what you want. The adjustment handles splits and dividends so the return calculation (price_n / price_t) - 1.0 gives you total return.

One thing to verify: make sure your SEP data's closeadj is point-in-time adjusted (adjusted as of each date) vs. back-adjusted from today. If it's back-adjusted from a single reference point, the ratios are fine and the returns are correct. If it's point-in-time adjusted (adjustment factor changes over time), you'd need to use the same adjustment vintage for both entry and exit. Sharadar SEP is back-adjusted from the latest date, so the ratio works correctly.
Performance

The rebalance filter is actually a performance win over what you have now. Here's why:

Before: You compute labels for every (ticker, date) in the grid. If you have 5,000 tickers × 5,000 trading days, that's 25M rows going through the fwd_N join for each horizon.

After with calendar anchoring:

    Weekly: ~260 entry dates/year → maybe 20% of grid rows
    Monthly: ~12 entry dates/year → ~5% of grid rows
    Quarterly: ~4 entry dates/year → ~1.5% of grid rows
    Yearly: 1 entry date/year → ~0.4% of grid rows

The INNER JOIN {rebal_table} rb ON g.date = rb.entry_date filters the grid down before any of the expensive per-ticker price lookups happen. Monthly labels process 20x fewer rows than before. Quarterly is 65x fewer. The terminal logic, which is the most complex part with multiple LEFT JOINs, runs on dramatically fewer rows.

The rebalance tables themselves are tiny — rebalance_weekly has maybe 1,400 rows for 27 years of data. Building them is instant.

The one cost is the final assembly: LEFT JOINing sparse horizon parquets back to the full grid. But that's a merge join on (ticker, date) over parquet files, which DuckDB handles very efficiently. And if you go with separate parquets per horizon (Option B) instead of the wide sparse table, you skip that entirely.

Net effect: This should be meaningfully faster than what you have, not slower.

review your implementation and let me know if there any other things we should consider or need answered before we can proceed
2:12 PM

Let me think through this carefully.
Issues to Resolve
1. Grid output shape — what does downstream expect?

Right now your grid is every (ticker, date) and the output is one wide parquet with all horizons. If we filter to rebalance-only dates per horizon, the rows for weekly vs monthly vs quarterly are different sets. The final wide join back to the full grid will be very sparse — a row that's a monthly entry but not a weekly entry will have NULL weekly columns.

Question: Does your downstream training pipeline join on (ticker, date) expecting every grid row to have labels? Or can it consume separate parquets per horizon? Separate parquets is cleaner and avoids a massive sparse table, but it changes the interface.
2. Feature date: which T-1 are we talking about?

I proposed feature_date = td_index - 1 relative to entry_date. But your features pipeline already computes features for every (ticker, date) in the grid keyed on price_date. So if entry is the first Monday of the month, feature_date is the prior Friday.

Question: Is your features parquet keyed on price_date where that date is the close date used to compute the features? If so the join is just features.price_date = labels.feature_date and we're good. Just confirming the key.
3. Weekly: partial weeks at month/year boundaries

ISO weeks can span year boundaries (e.g., Dec 29 might be ISO week 1 of next year). This is fine for the rebalance schedule itself, but if your grid date range starts on a Wednesday, the first "weekly" entry_date is that Wednesday (first trading day of that ISO week). That first period is a partial week — 3 trading days instead of 5.

Options: Either accept partial first/last weeks, or filter out weeks that have fewer than N trading days. I'd accept them — the return is still close-to-close over the actual holding period, and fwd_holding_days captures the actual length.
4. fwd_holding_days calculation changes

Currently it's always N or (term_rn - rn_t) for terminal cases. With calendar anchoring, the "normal" holding days aren't a fixed N anymore — a week might be 4 trading days if there's a holiday. You should compute it from the actual entry/exit rn difference:
sql

-- Instead of hardcoding N:
CASE
    WHEN f.rn IS NOT NULL THEN (f.rn - g.rn_t)::INTEGER
    WHEN t.term_rn IS NOT NULL THEN (t.term_rn - g.rn_t)::INTEGER
    ELSE NULL
END AS fwd_holding_days

This is more accurate and you already have the rn values to do it.
5. Terminal event window check

Current terminal logic checks if the stock ran out of prices before rn + N. With calendar anchoring, the question becomes: did the stock run out of prices before the exit_date? The LEFT JOIN sep_ranked f ON f.ticker = g.ticker AND f.date = g.exit_date returning NULL already tells you this. But there's a subtle case: what if the stock is delisted and then the SEP data just stops, but the exit_date is after the last price? The current sep_max_rn logic handles this — price_n IS NULL triggers the terminal path. This still works with calendar anchoring since we're joining on date instead of rn, and a missing date means no price. No change needed.
6. Entry date with no price for a ticker

Calendar says first trading day of the month is Jan 2. Most stocks trade, but ticker XYZ was halted or just IPO'd on Jan 5. The INNER JOIN sep_ranked s ON s.ticker = g.ticker AND s.date = g.date in grid_cur will drop XYZ for that period entirely.

Question: Is that acceptable? The alternative is using the first available price after entry_date as the buy price, but that introduces complexity and a different effective entry date per ticker. I'd say dropping is correct — if you can't buy at close on rebalance day, you don't enter that position.
7. Do you want exit_date in the output?

Currently you output fwd_holding_days but not the actual exit date. With calendar anchoring, having entry_date (= date), exit_date, and feature_date in the output makes the labels self-documenting and easier to debug. Low cost, high value.

The only real blocker is #1 — the output shape question. Everything else I'd either accept the simple answer or it's a minor code tweak. What does your downstream pipeline expect?

    yeah its ticker, date. so its acceptable to have nulls. we will have to guard against that in the experiment

2:15 PM

Got it. So wide sparse table it is — the existing pattern works, just with NULLs where a date isn't a rebalance entry for that horizon. Downstream experiments filter to non-NULL labels for the horizon they care about.

Let me address the rest then:

    #2 (feature key): I'll include feature_date in the output. You confirm the features parquet key and wire up the join downstream.
    #3 (partial weeks): Accept them. fwd_holding_days tells you the actual length.
    #4 (holding days): Use (exit_rn - entry_rn) instead of hardcoded N. Trivial change.
    #5 (terminal window): No change needed — already works.
    #6 (no price on entry): Drop the ticker for that period. Correct behavior — can't buy what you can't trade.
    #7 (exit_date in output): I'll add exit_date and feature_date per horizon to the output columns.

So the output schema per horizon becomes:

fwd_ret_{horizon}
fwd_holding_days_{horizon}
fwd_delisted_{horizon}
fwd_delist_type_{horizon}
fwd_exit_date_{horizon}
fwd_feature_date_{horizon}

And the horizons are now:
python

HORIZON_CONFIG = {
    5:   'rebalance_weekly',
    21:  'rebalance_monthly',
    63:  'rebalance_quarterly',
    252: 'rebalance_annual',
}

I think this is ready to implement. Want me to write the full revised script?

here is the code we use to merge everything at the end

it will answer some questions and also need to be updated
2:16 PM

Looking at the merge script, a few things become clear:

Questions answered:

    Features join on (ticker, date) where date is the grid date — confirmed. So feature_date in the labels output lets downstream do features.price_date = labels.feature_date but the merge itself still joins on labels.date = universe.date. The feature lag is something you apply at experiment time, not at merge time. That's fine.
    The merge currently pulls all 6 horizons (5, 10, 21, 63, 126, 252). We're dropping 10 and 126, so the merge needs updating.

Changes needed in merge (08_merge):

    Remove fwd_ret_10td, fwd_holding_days_10td, fwd_delisted_10td, fwd_delist_type_10td — all 10td columns
    Remove all 126td columns
    Add per-horizon: fwd_exit_date_{N}td, fwd_feature_date_{N}td for N in (5, 21, 63, 252)
    Update the empty schema fallback to match
    Update validation if any of it assumes specific horizons

One thing to flag: The merge joins forward_labels l ON l.ticker = u.ticker AND l.date = u.date. With calendar-anchored labels, most (ticker, date) rows won't have weekly labels (only Monday entries do), won't have monthly labels (only first-of-month entries do), etc. The LEFT JOIN handles this — those columns are just NULL. But your validation at the bottom doesn't currently check label null rates. If someone sees 95% NULL on fwd_ret_63td and panics, that's expected. Might be worth adding a log line showing the non-null rate per horizon so it's obvious.

Here's the roadmap for cursor — both files:
07_labels changes:

    After sep_ranked, build trading_calendar from distinct dates
    Build rebalance_weekly, rebalance_monthly, rebalance_quarterly, rebalance_annual tables
    Change HORIZONS_TD from (5, 10, 21, 63, 126, 252) to a dict:

python

HORIZON_CONFIG = {
    5:   'rebalance_weekly',
    21:  'rebalance_monthly',
    63:  'rebalance_quarterly',
    252: 'rebalance_annual',
}

    In the horizon loop, replace INNER JOIN sep_ranked f ON f.rn = g.rn_t + {N} with INNER JOIN {rebal_table} rb ON g.date = rb.entry_date for filtering, and LEFT JOIN sep_ranked f ON f.ticker = g.ticker AND f.date = rb.exit_date for exit price
    Compute fwd_holding_days as (exit_rn - entry_rn) not hardcoded N
    Add fwd_exit_date and fwd_feature_date (via td_index - 1 on the global trading calendar) to each horizon's output
    Update the final wide join and output schema to reflect 4 horizons × 6 columns

08_merge changes:

    Drop all 10td and 126td column references from the SELECT, empty schema, and validation
    Add fwd_exit_date_{N}td (DATE) and fwd_feature_date_{N}td (DATE) for N in (5, 21, 63, 252)
    Add a validation log line showing non-null percentage per horizon label so sparse NULLs don't look like bugs:

python

for h in ['5td', '21td', '63td', '252td']:
    rate = con.execute(f"SELECT AVG(CASE WHEN fwd_ret_{h} IS NOT NULL THEN 1.0 ELSE 0.0 END) FROM master_features").fetchone()[0]
    log.info("  fwd_ret_%s non-null: %.1f%%", h, rate * 100)

Want me to write the full revised files?


Claude is AI and can make mistakes. Please double-check responses.

