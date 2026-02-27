This is really clean data. The key findings:
Acquisitions are a non-problem. Gap is 0 days across the board, last SEP price reflects the deal price (DNKN pinned at ~$106.48, TIF at ~$131.46, ETFC at ~$49.26). The final day often shows volume = 1 or a huge spike (last cross). No price adjustment needed for acquisitions — the return series is already correct.
Bankruptcies are where the bias lives. Median 252-day return of -85.9% shows the price series does capture most of the decline. But MON is interesting — it's tagged bankruptcyliquidation but last traded at $10.07 with flat returns, which looks more like a structured wind-down than a zero-recovery bankruptcy. So blindly appending closeadj = 0 for all bankruptcies would be wrong for some cases.
The mergerfrom row is broken. Average gap of -3,042 days and 127 out of 129 events showing "event before last price" means the ACTIONS date for mergerfrom is the original merger date, but the ticker continued trading under a new name or structure. Don't use mergerfrom for terminal event detection — use delisted paired with acquisitionby instead.
What you actually need to do:

Add delist_type to universe — join ACTIONS on ticker and date to tag each delist with its cause. Since delisted always co-occurs with the specific reason (acquisitionby, bankruptcyliquidation, etc.), join on the specific action, not the generic delisted.
Split forward labels — fwd_bankruptcy_30d and fwd_acquired_30d instead of a single fwd_delisted_30d. For a Dreman-style strategy these are opposite signals: acquisition is a positive outcome (premium capture), bankruptcy is capital destruction.
Skip synthetic terminal prices — the data shows gap = 0 for all event types except the broken mergerfrom. SEP already captures the terminal price. The bankruptcy return series already shows the decline. Adding a synthetic $0 row would double-count the loss for any ticker that already traded down to pennies.
Ignore mergerfrom — the date semantics are wrong for your use case. If you need merger info, use the acquisitionby side which has correct dates and gaps.

Section 7 on universe flag timing came back empty — did HTZ not make it into the universe, or was the date range off?we do have HTZ. not much dataduckdb.sql("select max(datekey), min(datekey) from sf1 where ticker = 'HTZ'")
┌──────────────┬──────────────┐
│ max(datekey) │ min(datekey) │
│   varchar    │   varchar    │
├──────────────┼──────────────┤review your work. are you confident in your output. we are trying to make an instiutional backtest ready dataset11:55 AMYou're right to push back. Let me reconsider.
On "skip synthetic terminal prices": I was wrong to dismiss this. The fact that SEP gap = 0 and bankruptcies show -85.9% median return over 252 days doesn't mean the terminal value is captured correctly. A stock that traded at $10 on its last day but the company's equity is worth $0 in liquidation — that -85.9% is from $70 to $10, but the investor's actual outcome is $70 to $0. The last traded price on SEP is not the realized return for a holder through bankruptcy. For an institutional backtest, you need the actual economic outcome, not just the last exchange print.
On "acquisitions are a non-problem": I was too quick here too. The last SEP price reflects the deal price, but that's the last day. For forward return calculations in your feature table, you need to know: if I held this stock on day T, what's my return at T+30? If the stock gets acquired at T+15, the return is (deal price / price_T) - 1, and then the capital is returned. Your current ret_1m likely goes NULL after the last SEP date, which means any forward return window that spans a delist date is either NULL or truncated. That's not wrong per se, but it means your model can't learn from the actual outcomes of holding stocks that get acquired. An institutional dataset would carry the terminal return through.
On mergerfrom being "broken": The negative gaps probably mean the ticker was recycled — Sharadar appends a number to delisted tickers that get reused. The mergerfrom date is correct for the original merger, but last_prices picks up the recycled ticker's later prices. So it's not broken data, it's a flaw in my study query that doesn't account for ticker recycling. Before dismissing mergerfrom entirely I should have flagged this.
On the HTZ question: HTZ has no SF1 data in your database at all — empty result. That means it might be in SEP and ACTIONS but never made it into the universe because the universe join requires fundamentals. This is actually a survivorship bias issue: companies that go bankrupt sometimes stop filing before they delist, so they drop out of your universe before the terminal event. Your backtest would never hold HTZ in the months before bankruptcy because it's not in the grid — which might be the correct behavior (you can't compute fundamental features without filings), but it should be documented as a known limitation.
What actually needs to happen for institutional quality:

Verify how many bankruptcies are missing from the universe entirely because SF1 filings stopped before the delist date — that's the real survivorship bias question
For tickers that ARE in the universe, forward returns that span a terminal event need to reflect the economic outcome (deal price for acquisitions, recovery value or zero for bankruptcies), not just go NULL
The mergerfrom query needs to filter by ticker date ranges (use TICKERS.lastpricedate) to avoid the recycled ticker issue before drawing conclusions about that event type
HTZ specifically — check SEP and ACTIONS to see if it has price data even without SF1, and trace why it's missing from fundamentals
>>> duckdb.sql("select * from sf1 where ticker = 'HTZ'")
┌─────────┬───────────┬──────────────┬────────────┬──────────────┬──────────────┬─────────────┬───┬───────────────┬───────────┬──────────────┬────────────────┬────────┬────────────────┐
│ ticker  │ dimension │ calendardate │  datekey   │ reporpastedwe have htz records. yes we do need to factor in the events into the forrward returns. We also need to make sure that the flags time horizon matches the ret time horizon we are calculating from. how do we do 1.11:57 AMGood catch on the flag/return horizon alignment — that's exactly the kind of thing that introduces subtle lookahead bias if mismatched.
For #1, let me think about what's actually happening with HTZ. The SF1 data shows:

No ARQ filings during the bankruptcy period (filed Ch11 May 2020, emerged late 2021)
First post-emergence ARQ is 2021-Q2 with datekey 2021-08-09
The MRQ/MRT rows during 2020 have datekey = reportperiod (not a real filing date — that's how Sharadar handles restated dimensions)

So HTZ is in SF1, but the pipeline's ARQ filter dimension = 'ARQ' finds no filings between roughly Feb 2020 and Aug 2021. If your universe construction requires a valid fundamental filing within some staleness window, HTZ drops out during the bankruptcy period — which is actually when you most need it for the delist labels.
For verifying how many bankruptcies are missing from the universe, the query is straightforward. You want to join terminal bankruptcy events against the universe and check coverage:
sqlWITH bankruptcies AS (
    SELECT ticker, 
           CAST(date AS DATE) AS event_date
    FROM actions 
    WHERE action = 'bankruptcyliquidation'
),
-- For each bankruptcy, check if the ticker appears in universe
-- in the 90 days before the event (the window where fwd_delisted flags matter)
universe_coverage AS (
    SELECT b.ticker,
           b.event_date,
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
    -- Of those in universe, how many have coverage right up to the event?
    SUM(CASE WHEN DATEDIFF('day', last_universe_date, event_date) <= 5 THEN 1 ELSE 0 END) AS coverage_to_event,
    -- How many dropped out early (last universe date > 30 days before event)?
    SUM(CASE WHEN universe_days_in_window > 0 
             AND DATEDIFF('day', last_universe_date, event_date) > 30 THEN 1 ELSE 0 END) AS dropped_early
FROM universe_coverage
Run that and also the per-ticker detail for the ones that dropped early — those are your survivorship bias cases. Then you'll know the scale of the problem before deciding how to fix it.
The fix options depend on what you find. If most bankruptcies have universe coverage up to the event, then the forward return adjustment is the main work. If a large fraction drops out months before the event because filings stopped, you need to decide whether to keep stale tickers in the universe (with NULL fundamentals but valid price data) specifically so the delist labels and terminal returns are captured.we do have a permaticker. we want to handle cases where a ticker is renamed. that can throw off the returns.11:59 AMRight — ticker changes are the silent killer for return series. If TICKER_A becomes TICKER_B on some date, your LAG-based return calculation sees TICKER_A's last price and TICKER_B's first price as two separate tickers. The return across the rename is lost, and worse, both tickers show a truncated series — TICKER_A looks like it delisted and TICKER_B looks like an IPO.
The permaticker from the TICKERS table solves this because it's stable across renames. The question is where in the pipeline to apply it.
The cleanest approach is to resolve at the SEP level before anything else touches it. You want a mapping of (ticker, date) → permaticker so that the price series is continuous across renames. Let me check what the TICKERS and ACTIONS tables give us to build that mapping.
sql-- What does a ticker change look like in ACTIONS?
SELECT ticker, CAST(date AS DATE) AS date, action, contraticker, name
FROM actions 
WHERE action IN ('tickerchangefrom', 'tickerchangeto')
ORDER BY date DESC
LIMIT 20
sql-- What does TICKERS give us?
SELECT permaticker, ticker, name, firstpricedate, lastpricedate, isdelisted
FROM tickers
WHERE permaticker IN (
    -- Find permatickers with multiple tickers
    SELECT permaticker FROM tickers 
    WHERE table = 'SEP'
    GROUP BY permaticker 
    HAVING COUNT(DISTINCT ticker) > 1
)
ORDER BY permaticker, firstpricedate
LIMIT 30
Run those two and we can see the shape of the problem — how many permatickers map to multiple tickers, and whether the date ranges are contiguous. From there the implementation is:

Build a permaticker_map view: for each (ticker, date) in SEP, resolve to permaticker using TICKERS date ranges
In 03_price_features.py, compute returns partitioned by permaticker instead of ticker, so the LAG window spans renames
The rest of the pipeline (universe, fundamentals, merge) can continue using ticker as the key, since SF1 filings use the ticker that was active at filing time

The tricky part is that SF1 datekeys reference the ticker at the time of filing, and SEP uses the ticker at the time of the price. If a company renamed between a filing and a price date, the ASOF join in 02_fundamentals could miss the match. That's where permaticker as a join key (or a ticker-chain lookup) becomes necessary for the fundamental-to-price join too.are you sure. review that12:00 PMYou're right, let me reconsider.
Sharadar already handles ticker changes internally. From the TICKERS docs: "Where a company is delisted and the ticker subsequently recycled for use by a different company, we utilise that ticker for the currently active company and append a number to the ticker of the delisted company." And the tickerchangefrom/tickerchangeto ACTIONS entries document the history.
So in SEP, if a company renames from TICKER_A to TICKER_B, Sharadar retroactively updates all historical prices to use TICKER_B. The entire price history is under the current ticker. Same for SF1 — all filings get the current ticker. That's why permaticker exists as a stable ID, but the ticker column is already consistent within each table at any point in time.
This means:

Return calculations using PARTITION BY ticker in SEP are already correct across renames — there's no break in the series
The ASOF join from fundamentals to prices on ticker is also correct — both tables use the same current ticker
The tickerchangefrom/tickerchangeto actions are informational, not something you need to resolve in the pipeline

Where permaticker actually matters is if you're joining across Sharadar tables that were snapshotted at different times, or if you're joining to external data sources that use historical tickers. Within a single Sharadar export, ticker should be consistent.
The real question is: have you verified this assumption? Run a quick check:
sql-- Do any tickers in SEP not appear in SF1, or vice versa?
-- If Sharadar retroactively renames, these should be consistent
SELECT s.ticker, COUNT(*) AS sep_days
FROM sep s
LEFT JOIN (SELECT DISTINCT ticker FROM sf1) f ON f.ticker = s.ticker
WHERE f.ticker IS NULL
GROUP BY s.ticker
ORDER BY sep_days DESC
LIMIT 20
That would confirm whether the ticker namespace is actually unified across tables in your export, or if there are mismatches that need permaticker to resolve.duckdb.sql(query)
┌─────────┬──────────┐
│ ticker  │ sep_days │
│ varchar │  int64   │
├─────────┼──────────┤
│ HIFS    │     7080 │
│ CRD.B   │     7080 │
│ RUSHB   │     7080 │
│ BBAR    │     7080 │
│ SAN     │     7080 │
│ WLYB    │     7080 │
│ AGM.A   │     7080 │
│ GTN.A   │     7080 │
│ MOG.B   │     7080 │
│ BRK.A   │     7080 │
│ HMY     │     7080 │
│ DRD     │     7080 │
│ KELYB   │     7080 │
│ BF.A    │     7080 │
│ BBVA    │     7080 │
│ SENEB   │     7079 │
│ DGICB   │     7079 │
│ AKO.A   │     7079 │
│ WSO.B   │     7079 │
│ HVT.A   │     7078 │
├─────────┴──────────┤
│ 20 rows  2 columns │
└────────────────────┘Show more12:04 PMThese are mostly secondary share classes (BRK.A, BF.A, MOG.B, CRD.B) and ADRs (BBAR, SAN, BBVA, HMY, DRD). They have prices in SEP but no fundamentals in SF1 because Sharadar files fundamentals under the primary ticker only — BRK.B has the SF1 data, not BRK.A, etc.
This isn't a ticker rename problem. These are tickers that legitimately exist in SEP but will never have SF1 data. Your universe construction should already exclude them if it requires a fundamental join, which is correct behavior — you can't compute PE or ROIC for a secondary share class without its own filings.
So the original concern about ticker renames breaking returns is a non-issue in Sharadar. The namespace is unified and retroactively updated. Permaticker is useful if you ever need to join to external data or track entity identity across time, but for your internal pipeline the ticker key is consistent.
Which means the real work is what you identified: getting the terminal events into forward returns with properly aligned horizons. Want to spec that out?yes explain that to me12:04 PMRight now your forward return labels and forward return features have a mismatch problem at terminal events. Let me walk through it.
What you currently have:
Your price features compute ret_1m as a backward-looking return: price today / price 21 trading days ago - 1. Your universe computes fwd_delisted_30d as a flag: will this ticker delist within the next 30 calendar days.
But you don't have a forward return feature — something like fwd_ret_1m: what return would I earn if I bought today and held for 21 trading days. For a backtest, that's what you actually need as your target variable. The backward return is a feature; the forward return is the label.
The problem at terminal events:
Say a stock has 10 trading days left before it gets acquired at $50. You're sitting on day T, the stock is at $45.

fwd_delisted_30d = 1 (correctly flags the upcoming delist)
But what's fwd_ret_1m? If you compute it as price at T+21 / price at T - 1, there is no price at T+21 because the stock stopped trading at T+10. So it's NULL.

For a backtest this is wrong. The actual economic outcome of buying at $45 on day T is: you hold for 10 days, receive $50 at acquisition close, and then your capital is returned. Your realized return is $50/$45 - 1 = 11.1%, earned over 10 days instead of 21. The stock didn't vanish — you got paid.
For bankruptcy it's the opposite problem. Stock is at $2 on day T, goes to $0 at bankruptcy. Your realized return is -100%, but your pipeline shows NULL because there's no price at T+21.
What needs to happen:
For every row where the forward return window overlaps a terminal event, you need to compute the return using the terminal price instead of the price at T+N:
fwd_ret_1m = terminal_price / price_today - 1
Where terminal_price depends on event type:

Acquisition: last SEP closeadj (the deal price — your study confirmed this is captured correctly with 0-day gap)
Bankruptcy: you need to decide — $0 (full loss), or some recovery fraction. For the top 1500 by market cap, most bankruptcies in Ch11 do have some recovery, but for equityholders it's usually near zero. Using $0 is the conservative and standard choice.
Regulatory/voluntary delist: last SEP closeadj. The stock might trade OTC afterward but from your portfolio's perspective, you exit at the last exchange price.

The horizon alignment issue you flagged:
Your flag fwd_delisted_30d uses a 30 calendar day window. Your return fwd_ret_1m would use a 21 trading day window. These don't align perfectly — 30 calendar days is roughly 21 trading days, but not exactly. A stock could delist on calendar day 28 (within the 30d flag window) but trading day 22 (outside the 21 trading day return window). Or vice versa.
The fix is to make the horizons consistent. Either:
Option A: Compute everything in trading days. fwd_delisted_21td = will this ticker delist within the next 21 trading days. fwd_ret_21td = forward return over 21 trading days, using terminal price if delist occurs within that window. The flag and the return cover exactly the same rows.
Option B: Compute everything in calendar days. fwd_delisted_30d stays as-is. fwd_ret_30d = return from today to the price 30 calendar days forward (or terminal price if delist occurs first). Easier to implement but calendar day returns aren't directly comparable across weekends and holidays.
Option A is cleaner for a trading system. Your price features already use trading day lags, so this keeps everything consistent.
Implementation in the pipeline:
This lives in a new step or an extension of 03_price_features, after the universe and ACTIONS are available:

For each (ticker, date) in the grid, look forward 21 trading days in SEP
If a price exists at T+21, fwd_ret_21td = closeadj_T21 / closeadj_T - 1
If the ticker terminates before T+21, look up the terminal event type from ACTIONS. Use last SEP closeadj for acquisitions/voluntary delists, $0 for bankruptcies. fwd_ret_21td = terminal_price / closeadj_T - 1
fwd_delisted_21td = 1 if terminal event occurs within 21 trading days, 0 otherwise
fwd_delist_type = 'acquisitionby', 'bankruptcyliquidation', etc. (NULL if no delist in window)

Then for your model targets, you have matched pairs: fwd_ret_21td always has a value when fwd_delisted_21td = 1, and the return reflects the actual economic outcome.
Do the same for each horizon you care about — 5td, 21td, 63td, 126td, 252td.review that12:07 PMLooking at it again, there are a few issues.
The forward return shouldn't live in 03_price_features. That script runs on SEP alone and doesn't know about the universe or ACTIONS. The forward return needs terminal event data from ACTIONS and the grid from the universe. It should either be a new pipeline step after 03 and after the universe has ACTIONS joined, or it should be part of the merge step.
I conflated the label with the feature. Forward returns serve two different purposes in your pipeline:

As a target variable for modeling (what happens next) — this is what the backtest evaluates against
As a feature (momentum, trend) — this is backward-looking and already handled by ret_1m, ret_3m, etc.

The forward return is purely a label. It shouldn't be in the feature table at all — it should be a separate artifact or clearly segregated in master so there's zero chance of it leaking into features during modeling.
The bankruptcy terminal price of $0 is wrong for many cases. Your event study showed MON with a last traded price of $10.07 at bankruptcy. For your top 1500 universe, most companies that go through Ch11 restructuring have equity that trades through the process. Setting terminal price to $0 assumes Ch7 liquidation with zero recovery. The more defensible approach: use the last SEP closeadj as the terminal price for ALL event types. That's the actual price you could have exited at. If the stock traded down to pennies before bankruptcy, that's captured in the price series. If it traded at $10 like MON, that was the real exit price. Don't impute a value that the market didn't give you.
The only exception would be if there's a gap — the stock halts trading and then delists. But your study showed the gap is 0 days for essentially all events. So last SEP closeadj IS the terminal price.
The horizon logic has a subtlety I missed. When a stock terminates at T+10 and your window is T+21, the return from T to T+10 is real. But the capital is returned at T+10, not T+21. For a backtest simulation, the annualized return is different — you earned X% in 10 days, not 21. If you're comparing forward returns across stocks, a stock that returned 5% in 10 days before being acquired looks the same as one that returned 5% in 21 days, but the first is much better risk-adjusted. You may want to carry fwd_holding_days alongside fwd_ret so the backtest engine can handle reinvestment correctly.
Revised structure:
A separate pipeline step, maybe 08_labels.py, that produces outputs/labels/forward_labels.parquet with:

ticker, date (join key to master)
fwd_ret_21td — return to min(T+21, last trading day), using last SEP closeadj as terminal price
fwd_ret_63td, fwd_ret_126td, fwd_ret_252td — same logic at longer horizons
fwd_holding_days_21td — actual number of trading days held (21 if no terminal event, fewer if terminated early)
fwd_delisted_21td — binary flag, matched to same horizon
fwd_delist_type — from ACTIONS, NULL if no terminal event in the window
Same set for each horizon

Then master gets the features only, and the labels file is joined at modeling time. Clean separation, no leakage risk.that is good but it would be 09

# Summary 

Pipeline bug fixThe ncfo_r2_adjusted 100% null rate is caused by the ARQ pull in compute_quality_metrics_table filtering to datekey BETWEEN DATE_START AND DATE_END. Quality metrics need 10 years of history but the filter only pulls filings within the pipeline date range. Fix: extend the lookback to DATE_START - INTERVAL '10 years'.Event study findingsYour delist event study confirmed:
Acquisitions: 0-day gap between last SEP price and event date. Last price reflects deal price. No price adjustment needed. ~7,400 distinct tickers.
Bankruptcies: 0-day gap. Last SEP price captures where the stock actually traded at termination. ~3,300 distinct tickers.
Regulatory/voluntary delists: 0-day gap, clean data.
mergerfrom: Broken date semantics due to ticker recycling in the study query. Don't use for terminal event detection — use delisted paired with the specific action type.
Ticker renames: Non-issue. Sharadar retroactively updates all tables to use the current ticker. Permaticker exists but isn't needed for internal pipeline joins. The SEP/SF1 mismatches are secondary share classes and ADRs, not rename artifacts.
Forward labels — new pipeline step 09_labels.pyCurrent problem: forward return windows that span a terminal event go NULL, losing the actual economic outcome. fwd_delisted_30d uses calendar days while returns use trading day lags, creating horizon misalignment.The fix is a separate labels artifact (outputs/labels/forward_labels.parquet) with matched horizons in trading days:
fwd_ret_21td, fwd_ret_63td, fwd_ret_126td, fwd_ret_252td — return to min(T+N, last trading day), using last SEP closeadj as terminal price for all event types
fwd_holding_days_21td etc. — actual days held (for annualization and reinvestment logic in the backtest)
fwd_delisted_21td etc. — binary flag on the same trading-day horizon as the corresponding return
fwd_delist_type — from ACTIONS (acquisitionby, bankruptcyliquidation, etc.), NULL if no terminal event in window
Labels are kept separate from features to eliminate leakage risk. Master contains features only; labels are joined at modeling time.Validate debug enhancementsDiscussed additions to validate_debug.py:
Cross-artifact join integrity — master rows == universe in_universe=True, EXCEPT both directions
Macro date alignment — EXCEPT instead of just count comparison
HTZ delist label verification (add HTZ to debug tickers)
Coverage matrix — per-ticker non-null rates for key columns
Staleness bounds — days_since_filing median < 120, max < 400 where datekey is not NULL
Fundamental spot check — AAPL pe_pit on a hardcoded date derived independently from SEP and SF1
Temporal continuity — no null gaps mid-series in ret_12m after warm

## Extra context

is there anything that cursor would get confused about. only focus on the event study output. disregard our conversation about new features12:12 PMLooking at the event study output specifically:
The delisted action is not a separate event type — it's a companion row. Every acquisition, bankruptcy, regulatory delist, and voluntary delist also has a delisted row on the same date. CIT has both delisted and acquisitionby on 2022-01-03. HTZ would have both delisted and bankruptcyliquidation. If an agent builds 09_labels.py and queries for terminal events, it needs to deduplicate — join on the specific action type, not delisted, otherwise every terminal event gets counted twice. And if it tries to classify delist type by looking at the delisted action alone, it gets nothing — the type information is on the companion row.
The 19,026 delisted count vs the sum of specific terminal types (~12,068) also means there are ~7,000 delisted tickers whose only ACTIONS entry is delisted with no companion row explaining why. The agent needs to handle that — probably as delist_type = 'unknown' or by checking if there's a tickerchangefrom nearby (rename, not a real delist).
The value column is NaN for generic delisted rows but populated on companion rows. Look at the samples — the 5 most recent delisted entries (BlackRock funds) all have value = NaN, but the acquisitionby and bankruptcyliquidation samples have values. An agent that tries to use value from the delisted row will get nulls.
ETFC's last day has volume = 1. Several terminal events show a final day with volume of 1 or very small. This is the exchange recording a final administrative print, not real trading. If the agent uses volume-weighted anything on the terminal day, or if it filters on minimum volume, it could drop the terminal price.
The contraticker is blank (not NULL) for some acquisitions. DNKN's acquisitionby has no contraticker even though Inspire Brands acquired it. SNCR and SOHO also blank. The agent can't assume contraticker is always populated for acquisitions — it needs IS NOT NULL AND contraticker != '' guards.
MON is tagged bankruptcyliquidation but traded flat at $10 with low volume. This looks like a SPAC or shell, not the original Monsanto (which was acquired by Bayer under a different ticker). An agent that uses MON as a representative bankruptcy example would draw wrong conclusions about what bankruptcy looks like in the top 1500.do you have solutions for that12:13 PMFor 09_labels.py, the logic should be:
Terminal event resolution: For each ticker, find all ACTIONS rows where the ticker delists. Build the event type by joining the delisted row to any companion row on the same ticker and date:
sqlWITH delist_dates AS (
    SELECT ticker, CAST(date AS DATE) AS event_date
    FROM actions WHERE action = 'delisted'
),
delist_reasons AS (
    SELECT ticker, CAST(date AS DATE) AS event_date, action, value, contraticker
    FROM actions 
    WHERE action IN ('acquisitionby', 'bankruptcyliquidation', 
                     'regulatorydelisting', 'voluntarydelisting', 'mergerfrom')
)
SELECT d.ticker, d.event_date,
       COALESCE(r.action, 'unknown') AS delist_type,
       r.value AS terminal_mktcap_m,
       NULLIF(TRIM(COALESCE(r.contraticker, '')), '') AS contraticker
FROM delist_dates d
LEFT JOIN delist_reasons r ON r.ticker = d.ticker AND r.event_date = d.event_date
This gives exactly one row per delist event with the type resolved. The ~7,000 with no companion row get delist_type = 'unknown'.
For the unknowns, check if there's a tickerchangefrom within ±5 days — those are renames, not real delists, and should be excluded from terminal events entirely:
sqlLEFT JOIN (
    SELECT ticker, CAST(date AS DATE) AS rename_date
    FROM actions WHERE action = 'tickerchangefrom'
) tc ON tc.ticker = d.ticker 
    AND ABS(DATEDIFF('day', tc.rename_date, d.event_date)) <= 5
If tc.rename_date IS NOT NULL, drop the row — it's a rename, not a terminal event. The remaining unknowns with no companion and no rename are genuine delists with unknown cause; tag them but don't impute a terminal return adjustment.
Volume = 1 on terminal day: Don't filter on volume when identifying the terminal price. The last SEP closeadj is the terminal price regardless of volume. If you have minimum volume filters elsewhere in the pipeline (universe construction, price feature validity), make sure they don't apply to the terminal price lookup in 09_labels.py. The terminal price query should be a simple max-date lookup on SEP with no volume guard.
Blank contraticker: Already handled by the NULLIF(TRIM(...), '') above. Downstream code should never assume contraticker is populated for acquisitions — it's informational only and not needed for return calculation.
MON / ticker confusion: This is a broader issue — some tickers get recycled. MON in your data is not Monsanto (which became part of Bayer and was delisted years ago under a different timeline). The recycled MON is a small company that went bankrupt. For the top 1500 universe this solves itself — recycled tickers for small shells and SPACs won't make the market cap cut. But 09_labels.py operates on all tickers before the universe filter, so the label generation should be ticker-agnostic and let the universe filter handle which labels actually matter for modeling. Don't hardcode assumptions about what specific tickers represent.
Putting it together as a spec for 09_labels.py:

Build terminal event table from ACTIONS: one row per (ticker, event_date) with delist_type, excluding renames
For each (ticker, date) in the grid, and for each horizon (21td, 63td, 126td, 252td):

Count forward N trading days in SEP for that ticker
If price exists at T+N: fwd_ret = closeadj_TN / closeadj_T - 1, fwd_holding_days = N, fwd_delisted = 0
If ticker terminates before T+N: find last SEP closeadj, fwd_ret = last_closeadj / closeadj_T - 1, fwd_holding_days = actual trading days to terminal, fwd_delisted = 1, fwd_delist_type from the terminal event table
If no forward prices exist at all (ticker already on its last day or not in SEP): NULL


Write to outputs/labels/forward_labels.parquet, separate from master

Want me to write the script?reivew the correctness of that spec12:14 PMThe spec has a problem with step 2. The terminal price lookup says "find last SEP closeadj" but that's the last price the ticker ever traded at, not necessarily the price within your forward window. If you're on day T and looking at a 252td horizon, and the stock terminates at T+10, the last SEP closeadj is correct. But the query needs to be scoped to that ticker's prices after T, not just the global last price — otherwise you could pick up a price before T if the ticker had already stopped trading.
More importantly, the spec doesn't handle the case where the terminal event falls exactly on date T. If today is the last trading day, there are zero forward prices. The spec says "NULL" for that case, but you actually know the outcome — the stock terminated today. The forward return should be 0% with holding days = 0, and fwd_delisted = 1. This matters because the row before it (T-1) needs to compute a return to T, and the row at T itself is a valid observation where the investor can't enter a new position.
There's also an issue with how the forward return interacts with the grid. The grid comes from the universe, which only contains dates where the ticker is in_universe = True. But the forward price at T+N might be on a date where the ticker is no longer in the universe (it fell out due to stale fundamentals before the delist). The price still exists in SEP even though the grid doesn't have that date. So the forward price lookup needs to go against SEP directly, not the grid.
One more: the spec treats all terminal events the same — use last SEP closeadj as terminal price. But we discussed earlier that for bankruptcies the last traded price may not reflect the actual economic outcome. I walked that back and said using last SEP closeadj for everything is the defensible choice since the gap is 0 days. I still think that's correct for a first implementation, but it should be documented as an assumption that can be revisited. Some institutional datasets apply a haircut to bankruptcy terminal prices or use recovery rate estimates. For now, last traded price is clean and auditable.
Revised step 2:

Forward prices come from SEP (not the grid), filtered to date > T for the same ticker
Rank those forward prices by date, take the one at rank N for the N-trading-day horizon
If fewer than N forward prices exist, the ticker terminated early — use the last available forward price as the terminal price
If zero forward prices exist after T, the stock terminated on T — fwd_ret = 0, fwd_holding_days = 0, fwd_delisted = 1
fwd_delist_type is populated from the terminal event table whenever fwd_delisted = 1