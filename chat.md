the code is getting really long. how should I think about organizing this into a module 

Start by splitting it by stage of the pipeline, not by model type.

Right now one file is doing too many jobs:

    config / constants

    screener construction

    panel construction

    labels

    model fitting

    evaluation

    CLI

That’s why it feels heavy.
Best mental model

Think of the project as a small research package with five layers:
1. Data definition

What rows exist?

    screener candidates

    panel rows

    folds / rebal dates

2. Feature + label construction

What columns exist?

    screener fields

    setup features

    MFE/MAE labels

    trigger flags

3. Modeling

How do we fit and predict?

    Ridge/XGB

    train / predict wrappers

    score construction

4. Evaluation

How do we measure performance?

    regression diagnostics

    deciles

    portfolio stats

    stitched OOS

5. Orchestration

How do we run the whole thing?

    CLI

    cache / file paths

    “screen”, “panel”, “train”, “both”

A clean module layout

I’d do something like this:

experiments/
  qullamaggie/
    __init__.py
    config.py
    screener.py
    panel.py
    labels.py
    triggers.py
    models.py
    evaluation.py
    walkforward_runner.py
    cli.py

Or if you want to keep experiment code separate from reusable code:

src/
  momentum/
    __init__.py
    config.py
    screener.py
    panel.py
    labels.py
    triggers.py
    models.py
    evaluation.py
    walkforward.py

experiments/
  qullamaggie.py

That second structure is usually better:

    src/momentum/ = reusable logic

    experiments/qullamaggie.py = thin runner

How to split your current file
config.py

Only constants and dataclasses.

Put here:

    ScreenerParams

    output paths

    date ranges

    model params

    hold days

    alpha score

    feature column names

You want one place for:

    experiment settings

    path settings

screener.py

Only Stage 1 candidate generation.

Functions like:

    build_screener_candidates(con, params, ...)

    load_screener_candidates(path)

    screen_stats(df)

This file should answer:

    which (ticker, date) rows are candidates?

panel.py

Only feature panel construction.

Functions like:

    build_panel(con, screener_path, ...)

    load_panel(path)

    add_days_since_high(df)

This file should answer:

    for candidate rows, what features do we compute?

labels.py

All forward-path and entry logic.

Functions like:

    register_terminal_event_views(con)

    build_entry_labels(con, hold_days, ...)

    maybe label transforms

This file should answer:

    given a signal row, how do we define entry and realized path?

triggers.py

Keep this isolated, even if simple.

Functions like:

    add_breakout_flags(con, windows=(10,20,50))

    or pandas post-processing trigger helpers

This file should answer:

    what is the timing condition?

This will save you later when you test:

    10d breakout

    20d breakout

    close breakout

    intraday proxy

    volume-confirmed breakout

models.py

Only fitting / prediction logic.

Functions like:

    fit_predict_fold(...)

    make_model(kind, params)

    score_from_predictions(...)

This file should answer:

    how do we go from features to predicted MFE/MAE/score?

evaluation.py

All metrics and portfolio calculations.

Functions like:

    regression_diagnostics

    mean_cs_spearman

    decile_lift_table

    portfolio_period_returns_decile

    trade_level_stats

    capacity_stats

    selection_quality_table

This file should answer:

    did the model and portfolio work?

walkforward_runner.py

Only fold orchestration.

Functions like:

    run_walk_forward(panel_path, ...)

    maybe run_single_fold(...)

This file should glue together:

    data loading

    train/oos split

    model calls

    evaluation calls

But it should not contain the actual math for all of those pieces.
cli.py

Only argparse and command routing.

Very thin:

    parse args

    call build_screener

    call build_panel

    call run_walk_forward

This should be the smallest file.
Practical principle: each file should answer one question

Use this test:

    screener.py → “Who is in the watchlist?”

    panel.py → “What does the setup look like?”

    labels.py → “What happened after entry?”

    models.py → “What do we predict?”

    evaluation.py → “Was it useful?”

    cli.py → “How do I run it?”

If a function doesn’t clearly belong to one of those questions, that’s a sign to rethink placement.
What not to do

Do not split by “small helper vs large helper.”
Do not split by “DuckDB code vs pandas code.”
Do not split by “Ridge file” and “XGB file.”

Those are implementation details. The real structure is the research pipeline.
A very good first refactor

You do not need to fully modularize everything at once.

I’d do this in two passes.
Pass 1: extract the obvious chunks

Move out:

    config/constants → config.py

    metrics/evaluation functions → evaluation.py

    model fitting → models.py

That alone will make the main file much smaller.
Pass 2: extract data-building logic

Then move out:

    screener build → screener.py

    panel/labels build → panel.py and maybe labels.py

At that point the main runner becomes much cleaner.
What the top-level runner should look like

Your final experiment runner should feel like this:

def main():
    args = parse_args()

    if args.command in ("screen", "both"):
        build_screener_candidates(...)

    if args.command in ("panel", "both"):
        build_panel(...)

    if args.command in ("train", "both"):
        run_walk_forward(...)

That’s it.

If main() is long, your module boundaries are still too weak.
Dataclasses help a lot

Use dataclasses for grouped settings.

For example:

@dataclass
class ScreenerParams:
    min_dollar_volume_1d: float
    min_adr_pct: float
    top_gainer_fraction: float
    horizons: tuple[int, ...]

@dataclass
class BacktestParams:
    hold_days: int
    alpha_score: float
    wf_freq: str
    top_frac: float

@dataclass
class ModelParams:
    ridge_alpha: float
    xgb_params: dict

This keeps functions cleaner and avoids giant argument lists.
One more useful idea: separate artifacts from logic

I would also think in terms of artifact boundaries:

    daily_screener_candidates.parquet

    qullamaggie_panel.parquet

    fold predictions / stitched OOS outputs maybe later

That helps define the module boundaries naturally:

    screener.py writes screener artifact

    panel.py writes panel artifact

    walkforward_runner.py consumes panel artifact

That is a very natural architecture.
My strongest recommendation

If you only do one thing, do this:
Turn the current giant file into:

    experiments/qullamaggie.py → thin CLI

    src/momentum/screener.py

    src/momentum/panel.py

    src/momentum/models.py

    src/momentum/evaluation.py

That alone will make the project feel much more manageable.
Rule of thumb

When a file contains both:

    SQL / feature engineering

    model fitting

    evaluation

    CLI

it is already at least 3 files too large.

You are there now.

If you want, I can sketch the exact file skeletons and function signatures for your current codebase so you can refactor without guessing.


