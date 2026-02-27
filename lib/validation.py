"""
PIT integrity checks for pipeline validation.
Use in notebooks/01_validate_pipeline.ipynb and code reviews.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


def spot_check_no_future_info(
    df: Any,
    date_col: str = "date",
    ticker_col: str = "ticker",
    filing_date_col: str | None = "datekey",
) -> None:
    """
    Assert that for each row, any filing_date_col is <= date_col.
    Use for fundamental_pit or master to catch lookahead.
    """
    if df is None or filing_date_col is None or filing_date_col not in getattr(df, "columns", []):
        return
    import pandas as pd
    if not isinstance(df, pd.DataFrame):
        return
    bad = df[df[filing_date_col] > df[date_col]]
    if len(bad) > 0:
        log.warning("Found %d rows with %s > %s (lookahead)", len(bad), filing_date_col, date_col)
    else:
        log.info("Spot check passed: no %s > %s", filing_date_col, date_col)


def check_delisted_sequence(
    universe_df: Any,
    ticker: str,
    delist_date: Any,
    date_col: str = "date",
    in_universe_col: str = "in_universe",
    ticker_col: str = "ticker",
) -> bool:
    """
    For a known delisted ticker, check it appears in universe up to (before) delist_date
    and is out on/after. Returns True if check passes.
    """
    import pandas as pd
    if not isinstance(universe_df, pd.DataFrame) or ticker_col not in universe_df.columns:
        return False
    if ticker not in universe_df[ticker_col].values:
        return False
    sub = universe_df[universe_df[ticker_col] == ticker].sort_values(date_col)
    if sub.empty:
        return False
    before = sub[sub[date_col] < delist_date]
    after = sub[sub[date_col] >= delist_date]
    return before[in_universe_col].all() and (after.empty or not after[in_universe_col].any())


def distribution_summary(df: Any, columns: list[str] | None = None) -> Any:
    """Basic distribution (min, max, mean, null count) for numeric columns to spot errors."""
    import pandas as pd
    if not isinstance(df, pd.DataFrame):
        return None
    cols = columns or df.select_dtypes(include=["number"]).columns.tolist()
    return df[cols].describe(include="all").T if cols else None
