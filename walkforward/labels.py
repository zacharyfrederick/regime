"""
Target construction for walk-forward validation.
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def add_top_bucket_label(
    df: pd.DataFrame,
    ret_col: str = "fwd_ret_21td",
    top_frac: float = 0.2,
) -> pd.DataFrame:
    """
    Assign label=1 to top top_frac by forward return within each date, 0 otherwise. Rank-based.
    Adds column "label_top".
    """
    d = df.copy()

    def _label_per_date(x: pd.Series) -> pd.Series:
        n_valid = x.notna().sum()
        if n_valid == 0:
            return pd.Series(np.nan, index=x.index, dtype=float)
        n_top = max(1, int(np.ceil(top_frac * n_valid)))
        rank_desc = x.rank(method="first", ascending=False)
        return (rank_desc <= n_top).astype(int).where(x.notna(), np.nan)

    d["label_top"] = d.groupby("date", group_keys=False)[ret_col].transform(
        _label_per_date
    )
    return d


def add_target_xs(df: pd.DataFrame, ret_col: str = "fwd_ret_21td") -> pd.DataFrame:
    """
    Cross-sectional excess return: ret_col - mean within date (for regression target).
    Adds column "target_xs".
    """
    d = df.copy()
    d["target_xs"] = d.groupby("date")[ret_col].transform(lambda x: x - x.mean())
    return d
