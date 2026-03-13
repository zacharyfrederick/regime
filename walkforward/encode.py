"""
Categorical encoding for walk-forward folds. Fit on IS, transform OOS.
"""
from __future__ import annotations

import pandas as pd
from sklearn.preprocessing import OrdinalEncoder


def encode_fold(
    is_df: pd.DataFrame,
    oos_df: pd.DataFrame,
    cat_cols: tuple[str, ...] = ("sector", "famaindustry"),
    time_feature: str = "month_of_year",
) -> OrdinalEncoder:
    """
    Fit OrdinalEncoder on IS, transform IS and OOS. Add time feature (month_of_year or week_of_year).
    Modifies is_df and oos_df in place. Returns the fitted encoder.
    """
    enc = OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1)
    is_cat = is_df[list(cat_cols)].fillna("MISSING").astype(str)
    oos_cat = oos_df[list(cat_cols)].fillna("MISSING").astype(str)
    enc.fit(is_cat)
    is_enc = enc.transform(is_cat)
    oos_enc = enc.transform(oos_cat)
    is_df["sector_enc"] = is_enc[:, 0]
    is_df["famaindustry_enc"] = is_enc[:, 1]
    oos_df["sector_enc"] = oos_enc[:, 0]
    oos_df["famaindustry_enc"] = oos_enc[:, 1]
    if time_feature == "month_of_year":
        is_df["month_of_year"] = pd.to_datetime(is_df["date"]).dt.month.astype(int)
        oos_df["month_of_year"] = pd.to_datetime(oos_df["date"]).dt.month.astype(int)
    elif time_feature == "week_of_year":
        is_df["week_of_year"] = pd.to_datetime(is_df["date"]).dt.isocalendar().week.astype(int)
        oos_df["week_of_year"] = pd.to_datetime(oos_df["date"]).dt.isocalendar().week.astype(int)
    else:
        raise ValueError(f"time_feature must be 'month_of_year' or 'week_of_year', got {time_feature!r}")
    return enc
