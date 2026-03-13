"""Unit tests for walkforward.labels."""
import numpy as np
import pandas as pd
import pytest

from walkforward.labels import add_top_bucket_label


def test_add_top_bucket_label():
    """Single-date panel: top 20% get label 1, rest 0; count of 1s matches."""
    # 10 rows, top_frac=0.2 => top 2 get 1
    df = pd.DataFrame({
        "date": ["2020-01-31"] * 10,
        "fwd_ret_21td": [0.01 * i for i in range(10)],  # 0, 0.01, ..., 0.09 → top 2 are 0.09, 0.08
    })
    out = add_top_bucket_label(df, ret_col="fwd_ret_21td", top_frac=0.2)
    assert "label_top" in out.columns
    assert out["label_top"].sum() == 2
    assert (out["label_top"].dropna() == 1).sum() == 2
    assert (out["label_top"].dropna() == 0).sum() == 8
