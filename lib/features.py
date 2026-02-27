"""
Feature computation helpers (e.g. NCFO R², CAGR, sector-relative).
Used by pipeline scripts and optionally by notebooks.
"""
from __future__ import annotations


def ncfo_r2_cagr(ncfo_series):
    """
    OLS R² of log(ncfo) ~ time and CAGR for a series of annual NCFO values.
    Returns (r_squared, cagr). Requires at least 5 positive values.
    """
    import numpy as np
    from scipy import stats
    ncfo = ncfo_series[np.asarray(ncfo_series) > 0]
    ncfo = ncfo[~np.isnan(ncfo)]
    if len(ncfo) < 5:
        return np.nan, np.nan
    t = np.arange(len(ncfo))
    slope, _, r, _, _ = stats.linregress(t, np.log(ncfo))
    cagr = np.exp(slope) - 1
    return r ** 2, cagr
