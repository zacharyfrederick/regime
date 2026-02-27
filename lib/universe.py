"""
Universe helper functions: active-on-date logic and forward event flags.
Used by pipeline/01_universe.py and by merge/validation.
"""
from __future__ import annotations

from typing import Any

# ACTIONS action types that remove a ticker from the universe (on or after action date)
REMOVAL_ACTIONS = frozenset({
    "delisted",
    "bankruptcyliquidation",
    "voluntarydelisting",
    "regulatorydelisting",
    "mergerfrom",
})

# For forward_delisted_* flags
DELIST_ACTIONS = frozenset({
    "delisted",
    "bankruptcyliquidation",
    "voluntarydelisting",
    "regulatorydelisting",
})

# For forward_acquired_90d
ACQUISITION_ACTIONS = frozenset({"acquisitionby", "mergerfrom"})


def is_active_on_date(
    firstpricedate: Any,
    lastpricedate: Any,
    removal_action_date: Any,
    sim_date: Any,
) -> bool:
    """
    True if ticker was in the active universe on sim_date given:
    - firstpricedate <= sim_date
    - lastpricedate >= sim_date (or null = still active)
    - no removal action with action_date <= sim_date (ticker leaves on or after action date)
    """
    if firstpricedate is None or sim_date is None:
        return False
    if firstpricedate > sim_date:
        return False
    if lastpricedate is not None and lastpricedate < sim_date:
        return False
    if removal_action_date is not None and removal_action_date <= sim_date:
        return False
    return True


def forward_delisted_30d(action_dates: list[Any], sim_date: Any, window_end: Any) -> bool:
    """True if any delist action occurred in [sim_date, sim_date+30] (or window_end)."""
    for d in action_dates:
        if d is not None and sim_date <= d <= window_end:
            return True
    return False


def forward_acquired_90d(action_dates: list[Any], sim_date: Any, window_end: Any) -> bool:
    """True if any acquisition/merger action in [sim_date, window_end]."""
    for d in action_dates:
        if d is not None and sim_date <= d <= window_end:
            return True
    return False


def forward_spinoff_60d(action_dates: list[Any], sim_date: Any, window_end: Any) -> bool:
    """True if spinoff action in [sim_date, window_end]."""
    for d in action_dates:
        if d is not None and sim_date <= d <= window_end:
            return True
    return False
