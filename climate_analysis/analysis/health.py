"""Health-oriented indices HRW and HRD."""

from __future__ import annotations

import pandas as pd


def compute_hrw(nat_tmax: pd.Series, present_tmax: pd.Series) -> dict:
    """Compute the HRW index (increase in 84th percentile)."""
    nat_p84 = float(nat_tmax.dropna().quantile(0.84))
    present_p84 = float(present_tmax.dropna().quantile(0.84))
    return {
        "nat_p84": nat_p84,
        "present_p84": present_p84,
        "hrw_delta_c": present_p84 - nat_p84,
    }


def compute_hrd(
    daily_tmax: pd.Series,
    baseline_threshold_c: float,
    baseline_expected_days: float = 58.0,
) -> pd.Series:
    """Compute HRD: additional hot days per year relative to baseline threshold."""
    if not isinstance(daily_tmax.index, pd.DatetimeIndex):
        daily_tmax = daily_tmax.copy()
        daily_tmax.index = pd.to_datetime(daily_tmax.index)

    def _count_hot_days(group: pd.Series) -> float:
        return float((group > baseline_threshold_c).sum() - baseline_expected_days)

    hrd = daily_tmax.groupby(daily_tmax.index.year).apply(_count_hot_days)
    hrd.name = "hrd_days"
    return hrd
