"""Annual maximum temperature (tx01) computation."""

from __future__ import annotations

import pandas as pd


def compute_tx01_from_daily(daily_tmax_c: pd.Series, min_valid_days: int = 300) -> pd.Series:
    """Compute tx01 (warmest day of each year) from daily Tmax values.

    Args:
        daily_tmax_c: Pandas Series indexed by datetime-like values with temperatures in °C.
        min_valid_days: Require at least this many valid days per year to accept the annual maximum.

    Returns:
        Series indexed by year with the annual maximum Tmax (°C).
    """
    if not isinstance(daily_tmax_c.index, pd.DatetimeIndex):
        daily_tmax_c = daily_tmax_c.copy()
        daily_tmax_c.index = pd.to_datetime(daily_tmax_c.index)

    def _year_max(group: pd.Series) -> float | None:
        # Skip poorly sampled years to avoid biasing the GEV fit.
        if group.dropna().shape[0] < min_valid_days:
            return None
        return group.max()

    tx01 = daily_tmax_c.groupby(daily_tmax_c.index.year).apply(_year_max)
    tx01 = tx01.dropna().astype(float)
    tx01.index.name = "year"
    tx01.name = "tx01_c"
    return tx01
