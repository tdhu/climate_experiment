"""Generalised Extreme Value (GEV) fitting."""

from __future__ import annotations

import numpy as np
import pandas as pd
from scipy import stats


def fit_gev_distribution(tx01: pd.Series) -> dict:
    """Fit a GEV distribution to annual maxima (tx01).

    SciPy uses the reverse sign convention for the shape parameter (c = -xi in some texts).
    This routine keeps the raw SciPy parameterization but documents it clearly so SMEs know
    how to interpret the output.
    """
    sample = tx01.dropna().astype(float).to_numpy()
    if sample.size < 5:
        raise ValueError("Need at least 5 years of data to fit a stable GEV distribution.")
    shape, loc, scale = stats.genextreme.fit(sample)
    # SciPy expects parameters under names (c, loc, scale).
    return {"c": shape, "loc": loc, "scale": scale}


def gev_ppf(p: float, params: dict) -> float:
    """Return the pth percentile from a fitted GEV distribution."""
    return float(stats.genextreme.ppf(p, **params))


def gev_sf(x: float, params: dict) -> float:
    """Survival function (1 - CDF) for a threshold x."""
    return float(stats.genextreme.sf(x, **params))
