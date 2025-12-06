"""Probability and return-time calculations for temperature thresholds."""

from __future__ import annotations

import math
from typing import Dict

import numpy as np
import pandas as pd
from scipy import stats

from climate_analysis.analysis.gev_fit import fit_gev_distribution


def exceedance_probability(tx01: pd.Series, threshold_c: float) -> float:
    """Compute the probability of exceeding a threshold using a GEV fit."""
    params = fit_gev_distribution(tx01)
    prob = stats.genextreme.sf(threshold_c, **params)
    return float(prob)


def return_time_bootstrap(
    tx01: pd.Series,
    threshold_c: float,
    n_boot: int = 1000,
    random_seed: int = 42,
) -> Dict[str, float]:
    """Estimate return time (1/probability) with bootstrap uncertainty."""
    base_prob = exceedance_probability(tx01, threshold_c)
    if base_prob <= 0:
        # Extremely rare or unseen events yield zero probability under the fitted GEV.
        # Use a tiny floor so downstream code can proceed while clearly signaling rarity.
        base_prob = 1e-12
    rng = np.random.default_rng(random_seed)
    tx01_clean = tx01.dropna().astype(float).to_numpy()

    boot_probs: list[float] = []
    for _ in range(n_boot):
        sample = rng.choice(tx01_clean, size=tx01_clean.size, replace=True)
        shape, loc, scale = stats.genextreme.fit(sample)
        p = stats.genextreme.sf(threshold_c, shape, loc=loc, scale=scale)
        boot_probs.append(float(p))

    boot_probs = np.array(boot_probs)
    # Avoid division by zero with a very small floor.
    boot_probs = np.clip(boot_probs, 1e-12, 1.0)
    return {
        "probability": base_prob,
        "return_time_years": 1.0 / base_prob,
        "return_time_p5": 1.0 / np.percentile(boot_probs, 95),
        "return_time_p50": 1.0 / np.percentile(boot_probs, 50),
        "return_time_p95": 1.0 / np.percentile(boot_probs, 5),
    }
