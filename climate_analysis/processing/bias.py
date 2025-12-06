"""Mean bias correction utilities."""

from __future__ import annotations

import pandas as pd


def apply_mean_bias_correction(
    model_tx01: pd.Series,
    obs_tx01: pd.Series,
) -> pd.Series:
    """Apply simple mean bias correction to model tx01 using observed overlap.

    Mirrors the paper's approach: subtract the mean model-observation difference over the
    observation period from all model values.
    """
    # Align on overlapping years.
    overlapping = model_tx01.index.intersection(obs_tx01.index)
    if overlapping.empty:
        raise ValueError("No overlapping years between model and observations for bias correction.")

    model_mean = model_tx01.loc[overlapping].mean()
    obs_mean = obs_tx01.loc[overlapping].mean()
    bias = model_mean - obs_mean
    corrected = model_tx01 - bias
    corrected.name = f"{model_tx01.name}_bias_corrected"
    return corrected
