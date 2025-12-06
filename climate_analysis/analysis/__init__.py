"""Statistical analysis routines: GEV fitting, probabilities, risk ratios, health metrics."""

from climate_analysis.analysis.gev_fit import fit_gev_distribution
from climate_analysis.analysis.probabilities import exceedance_probability, return_time_bootstrap
from climate_analysis.analysis.risk import compute_risk_ratio
from climate_analysis.analysis.health import compute_hrw, compute_hrd

__all__ = [
    "fit_gev_distribution",
    "exceedance_probability",
    "return_time_bootstrap",
    "compute_risk_ratio",
    "compute_hrw",
    "compute_hrd",
]
