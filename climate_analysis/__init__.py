"""Climate analysis toolkit implementing the exceedance methodology from Christidis et al. (2023).

The modules are intentionally small and heavily commented so subject-matter experts can
trace each analytical step. The package supports:

- live station downloads (via Meteostat)
- CMIP6 ALL/NAT pulls from the Pangeo intake catalog
- computation of tx01, bias correction, GEV fitting, exceedance probabilities, and risk ratios
- health metrics HRW/HRD
"""

from climate_analysis.config import load_data_catalog, load_sites, load_thresholds

__all__ = [
    "load_data_catalog",
    "load_sites",
    "load_thresholds",
]
