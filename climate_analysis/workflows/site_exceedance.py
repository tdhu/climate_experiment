"""End-to-end exceedance analysis for a single site.

This mirrors the paper's approach: compute tx01 for NAT and ALL climates, apply mean bias
correction against observations, fit GEVs, estimate probabilities and risk ratios for user
thresholds, and provide HRW/HRD hooks.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Iterable, Optional

import pandas as pd
import xarray as xr

from climate_analysis.analysis import compute_hrd, compute_hrw, return_time_bootstrap
from climate_analysis.analysis.risk import compute_risk_ratio
from climate_analysis.config import load_thresholds
from climate_analysis.data_access import CMIP6Client, StationClient
from climate_analysis.processing import apply_mean_bias_correction, compute_tx01_from_daily
from climate_analysis.sites import get_site


@dataclass
class ExceedanceResult:
    threshold_c: float
    nat_return_time: float
    nat_return_time_p5: float
    nat_return_time_p95: float
    all_return_time_present: float
    all_return_time_mid: float
    all_return_time_late: float
    risk_ratio_present: float
    risk_ratio_mid: float
    risk_ratio_late: float


def _slice_tx01_window(tx01: pd.Series, start_year: int, end_year: int) -> pd.Series:
    """Helper to extract a time slice for the warming ALL climate."""
    return tx01.loc[(tx01.index >= start_year) & (tx01.index <= end_year)]


def analyze_site_exceedance(
    site_id: str,
    thresholds_c: Optional[Iterable[float]] = None,
    model: str = "CanESM5",
    member: Optional[str] = None,
    present_center_year: int = 2022,
    mid_center_year: int = 2050,
    late_end_year: int = 2100,
    window: int = 30,
    n_boot: int = 300,
    model_start_year: int = 1950,
) -> dict:
    """Run the exceedance pipeline for one site using a single CMIP6 model/member.

    Returns a dictionary with:
    - station tx01
    - model tx01 (nat, all)
    - return-time tables and risk ratios per threshold
    """
    site = get_site(site_id)
    thresholds = sorted(
        {*(load_thresholds()["default_thresholds_c"]), *(thresholds_c or [])}
    )

    # 1) Observations
    station_client = StationClient()
    obs_daily = station_client.fetch_daily_tmax(
        site=site,
        start=date(site["obs_period_start"], 1, 1),
        end=date(site["obs_period_end"], 12, 31),
    )
    obs_tx01 = compute_tx01_from_daily(obs_daily)

    # 2) CMIP6 model data
    cmip = CMIP6Client()
    nat = cmip.open_nat_timeseries(
        model=model,
        variable_id="tasmax",
        latitude=site["latitude"],
        longitude=site["longitude"],
        member=member,
        start_year=model_start_year,
    )
    all_times = cmip.open_all_timeseries(
        model=model,
        variable_id="tasmax",
        latitude=site["latitude"],
        longitude=site["longitude"],
        member=member,
        hist_start_year=model_start_year,
    )

    def _to_celsius(series: xr.DataArray) -> pd.Series:
        # CMIP6 tasmax is in Kelvin; convert to Celsius for consistency with observations.
        series = series.reset_coords(drop=True)  # drop scalar lat/lon to avoid MultiIndex
        df = series.to_dataframe(name="kelvin").reset_index()
        # Convert CFTime to pandas datetime via string casting; acceptable for daily data.
        df["time"] = pd.to_datetime(df["time"].astype(str))
        kelvin = df.set_index("time")["kelvin"]
        return kelvin - 273.15

    nat_c = _to_celsius(nat)
    all_c = _to_celsius(all_times)

    nat_tx01 = compute_tx01_from_daily(nat_c, min_valid_days=300)
    all_tx01 = compute_tx01_from_daily(all_c, min_valid_days=300)

    # 3) Bias correction applied separately to NAT and ALL tx01.
    nat_tx01_bc = apply_mean_bias_correction(nat_tx01, obs_tx01)
    all_tx01_bc = apply_mean_bias_correction(all_tx01, obs_tx01)

    # 4) ALL climate windows: present (center 2022), mid-century (2050), late (last 30 years)
    half = window // 2
    present_slice = _slice_tx01_window(all_tx01_bc, present_center_year - half, present_center_year + half)
    mid_slice = _slice_tx01_window(all_tx01_bc, mid_center_year - half, mid_center_year + half)
    late_slice = _slice_tx01_window(all_tx01_bc, late_end_year - window + 1, late_end_year)

    results: list[ExceedanceResult] = []
    for thr in thresholds:
        nat_rt = return_time_bootstrap(nat_tx01_bc, thr, n_boot=n_boot)
        pres_rt = return_time_bootstrap(present_slice, thr, n_boot=n_boot)
        mid_rt = return_time_bootstrap(mid_slice, thr, n_boot=n_boot)
        late_rt = return_time_bootstrap(late_slice, thr, n_boot=n_boot)
        results.append(
            ExceedanceResult(
                threshold_c=thr,
                nat_return_time=nat_rt["return_time_years"],
                nat_return_time_p5=nat_rt["return_time_p5"],
                nat_return_time_p95=nat_rt["return_time_p95"],
                all_return_time_present=pres_rt["return_time_years"],
                all_return_time_mid=mid_rt["return_time_years"],
                all_return_time_late=late_rt["return_time_years"],
                risk_ratio_present=compute_risk_ratio(pres_rt["probability"], nat_rt["probability"]),
                risk_ratio_mid=compute_risk_ratio(mid_rt["probability"], nat_rt["probability"]),
                risk_ratio_late=compute_risk_ratio(late_rt["probability"], nat_rt["probability"]),
            )
        )

    # 5) Health metrics (using ALL daily Tmax for baseline 2008-2037 as per paper)
    baseline_window = slice("2008-01-01", "2037-12-31")
    baseline_data = all_c.loc[baseline_window]
    baseline_p84 = float(baseline_data.quantile(0.84))
    hrd_series = compute_hrd(all_c, baseline_threshold_c=baseline_p84)
    hrw = compute_hrw(nat_c, all_c.loc[baseline_window])

    return {
        "site": site,
        "thresholds": thresholds,
        "obs_tx01": obs_tx01,
        "nat_tx01_bc": nat_tx01_bc,
        "all_tx01_bc": all_tx01_bc,
        "present_tx01": present_slice,
        "mid_tx01": mid_slice,
        "late_tx01": late_slice,
        "exceedance_results": results,
        "hrd_series": hrd_series,
        "hrw": hrw,
    }
