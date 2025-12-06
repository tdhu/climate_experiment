"""Minimal helpers to stream point data from ESGF files.

The goal is to avoid downloading full NetCDFs. We open OPeNDAP (preferred) or
HTTP-served NetCDF files, read coordinates once to find the nearest grid point,
and then fetch only that point's timeseries.
"""

from __future__ import annotations

from typing import Optional

import xarray as xr


def _guess_coords(ds: xr.Dataset) -> tuple[str, str]:
    """Return likely latitude and longitude coordinate names."""

    for lat_name in ("lat", "latitude", "nav_lat"):
        if lat_name in ds.coords:
            break
    else:
        raise KeyError("Could not find a latitude coordinate in dataset.")

    for lon_name in ("lon", "longitude", "nav_lon"):
        if lon_name in ds.coords:
            break
    else:
        raise KeyError("Could not find a longitude coordinate in dataset.")

    return lat_name, lon_name


def open_point_timeseries(
    url: str,
    variable_id: str,
    latitude: float,
    longitude: float,
    time_start: Optional[str] = None,
    time_end: Optional[str] = None,
    engine: Optional[str] = None,
) -> xr.DataArray:
    """Open a remote file and return a single-point timeseries.

    The function tries to use OPeNDAP streaming; if unavailable, the default
    engine is attempted. Only the requested point and time window are loaded.
    """

    # Open lazily; pydap engine can better handle some OPeNDAP servers.
    ds = xr.open_dataset(url, engine=engine, chunks={})
    lat_name, lon_name = _guess_coords(ds)

    subset = ds[variable_id].sel(lat=latitude, lon=longitude, method="nearest")
    if time_start or time_end:
        subset = subset.sel(time=slice(time_start, time_end))

    # Pull the small slice into memory.
    return subset.load()
