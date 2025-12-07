"""Minimal helpers to stream point data from ESGF files.

The goal is to avoid downloading full NetCDFs. We open OPeNDAP (preferred) or
HTTP-served NetCDF files, read coordinates once to find the nearest grid point,
and then fetch only that point's timeseries.
"""

from __future__ import annotations

from typing import Optional
import os
from pathlib import Path

import requests
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
    session: Optional[requests.Session] = None,
) -> xr.DataArray:
    """Open a remote file and return a single-point timeseries.

    Primary HTTPServer path: download the remote NetCDF (with resume support)
    to a local cache, slice the desired point/time window, then return it.
    """

    def _auth_headers() -> dict:
        if session and getattr(session, "auth", None):
            try:
                auth = session.auth
                if hasattr(auth, "authorizer"):
                    auth.authorizer.ensure_valid_token()
                    token = auth.authorizer.access_token
                    return {"Authorization": f"Bearer {token}"}
            except Exception:
                return {}
        return {}

    def _get_remote_size(src: str, headers: dict) -> Optional[int]:
        http = session or requests
        try:
            resp = http.head(src, headers=headers, timeout=60, allow_redirects=True)
            resp.raise_for_status()
            length = resp.headers.get("Content-Length")
            return int(length) if length is not None else None
        except Exception:
            return None

    def _download_with_resume(src: str, dest: Path) -> Path:
        dest.parent.mkdir(parents=True, exist_ok=True)
        headers = _auth_headers()
        expected_size = _get_remote_size(src, headers)
        resume_at = dest.stat().st_size if dest.exists() else 0
        if expected_size and resume_at >= expected_size:
            return dest
        if resume_at > 0:
            headers["Range"] = f"bytes={resume_at}-"
        http = session or requests
        with http.get(src, headers=headers, stream=True, timeout=1800) as resp:
            # Accept 206 (partial) or 200 (fresh)
            if resp.status_code not in (200, 206):
                resp.raise_for_status()
            mode = "ab" if resume_at and resp.status_code == 206 else "wb"
            with open(dest, mode) as out:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        out.write(chunk)
        final_size = dest.stat().st_size
        if expected_size and final_size < expected_size:
            raise IOError(
                f"Incomplete download: got {final_size} bytes, expected {expected_size} for {src}"
            )
        return dest

    cache_root = Path(__file__).resolve().parent.parent.parent / ".cache" / "esgf"
    filename = os.path.basename(url.split("?")[0]) or "esgf.nc"
    local_path = cache_root / filename
    downloaded = _download_with_resume(url, local_path)

    ds = xr.open_dataset(downloaded, engine="h5netcdf", chunks={"time": 90})
    lat_name, lon_name = _guess_coords(ds)
    subset = ds[variable_id].sel(lat=latitude, lon=longitude, method="nearest")
    if time_start or time_end:
        subset = subset.sel(time=slice(time_start, time_end))
    result = subset.load()
    return result
