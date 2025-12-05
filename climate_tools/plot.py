"""Plotting utilities for climate data visualization."""

from typing import Optional

import matplotlib.pyplot as plt
import xarray as xr


def plot_climate_data(
    ds: xr.Dataset,
    variable: str,
    time_index: int = 0,
    figsize: tuple[int, int] = (12, 8),
    cmap: str = "coolwarm",
    title: Optional[str] = None,
) -> plt.Figure:
    """Plot a 2D slice of climate data.

    Args:
        ds: Dataset containing the climate data.
        variable: Name of the variable to plot.
        time_index: Index of the time slice to plot.
        figsize: Figure size as (width, height).
        cmap: Colormap to use.
        title: Plot title. If None, auto-generated from variable name.

    Returns:
        plt.Figure: The matplotlib figure object.
    """
    fig, ax = plt.subplots(figsize=figsize)

    data = ds[variable]
    if "time" in data.dims:
        data = data.isel(time=time_index)

    data.plot(ax=ax, cmap=cmap, add_colorbar=True)

    if title:
        ax.set_title(title)
    else:
        ax.set_title(f"{variable} - Time index {time_index}")

    plt.tight_layout()
    return fig


def plot_temperature_map(
    ds: xr.Dataset,
    variable: str = "air",
    time_index: int = 0,
    figsize: tuple[int, int] = (14, 8),
) -> plt.Figure:
    """Plot a temperature map from climate data.

    Args:
        ds: Dataset containing temperature data.
        variable: Name of the temperature variable.
        time_index: Index of the time slice to plot.
        figsize: Figure size as (width, height).

    Returns:
        plt.Figure: The matplotlib figure object.
    """
    fig, ax = plt.subplots(figsize=figsize)

    data = ds[variable]
    if "time" in data.dims:
        data = data.isel(time=time_index)

    # Convert to Celsius if in Kelvin
    if data.max() > 200:  # Likely in Kelvin
        data = data - 273.15
        units = "°C"
    else:
        units = data.attrs.get("units", "")

    data.plot(ax=ax, cmap="RdYlBu_r", add_colorbar=True)
    ax.set_title(f"Temperature ({units})")

    plt.tight_layout()
    return fig


def plot_time_series(
    ds: xr.Dataset,
    variable: str,
    lat: Optional[float] = None,
    lon: Optional[float] = None,
    figsize: tuple[int, int] = (12, 6),
) -> plt.Figure:
    """Plot a time series of a variable at a specific location.

    Args:
        ds: Dataset containing the climate data.
        variable: Name of the variable to plot.
        lat: Latitude of the location. If None, uses mean over all latitudes.
        lon: Longitude of the location. If None, uses mean over all longitudes.
        figsize: Figure size as (width, height).

    Returns:
        plt.Figure: The matplotlib figure object.
    """
    fig, ax = plt.subplots(figsize=figsize)

    data = ds[variable]

    if lat is not None and "lat" in data.dims:
        data = data.sel(lat=lat, method="nearest")
    elif "lat" in data.dims:
        data = data.mean(dim="lat")

    if lon is not None and "lon" in data.dims:
        data = data.sel(lon=lon, method="nearest")
    elif "lon" in data.dims:
        data = data.mean(dim="lon")

    data.plot(ax=ax)
    ax.set_title(f"{variable} Time Series")
    ax.set_xlabel("Time")
    ax.set_ylabel(variable)

    plt.tight_layout()
    return fig
