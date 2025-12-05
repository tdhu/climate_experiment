"""Tools for downloading climate data using intake and xarray."""

from pathlib import Path
from typing import Optional

import xarray as xr


class ClimateDataDownloader:
    """Download and manage climate data from various sources."""

    def __init__(self, data_dir: Optional[str | Path] = None):
        """Initialize the downloader with a data directory.

        Args:
            data_dir: Directory to store downloaded data. Defaults to 'data/' in project root.
        """
        if data_dir is None:
            self.data_dir = Path(__file__).parent.parent / "data"
        else:
            self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def download_sample_data(self) -> xr.Dataset:
        """Download sample climate data for testing.

        Returns:
            xr.Dataset: Sample climate dataset.
        """
        # Create sample data using xarray's tutorial data
        ds = xr.tutorial.open_dataset("air_temperature")
        return ds

    def load_netcdf(self, filepath: str | Path) -> xr.Dataset:
        """Load a NetCDF file as an xarray Dataset.

        Args:
            filepath: Path to the NetCDF file.

        Returns:
            xr.Dataset: Loaded dataset.
        """
        return xr.open_dataset(filepath)

    def save_dataset(self, ds: xr.Dataset, filename: str) -> Path:
        """Save a dataset to the data directory.

        Args:
            ds: Dataset to save.
            filename: Name of the file to save.

        Returns:
            Path: Path to the saved file.
        """
        filepath = self.data_dir / filename
        ds.to_netcdf(filepath)
        return filepath

    def list_available_data(self) -> list[Path]:
        """List all available data files in the data directory.

        Returns:
            list[Path]: List of data file paths.
        """
        return list(self.data_dir.glob("*.nc"))

    def get_dataset_info(self, ds: xr.Dataset) -> dict:
        """Get information about a dataset.

        Args:
            ds: Dataset to inspect.

        Returns:
            dict: Dictionary with dataset information.
        """
        return {
            "dimensions": dict(ds.sizes),
            "coordinates": list(ds.coords.keys()),
            "data_variables": list(ds.data_vars.keys()),
            "attributes": dict(ds.attrs),
        }
