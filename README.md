# Climate Experiment

Tools and notebooks for downloading, analyzing, and visualizing climate data.

## Features

- **Climate Data Tools**: Python library for downloading climate data using intake and xarray
- **Visualization**: Matplotlib-based plotting utilities for climate data
- **Jupyter Notebooks**: Interactive notebooks for prototyping and experimentation
- **Modern Package Management**: Uses `uv` for fast, reliable dependency management
- **Dev Container**: Ready-to-use development environment with VS Code

## Project Structure

```
climate_experiment/
├── climate_tools/          # Python library for climate data
│   ├── __init__.py
│   ├── download.py         # Data downloading utilities
│   └── plot.py             # Visualization utilities
├── data/                   # Directory for downloaded climate data
├── notebooks/              # Jupyter notebooks for experimentation
├── .devcontainer/          # VS Code dev container configuration
└── pyproject.toml          # Project configuration and dependencies
```

## Quick Start

### Installation

1. Install [uv](https://docs.astral.sh/uv/getting-started/installation/):
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. Install dependencies:
   ```bash
   uv sync --all-extras
   ```

### Usage

```python
from climate_tools import ClimateDataDownloader, plot_temperature_map

# Initialize downloader
downloader = ClimateDataDownloader()

# Download sample data
ds = downloader.download_sample_data()

# Plot temperature map
fig = plot_temperature_map(ds)
```

### Running Jupyter Notebooks

```bash
uv run jupyter lab
```

Then open `notebooks/climate_analysis.ipynb` to get started.

## Development

### Using Dev Container (Recommended)

1. Install [VS Code](https://code.visualstudio.com/) and the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)
2. Open this project in VS Code
3. Click "Reopen in Container" when prompted

### Manual Setup

```bash
# Install all dependencies including dev tools
uv sync --all-extras

# Run tests
uv run pytest

# Run linter
uv run ruff check .
```

## Dependencies

- **intake**: Data catalog for easy data access
- **xarray**: N-D labeled arrays and datasets
- **matplotlib**: Visualization library
- **netcdf4**: NetCDF file support
- **dask**: Parallel computing support
