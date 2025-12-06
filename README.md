# Climate Analysis

Implements the exceedance methodology from Christidis et al. (2023) with live data pulls, clear commenting, and a Doha walkthrough notebook.

## Key pieces

- **Package:** `climate_analysis/` with data access (Meteostat stations, CMIP6 via Pangeo), processing (tx01, bias correction), analysis (GEV, probabilities, risk ratios, HRW/HRD).
- **Config-driven:** `config/sites.yaml` (site metadata), `config/data_catalog.yaml` (remote sources), `config/thresholds.yaml` (default 45/50 °C).
- **Notebook:** `notebooks/doha_exceedance_walkthrough.ipynb` shows the full flow for Doha International Airport with editable thresholds.
- **Caching:** Downloads are cached under `data/` to keep reruns fast.

## Quick start

```bash
uv sync --all-extras         # install dependencies
uv run python -c "from climate_analysis.sites import list_sites; print(list_sites())"
uv run jupyter lab           # open the notebooks
```

## Doha demo (CLI)

```bash
uv run python - <<'PY'
from datetime import date
from climate_analysis.sites import get_site
from climate_analysis.data_access import StationClient
from climate_analysis.processing import compute_tx01_from_daily
from climate_analysis.analysis import return_time_bootstrap

site = get_site("QAT")
client = StationClient()
daily = client.fetch_daily_tmax(site, date(1983,1,1), date(2023,12,31))
tx01 = compute_tx01_from_daily(daily)
print(return_time_bootstrap(tx01, threshold_c=50, n_boot=300))
PY
```

Or run the end-to-end workflow (station + CMIP6) headlessly:

```bash
uv run python scripts/doha_quickstart.py
```

## Running the notebook

1. `uv run jupyter lab`
2. Open `notebooks/doha_exceedance_walkthrough.ipynb`
3. Execute cells; thresholds are editable in the first cell.

## Notes

- CMIP6 data are pulled lazily through the Pangeo intake catalog to reduce transfer size; station data come from Meteostat (no API key).
- All public functions include docstrings and inline comments explaining the statistical steps for SMEs.
- Some CMIP6 catalogs lack NAT (hist-nat) daily tasmax. The CMIP6 client will fall back to `historical` as a proxy for NAT and warn; swap in a catalog with true NAT runs when available for faithful risk ratios.
