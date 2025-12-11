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

- CMIP6 via ESGF HTTPServer (primary path). We search the DKRZ index, pick HTTPServer URLs, download NetCDFs to `.cache/esgf/` with resume support, then slice locally. Files are reused on reruns and are git-ignored.
- Station data come from Meteostat (no API key).
- ESGF/Globus auth: set `ESGF_GLOBUS_CLIENT_ID` (or fill `cmip6.esgf.auth.globus_client_id`), then:
  ```bash
  uv run python -c "from climate_analysis.data_access.esgf_auth import build_authenticated_session; build_authenticated_session()"
  ```
  Paste the auth code when prompted. Tokens land in `.cache/globus_tokens.json` (git-ignored) and auto-refresh.
- Default model/site workflow:
  ```bash
  uv run python scripts/doha_quickstart.py           # QAT site, CanESM5 model
  ```
  Or programmatic:
  ```python
  from climate_analysis.workflows import analyze_site_exceedance
  res = analyze_site_exceedance("QAT", model="CanESM5")
  ```
- Large downloads: Each CanESM5 daily tasmax file is ~1–2 GB. If interrupted, rerun; downloads resume. Cache is at `.cache/esgf/`.
- Bulk download (ALL/NAT/SSP tasmax for default models):
  ```bash
  uv run python scripts/download_cmip6_bulk.py
  ```
