"""Station data download utilities (uses Meteostat for frictionless access).

The goal is to minimise barriers for SMEs: no API keys, small downloads, and clear logging.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd
from meteostat import Daily, Stations


@dataclass
class StationMeta:
    """Minimal station metadata resolved from config."""

    site_id: str
    name: str
    latitude: float
    longitude: float
    meteostat_id: str


class StationClient:
    """Fetch daily Tmax observations for the reference sites."""

    def __init__(self, data_dir: Optional[str | Path] = None):
        base_dir = Path(data_dir) if data_dir else Path(__file__).resolve().parent.parent / "data"
        self.data_dir = base_dir / "stations"
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def _resolve_station(self, site: dict) -> StationMeta:
        """Resolve a Meteostat station ID from config; fallback to nearest-neighbor lookup."""
        if site.get("meteostat_id"):
            station_id = site["meteostat_id"]
        else:
            stations = (
                Stations()
                .nearby(site["latitude"], site["longitude"])
                .fetch(1)
            )
            if stations.empty:
                raise ValueError(f"No Meteostat station found near {site['name']}")
            station_id = stations.index[0]
        return StationMeta(
            site_id=site["id"],
            name=site["name"],
            latitude=site["latitude"],
            longitude=site["longitude"],
            meteostat_id=station_id,
        )

    def fetch_daily_tmax(
        self,
        site: dict,
        start: date,
        end: date,
        refresh: bool = False,
    ) -> pd.Series:
        """Download (or read cached) daily Tmax for a site."""
        meta = self._resolve_station(site)
        start_ts = pd.to_datetime(start)
        end_ts = pd.to_datetime(end)
        cache_path = self.data_dir / f"{meta.site_id}_{start.isoformat()}_{end.isoformat()}.parquet"
        if cache_path.exists() and not refresh:
            df = pd.read_parquet(cache_path)
            return df["tmax_c"]

        # Meteostat returns temperatures in °C; we retain as-is to mirror paper.
        daily = Daily(meta.meteostat_id, start_ts, end_ts).fetch()
        if "tmax" not in daily.columns:
            raise ValueError(f"Meteostat response missing tmax for {meta.meteostat_id}")
        daily = daily.rename(columns={"tmax": "tmax_c"})
        # Drop obvious gaps to keep GEV fit stable; SMEs can see the rule here.
        daily = daily.dropna(subset=["tmax_c"])
        daily.to_parquet(cache_path)
        return daily["tmax_c"]
