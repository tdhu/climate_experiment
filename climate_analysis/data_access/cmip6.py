"""CMIP6 access helpers using ESGF search and point streaming.

We avoid bulk downloads by querying ESGF for matching files and pulling only the
nearest grid-point timeseries via OPeNDAP (preferred) or HTTP-served NetCDF.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

import pandas as pd
import xarray as xr

from climate_analysis.config import load_data_catalog
from climate_analysis.data_access.esgf_auth import build_authenticated_session
from climate_analysis.data_access.esgf_opendap import open_point_timeseries
from climate_analysis.data_access.esgf_search import ESGFSearchConfig, search_files


@dataclass
class CMIP6Config:
    esgf: ESGFSearchConfig
    default_models: list[str]
    experiments: dict[str, str]
    variable_id: str


class CMIP6Client:
    """Open CMIP6 NAT/ALL/SSP data via ESGF point streaming."""

    def __init__(
        self,
        config: Optional[CMIP6Config] = None,
    ):
        cfg = config or self._load_default_config()
        self.config = cfg if isinstance(cfg, CMIP6Config) else self._coerce_config(cfg)
        self.session = build_authenticated_session(
            cfg_client_id=self.config.esgf.auth_client_id,
            cfg_scopes=self.config.esgf.auth_scopes,
        )

    @staticmethod
    def _load_default_config() -> CMIP6Config:
        catalog = load_data_catalog()
        return CMIP6Client._coerce_config(catalog["cmip6"])

    @staticmethod
    def _coerce_config(raw: dict) -> CMIP6Config:
        esgf_raw = raw["esgf"]
        auth_raw = esgf_raw.get("auth", {}) if isinstance(esgf_raw, dict) else {}
        esgf_cfg = ESGFSearchConfig(
            index_url=esgf_raw["index_url"],
            distrib=bool(esgf_raw.get("distrib", True)),
            replica=bool(esgf_raw.get("replica", False)),
            latest=bool(esgf_raw.get("latest", True)),
            response_format=esgf_raw.get("format", "application/solr+json"),
            project=esgf_raw["project"],
            activity_id=esgf_raw["activity_id"],
            experiments=esgf_raw["experiments"],
            variable_id=esgf_raw["variable_id"],
            table_id=esgf_raw["table_id"],
            preferred_grid_labels=esgf_raw.get("preferred_grid_labels", []),
            limit=int(esgf_raw.get("paging", {}).get("limit", 500)),
            max_pages=int(esgf_raw.get("paging", {}).get("max_pages", 200)),
            shards=esgf_raw.get("shards"),
            auth_client_id=auth_raw.get("globus_client_id"),
            auth_scopes=auth_raw.get("scopes", []),
        )
        return CMIP6Config(
            esgf=esgf_cfg,
            default_models=esgf_raw["default_models"],
            experiments=esgf_raw["experiments"],
            variable_id=esgf_raw["variable_id"],
        )

    def available_members(
        self,
        models: Optional[Iterable[str]] = None,
        experiments: Optional[Iterable[str]] = None,
        variable_id: str = "tasmax",
    ) -> pd.DataFrame:
        """List available ensemble members after filtering.

        This runs a cheap ESGF query per experiment and model and returns rows
        for each member found.
        """

        model_sel = list(models) if models else self.config.default_models
        exp_sel = list(experiments) if experiments else list(self.config.experiments.values())

        rows = []
        for model in model_sel:
            for exp in exp_sel:
                records = search_files(
                    self.config.esgf, model=model, experiment=exp, session=self.session
                )
                for rec in records:
                    rows.append(
                        {
                            "model": model,
                            "experiment": exp,
                            "member_id": rec.member_id,
                            "grid_label": rec.grid_label,
                        }
                    )
        return pd.DataFrame(rows)

    def _open_member_timeseries(
        self,
        model: str,
        experiment: str,
        variable_id: str,
        latitude: float,
        longitude: float,
        start_year: int,
        end_year: int,
        member: Optional[str],
    ) -> xr.DataArray:
        """Open and concatenate a member's point timeseries via ESGF.

        Downloads are limited to the requested point/time window using
        HTTPServer streaming via fsspec (primary path).
        """

        records = search_files(
            self.config.esgf, model=model, experiment=experiment, session=self.session
        )
        if not records:
            raise ValueError(f"No ESGF entries for {model} {experiment} {variable_id}")

        # Choose member deterministically.
        if member is None:
            member_choices = sorted({rec.member_id for rec in records if rec.member_id})
            if not member_choices:
                # Fall back to the first record even if member_id is missing.
                member = records[0].member_id
            else:
                member = member_choices[0]
        member_records = [rec for rec in records if rec.member_id == member]
        if not member_records:
            raise ValueError(f"Member {member} not found for {model} {experiment}")

        # Sort by datetime_start if present so concatenation is ordered.
        def _sort_key(rec):
            svc_rank = 0 if rec.access.service == "HTTPServer" else 1
            return (svc_rank, rec.datetime_start or "")

        member_records.sort(key=_sort_key)

        slices = []
        for rec in member_records:
            time_start = f"{start_year}-01-01"
            time_end = f"{end_year}-12-31"
            da = open_point_timeseries(
                url=rec.access.url,
                variable_id=variable_id,
                latitude=latitude,
                longitude=longitude,
                time_start=time_start,
                time_end=time_end,
                session=self.session,
            )
            slices.append(da)

        combined = xr.concat(slices, dim="time") if len(slices) > 1 else slices[0]
        combined = combined.sortby("time")
        return combined

    def open_point_timeseries(
        self,
        model: str,
        experiment: str,
        variable_id: str,
        latitude: float,
        longitude: float,
        start_year: int,
        end_year: int,
        member: Optional[str] = None,
    ) -> xr.DataArray:
        """Open a CMIP6 tasmax series at a given point."""
        tas = self._open_member_timeseries(
            model=model,
            experiment=experiment,
            variable_id=variable_id,
            latitude=latitude,
            longitude=longitude,
            start_year=start_year,
            end_year=end_year,
            member=member,
        )
        return tas

    def open_all_timeseries(
        self,
        model: str,
        variable_id: str,
        latitude: float,
        longitude: float,
        member: Optional[str] = None,
        hist_start_year: int = 1950,
        hist_end_year: int = 2014,
        ssp_end_year: int = 2100,
    ) -> xr.DataArray:
        """Open historical + SSP2-4.5 concatenated to mirror ALL in the paper."""
        hist_exp = self.config.experiments["all"]
        ssp_exp = self.config.experiments["scenario"]
        hist = self._open_member_timeseries(
            model=model,
            experiment=hist_exp,
            variable_id=variable_id,
            latitude=latitude,
            longitude=longitude,
            start_year=hist_start_year,
            end_year=hist_end_year,
            member=member,
        )
        ssp = self._open_member_timeseries(
            model=model,
            experiment=ssp_exp,
            variable_id=variable_id,
            latitude=latitude,
            longitude=longitude,
            start_year=2015,
            end_year=ssp_end_year,
            member=member,
        )
        combined = xr.concat([hist, ssp], dim="time")
        return combined

    def open_nat_timeseries(
        self,
        model: str,
        variable_id: str,
        latitude: float,
        longitude: float,
        member: Optional[str] = None,
        start_year: int = 1950,
        end_year: int = 2020,
    ) -> xr.DataArray:
        """Open NAT experiment timeseries for a point."""
        nat_exp = self.config.experiments["nat"]
        tas = self._open_member_timeseries(
            model=model,
            experiment=nat_exp,
            variable_id=variable_id,
            latitude=latitude,
            longitude=longitude,
            start_year=start_year,
            end_year=end_year,
            member=member,
        )
        return tas
