"""Lightweight ESGF search client.

This helper hits the ESGF REST API directly via ``requests`` and returns a
per-file manifest with access URLs (preferring OPeNDAP when available) so
callers can stream narrow slices instead of downloading full files.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, List, Optional
import urllib.parse

import requests


@dataclass
class ESGFSearchConfig:
    """Configuration needed to build ESGF search URLs."""

    index_url: str
    distrib: bool
    replica: bool
    latest: bool
    response_format: str
    project: str
    activity_id: str
    experiments: dict
    variable_id: str
    table_id: str
    preferred_grid_labels: list[str]
    limit: int = 500
    max_pages: int = 200
    shards: Optional[str] = None
    auth_client_id: Optional[str] = None
    auth_scopes: list[str] = field(default_factory=list)


@dataclass
class FileAccess:
    """Parsed access URL for a single ESGF file."""

    url: str
    service: str
    mime: str


@dataclass
class FileRecord:
    """Minimal file manifest entry returned by ESGF search."""

    dataset_id: str
    member_id: Optional[str]
    grid_label: Optional[str]
    datetime_start: Optional[str]
    datetime_stop: Optional[str]
    access: FileAccess


def _choose_access(url_field: str) -> Optional[FileAccess]:
    """Pick the best access tuple from a pipe-delimited URL field.

    Simplified: prefer HTTPServer (most reliable), else first available.
    """

    def _normalize(url: str, service: str) -> str:
        if url.startswith("http://"):
            return "https://" + url[len("http://") :]
        return url

    if isinstance(url_field, list):
        entries = "|".join(url_field).split("|")
    else:
        entries = str(url_field).split("|")
    # URLs are grouped in triples: url|mime|service. Protect against malformed.
    triples = [
        entries[i : i + 3] for i in range(0, len(entries), 3) if len(entries[i : i + 3]) == 3
    ]
    # Prefer HTTPServer; if absent, take first available.
    for url, mime, svc in triples:
        if svc == "HTTPServer":
            return FileAccess(url=_normalize(url, svc), service=svc, mime=mime)
    if triples:
        url, mime, svc = triples[0]
        return FileAccess(url=_normalize(url, svc), service=svc, mime=mime)
    return None


def _build_params(cfg: ESGFSearchConfig, model: str, experiment: str) -> dict:
    params = {
        "type": "File",
        "project": cfg.project,
        "experiment_id": experiment,
        "source_id": model,
        "variable_id": cfg.variable_id,
        "table_id": cfg.table_id,
        "latest": str(cfg.latest).lower(),
        "replica": str(cfg.replica).lower(),
        "distrib": str(cfg.distrib).lower(),
        "format": cfg.response_format,
        "limit": cfg.limit,
    }
    exp_lower = experiment.lower()
    if cfg.activity_id and not (exp_lower.startswith("ssp") or exp_lower == "historical"):
        params["activity_id"] = cfg.activity_id
    if cfg.shards:
        params["shards"] = cfg.shards
    return params


def _parse_docs(docs: list, preferred_grid_labels: Iterable[str]) -> List[FileRecord]:
    records: List[FileRecord] = []
    for doc in docs:
        url_field = doc.get("url")
        access = _choose_access(url_field) if url_field else None
        if not access:
            continue
        grid_label_raw = doc.get("grid_label")
        grid_label = grid_label_raw[0] if isinstance(grid_label_raw, list) else grid_label_raw
        if preferred_grid_labels and grid_label not in preferred_grid_labels:
            # Skip grids we don't want to touch when we can avoid it.
            continue
        member_raw = doc.get("member_id")
        member_id = member_raw[0] if isinstance(member_raw, list) else member_raw
        record = FileRecord(
            dataset_id=doc.get("dataset_id", ""),
            member_id=member_id,
            grid_label=grid_label,
            datetime_start=doc.get("datetime_start"),
            datetime_stop=doc.get("datetime_stop"),
            access=access,
        )
        records.append(record)
    return records


def search_files(
    cfg: ESGFSearchConfig,
    model: str,
    experiment: str,
    session: Optional[requests.Session] = None,
) -> List[FileRecord]:
    """Query ESGF for files matching model/experiment and return access manifests."""

    params = _build_params(cfg, model=model, experiment=experiment)
    preferred_grids = tuple(cfg.preferred_grid_labels)
    records: List[FileRecord] = []
    offset = 0
    http = session or requests
    for _ in range(cfg.max_pages):
        params_with_offset = {**params, "offset": offset}
        url = f"{cfg.index_url.rstrip('/')}/search"
        resp = http.get(url, params=params_with_offset, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        docs = payload.get("response", {}).get("docs", [])
        if not docs:
            break
        records.extend(_parse_docs(docs, preferred_grids))
        if len(docs) < cfg.limit:
            break
        offset += cfg.limit
    return records
