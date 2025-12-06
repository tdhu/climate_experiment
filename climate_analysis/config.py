"""Configuration helpers for loading YAML-driven settings.

All config files live under the repository's ``config/`` directory by default. The helpers
return plain Python objects so downstream modules stay dependency-light.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_DIR = REPO_ROOT / "config"


def _load_yaml(path: Path) -> Any:
    """Load a YAML file and return its contents."""
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def load_sites(config_path: Path | None = None) -> dict[str, Any]:
    """Load site metadata (Table 1 locations) from YAML."""
    path = config_path or DEFAULT_CONFIG_DIR / "sites.yaml"
    data = _load_yaml(path)
    if not isinstance(data, dict) or "sites" not in data:
        raise ValueError(f"sites config missing expected structure: {path}")
    return data


def load_thresholds(config_path: Path | None = None) -> dict[str, Any]:
    """Load default exceedance thresholds (°C)."""
    path = config_path or DEFAULT_CONFIG_DIR / "thresholds.yaml"
    data = _load_yaml(path)
    if "default_thresholds_c" not in data:
        raise ValueError(f"threshold config missing defaults: {path}")
    return data


def load_data_catalog(config_path: Path | None = None) -> dict[str, Any]:
    """Load data catalog definitions (CMIP6, reanalysis, stations)."""
    path = config_path or DEFAULT_CONFIG_DIR / "data_catalog.yaml"
    return _load_yaml(path)


def dump_json(data: Any) -> str:
    """Pretty-print helper used in notebooks and logging."""
    return json.dumps(data, indent=2, sort_keys=True, default=str)
