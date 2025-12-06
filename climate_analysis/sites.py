"""Helpers for accessing configured sites."""

from __future__ import annotations

from typing import Dict

from climate_analysis.config import load_sites


def get_site(site_id: str) -> Dict:
    """Return site metadata by ID (e.g., 'QAT')."""
    cfg = load_sites()
    matches = [s for s in cfg["sites"] if s["id"] == site_id]
    if not matches:
        raise KeyError(f"Site {site_id} not found in config/sites.yaml")
    return matches[0]


def list_sites() -> list[Dict]:
    """List all configured sites."""
    return load_sites()["sites"]
