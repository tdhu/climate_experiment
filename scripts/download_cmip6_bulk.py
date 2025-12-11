"""Download required CMIP6 tasmax files (NAT, ALL, SSP) for default models.

This uses the same Globus-authenticated HTTPServer URLs the client relies on.
Files are saved under .cache/esgf/ with resume support.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import requests

from climate_analysis.data_access.cmip6 import CMIP6Client
from climate_analysis.data_access.esgf_auth import build_authenticated_session
from climate_analysis.data_access.esgf_search import search_files

CACHE_DIR = Path(__file__).resolve().parent.parent / ".cache" / "esgf"
CHUNK = 1024 * 1024  # 1 MB


def _auth_headers(session: Optional[requests.Session]) -> dict:
    if session and getattr(session, "auth", None):
        auth = session.auth
        if hasattr(auth, "authorizer"):
            try:
                auth.authorizer.ensure_valid_token()
                return {"Authorization": f"Bearer {auth.authorizer.access_token}"}
            except Exception:
                return {}
    return {}


def _remote_size(url: str, headers: dict, session: Optional[requests.Session]) -> Optional[int]:
    http = session or requests
    try:
        resp = http.head(url, headers=headers, timeout=60, allow_redirects=True)
        resp.raise_for_status()
        length = resp.headers.get("Content-Length")
        return int(length) if length is not None else None
    except Exception:
        return None


def download_with_resume(url: str, dest: Path, session: Optional[requests.Session]) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    headers = _auth_headers(session)
    expected = _remote_size(url, headers, session)
    if dest.exists() and expected and dest.stat().st_size >= expected:
        print(f"[skip] {dest.name} already complete ({dest.stat().st_size} bytes)")
        return
    resume_at = dest.stat().st_size if dest.exists() else 0
    if resume_at:
        headers["Range"] = f"bytes={resume_at}-"
    http = session or requests
    with http.get(url, headers=headers, stream=True, timeout=1800) as resp:
        if resp.status_code not in (200, 206):
            resp.raise_for_status()
        mode = "ab" if resume_at and resp.status_code == 206 else "wb"
        with open(dest, mode) as out:
            for chunk in resp.iter_content(chunk_size=CHUNK):
                if chunk:
                    out.write(chunk)
    final = dest.stat().st_size
    if expected and final < expected:
        raise IOError(f"Incomplete download for {dest.name}: got {final}, expected {expected}")
    print(f"[ok]   {dest.name} ({final/1e6:.1f} MB)")


def main():
    client = CMIP6Client()
    session = client.session or build_authenticated_session()
    models = client.config.default_models
    exps = {
        "nat": client.config.experiments["nat"],
        "all": client.config.experiments["all"],
        "scenario": client.config.experiments["scenario"],
    }
    total = 0
    for model in models:
        for label, exp in exps.items():
            print(f"\n=== {model} {exp} ({label}) ===")
            records = search_files(client.config.esgf, model=model, experiment=exp, session=session)
            if not records:
                print("  [warn] no records")
                continue
            for rec in records:
                url = rec.access.url
                # Force https for consistency
                if url.startswith("http://"):
                    url = "https://" + url[len("http://") :]
                filename = os.path.basename(url.split("?")[0]) or f"{model}_{exp}.nc"
                dest = CACHE_DIR / filename
                try:
                    download_with_resume(url, dest, session)
                    total += 1
                except Exception as exc:
                    print(f"  [error] {filename}: {exc}")
    print(f"\nCompleted downloads: {total}")


if __name__ == "__main__":
    main()
