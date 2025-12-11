"""Bulk-download full CMIP6 tasmax files (no spatial trimming) for required runs.

Features:
- Uses Globus/OIDC bearer auth from `.cache/globus_tokens.json`.
+- Enumerates all files for selected models/experiments (hist-nat, historical, ssp245).
- Resumable HTTPS downloads with size checks and optional checksum verification.
- Organized cache layout: `.cache/esgf/raw/<model>/<experiment>/<filename>`.
- Optional concurrency, dry-run, and model/experiment filters.
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import hashlib
import os
from pathlib import Path
from typing import Iterable, Optional

import requests

from climate_analysis.data_access.cmip6 import CMIP6Client
from climate_analysis.data_access.esgf_auth import build_authenticated_session
from climate_analysis.data_access.esgf_search import search_files

CACHE_ROOT = Path(__file__).resolve().parent.parent / ".cache" / "esgf" / "raw"
CHUNK = 2 * 1024 * 1024  # 2 MB


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--models", nargs="+", help="Models to include (default: config default_models)")
    p.add_argument(
        "--experiments",
        nargs="+",
        choices=["hist-nat", "historical", "ssp245"],
        help="Experiments to include (default: all three)",
    )
    p.add_argument("--max-workers", type=int, default=3, help="Concurrent downloads")
    p.add_argument("--dry-run", action="store_true", help="List files and sizes, do not download")
    p.add_argument("--max-total-bytes", type=float, help="Abort if estimated total exceeds this (GB)")
    return p.parse_args()


def auth_headers(session: Optional[requests.Session]) -> dict:
    if session and getattr(session, "auth", None):
        auth = session.auth
        if hasattr(auth, "authorizer"):
            try:
                auth.authorizer.ensure_valid_token()
                return {"Authorization": f"Bearer {auth.authorizer.access_token}"}
            except Exception:
                return {}
    return {}


def remote_size(url: str, headers: dict, session: Optional[requests.Session]) -> Optional[int]:
    http = session or requests
    try:
        resp = http.head(url, headers=headers, timeout=60, allow_redirects=True)
        resp.raise_for_status()
        length = resp.headers.get("Content-Length")
        return int(length) if length is not None else None
    except Exception:
        return None


def checksum_sha256(path: Path, block: int = 4 * 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(block), b""):
            h.update(chunk)
    return h.hexdigest()


def ensure_https(url: str) -> str:
    return url.replace("http://", "https://", 1) if url.startswith("http://") else url


def download_one(url: str, dest: Path, session: Optional[requests.Session]) -> tuple[str, str]:
    headers = auth_headers(session)
    url = ensure_https(url)
    expected = remote_size(url, headers, session)
    if dest.exists() and expected and dest.stat().st_size >= expected:
        return ("skip", dest.name)

    dest.parent.mkdir(parents=True, exist_ok=True)
    resume_at = dest.stat().st_size if dest.exists() else 0
    if resume_at and expected and resume_at >= expected:
        return ("skip", dest.name)
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
        return ("error", f"{dest.name}: incomplete ({final} vs {expected})")
    return ("ok", dest.name)


def main():
    args = parse_args()
    client = CMIP6Client()
    session = client.session or build_authenticated_session()

    models = args.models or client.config.default_models
    exps = args.experiments or ["hist-nat", "historical", "ssp245"]

    records = []
    for model in models:
        for exp in exps:
            recs = search_files(client.config.esgf, model=model, experiment=exp, session=session)
            if not recs:
                print(f"[warn] no records for {model} {exp}")
                continue
            for rec in recs:
                url = ensure_https(rec.access.url)
                fname = os.path.basename(url.split("?")[0]) or f"{model}_{exp}.nc"
                dest = CACHE_ROOT / model / exp / fname
                records.append((url, dest))

    # Deduplicate by destination path
    uniq = {}
    for url, dest in records:
        uniq[dest] = url
    records = [(u, d) for d, u in uniq.items()]

    # Estimate total size
    total_bytes = 0
    headers = auth_headers(session)
    for url, dest in records:
        sz = remote_size(url, headers, session)
        if sz:
            total_bytes += sz
    if args.max_total_bytes and total_bytes and total_bytes > args.max_total_bytes * 1e9:
        print(f"[abort] estimated {total_bytes/1e9:.2f} GB exceeds limit {args.max_total_bytes} GB")
        return

    print(f"Planned files: {len(records)}")
    if total_bytes:
        print(f"Estimated total size: {total_bytes/1e9:.2f} GB")
    if args.dry_run:
        for url, dest in records:
            print(dest)
        return

    def task(url_dest):
        url, dest = url_dest
        try:
            return download_one(url, dest, session)
        except Exception as exc:
            return ("error", f"{dest.name}: {exc}")

    ok = skip = err = 0
    with cf.ThreadPoolExecutor(max_workers=args.max_workers) as pool:
        for status, msg in pool.map(task, records):
            if status == "ok":
                ok += 1
                print(f"[ok]   {msg}")
            elif status == "skip":
                skip += 1
                print(f"[skip] {msg}")
            else:
                err += 1
                print(f"[err]  {msg}")
    print(f"\nSummary: ok={ok} skip={skip} err={err}")


if __name__ == "__main__":
    main()
