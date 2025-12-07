"""Lightweight Globus/OIDC helper for ESGF access.

This module handles device-code login, token refresh, and wiring an authenticated
``requests.Session`` that can be reused by search and OPeNDAP calls. Tokens are
persisted under ``.cache/`` so they stay out of version control.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Iterable, Optional

import globus_sdk
import requests

from climate_analysis.config import REPO_ROOT

# Defaults keep tokens local and git-ignored.
DEFAULT_TOKEN_PATH = REPO_ROOT / ".cache" / "globus_tokens.json"
DEFAULT_SCOPES = (
    "openid",
    "profile",
    "email",
    "urn:globus:auth:scope:search.api.globus.org:search",
)

ENV_CLIENT_ID = "ESGF_GLOBUS_CLIENT_ID"
ENV_SCOPES = "ESGF_GLOBUS_SCOPES"


def _load_tokens(path: Path) -> Optional[dict]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError:
        return None


def _save_tokens(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)


def _extract_token_payload(token_response: globus_sdk.token_response.TokenResponse) -> dict:
    """Flatten the first resource server's tokens for local storage."""
    rs, tokens = next(iter(token_response.by_resource_server.items()))
    return {
        "resource_server": rs,
        "access_token": tokens["access_token"],
        "refresh_token": tokens.get("refresh_token"),
        "expires_at": tokens.get("expires_at_seconds"),
        "token_type": tokens.get("token_type", "Bearer"),
    }


def _on_refresh(path: Path):
    def _handler(token_response):
        _save_tokens(path, _extract_token_payload(token_response))

    return _handler


class GlobusRequestsAuth(requests.auth.AuthBase):
    """Requests auth wrapper that refreshes the token transparently."""

    def __init__(self, authorizer: globus_sdk.RefreshTokenAuthorizer):
        self.authorizer = authorizer

    def __call__(self, r: requests.PreparedRequest) -> requests.PreparedRequest:
        # Ensure a fresh access token, then attach it.
        self.authorizer.ensure_valid_token()
        r.headers["Authorization"] = f"Bearer {self.authorizer.access_token}"
        return r


def _get_scopes(cfg_scopes: Optional[Iterable[str]] = None) -> list[str]:
    if scopes_env := os.getenv(ENV_SCOPES):
        return scopes_env.split()
    if cfg_scopes:
        return list(cfg_scopes)
    return list(DEFAULT_SCOPES)


def _get_client_id(cfg_client_id: Optional[str] = None) -> Optional[str]:
    return os.getenv(ENV_CLIENT_ID) or cfg_client_id


def _start_device_flow(
    client: globus_sdk.NativeAppAuthClient, scopes: list[str], token_path: Path
) -> globus_sdk.RefreshTokenAuthorizer:
    """Interactive copy/paste login using the native app flow."""

    client.oauth2_start_flow(requested_scopes=scopes, refresh_tokens=True)
    authorize_url = client.oauth2_get_authorize_url()
    print("ESGF/Globus login required.")
    print(f"1) Open this URL in a browser:\n   {authorize_url}\n")
    auth_code = input("2) Paste the authorization code here: ").strip()
    token_response = client.oauth2_exchange_code_for_tokens(auth_code)
    payload = _extract_token_payload(token_response)
    _save_tokens(token_path, payload)
    return globus_sdk.RefreshTokenAuthorizer(
        payload["refresh_token"],
        client,
        access_token=payload["access_token"],
        expires_at=payload["expires_at"],
        on_refresh=_on_refresh(token_path),
    )


def _build_authorizer(
    client_id: Optional[str],
    scopes: list[str],
    token_path: Path = DEFAULT_TOKEN_PATH,
) -> Optional[globus_sdk.RefreshTokenAuthorizer]:
    if not client_id:
        return None

    client = globus_sdk.NativeAppAuthClient(client_id=client_id)
    stored = _load_tokens(token_path)
    if stored and stored.get("refresh_token"):
        return globus_sdk.RefreshTokenAuthorizer(
            stored["refresh_token"],
            client,
            access_token=stored.get("access_token"),
            expires_at=stored.get("expires_at"),
            on_refresh=_on_refresh(token_path),
        )

    return _start_device_flow(client, scopes, token_path)


def build_authenticated_session(
    cfg_client_id: Optional[str] = None,
    cfg_scopes: Optional[Iterable[str]] = None,
    token_path: Path = DEFAULT_TOKEN_PATH,
) -> Optional[requests.Session]:
    """Return a requests.Session with bearer auth attached, or None if disabled."""
    client_id = _get_client_id(cfg_client_id)
    scopes = _get_scopes(cfg_scopes)
    authorizer = _build_authorizer(client_id, scopes, token_path)
    if not authorizer:
        return None

    session = requests.Session()
    session.auth = GlobusRequestsAuth(authorizer)
    return session
