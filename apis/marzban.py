#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Helper functions for interacting with Marzban panel API.

This module mirrors the interface of :mod:`marzneshin` but targets the
Marzban API which uses slightly different endpoints and payloads.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import base64
import requests
SESSION = requests.Session()
import os
from cachetools import TTLCache, cached
from threading import RLock

ALLOWED_SCHEMES = ("vless://", "vmess://", "trojan://", "ss://")

FETCH_CACHE_TTL = int(os.getenv("FETCH_CACHE_TTL", "300"))
_links_cache = TTLCache(maxsize=256, ttl=FETCH_CACHE_TTL)
_links_lock = RLock()


def get_headers(token: str) -> Dict[str, str]:
    """Return authorization header for the given bearer token."""
    return {"Authorization": f"Bearer {token}"}


def fetch_user_services(panel_url: str, token: str, username: str) -> Tuple[Optional[List[int]], Optional[str]]:
    """Marzban does not expose service IDs; return an empty list."""
    return [], None


def create_user(panel_url: str, token: str, payload: Dict) -> Tuple[Optional[Dict], Optional[str]]:
    """Create a user on the remote panel."""
    try:
        r = SESSION.post(
            urljoin(panel_url.rstrip('/') + '/', '/api/user'),
            json=payload,
            headers={**get_headers(token), "Content-Type": "application/json"},
            timeout=20,
        )
        if r.status_code in (200, 201):
            return r.json(), None
        return None, f"{r.status_code} {r.text[:300]}"
    except Exception as e:  # pragma: no cover - network errors
        return None, str(e)[:200]


def get_user(panel_url: str, token: str, username: str) -> Tuple[Optional[Dict], Optional[str]]:
    """Fetch user details from the panel."""
    try:
        r = SESSION.get(
            urljoin(panel_url.rstrip('/') + '/', f"/api/user/{username}"),
            headers=get_headers(token),
            timeout=15,
        )
        if r.status_code != 200:
            return None, f"{r.status_code} {r.text[:200]}"
        obj = r.json()
        # normalise to fields expected by bot.py
        status = obj.get('status')
        obj['enabled'] = status != 'disabled'
        sub_url = obj.get('subscription_url') or ''
        token_part = sub_url.rstrip('/').split('/')[-1]
        if token_part:
            obj.setdefault('key', token_part)
        return obj, None
    except Exception as e:  # pragma: no cover - network errors
        return None, str(e)[:200]


@cached(cache=_links_cache, lock=_links_lock)
def fetch_links_from_panel(panel_url: str, username: str, key: str) -> List[str]:
    """Return list of subscription links for a user token.

    Newer Marzban versions expose ``/v2ray`` which returns a base64 encoded
    blob of newline separated configs.  Older versions returned plain text at
    ``/sub/<key>/``.  Try the new endpoint first and fall back to the old one
    for compatibility.
    """
    try:
        url = urljoin(panel_url.rstrip('/') + '/', f"sub/{key}/v2ray")
        r = SESSION.get(url, headers={"accept": "text/plain"}, timeout=20)
        if r.status_code == 200:
            txt = (r.text or "").strip()
            if txt:
                try:
                    decoded = base64.b64decode(txt + "===")
                    txt = decoded.decode(errors="ignore")
                except Exception:
                    pass
                lines = [ln.strip() for ln in txt.splitlines() if ln.strip()]
                if any(ln.lower().startswith(ALLOWED_SCHEMES) for ln in lines):
                    return lines

        # Fallback to legacy plain-text endpoint
        url = urljoin(panel_url.rstrip('/') + '/', f"sub/{key}/")
        r = SESSION.get(url, headers={"accept": "application/json,text/plain"}, timeout=20)
        if r.status_code != 200:
            return []
        try:
            if r.headers.get("content-type", "").startswith("application/json"):
                data = r.json()
                if isinstance(data, list):
                    return [str(x) for x in data]
                if isinstance(data, dict) and "links" in data:
                    return [str(x) for x in data["links"]]
        except Exception:  # pragma: no cover - parsing errors
            pass
        return [
            ln.strip()
            for ln in (r.text or "").splitlines()
            if ln.strip() and ln.strip().lower().startswith(ALLOWED_SCHEMES)
        ]
    except Exception:  # pragma: no cover - network errors
        return []


def disable_remote_user(panel_url: str, token: str, username: str) -> Tuple[bool, Optional[str]]:
    """Disable a user on the panel."""
    try:
        r = SESSION.put(
            urljoin(panel_url.rstrip('/') + '/', f"/api/user/{username}"),
            json={"status": "disabled"},
            headers={**get_headers(token), "Content-Type": "application/json"},
            timeout=20,
        )
        if r.status_code == 200:
            return True, None
        return False, f"{r.status_code} {r.text[:200]}"
    except Exception as e:  # pragma: no cover - network errors
        return False, str(e)[:200]


def enable_remote_user(panel_url: str, token: str, username: str) -> Tuple[bool, Optional[str]]:
    """Enable a user on the panel."""
    try:
        r = SESSION.put(
            urljoin(panel_url.rstrip('/') + '/', f"/api/user/{username}"),
            json={"status": "active"},
            headers={**get_headers(token), "Content-Type": "application/json"},
            timeout=20,
        )
        if r.status_code == 200:
            return True, None
        return False, f"{r.status_code} {r.text[:200]}"
    except Exception as e:  # pragma: no cover - network errors
        return False, str(e)[:200]


def remove_remote_user(panel_url: str, token: str, username: str) -> Tuple[bool, Optional[str]]:
    """Delete a user on the panel."""
    try:
        r = SESSION.delete(
            urljoin(panel_url.rstrip('/') + '/', f"/api/user/{username}"),
            headers=get_headers(token),
            timeout=20,
        )
        if r.status_code == 200:
            return True, None
        return False, f"{r.status_code} {r.text[:200]}"
    except Exception as e:  # pragma: no cover - network errors
        return False, str(e)[:200]


def reset_remote_user_usage(panel_url: str, token: str, username: str) -> Tuple[bool, Optional[str]]:
    """Reset traffic statistics for *username* on the panel."""
    try:
        r = SESSION.post(
            urljoin(panel_url.rstrip('/') + '/', f"/api/user/{username}/reset"),
            headers=get_headers(token),
            timeout=20,
        )
        if r.status_code == 200:
            return True, None
        return False, f"{r.status_code} {r.text[:200]}"
    except Exception as e:  # pragma: no cover - network errors
        return False, str(e)[:200]


def update_remote_user(
    panel_url: str,
    token: str,
    username: str,
    data_limit: Optional[int] = None,
    expire: Optional[int] = None,
) -> Tuple[bool, Optional[str]]:
    """Update quota or expiry for *username* on the panel."""
    payload: Dict[str, int] = {}
    if data_limit is not None:
        payload["data_limit"] = int(data_limit)
        payload["data_limit_reset_strategy"] = "no_reset"
    if expire is not None:
        payload["expire"] = int(expire)
    if not payload:
        return True, None
    try:
        r = SESSION.put(
            urljoin(panel_url.rstrip('/') + '/', f"/api/user/{username}"),
            json=payload,
            headers={**get_headers(token), "Content-Type": "application/json"},
            timeout=20,
        )
        if r.status_code == 200:
            return True, None
        return False, f"{r.status_code} {r.text[:200]}"
    except Exception as e:  # pragma: no cover - network errors
        return False, str(e)[:200]


def fetch_subscription_links(sub_url: str) -> List[str]:
    """Return links from a subscription URL.

    Handles both plain-text lists and base64 encoded blobs returned by the
    ``/v2ray`` endpoint.
    """
    try:
        r = SESSION.get(sub_url, headers={"accept": "text/plain,application/json"}, timeout=20)
        if r.status_code != 200:
            return []
        txt = r.text or ""
        if r.headers.get("content-type", "").startswith("application/json"):
            try:
                data = r.json()
                if isinstance(data, list):
                    return [str(x) for x in data]
                if isinstance(data, dict) and "links" in data:
                    return [str(x) for x in data["links"]]
            except Exception:  # pragma: no cover - parsing errors
                pass
        else:
            try:
                decoded = base64.b64decode(txt.strip() + "===")
                txt = decoded.decode(errors="ignore")
            except Exception:
                pass
        return [
            ln.strip()
            for ln in txt.splitlines()
            if ln.strip() and ln.strip().lower().startswith(ALLOWED_SCHEMES)
        ]
    except Exception:  # pragma: no cover - network errors
        return []


def get_admin_token(panel_url: str, username: str, password: str) -> Tuple[Optional[str], Optional[str]]:
    """Authenticate against the panel and return an access token."""
    token_url = urljoin(panel_url.rstrip('/') + '/', '/api/admin/token')
    try:
        resp = SESSION.post(
            token_url,
            data={"username": username, "password": password, "grant_type": "password"},
            timeout=15,
        )
        if resp.status_code != 200:
            return None, f"{resp.status_code} {resp.text[:200]}"
        tok = (resp.json() or {}).get("access_token")
        if not tok:
            return None, "no access_token"
        return tok, None
    except Exception as e:  # pragma: no cover - network errors
        return None, str(e)[:200]
