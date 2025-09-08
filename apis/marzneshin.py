#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Helper functions for interacting with Marzneshin panel API.

This module centralizes all HTTP requests so that other modules (like
bot.py) can remain agnostic to the underlying API implementation.  In the
future additional panel API implementations can live alongside these
functions and bot.py can choose between them.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin
from datetime import datetime, timezone

import requests
SESSION = requests.Session()
import os
from cachetools import TTLCache, cached
from threading import RLock

FETCH_CACHE_TTL = int(os.getenv("FETCH_CACHE_TTL", "300"))
_links_cache = TTLCache(maxsize=256, ttl=FETCH_CACHE_TTL)
_links_lock = RLock()


def get_headers(token: str) -> Dict[str, str]:
    """Return authorization header for the given bearer token."""
    return {"Authorization": f"Bearer {token}"}


def fetch_user_services(panel_url: str, token: str, username: str) -> Tuple[Optional[List[int]], Optional[str]]:
    """Return list of service IDs for *username* or an error message."""
    try:
        r = SESSION.get(
            urljoin(panel_url.rstrip('/') + '/', f"/api/users/{username}/services"),
            headers=get_headers(token),
            timeout=15,
        )
        if r.status_code != 200:
            return None, f"{r.status_code} {r.text[:200]}"
        items = (r.json() or {}).get("items") or []
        return [it["id"] for it in items if isinstance(it.get("id"), int)], None
    except Exception as e:  # pragma: no cover - network errors
        return None, str(e)[:200]


def create_user(panel_url: str, token: str, payload: Dict) -> Tuple[Optional[Dict], Optional[str]]:
    """Create a user on the remote panel."""
    try:
        r = SESSION.post(
            urljoin(panel_url.rstrip('/') + '/', '/api/users'),
            json=payload,
            headers={**get_headers(token), "Content-Type": "application/json"},
            timeout=20,
        )
        if r.status_code == 200:
            return r.json(), None
        return None, f"{r.status_code} {r.text[:300]}"
    except Exception as e:  # pragma: no cover - network errors
        return None, str(e)[:200]


def get_user(panel_url: str, token: str, username: str) -> Tuple[Optional[Dict], Optional[str]]:
    """Fetch user details from the panel."""
    try:
        r = SESSION.get(
            urljoin(panel_url.rstrip('/') + '/', f"/api/users/{username}"),
            headers=get_headers(token),
            timeout=15,
        )
        if r.status_code == 200:
            return r.json(), None
        return None, f"{r.status_code} {r.text[:200]}"
    except Exception as e:  # pragma: no cover - network errors
        return None, str(e)[:200]


@cached(cache=_links_cache, lock=_links_lock)
def fetch_links_from_panel(panel_url: str, username: str, key: str) -> List[str]:
    """Return list of subscription links for a template user."""
    try:
        url = urljoin(panel_url.rstrip('/') + '/', f"sub/{username}/{key}/links")
        r = SESSION.get(url, headers={"accept": "application/json"}, timeout=20)
        try:
            if r.headers.get("content-type", "").startswith("application/json"):
                data = r.json()
                if isinstance(data, list):
                    return [str(x) for x in data]
                if isinstance(data, dict) and "links" in data:
                    return [str(x) for x in data["links"]]
        except Exception:  # pragma: no cover - parsing errors
            pass
        return [ln.strip() for ln in (r.text or "").splitlines() if ln.strip()]
    except Exception:  # pragma: no cover - network errors
        return []


def disable_remote_user(panel_url: str, token: str, username: str) -> Tuple[bool, Optional[str]]:
    """Disable a user on the panel."""
    try:
        r = SESSION.post(
            urljoin(panel_url.rstrip('/') + '/', f"/api/users/{username}/disable"),
            headers=get_headers(token),
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
        r = SESSION.post(
            urljoin(panel_url.rstrip('/') + '/', f"/api/users/{username}/enable"),
            headers=get_headers(token),
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
            urljoin(panel_url.rstrip('/') + '/', f"/api/users/{username}"),
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
            urljoin(panel_url.rstrip('/') + '/', f"/api/users/{username}/reset"),
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
    payload: Dict[str, object] = {"username": username}
    if data_limit is not None:
        payload["data_limit"] = int(data_limit)
        payload["data_limit_reset_strategy"] = "no_reset"
    if expire is not None:
        dt = datetime.fromtimestamp(int(expire), tz=timezone.utc)
        payload["expire_strategy"] = "fixed_date"
        payload["expire_date"] = dt.isoformat().replace("+00:00", "Z")
    if len(payload) == 1:
        return True, None
    try:
        r = SESSION.put(
            urljoin(panel_url.rstrip('/') + '/', f"/api/users/{username}"),
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
    """Return links from a subscription URL."""
    try:
        r = SESSION.get(sub_url, headers={"accept": "text/plain,application/json"}, timeout=20)
        if r.headers.get("content-type", "").startswith("application/json"):
            data = r.json()
            if isinstance(data, list):
                return [str(x) for x in data]
            if isinstance(data, dict) and "links" in data:
                return [str(x) for x in data["links"]]
        return [ln.strip() for ln in (r.text or "").splitlines() if ln.strip()]
    except Exception:  # pragma: no cover - network errors
        return []


def get_admin_token(panel_url: str, username: str, password: str) -> Tuple[Optional[str], Optional[str]]:
    """Authenticate against the panel and return an access token."""
    token_url = urljoin(panel_url.rstrip('/') + '/', '/api/admins/token')
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
