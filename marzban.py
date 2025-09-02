#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Helper functions for interacting with Marzban panel API.

This module mirrors the interface of :mod:`marzneshin` but targets the
Marzban API which uses slightly different endpoints and payloads.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests


def get_headers(token: str) -> Dict[str, str]:
    """Return authorization header for the given bearer token."""
    return {"Authorization": f"Bearer {token}"}


def fetch_user_services(panel_url: str, token: str, username: str) -> Tuple[Optional[List[int]], Optional[str]]:
    """Marzban does not expose service IDs; return an empty list."""
    return [], None


def create_user(panel_url: str, token: str, payload: Dict) -> Tuple[Optional[Dict], Optional[str]]:
    """Create a user on the remote panel."""
    try:
        r = requests.post(
            urljoin(panel_url.rstrip('/') + '/', '/api/user'),
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
        r = requests.get(
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


def fetch_links_from_panel(panel_url: str, username: str, key: str) -> List[str]:
    """Return list of subscription links for a user token."""
    try:
        url = urljoin(panel_url.rstrip('/') + '/', f"sub/{key}/")
        r = requests.get(url, headers={"accept": "application/json"}, timeout=20)
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
        r = requests.put(
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
        r = requests.put(
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


def fetch_subscription_links(sub_url: str) -> List[str]:
    """Return links from a subscription URL."""
    try:
        r = requests.get(sub_url, headers={"accept": "text/plain,application/json"}, timeout=20)
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
    token_url = urljoin(panel_url.rstrip('/') + '/', '/api/admin/token')
    try:
        resp = requests.post(
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
