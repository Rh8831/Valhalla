#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Helper functions for interacting with MHSanaei/3x-ui panel API.

This implementation provides a minimal subset of the behaviour exposed by
:mod:`marzneshin` and :mod:`marzban` so that other modules can treat the
3x-ui panel in a similar fashion.  The 3x-ui panel differs from Marzban and
Marzneshin in that it does not expose subscription endpoints.  As such,
configuration links are assembled directly from inbound information
retrieved via the API and the client's UUID.

The functions favour best-effort behaviour – network failures or unexpected
payloads are surfaced as error strings instead of raising exceptions.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import json
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
    """Return headers (cookie based) for the given session token."""
    return {"Cookie": token}


def fetch_user_services(panel_url: str, token: str, username: str) -> Tuple[Optional[List[int]], Optional[str]]:
    """3x-ui does not expose service identifiers; return an empty list."""
    return [], None


def create_user(panel_url: str, token: str, payload: Dict) -> Tuple[Optional[Dict], Optional[str]]:
    """Create a user on the remote panel.

    The 3x-ui API requires the inbound ID and the client object.  Because
    configuration details vary widely, callers must supply the appropriate
    payload.  This helper simply forwards the payload to the
    ``/panel/api/inbounds/addClient`` endpoint.
    """
    try:
        r = SESSION.post(
            urljoin(panel_url.rstrip('/') + '/', 'panel/api/inbounds/addClient'),
            json=payload,
            headers={**get_headers(token), 'Content-Type': 'application/json'},
            timeout=20,
        )
        if r.status_code == 200:
            return r.json(), None
        return None, f"{r.status_code} {r.text[:300]}"
    except Exception as e:  # pragma: no cover - network errors
        return None, str(e)[:200]


def _list_inbounds(panel_url: str, token: str) -> Tuple[Optional[List[Dict]], Optional[str]]:
    """Return list of inbounds or an error message."""
    try:
        r = SESSION.get(
            urljoin(panel_url.rstrip('/') + '/', 'panel/api/inbounds/list'),
            headers={"accept": "application/json", **get_headers(token)},
            timeout=15,
        )
        if r.status_code != 200:
            return None, f"{r.status_code} {r.text[:200]}"
        data = r.json() or {}
        inbounds = data.get('obj') or data.get('inbounds') or []
        return inbounds, None
    except Exception as e:  # pragma: no cover - network errors
        return None, str(e)[:200]


def _find_client(inbounds: List[Dict], username: str) -> Tuple[Optional[Dict], Optional[Dict]]:
    """Return ``(inbound, client)`` pair for *username* if present."""
    for inbound in inbounds:
        settings = inbound.get('settings') or '{}'
        try:
            settings_obj = json.loads(settings) if isinstance(settings, str) else settings
        except Exception:
            settings_obj = {}
        clients = settings_obj.get('clients') or []
        for cl in clients:
            email = cl.get('email') or cl.get('Email') or cl.get('username')
            if email == username:
                return inbound, cl
    return None, None


def get_user(panel_url: str, token: str, username: str) -> Tuple[Optional[Dict], Optional[str]]:
    """Fetch user details from the panel."""
    inbounds, err = _list_inbounds(panel_url, token)
    if err:
        return None, err
    inbound, client = _find_client(inbounds, username)
    if not client or not inbound:
        return None, 'not found'
    uuid = client.get('id') or client.get('uuid')
    try:
        r = SESSION.get(
            urljoin(panel_url.rstrip('/') + '/', f"panel/api/inbounds/getClientTraffics/{username}"),
            headers={"accept": "application/json", **get_headers(token)},
            timeout=15,
        )
        if r.status_code != 200:
            return None, f"{r.status_code} {r.text[:200]}"
        data = r.json() or {}
        obj = data.get('obj') or data
        up = int(obj.get('up', 0) or 0)
        down = int(obj.get('down', 0) or 0)
        enabled = bool(obj.get('enable', True))
        used = up + down
        exp = (
            obj.get('expiryTime')
            or obj.get('expiry_time')
            or client.get('expiryTime')
            or client.get('expiry_time')
        )
    except Exception as e:  # pragma: no cover - network errors
        return None, str(e)[:200]
    res = {
        'uuid': uuid,
        'enabled': enabled,
        'used_traffic': used,
        'expiryTime': exp,
        'expiry_time': exp,
        'protocol': inbound.get('protocol'),
        'port': inbound.get('port'),
        'listen': inbound.get('listen'),
        'remark': inbound.get('remark'),
    }
    return res, None


@cached(cache=_links_cache, lock=_links_lock)
def fetch_links_from_panel(panel_url: str, token: str, username: str) -> Tuple[List[str], Optional[str]]:
    """Return list of config links for *username*.

    Since the panel does not offer subscription endpoints, configuration
    links are constructed from the inbound information and the client's
    UUID.  Only a very small subset of link parameters is produced which is
    sufficient for most standard deployments.
    """
    user, err = get_user(panel_url, token, username)
    if err or not user:
        return [], err
    host = user.get('listen') or urlparse(panel_url).hostname or ''
    port = user.get('port')
    protocol = user.get('protocol') or 'vless'
    uuid = user.get('uuid') or ''
    name = user.get('remark') or username
    if not (host and port and uuid):
        return [], 'incomplete config'
    link = f"{protocol}://{uuid}@{host}:{port}?security=none#{name}"
    if not any(link.lower().startswith(s) for s in ALLOWED_SCHEMES):
        link = f"vless://{uuid}@{host}:{port}?security=none#{name}"
    return [link], None


def disable_remote_user(panel_url: str, token: str, username: str) -> Tuple[bool, Optional[str]]:
    """Disable (enable=false) a user on the panel."""
    try:
        inbounds, err = _list_inbounds(panel_url, token)
        if err:
            return False, err
        inbound, client = _find_client(inbounds, username)
        if not client or not inbound:
            return False, 'not found'
        client['enable'] = False
        settings = inbound.get('settings') or '{}'
        settings_obj = json.loads(settings) if isinstance(settings, str) else settings
        clients = settings_obj.get('clients') or []
        for idx, cl in enumerate(clients):
            email = cl.get('email') or cl.get('Email') or cl.get('username')
            if email == username:
                clients[idx] = client
                break
        settings_obj['clients'] = clients
        inbound['settings'] = json.dumps(settings_obj, separators=(',', ':'))
        r = SESSION.post(
            urljoin(panel_url.rstrip('/') + '/', f"panel/api/inbound/update/{inbound.get('id')}")
            ,json=inbound,
            headers={**get_headers(token), 'Content-Type': 'application/json'},
            timeout=20,
        )
        return r.status_code == 200, (None if r.status_code == 200 else f"{r.status_code} {r.text[:200]}")
    except Exception as e:  # pragma: no cover - network errors
        return False, str(e)[:200]


def enable_remote_user(panel_url: str, token: str, username: str) -> Tuple[bool, Optional[str]]:
    """Enable (enable=true) a user on the panel."""
    try:
        inbounds, err = _list_inbounds(panel_url, token)
        if err:
            return False, err
        inbound, client = _find_client(inbounds, username)
        if not client or not inbound:
            return False, 'not found'
        client['enable'] = True
        settings = inbound.get('settings') or '{}'
        settings_obj = json.loads(settings) if isinstance(settings, str) else settings
        clients = settings_obj.get('clients') or []
        for idx, cl in enumerate(clients):
            email = cl.get('email') or cl.get('Email') or cl.get('username')
            if email == username:
                clients[idx] = client
                break
        settings_obj['clients'] = clients
        inbound['settings'] = json.dumps(settings_obj, separators=(',', ':'))
        r = SESSION.post(
            urljoin(panel_url.rstrip('/') + '/', f"panel/api/inbound/update/{inbound.get('id')}")
            ,json=inbound,
            headers={**get_headers(token), 'Content-Type': 'application/json'},
            timeout=20,
        )
        return r.status_code == 200, (None if r.status_code == 200 else f"{r.status_code} {r.text[:200]}")
    except Exception as e:  # pragma: no cover - network errors
        return False, str(e)[:200]


def remove_remote_user(panel_url: str, token: str, username: str) -> Tuple[bool, Optional[str]]:
    """Delete a user (client) from the panel."""
    try:
        inbounds, err = _list_inbounds(panel_url, token)
        if err:
            return False, err
        inbound, client = _find_client(inbounds, username)
        if not client or not inbound:
            return False, 'not found'
        uuid = client.get('id') or client.get('uuid')
        url = urljoin(
            panel_url.rstrip('/') + '/',
            f"panel/api/inbounds/{inbound.get('id')}/delClient/{uuid}",
        )
        r = SESSION.post(url, headers=get_headers(token), timeout=20)
        if r.status_code == 200:
            return True, None
        return False, f"{r.status_code} {r.text[:200]}"
    except Exception as e:  # pragma: no cover - network errors
        return False, str(e)[:200]


def reset_remote_user_usage(panel_url: str, token: str, username: str) -> Tuple[bool, Optional[str]]:
    """Reset traffic statistics for *username* on the panel."""
    try:
        inbounds, err = _list_inbounds(panel_url, token)
        if err:
            return False, err
        inbound, client = _find_client(inbounds, username)
        if not inbound or not client:
            return False, 'not found'
        url = urljoin(
            panel_url.rstrip('/') + '/',
            f"panel/api/inbounds/{inbound.get('id')}/resetClientTraffic/{username}",
        )
        r = SESSION.post(url, headers=get_headers(token), timeout=20)
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
    try:
        inbounds, err = _list_inbounds(panel_url, token)
        if err:
            return False, err
        inbound, client = _find_client(inbounds, username)
        if not inbound or not client:
            return False, 'not found'
        if data_limit is not None:
            client['totalGB'] = int(data_limit)
        if expire is not None:
            client['expiryTime'] = int(expire) * 1000
        payload = {
            'id': inbound.get('id'),
            'settings': json.dumps({'clients': [client]}, separators=(',', ':')),
        }
        r = SESSION.post(
            urljoin(panel_url.rstrip('/') + '/', f"panel/api/inbounds/updateClient/{client.get('id')}")
            ,json=payload,
            headers={**get_headers(token), 'Content-Type': 'application/json'},
            timeout=20,
        )
        if r.status_code == 200:
            return True, None
        return False, f"{r.status_code} {r.text[:200]}"
    except Exception as e:  # pragma: no cover - network errors
        return False, str(e)[:200]


def fetch_subscription_links(sub_url: str) -> List[str]:
    """Return links from a subscription URL if provided.

    3x-ui does not natively support subscription URLs, but some operators may
    expose one via custom means.  This function performs a simple GET and
    returns any plain-text links.
    """
    try:
        r = SESSION.get(sub_url, headers={"accept": "text/plain"}, timeout=20)
        if r.status_code != 200:
            return []
        return [
            ln.strip()
            for ln in (r.text or '').splitlines()
            if ln.strip() and ln.strip().lower().startswith(ALLOWED_SCHEMES)
        ]
    except Exception:  # pragma: no cover - network errors
        return []


def get_admin_token(panel_url: str, username: str, password: str) -> Tuple[Optional[str], Optional[str]]:
    """Authenticate against the panel and return a session token."""
    login_url = urljoin(panel_url.rstrip('/') + '/', 'login')
    try:
        resp = SESSION.post(
            login_url,
            data={"username": username, "password": password},
            timeout=15,
        )
        if resp.status_code != 200:
            return None, f"{resp.status_code} {resp.text[:200]}"
        jar = resp.cookies.get_dict()
        cookie_name = None
        cookie_val = None
        # Prefer known cookie names but fall back to any provided cookie.
        if '3x-ui' in jar:
            cookie_name, cookie_val = '3x-ui', jar['3x-ui']
        elif 'session' in jar:
            cookie_name, cookie_val = 'session', jar['session']
        elif jar:
            cookie_name, cookie_val = next(iter(jar.items()))
        if not cookie_name or not cookie_val:
            return None, 'no session cookie'
        return f"{cookie_name}={cookie_val}", None
    except Exception as e:  # pragma: no cover - network errors
        return None, str(e)[:200]
