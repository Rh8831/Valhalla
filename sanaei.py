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

ALLOWED_SCHEMES = ("vless://", "vmess://", "trojan://", "ss://")


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
        r = requests.post(
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
        r = requests.get(
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


def _find_clients(inbounds: List[Dict], username: str) -> List[Tuple[Dict, Dict]]:
    """Return list of ``(inbound, client)`` pairs for *username*."""
    matches: List[Tuple[Dict, Dict]] = []
    for inbound in inbounds:
        settings = inbound.get('settings') or '{}'
        try:
            settings_obj = json.loads(settings) if isinstance(settings, str) else settings
        except Exception:
            settings_obj = {}
        clients = settings_obj.get('clients') or []
        for cl in clients:
            email = (
                cl.get('email')
                or cl.get('Email')
                or cl.get('username')
                or ''
            )
            base = email.split('__', 1)[0]
            if base == username:
                matches.append((inbound, cl))
    return matches


def get_user(panel_url: str, token: str, username: str) -> Tuple[Optional[Dict], Optional[str]]:
    """Fetch user details from the panel."""
    inbounds, err = _list_inbounds(panel_url, token)
    if err:
        return None, err
    matches = _find_clients(inbounds, username)
    if not matches:
        return None, 'not found'
    total_used = 0
    enabled_all = True
    inbound_ids: List[int] = []
    first_inbound, first_client = matches[0]
    for inbound, client in matches:
        uuid = client.get('id') or client.get('uuid')
        inbound_ids.append(int(inbound.get('id'))) if inbound.get('id') is not None else None
        for st in inbound.get('clientStats', []) or []:
            if st.get('id') == uuid:
                up = int(st.get('up', 0) or 0)
                down = int(st.get('down', 0) or 0)
                total_used += up + down
                break
        enabled_all = enabled_all and bool(client.get('enable', True))
    obj = {
        'uuid': first_client.get('id') or first_client.get('uuid'),
        'enabled': enabled_all,
        'used_traffic': total_used,
        'protocol': first_inbound.get('protocol'),
        'port': first_inbound.get('port'),
        'listen': first_inbound.get('listen'),
        'remark': first_inbound.get('remark'),
        'inbound_ids': inbound_ids,
    }
    return obj, None


def fetch_links_from_panel(panel_url: str, token: str, username: str) -> Tuple[List[str], Optional[str]]:
    """Return list of config links for *username*.

    Since the panel does not offer subscription endpoints, configuration
    links are constructed from inbound information and each client's UUID.
    """
    inbounds, err = _list_inbounds(panel_url, token)
    if err:
        return [], err
    matches = _find_clients(inbounds, username)
    if not matches:
        return [], 'not found'
    links: List[str] = []
    for inbound, client in matches:
        host = inbound.get('listen') or urlparse(panel_url).hostname or ''
        port = inbound.get('port')
        protocol = inbound.get('protocol') or 'vless'
        uuid = client.get('id') or client.get('uuid') or ''
        name = client.get('remark') or inbound.get('remark') or username
        if not (host and port and uuid):
            continue
        link = f"{protocol}://{uuid}@{host}:{port}?security=none#{name}"
        if not any(link.lower().startswith(s) for s in ALLOWED_SCHEMES):
            link = f"vless://{uuid}@{host}:{port}?security=none#{name}"
        links.append(link)
    if not links:
        return [], 'incomplete config'
    return links, None


def disable_remote_user(panel_url: str, token: str, username: str) -> Tuple[bool, Optional[str]]:
    """Disable (enable=false) a user on the panel."""
    try:
        inbounds, err = _list_inbounds(panel_url, token)
        if err:
            return False, err
        matches = _find_clients(inbounds, username)
        if not matches:
            return False, 'not found'
        ok_any = False
        errs = []
        for inbound, client in matches:
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
            r = requests.post(
                urljoin(panel_url.rstrip('/') + '/', f"panel/api/inbounds/update/{inbound.get('id')}")
                ,json=inbound,
                headers={**get_headers(token), 'Content-Type': 'application/json'},
                timeout=20,
            )
            if r.status_code == 200:
                ok_any = True
            else:
                errs.append(f"{inbound.get('id')}: {r.status_code} {r.text[:200]}")
        return ok_any, (None if ok_any else "; ".join(errs))
    except Exception as e:  # pragma: no cover - network errors
        return False, str(e)[:200]


def enable_remote_user(panel_url: str, token: str, username: str) -> Tuple[bool, Optional[str]]:
    """Enable (enable=true) a user on the panel."""
    try:
        inbounds, err = _list_inbounds(panel_url, token)
        if err:
            return False, err
        matches = _find_clients(inbounds, username)
        if not matches:
            return False, 'not found'
        ok_any = False
        errs = []
        for inbound, client in matches:
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
            r = requests.post(
                urljoin(panel_url.rstrip('/') + '/', f"panel/api/inbounds/update/{inbound.get('id')}")
                ,json=inbound,
                headers={**get_headers(token), 'Content-Type': 'application/json'},
                timeout=20,
            )
            if r.status_code == 200:
                ok_any = True
            else:
                errs.append(f"{inbound.get('id')}: {r.status_code} {r.text[:200]}")
        return ok_any, (None if ok_any else "; ".join(errs))
    except Exception as e:  # pragma: no cover - network errors
        return False, str(e)[:200]


def fetch_subscription_links(sub_url: str) -> List[str]:
    """Return links from a subscription URL if provided.

    3x-ui does not natively support subscription URLs, but some operators may
    expose one via custom means.  This function performs a simple GET and
    returns any plain-text links.
    """
    try:
        r = requests.get(sub_url, headers={"accept": "text/plain"}, timeout=20)
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
        resp = requests.post(
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
