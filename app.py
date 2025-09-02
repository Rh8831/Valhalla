#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flask subscription aggregator for Marzneshin and Marzban
- GET /sub/<local_username>/<app_key>/links
- Returns only configs (ss://, vless://, vmess://, trojan://), one per line (text/plain)
- Enforces local quota. If user quota exceeded -> empty body + DISABLE remote (once).
- NEW: Enforces AGENT-level quota/expiry too: if agent exhausted/expired -> empty body + DISABLE ALL agent users (once).
- Supports per-panel disabled config-name filters (anything after '#' is the name).
- Handles Marzban's base64 subscriptions served from /v2ray endpoints.
"""

import os
import logging
import re
import base64
from urllib.parse import urljoin, unquote

import requests
from flask import Flask, Response, abort
from dotenv import load_dotenv
from mysql.connector import pooling

import marzneshin
import marzban

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | flask_agg | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("flask_agg")

POOL = None
ALLOWED_SCHEMES = ("vless://", "vmess://", "trojan://", "ss://")

API_MODULES = {
    "marzneshin": marzneshin,
    "marzban": marzban,
}

def get_api(panel_type: str):
    return API_MODULES.get(panel_type or "marzneshin", marzneshin)

def init_pool():
    global POOL
    POOL = pooling.MySQLConnectionPool(
        pool_name="flask_pool",
        pool_size=5,
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", ""),
        database=os.getenv("MYSQL_DATABASE", "botdb"),
        charset="utf8mb4",
        use_pure=True,
    )

class CurCtx:
    def __init__(self, dict_=True):
        self.dict_ = dict_
    def __enter__(self):
        self.conn = POOL.get_connection()
        self.cur = self.conn.cursor(dictionary=self.dict_)
        return self.cur
    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type is None:
                self.conn.commit()
        finally:
            self.cur.close()
            self.conn.close()

# ---------- queries ----------
def get_owner_id(app_username, app_key):
    with CurCtx() as cur:
        cur.execute(
            "SELECT telegram_user_id FROM app_users WHERE username=%s AND app_key=%s LIMIT 1",
            (app_username, app_key),
        )
        row = cur.fetchone()
        return int(row["telegram_user_id"]) if row else None

def get_local_user(owner_id, local_username):
    with CurCtx() as cur:
        cur.execute("""
            SELECT owner_id, username, plan_limit_bytes, used_bytes, disabled_pushed
            FROM local_users
            WHERE owner_id=%s AND username=%s
            LIMIT 1
        """, (owner_id, local_username))
        return cur.fetchone()

def list_mapped_links(owner_id, local_username):
    """Return panel link mappings for a local user.

    Only the data required for API-based subscription fetching is selected; any
    panel-level subscription URL configured for name filtering is ignored here.
    """
    with CurCtx() as cur:
        cur.execute(
            """
            SELECT lup.panel_id, lup.remote_username,
                   p.panel_url, p.access_token, p.panel_type
            FROM local_user_panel_links lup
            JOIN panels p ON p.id = lup.panel_id
            WHERE lup.owner_id=%s AND lup.local_username=%s
            """,
            (owner_id, local_username),
        )
        return cur.fetchall()

def list_all_panels(owner_id):
    """List all panels for an owner for fallback resolution.

    Subscription URLs stored for config-name filtering are intentionally not
    returned as the unified subscription now fetches configs directly via the
    panel API.
    """
    with CurCtx() as cur:
        cur.execute(
            "SELECT id, panel_url, access_token, panel_type FROM panels WHERE telegram_user_id=%s",
            (owner_id,),
        )
        return cur.fetchall()

def mark_user_disabled(owner_id, local_username):
    with CurCtx() as cur:
        cur.execute("""
            UPDATE local_users
            SET disabled_pushed=1, disabled_pushed_at=NOW()
            WHERE owner_id=%s AND username=%s
        """, (owner_id, local_username))

def disable_remote(panel_type, panel_url, token, remote_username):
    api = get_api(panel_type)
    try:
        ok, msg = api.disable_remote_user(panel_url, token, remote_username)
        return (200 if ok else None), msg
    except Exception as e:
        return None, str(e)

def fetch_user(panel_type: str, panel_url: str, token: str, remote_username: str):
    api = get_api(panel_type)
    try:
        obj, err = api.get_user(panel_url, token, remote_username)
        if obj:
            return obj
    except Exception:
        pass
    return None

def fetch_links_from_panel(panel_url: str, remote_username: str, key: str):
    """Fetch subscription configs from a panel.

    Marzban panels expose base64-encoded subscriptions at the ``/v2ray``
    endpoint only, while Marzneshin panels continue to serve plain-text lists
    at ``/links``.  We therefore attempt ``/v2ray`` first to properly decode
    Marzban responses and fall back to ``/links`` for Marzneshin or legacy
    panels.
    """
    paths = ("v2ray", "links")
    for suffix in paths:
        try:
            url = urljoin(panel_url.rstrip("/") + "/", f"sub/{remote_username}/{key}/{suffix}")
            r = requests.get(url, headers={"accept": "application/json"}, timeout=20)
            if suffix == "v2ray":
                text = (r.text or "").strip()
                if text:
                    try:
                        decoded = base64.b64decode(text).decode("utf-8", "ignore")
                        lines = [ln.strip() for ln in decoded.splitlines() if ln.strip()]
                        if lines:
                            return lines
                    except Exception:
                        pass
            else:
                try:
                    if r.headers.get("content-type", "").startswith("application/json"):
                        data = r.json()
                        if isinstance(data, list):
                            return [str(x) for x in data]
                        if isinstance(data, dict) and "links" in data:
                            return [str(x) for x in data["links"]]
                except Exception:
                    pass
                lines = [ln.strip() for ln in (r.text or "").splitlines() if ln.strip()]
                if lines:
                    return lines
        except Exception:
            continue
    return []

def filter_dedupe(links):
    out, seen = [], set()
    for s in links:
        ss = s.strip().strip('"').strip("'")
        if not ss.lower().startswith(ALLOWED_SCHEMES):
            continue
        if ss not in seen:
            seen.add(ss)
            out.append(ss)
    return out

def canonicalize_name(name: str) -> str:
    """Normalize a config name by stripping user-specific details."""
    try:
        nm = unquote(name or "").strip()
        nm = re.sub(r"\s*\d+(?:\.\d+)?\s*[KMGT]?B/\d+(?:\.\d+)?\s*[KMGT]?B", "", nm, flags=re.I)
        nm = re.sub(r"\s*👤.*", "", nm)
        nm = re.sub(r"\s*\([a-zA-Z0-9_-]{3,}\)", "", nm)
        nm = re.sub(r"\s+", " ", nm)
        return nm.strip()[:255]
    except Exception:
        return ""

def extract_name(link: str) -> str:
    try:
        i = link.find("#")
        if i == -1:
            return ""
        nm = link[i+1:]
        return canonicalize_name(nm)
    except Exception:
        return ""

def get_panel_disabled_names(panel_id: int):
    with CurCtx() as cur:
        cur.execute(
            "SELECT config_name FROM panel_disabled_configs WHERE panel_id=%s",
            (int(panel_id),),
        )
        # Normalize names to match extract_name() output
        return {
            cn
            for r in cur.fetchall()
            for cn in [canonicalize_name(r["config_name"])]
            if (r["config_name"] or "").strip() and cn
        }

def get_panel_disabled_nums(panel_id: int):
    with CurCtx() as cur:
        cur.execute(
            "SELECT config_index FROM panel_disabled_numbers WHERE panel_id=%s",
            (int(panel_id),),
        )
        return {
            int(r["config_index"])
            for r in cur.fetchall()
            if isinstance(r["config_index"], (int,)) and int(r["config_index"]) > 0
        }

# ---- agent-level ----
def get_agent(owner_id: int):
    with CurCtx() as cur:
        cur.execute("""
            SELECT telegram_user_id, plan_limit_bytes, expire_at, disabled_pushed
            FROM agents
            WHERE telegram_user_id=%s AND active=1
            LIMIT 1
        """, (owner_id,))
        return cur.fetchone()

def get_agent_total_used(owner_id: int) -> int:
    with CurCtx() as cur:
        cur.execute("SELECT COALESCE(SUM(used_bytes),0) AS su FROM local_users WHERE owner_id=%s", (owner_id,))
        return int(cur.fetchone()["su"] or 0)

def list_all_agent_links(owner_id: int):
    with CurCtx() as cur:
        cur.execute("""
            SELECT lup.local_username, lup.remote_username, p.panel_url, p.access_token, p.panel_type
            FROM local_user_panel_links lup
            JOIN panels p ON p.id = lup.panel_id
            WHERE lup.owner_id=%s
        """, (owner_id,))
        return cur.fetchall()

def mark_agent_disabled(owner_id: int):
    with CurCtx() as cur:
        cur.execute("""
            UPDATE agents
            SET disabled_pushed=1, disabled_pushed_at=NOW()
            WHERE telegram_user_id=%s
        """, (owner_id,))

# ---------- app ----------
app = Flask(__name__)

@app.route("/sub/<local_username>/<app_key>/links", methods=["GET"])
def unified_links(local_username, app_key):
    owner_id = get_owner_id(local_username, app_key)
    if not owner_id:
        abort(404)

    # ---- Agent-level quota/expiry enforcement (global gate) ----
    ag = get_agent(owner_id)
    if ag:
        limit_b = int(ag.get("plan_limit_bytes") or 0)
        exp = ag.get("expire_at")
        pushed_a = int(ag.get("disabled_pushed", 0) or 0)
        expired = bool(exp and exp <= __import__("datetime").datetime.utcnow())
        exceeded = False
        if limit_b > 0:
            used_total = get_agent_total_used(owner_id)
            exceeded = used_total >= limit_b
        if expired or exceeded:
            if not pushed_a:
                # disable ALL users of this agent across all panels (once)
                for l in list_all_agent_links(owner_id):
                    code, msg = disable_remote(
                        l["panel_type"], l["panel_url"], l["access_token"], l["remote_username"]
                    )
                    if code and code != 200:
                        log.warning("AGENT disable on %s@%s -> %s %s",
                                    l["remote_username"], l["panel_url"], code, msg)
                mark_agent_disabled(owner_id)
            return Response("", mimetype="text/plain")

    # ---- User-level quota enforcement ----
    lu = get_local_user(owner_id, local_username)
    if not lu:
        return Response("", mimetype="text/plain")

    limit = int(lu["plan_limit_bytes"])
    used  = int(lu["used_bytes"])
    pushed = int(lu.get("disabled_pushed", 0) or 0)

    if limit > 0 and used >= limit:
        if not pushed:
            links = list_mapped_links(owner_id, local_username)
            if not links:
                panels = list_all_panels(owner_id)
                links = [
                    {
                        "panel_id": p["id"],
                        "remote_username": local_username,
                        "panel_url": p["panel_url"],
                        "access_token": p["access_token"],
                        "panel_type": p["panel_type"],
                    }
                    for p in panels
                ]
            for l in links:
                code, msg = disable_remote(
                    l.get("panel_type"), l["panel_url"], l["access_token"], l["remote_username"]
                )
                if code and code != 200:
                    log.warning("disable on %s@%s -> %s %s", l["remote_username"], l["panel_url"], code, msg)
            mark_user_disabled(owner_id, local_username)
        resp = Response("", mimetype="text/plain")
        resp.headers["X-Plan-Limit-Bytes"] = str(limit)
        resp.headers["X-Used-Bytes"] = str(used)
        resp.headers["X-Remaining-Bytes"] = "0"
        resp.headers["X-Disabled-Pushed"] = "1"
        return resp

    # ---- Aggregate & filter links (per-panel config-name filters) ----
    mapped = list_mapped_links(owner_id, local_username)
    all_links = []
    if mapped:
        for l in mapped:
            disabled_names = get_panel_disabled_names(l["panel_id"])
            disabled_nums = get_panel_disabled_nums(l["panel_id"])
            links = []
            u = fetch_user(l.get("panel_type"), l["panel_url"], l["access_token"], l["remote_username"])
            if u and u.get("key"):
                links = fetch_links_from_panel(
                    l["panel_url"], l["remote_username"], u["key"]
                )
            if disabled_names:
                links = [x for x in links if (extract_name(x) or "") not in disabled_names]
            if disabled_nums:
                links = [x for idx, x in enumerate(links, 1) if idx not in disabled_nums]
            all_links.extend(links)
    else:
        for p in list_all_panels(owner_id):
            disabled_names = get_panel_disabled_names(p["id"])
            disabled_nums = get_panel_disabled_nums(p["id"])
            links = []
            u = fetch_user(p.get("panel_type"), p["panel_url"], p["access_token"], local_username)
            if u and u.get("key"):
                links = fetch_links_from_panel(
                    p["panel_url"], local_username, u["key"]
                )
            if disabled_names:
                links = [x for x in links if (extract_name(x) or "") not in disabled_names]
            if disabled_nums:
                links = [x for idx, x in enumerate(links, 1) if idx not in disabled_nums]
            all_links.extend(links)

    uniq = filter_dedupe(all_links)
    body = "\n".join(uniq) + ("\n" if uniq else "")

    remaining = (limit - used) if limit > 0 else -1
    resp = Response(body, mimetype="text/plain")
    resp.headers["X-Plan-Limit-Bytes"] = str(limit)
    resp.headers["X-Used-Bytes"] = str(used)
    resp.headers["X-Remaining-Bytes"] = str(max(0, remaining)) if remaining >= 0 else "unlimited"
    resp.headers["X-Disabled-Pushed"] = str(pushed)
    return resp

def main():
    load_dotenv()
    init_pool()
    host = os.getenv("FLASK_HOST", "0.0.0.0")
    port = int(os.getenv("FLASK_PORT", "5000"))
    app.run(host=host, port=port, debug=False)

if __name__ == "__main__":
    main()
