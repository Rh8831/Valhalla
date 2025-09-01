#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flask subscription aggregator for Marzneshin
- GET /sub/<local_username>/<app_key>/links
- Returns only configs (ss://, vless://, vmess://, trojan://), one per line (text/plain)
- Enforces local quota. If user quota exceeded -> empty body + DISABLE remote (once).
- NEW: Enforces AGENT-level quota/expiry too: if agent exhausted/expired -> empty body + DISABLE ALL agent users (once).
- Supports per-panel disabled config-name filters (anything after '#' is the name).
"""

import os
import logging
from urllib.parse import urljoin, unquote

import requests
from flask import Flask, Response, abort
from dotenv import load_dotenv
from mysql.connector import pooling

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | flask_agg | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("flask_agg")

POOL = None
ALLOWED_SCHEMES = ("vless://", "vmess://", "trojan://", "ss://")

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
                   p.panel_url, p.access_token
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
            "SELECT id, panel_url, access_token FROM panels WHERE telegram_user_id=%s",
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

def disable_remote(panel_url, token, remote_username):
    try:
        # panel_url may already include a path component; urljoin with a leading
        # slash would discard it. Join paths relative to preserve subpaths.
        url = urljoin(panel_url.rstrip("/") + "/", f"api/users/{remote_username}/disable")
        r = requests.post(url, headers={"Authorization": f"Bearer {token}"}, timeout=20)
        return r.status_code, r.text[:200]
    except Exception as e:
        return None, str(e)

def fetch_user(panel_url: str, token: str, remote_username: str):
    try:
        url = urljoin(panel_url.rstrip("/") + "/", f"api/users/{remote_username}")
        r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=15)
        if r.status_code != 200:
            return None
        return r.json()
    except:
        return None

def fetch_links_from_panel(panel_url: str, remote_username: str, key: str):
    try:
        url = urljoin(panel_url.rstrip("/") + "/", f"sub/{remote_username}/{key}/links")
        r = requests.get(url, headers={"accept": "application/json"}, timeout=20)
        try:
            if r.headers.get("content-type","").startswith("application/json"):
                data = r.json()
                if isinstance(data, list):
                    return [str(x) for x in data]
                if isinstance(data, dict) and "links" in data:
                    return [str(x) for x in data["links"]]
        except:
            pass
        return [ln.strip() for ln in (r.text or "").splitlines() if ln.strip()]
    except:
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

def extract_name(link: str) -> str:
    try:
        i = link.find("#")
        if i == -1:
            return ""
        nm = unquote(link[i+1:]).strip()
        return nm[:255]
    except Exception:
        return ""

def get_panel_disabled_names(panel_id: int):
    with CurCtx() as cur:
        cur.execute("SELECT config_name FROM panel_disabled_configs WHERE panel_id=%s", (int(panel_id),))
        return {r["config_name"] for r in cur.fetchall()}

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
            SELECT lup.local_username, lup.remote_username, p.panel_url, p.access_token
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
                    code, msg = disable_remote(l["panel_url"], l["access_token"], l["remote_username"])
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
                links = [{"panel_id": p["id"], "remote_username": local_username,
                          "panel_url": p["panel_url"], "access_token": p["access_token"]} for p in panels]
            for l in links:
                code, msg = disable_remote(l["panel_url"], l["access_token"], l["remote_username"])
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
            disabled = get_panel_disabled_names(l["panel_id"])
            links = []
            u = fetch_user(l["panel_url"], l["access_token"], l["remote_username"])
            if u and u.get("key"):
                links = fetch_links_from_panel(
                    l["panel_url"], l["remote_username"], u["key"]
                )
            if disabled:
                links = [x for x in links if (extract_name(x) or "") not in disabled]
            all_links.extend(links)
    else:
        for p in list_all_panels(owner_id):
            disabled = get_panel_disabled_names(p["id"])
            links = []
            u = fetch_user(p["panel_url"], p["access_token"], local_username)
            if u and u.get("key"):
                links = fetch_links_from_panel(
                    p["panel_url"], local_username, u["key"]
                )
            if disabled:
                links = [x for x in links if (extract_name(x) or "") not in disabled]
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
