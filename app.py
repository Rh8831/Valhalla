#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flask subscription aggregator for Marzneshin/Marzban panels
- GET /sub/<local_username>/<app_key>/links
- Returns only configs (ss://, vless://, vmess://, trojan://), one per line (text/plain)
- Enforces local quota. If user quota exceeded -> empty body + DISABLE remote (once).
- NEW: Enforces AGENT-level quota/expiry too: if agent exhausted/expired -> empty body + DISABLE ALL agent users (once).
- Supports per-panel disabled config-name filters (anything after '#' is the name).
"""

import os
import logging
import re
from urllib.parse import urljoin, unquote, quote

import base64
import requests
SESSION = requests.Session()
from cachetools import TTLCache, cached
from threading import RLock
from flask import Flask, Response, abort, request, render_template_string
from types import SimpleNamespace
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from mysql.connector import pooling
from apis import sanaei

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | flask_agg | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("flask_agg")

POOL = None
ALLOWED_SCHEMES = ("vless://", "vmess://", "trojan://", "ss://")

with open(
    os.path.join(os.path.dirname(__file__), "templates", "index.html"),
    encoding="utf-8",
) as f:
    HTML_TEMPLATE = f.read()

def init_pool():
    global POOL
    # Allow tuning the number of MySQL connections via the MYSQL_POOL_SIZE
    # environment variable.  Default to a value based on CPU cores to better
    # match the level of concurrency the host can sustain.
    default_pool = (os.cpu_count() or 1) * 5
    pool_size = int(os.getenv("MYSQL_POOL_SIZE", default_pool))
    POOL = pooling.MySQLConnectionPool(
        pool_name="flask_pool",
        pool_size=pool_size,
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", ""),
        database=os.getenv("MYSQL_DATABASE", "botdb"),
        charset="utf8mb4",
        use_pure=True,
    )

# Load environment variables and initialize the MySQL pool on import so that
# the application is ready for WSGI servers like Gunicorn.
load_dotenv()
init_pool()

FETCH_CACHE_TTL = int(os.getenv("FETCH_CACHE_TTL", "300"))
_fetch_user_cache = TTLCache(maxsize=256, ttl=FETCH_CACHE_TTL)
_fetch_user_lock = RLock()
_fetch_links_cache = TTLCache(maxsize=256, ttl=FETCH_CACHE_TTL)
_fetch_links_lock = RLock()

class CurCtx:
    def __init__(self, dict_=True):
        self.dict_ = dict_
    def __enter__(self):
        try:
            self.conn = POOL.get_connection()
        except pooling.PoolError:
            log.error("MySQL connection pool exhausted; consider increasing MYSQL_POOL_SIZE")
            raise
        self.cur = self.conn.cursor(dictionary=self.dict_)
        return self.cur
    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type is None:
                self.conn.commit()
        finally:
            self.cur.close()
            self.conn.close()

def admin_ids():
    ids = (os.getenv("ADMIN_IDS") or "").strip()
    if not ids:
        return set()
    return {int(x.strip()) for x in ids.split(",") if x.strip().isdigit()}

def expand_owner_ids(owner_id: int) -> list[int]:
    ids = admin_ids()
    return list(ids) if owner_id in ids else [owner_id]

def canonical_owner_id(owner_id: int) -> int:
    ids = expand_owner_ids(owner_id)
    return ids[0]


def get_setting(owner_id: int, key: str):
    oid = canonical_owner_id(owner_id)
    with CurCtx() as cur:
        cur.execute(
            "SELECT value FROM settings WHERE owner_id=%s AND `key`=%s LIMIT 1",
            (oid, key),
        )
        row = cur.fetchone()
        return row["value"] if row else None

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
    ids = expand_owner_ids(owner_id)
    placeholders = ",".join(["%s"] * len(ids))
    with CurCtx() as cur:
        cur.execute(
            f"""
            SELECT owner_id, username, plan_limit_bytes, used_bytes, expire_at, disabled_pushed
            FROM local_users
            WHERE owner_id IN ({placeholders}) AND username=%s
            LIMIT 1
        """,
            tuple(ids) + (local_username,),
        )
        return cur.fetchone()

def list_mapped_links(owner_id, local_username):
    """Return panel link mappings for a local user.

    Only the data required for API-based subscription fetching is selected; any
    panel-level subscription URL configured for name filtering is ignored here.
    """
    ids = expand_owner_ids(owner_id)
    placeholders = ",".join(["%s"] * len(ids))
    with CurCtx() as cur:
        cur.execute(
            f"""
            SELECT lup.panel_id, lup.remote_username,
                   p.panel_url, p.access_token, p.panel_type
            FROM local_user_panel_links lup
            JOIN panels p ON p.id = lup.panel_id
            WHERE lup.owner_id IN ({placeholders}) AND lup.local_username=%s
            """,
            tuple(ids) + (local_username,),
        )
        return cur.fetchall()

def list_all_panels(owner_id):
    """List all panels for an owner for fallback resolution.

    Subscription URLs stored for config-name filtering are intentionally not
    returned as the unified subscription now fetches configs directly via the
    panel API.
    """
    ids = expand_owner_ids(owner_id)
    placeholders = ",".join(["%s"] * len(ids))
    with CurCtx() as cur:
        cur.execute(
            f"SELECT id, panel_url, access_token, panel_type FROM panels WHERE telegram_user_id IN ({placeholders})",
            tuple(ids),
        )
        return cur.fetchall()

def mark_user_disabled(owner_id, local_username):
    ids = expand_owner_ids(owner_id)
    placeholders = ",".join(["%s"] * len(ids))
    with CurCtx() as cur:
        cur.execute(
            f"""
            UPDATE local_users
            SET disabled_pushed=1, disabled_pushed_at=NOW()
            WHERE owner_id IN ({placeholders}) AND username=%s
        """,
            tuple(ids) + (local_username,),
        )

def disable_remote(panel_type, panel_url, token, remote_username):
    try:
        if panel_type == "sanaei":
            remotes = [r.strip() for r in remote_username.split(",") if r.strip()]
            all_ok, last_msg = True, None
            for rn in remotes:
                ok, msg = sanaei.disable_remote_user(panel_url, token, rn)
                if not ok:
                    all_ok = False
                    last_msg = msg
            return (200 if all_ok else None), last_msg
        # Try Marzneshin style first
        url = urljoin(panel_url.rstrip("/") + "/", f"api/users/{remote_username}/disable")
        r = SESSION.post(url, headers={"Authorization": f"Bearer {token}"}, timeout=20)
        if r.status_code == 200:
            return r.status_code, r.text[:200]
        # Fallback to Marzban style
        url = urljoin(panel_url.rstrip("/") + "/", f"api/user/{remote_username}")
        r = SESSION.put(
            url,
            json={"status": "disabled"},
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            timeout=20,
        )
        return r.status_code, r.text[:200]
    except Exception as e:
        return None, str(e)

@cached(cache=_fetch_user_cache, lock=_fetch_user_lock)
def fetch_user(panel_url: str, token: str, remote_username: str):
    try:
        url = urljoin(panel_url.rstrip("/") + "/", f"api/users/{remote_username}")
        r = SESSION.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=15)
        if r.status_code == 200:
            return r.json()
        # Fallback to Marzban endpoint
        url = urljoin(panel_url.rstrip("/") + "/", f"api/user/{remote_username}")
        r = SESSION.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=15)
        if r.status_code != 200:
            return None
        obj = r.json()
        status = obj.get("status")
        obj["enabled"] = status != "disabled"
        sub_url = obj.get("subscription_url") or ""
        token_part = sub_url.rstrip("/").split("/")[-1]
        if token_part:
            obj.setdefault("key", token_part)
        return obj
    except:
        return None

@cached(cache=_fetch_links_cache, lock=_fetch_links_lock)
def fetch_links_from_panel(panel_url: str, remote_username: str, key: str):
    """Return links and an optional error message for debugging."""
    errors = []
    try:
        # Try Marzban style first (/v2ray base64)
        url = urljoin(panel_url.rstrip("/") + "/", f"sub/{key}/v2ray")
        r = SESSION.get(url, headers={"accept": "text/plain"}, timeout=20)
        if r.status_code == 200:
            txt = (r.text or "").strip()
            if txt:
                try:
                    decoded = base64.b64decode(txt + "===")
                    txt = decoded.decode(errors="ignore")
                except Exception as e:
                    errors.append(f"v2ray b64 {e}")
                lines = [ln.strip() for ln in txt.splitlines() if ln.strip()]
                if any(ln.lower().startswith(ALLOWED_SCHEMES) for ln in lines):
                    return lines, None
                errors.append("v2ray empty")
        else:
            errors.append(f"v2ray HTTP {r.status_code}")

        # Fallback to Marzneshin style
        url = urljoin(panel_url.rstrip("/") + "/", f"sub/{remote_username}/{key}/links")
        r = SESSION.get(url, headers={"accept": "application/json,text/plain"}, timeout=20)
        if r.status_code != 200:
            errors.append(f"links HTTP {r.status_code}")
            return [], "; ".join(errors)
        try:
            if r.headers.get("content-type", "").startswith("application/json"):
                data = r.json()
                if isinstance(data, list):
                    return [str(x) for x in data], None
                if isinstance(data, dict) and "links" in data:
                    return [str(x) for x in data["links"]], None
        except Exception as e:
            errors.append(f"json {e}")
        lines = [
            ln.strip()
            for ln in (r.text or "").splitlines()
            if ln.strip() and ln.strip().lower().startswith(ALLOWED_SCHEMES)
        ]
        if lines:
            return lines, None
        errors.append("links empty")
        return [], "; ".join(errors)
    except Exception as e:
        errors.append(str(e))
        return [], "; ".join(errors)


def collect_links(mapped, local_username: str, want_html: bool):
    """Fetch links for multiple panel mappings concurrently.

    Using a thread pool allows resolving subscription URLs from different
    panels in parallel which significantly reduces the overall response
    time when many panels are configured.
    """
    all_links, errors = [], []
    remote_info = None

    panel_ids = [m["panel_id"] for m in mapped]
    disabled_name_map, disabled_num_map = load_disabled_filters(panel_ids)

    def worker(l, dn_map, di_map):
        disabled_names = dn_map.get(l["panel_id"], set())
        disabled_nums = di_map.get(l["panel_id"], set())
        links, errs, rinfo = [], [], None
        if l.get("panel_type") == "sanaei":
            remotes = [r.strip() for r in l["remote_username"].split(",") if r.strip()]

            def remote_worker(rn: str):
                info = None
                if want_html:
                    u, uerr = sanaei.get_user(l["panel_url"], l["access_token"], rn)
                    if not uerr:
                        info = u
                ls, err = sanaei.fetch_links_from_panel(l["panel_url"], l["access_token"], rn)
                if err:
                    err = f"{rn}@{l['panel_url']}: {err}"
                return ls, err, info

            if len(remotes) > 1:
                inner_workers = min(3, len(remotes)) or 1
                with ThreadPoolExecutor(max_workers=inner_workers) as inner_ex:
                    futures = [inner_ex.submit(remote_worker, rn) for rn in remotes]
                    for fut in as_completed(futures):
                        ls, err, info = fut.result()
                        links.extend(ls)
                        if err:
                            errs.append(err)
                        if want_html and rinfo is None and info:
                            rinfo = info
            else:
                for rn in remotes:
                    ls, err, info = remote_worker(rn)
                    links.extend(ls)
                    if err:
                        errs.append(err)
                    if want_html and rinfo is None and info:
                        rinfo = info
        else:
            u = fetch_user(l["panel_url"], l["access_token"], l["remote_username"])
            if want_html:
                rinfo = u
            if u and u.get("key"):
                ls, err = fetch_links_from_panel(l["panel_url"], l["remote_username"], u["key"])
                if err:
                    errs.append(f"{l['remote_username']}@{l['panel_url']}: {err}")
                links.extend(ls)
        if disabled_names:
            links = [x for x in links if (extract_name(x) or "") not in disabled_names]
        if disabled_nums:
            links = [x for idx, x in enumerate(links, 1) if idx not in disabled_nums]
        return links, errs, rinfo

    max_workers_env = int(os.getenv("FETCH_MAX_WORKERS", "5"))
    max_workers = min(max_workers_env, len(mapped)) or 1
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(worker, m, disabled_name_map, disabled_num_map) for m in mapped]
        for fut in as_completed(futures):
            ls, errs, rinfo = fut.result()
            all_links.extend(ls)
            errors.extend(errs)
            if remote_info is None and rinfo:
                remote_info = rinfo

    return all_links, errors, remote_info

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

def load_disabled_filters(panel_ids: list[int]):
    """Return disabled config names and numbers for panels in bulk."""
    if not panel_ids:
        return {}, {}
    placeholders = ",".join(["%s"] * len(panel_ids))
    names: dict[int, set[str]] = {}
    nums: dict[int, set[int]] = {}
    with CurCtx() as cur:
        cur.execute(
            f"SELECT panel_id, config_name FROM panel_disabled_configs WHERE panel_id IN ({placeholders})",
            tuple(panel_ids),
        )
        for r in cur.fetchall():
            cn = canonicalize_name(r.get("config_name"))
            if cn:
                names.setdefault(int(r["panel_id"]), set()).add(cn)
        cur.execute(
            f"SELECT panel_id, config_index FROM panel_disabled_numbers WHERE panel_id IN ({placeholders})",
            tuple(panel_ids),
        )
        for r in cur.fetchall():
            idx = r.get("config_index")
            if isinstance(idx, (int,)) and int(idx) > 0:
                nums.setdefault(int(r["panel_id"]), set()).add(int(idx))
    return names, nums

# ---- agent-level ----
def get_agent(owner_id: int):
    ids = expand_owner_ids(owner_id)
    placeholders = ",".join(["%s"] * len(ids))
    with CurCtx() as cur:
        cur.execute(
            f"""
            SELECT telegram_user_id, plan_limit_bytes, expire_at, disabled_pushed
            FROM agents
            WHERE telegram_user_id IN ({placeholders}) AND active=1
            LIMIT 1
        """,
            tuple(ids),
        )
        return cur.fetchone()

def get_agent_total_used(owner_id: int) -> int:
    ids = expand_owner_ids(owner_id)
    placeholders = ",".join(["%s"] * len(ids))
    with CurCtx() as cur:
        cur.execute(
            f"SELECT total_used_bytes AS su FROM agents WHERE telegram_user_id IN ({placeholders}) AND active=1 LIMIT 1",
            tuple(ids),
        )
        row = cur.fetchone()
        return int(row.get("su") or 0) if row else 0

def list_all_agent_links(owner_id: int):
    ids = expand_owner_ids(owner_id)
    placeholders = ",".join(["%s"] * len(ids))
    with CurCtx() as cur:
        cur.execute(
            f"""
            SELECT lup.local_username, lup.remote_username, p.panel_url, p.access_token, p.panel_type
            FROM local_user_panel_links lup
            JOIN panels p ON p.id = lup.panel_id
            WHERE lup.owner_id IN ({placeholders})
        """,
            tuple(ids),
        )
        return cur.fetchall()

def mark_agent_disabled(owner_id: int):
    ids = expand_owner_ids(owner_id)
    placeholders = ",".join(["%s"] * len(ids))
    with CurCtx() as cur:
        cur.execute(
            f"""
            UPDATE agents
            SET disabled_pushed=1, disabled_pushed_at=NOW()
            WHERE telegram_user_id IN ({placeholders})
        """,
            tuple(ids),
        )

# ---------- app ----------
app = Flask(__name__)


def bytesformat(num):
    try:
        num = float(num)
    except (TypeError, ValueError):
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    for u in units:
        if abs(num) < 1024.0:
            return f"{num:.2f} {u}"
        num /= 1024.0
    return f"{num:.2f} PB"


app.jinja_env.filters["bytesformat"] = bytesformat


def build_user(local_username, app_key, lu, remote=None):
    limit = int(lu.get("plan_limit_bytes") or 0) if lu else 0
    used = int(lu.get("used_bytes") or 0) if lu else 0
    expire_raw = ""
    enabled = True
    if remote:
        enabled = remote.get("enabled", True)
        expire_raw = (
            remote.get("expire_date")
            or remote.get("expire")
            or remote.get("expiryTime")
            or remote.get("expiry_time")
            or remote.get("expire_at")
            or ""
        )
    if not expire_raw and lu:
        exp_l = lu.get("expire_at")
        if exp_l:
            try:
                if isinstance(exp_l, datetime):
                    expire_raw = str(int(exp_l.timestamp()))
                else:
                    expire_raw = str(int(datetime.fromisoformat(str(exp_l)).timestamp()))
            except Exception:
                expire_raw = ""
    data_limit_reached = bool(limit > 0 and used >= limit)
    expired = False
    try:
        if expire_raw:
            if isinstance(expire_raw, str) and not expire_raw.isdigit():
                exp_str = expire_raw.replace("Z", "+00:00")
                exp_ts = datetime.fromisoformat(exp_str).timestamp()
            else:
                exp_ts = float(expire_raw)
            if exp_ts > 1e12:
                exp_ts /= 1000.0
            if exp_ts > 0:
                expired = exp_ts <= datetime.utcnow().timestamp()
                expire_raw = str(int(exp_ts))
            else:
                expire_raw = ""
    except Exception:
        expired = False
        expire_raw = ""
    user = {
        "username": local_username,
        "subscription_url": f"/sub/{local_username}/{app_key}/links",
        "used_traffic": used,
        "data_limit": limit or None,
        "expire_date": expire_raw,
        "data_limit_reset_strategy": SimpleNamespace(value="no_reset"),
        "enabled": enabled,
        "expired": expired,
        "data_limit_reached": data_limit_reached,
    }
    user["is_active"] = user["enabled"] and not user["expired"] and not user["data_limit_reached"]
    return user

@app.route("/sub/<local_username>/<app_key>/links", methods=["GET"])
def unified_links(local_username, app_key):
    owner_id = get_owner_id(local_username, app_key)
    if not owner_id:
        abort(404)

    want_html = "text/html" in request.headers.get("Accept", "")

    lu = get_local_user(owner_id, local_username)
    if not lu:
        if want_html:
            user = build_user(local_username, app_key, {})
            return render_template_string(HTML_TEMPLATE, user=user)
        return Response("", mimetype="text/plain")

    # ---- Agent-level quota/expiry enforcement (global gate) ----
    ag = get_agent(owner_id)
    agent_blocked = False
    if ag:
        limit_b = int(ag.get("plan_limit_bytes") or 0)
        exp = ag.get("expire_at")
        pushed_a = int(ag.get("disabled_pushed", 0) or 0)
        expired = bool(exp and exp <= datetime.utcnow())
        exceeded = False
        if limit_b > 0:
            used_total = get_agent_total_used(owner_id)
            exceeded = used_total >= limit_b
        if expired or exceeded:
            agent_blocked = True
            if not pushed_a:
                for l in list_all_agent_links(owner_id):
                    code, msg = disable_remote(l["panel_type"], l["panel_url"], l["access_token"], l["remote_username"])
                    if code and code != 200:
                        log.warning("AGENT disable on %s@%s -> %s %s",
                                    l["remote_username"], l["panel_url"], code, msg)
                mark_agent_disabled(owner_id)
            if not want_html:
                return Response("", mimetype="text/plain")

    # ---- User-level quota enforcement ----
    limit = int(lu["plan_limit_bytes"])
    used = int(lu["used_bytes"])
    pushed = int(lu.get("disabled_pushed", 0) or 0)
    limit_reached = False
    if limit > 0 and used >= limit:
        limit_reached = True
        if not pushed:
            links = list_mapped_links(owner_id, local_username)
            if not links:
                panels = list_all_panels(owner_id)
                links = [{"panel_id": p["id"], "remote_username": local_username,
                          "panel_url": p["panel_url"], "access_token": p["access_token"],
                          "panel_type": p["panel_type"]} for p in panels]
            for l in links:
                code, msg = disable_remote(l["panel_type"], l["panel_url"], l["access_token"], l["remote_username"])
                if code and code != 200:
                    log.warning("disable on %s@%s -> %s %s", l["remote_username"], l["panel_url"], code, msg)
            mark_user_disabled(owner_id, local_username)
        if not want_html:
            limit_config = os.getenv(
                "USER_LIMIT_REACHED_CONFIG",
                "vless://limitreached@info.info:80?encryption=none&security=none&type=tcp&headerType=none",
            )
            msg_template = get_setting(owner_id, "limit_message") or os.getenv(
                "USER_LIMIT_REACHED_MESSAGE",
                "User {username} has reached data limit ({used} / {limit})",
            )
            msg = msg_template.replace("{username}", local_username)
            msg = msg.replace("{limit}", bytesformat(limit))
            msg = msg.replace("{used}", bytesformat(used))
            body = limit_config + "#" + quote(msg)
            resp = Response(body, mimetype="text/plain")

            resp.headers["X-Plan-Limit-Bytes"] = str(limit)
            resp.headers["X-Used-Bytes"] = str(used)
            resp.headers["X-Remaining-Bytes"] = "0"
            resp.headers["X-Disabled-Pushed"] = "1"
            return resp

    # ---- Aggregate & filter links (per-panel config-name filters) ----
    mapped = list_mapped_links(owner_id, local_username)
    all_links, errors, remote_info = [], [], None
    if not agent_blocked and not limit_reached:
        if mapped:
            all_links, errors, remote_info = collect_links(mapped, local_username, want_html)
        else:
            panels = list_all_panels(owner_id)
            mappings = [
                {
                    "panel_id": p["id"],
                    "remote_username": local_username,
                    "panel_url": p["panel_url"],
                    "access_token": p["access_token"],
                    "panel_type": p["panel_type"],
                }
                for p in panels
            ]
            all_links, errors, remote_info = collect_links(mappings, local_username, want_html)

    uniq = filter_dedupe(all_links)
    if uniq:
        body = "\n".join(uniq) + "\n"
    elif errors:
        body = "\n".join(f"# {e}" for e in errors) + "\n"
    else:
        body = ""

    remaining = (limit - used) if limit > 0 else -1
    if want_html:
        user = build_user(local_username, app_key, lu, remote_info)
        return render_template_string(HTML_TEMPLATE, user=user)
    resp = Response(body, mimetype="text/plain")
    resp.headers["X-Plan-Limit-Bytes"] = str(limit)
    resp.headers["X-Used-Bytes"] = str(used)
    resp.headers["X-Remaining-Bytes"] = str(max(0, remaining)) if remaining >= 0 else "unlimited"
    resp.headers["X-Disabled-Pushed"] = str(pushed)
    return resp

def main():
    host = os.getenv("FLASK_HOST", "0.0.0.0")
    port = int(os.getenv("FLASK_PORT", "5000"))
    cert = os.getenv("SSL_CERT_PATH")
    key = os.getenv("SSL_KEY_PATH")
    ssl_context = (cert, key) if cert and key else None
    app.run(host=host, port=port, debug=False, ssl_context=ssl_context)

if __name__ == "__main__":
    main()
