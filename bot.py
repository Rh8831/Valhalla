#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram bot + MySQL (local users + panels) with Admin/Agent roles

Admin:
- Manage panels (add/edit creds/template/sub url, per-panel config filter)
- Remove panel (disables all mapped users on that panel first)
- Manage agents: add/edit (name), set agent quota (bytes), renew expiry by **days**, activate/deactivate
- Assign panels to agents (checkbox)
Agent:
- New local user (with panel multi-select limited to assigned panels)
- Search/list users
- Edit user (limit/reset/renew + panel selection limited to assigned)

Shared:
- Unified subscription link per user
- Remote disable/enable logic preserved

ENV:
- BOT_TOKEN
- ADMIN_IDS="11111,22222" (Telegram user IDs for admins; data for all admins is stored under the smallest ID)
- MYSQL_*  , PUBLIC_BASE_URL
"""

import os
import logging
import secrets
import re
import json
import uuid
from urllib.parse import urlparse, unquote
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from mysql.connector import pooling, Error as MySQLError

import marzneshin
import marzban
import sanaei

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, ConversationHandler,
    MessageHandler, ContextTypes, filters
)

import usage_sync

# ---------- logging ----------
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("marz_bot")

# ---------- api helpers ----------
API_MODULES = {
    "marzneshin": marzneshin,
    "marzban": marzban,
    "sanaei": sanaei,
}

def get_api(panel_type: str):
    return API_MODULES.get(panel_type or "marzneshin", marzneshin)

# ---------- roles ----------
def admin_ids():
    ids = (os.getenv("ADMIN_IDS") or "").strip()
    if not ids:
        return set()
    return {int(x.strip()) for x in ids.split(",") if x.strip().isdigit()}

def is_admin(tg_id: int) -> bool:
    return tg_id in admin_ids()

def resolve_owner_id(tg_id: int) -> int:
    """Return a canonical owner ID so all admins share the same data set.

    If the provided Telegram ID belongs to one of the configured admins,
    the smallest admin ID is returned. Otherwise the ID is returned
    unchanged. This ensures that multiple admin accounts operate on a
    single shared set of panels/users while agents keep their own data.
    """
    ids = admin_ids()
    if ids and tg_id in ids:
        return min(ids)
    return tg_id

# ---------- states ----------
(
    ASK_PANEL_NAME, ASK_PANEL_TYPE, ASK_PANEL_URL, ASK_PANEL_USER, ASK_PANEL_PASS,
    ASK_NEWUSER_NAME, ASK_LIMIT_GB, ASK_DURATION,
    ASK_SEARCH_USER, ASK_PANEL_TEMPLATE,
    ASK_EDIT_LIMIT, ASK_RENEW_DAYS,
    ASK_EDIT_PANEL_NAME, ASK_EDIT_PANEL_USER, ASK_EDIT_PANEL_PASS,
    ASK_SELECT_PANELS,
    ASK_PANEL_SUB_URL,

    # agent mgmt
    ASK_AGENT_NAME, ASK_AGENT_TGID,
    ASK_AGENT_LIMIT, ASK_AGENT_RENEW_DAYS,   # changed: renew by days
    ASK_AGENT_MAX_USERS, ASK_AGENT_MAX_USER_GB,
    ASK_ASSIGN_AGENT_PANELS,
    ASK_PANEL_REMOVE_CONFIRM,
) = range(25)

# ---------- MySQL ----------
MYSQL_POOL = None

def init_mysql_pool():
    global MYSQL_POOL
    MYSQL_POOL = pooling.MySQLConnectionPool(
        pool_name="bot_pool",
        pool_size=5,
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", ""),
        database=os.getenv("MYSQL_DATABASE", "botdb"),
        charset="utf8mb4",
        use_pure=True,
    )

def with_mysql_cursor(dict_=True):
    class _Ctx:
        def __enter__(self):
            self.conn = MYSQL_POOL.get_connection()
            self.cur  = self.conn.cursor(dictionary=dict_)
            return self.cur
        def __exit__(self, exc, e, tb):
            if exc is None:
                self.conn.commit()
            else:
                self.conn.rollback()
            self.cur.close()
            self.conn.close()
    return _Ctx()

def ensure_schema():
    with with_mysql_cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS panels(
                id BIGINT PRIMARY KEY AUTO_INCREMENT,
                telegram_user_id BIGINT NOT NULL,
                panel_url VARCHAR(255) NOT NULL,
                name VARCHAR(128) NOT NULL,
                panel_type VARCHAR(32) NOT NULL DEFAULT 'marzneshin',
                admin_username VARCHAR(64) NOT NULL,
                access_token VARCHAR(2048) NOT NULL,
                template_username VARCHAR(64) NULL,
                sub_url VARCHAR(2048) NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uq_user_url (telegram_user_id, panel_url)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        try:
            cur.execute("ALTER TABLE panels ADD COLUMN panel_type VARCHAR(32) NOT NULL DEFAULT 'marzneshin' AFTER name")
        except MySQLError:
            pass
        cur.execute("""
            CREATE TABLE IF NOT EXISTS app_users(
                id BIGINT PRIMARY KEY AUTO_INCREMENT,
                telegram_user_id BIGINT NOT NULL,
                username VARCHAR(64) NOT NULL,
                app_key VARCHAR(64) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uq_owner_username (telegram_user_id, username)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS local_users(
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                owner_id BIGINT NOT NULL,
                username VARCHAR(64) NOT NULL,
                plan_limit_bytes BIGINT NOT NULL,
                used_bytes BIGINT NOT NULL DEFAULT 0,
                expire_at DATETIME NULL,
                note VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                disabled_pushed TINYINT(1) NOT NULL DEFAULT 0,
                disabled_pushed_at DATETIME NULL,
                UNIQUE KEY uq_local(owner_id, username)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS local_user_panel_links(
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                owner_id BIGINT NOT NULL,
                local_username VARCHAR(64) NOT NULL,
                panel_id BIGINT NOT NULL,
                remote_username VARCHAR(128) NOT NULL,
                last_used_traffic BIGINT NOT NULL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY uq_link(owner_id, local_username, panel_id),
                FOREIGN KEY (panel_id) REFERENCES panels(id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS panel_disabled_configs(
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                telegram_user_id BIGINT NOT NULL,
                panel_id BIGINT NOT NULL,
                config_name VARCHAR(255) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uq_panel_cfg(panel_id, config_name),
                INDEX idx_panel(panel_id),
                FOREIGN KEY (panel_id) REFERENCES panels(id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS panel_disabled_numbers(
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                telegram_user_id BIGINT NOT NULL,
                panel_id BIGINT NOT NULL,
                config_index INT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uq_panel_idx(panel_id, config_index),
                INDEX idx_panel(panel_id),
                FOREIGN KEY (panel_id) REFERENCES panels(id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        # agents
        cur.execute("""
            CREATE TABLE IF NOT EXISTS agents(
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                telegram_user_id BIGINT NOT NULL UNIQUE,
                name VARCHAR(128) NOT NULL,
                plan_limit_bytes BIGINT NOT NULL DEFAULT 0,
                expire_at DATETIME NULL,
                active TINYINT(1) NOT NULL DEFAULT 1,
                user_limit BIGINT NOT NULL DEFAULT 0,
                max_user_bytes BIGINT NOT NULL DEFAULT 0,
                disabled_pushed TINYINT(1) NOT NULL DEFAULT 0,
                disabled_pushed_at DATETIME NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        try:
            cur.execute("ALTER TABLE agents ADD COLUMN user_limit BIGINT NOT NULL DEFAULT 0")
        except MySQLError:
            pass
        try:
            cur.execute("ALTER TABLE agents ADD COLUMN max_user_bytes BIGINT NOT NULL DEFAULT 0")
        except MySQLError:
            pass
        cur.execute("""
            CREATE TABLE IF NOT EXISTS agent_panels(
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                agent_tg_id BIGINT NOT NULL,
                panel_id BIGINT NOT NULL,
                UNIQUE KEY uq_agent_panel(agent_tg_id, panel_id),
                FOREIGN KEY (panel_id) REFERENCES panels(id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

# ---------- helpers ----------
UNIT = 1024

def fmt_bytes_short(n: int) -> str:
    if n <= 0:
        return "0 MB"
    tb = n / (UNIT**4)
    gb = n / (UNIT**3)
    mb = n / (UNIT**2)
    if tb >= 1:
        return f"{tb:.2f} TB"
    if gb >= 1:
        return f"{gb:.2f} GB"
    return f"{mb:.2f} MB"

def parse_human_size(s: str) -> int:
    if not s:
        return 0
    s = s.strip().lower()
    if s in ("0", "unlimited", "∞", "no limit", "nolimit"):
        return 0
    num, unit = "", ""
    for ch in s:
        if ch.isdigit() or ch in ".,": num += ch.replace(",", ".")
        else: unit += ch
    try:
        val = float(num) if num else 0.0
    except Exception:
        val = 0.0
    unit = unit.strip()
    if unit in ("", "g", "gb"):
        mul = UNIT**3
    elif unit in ("m", "mb"):
        mul = UNIT**2
    elif unit in ("t", "tb"):
        mul = UNIT**4
    else:
        mul = UNIT**3
    return int(max(0.0, val) * mul)

def gb_to_bytes(txt: str) -> int:
    try:
        gb = float((txt or "0").strip())
        gb = max(0.0, gb)
    except Exception:
        gb = 0.0
    return int(gb * (UNIT**3))

def make_panel_name(url, u):
    try:
        h = urlparse(url).hostname or url
    except Exception:
        h = url
    h = str(h).replace("www.", "")
    base = f"{h}-{u}".strip("-")
    return (base[:120] if len(base) > 120 else base) or "panel"

# ---------- data access ----------
def list_my_panels_admin(admin_tg_id: int):
    admin_tg_id = resolve_owner_id(admin_tg_id)
    with with_mysql_cursor() as cur:
        cur.execute("SELECT * FROM panels WHERE telegram_user_id=%s ORDER BY created_at DESC", (admin_tg_id,))
        return cur.fetchall()

def list_panels_for_agent(agent_tg_id: int):
    with with_mysql_cursor() as cur:
        cur.execute("""
            SELECT p.* FROM agent_panels ap
            JOIN panels p ON p.id = ap.panel_id
            WHERE ap.agent_tg_id=%s
            ORDER BY p.created_at DESC
        """, (agent_tg_id,))
        return cur.fetchall()

def upsert_app_user(tg_id: int, u: str) -> str:
    tg_id = resolve_owner_id(tg_id)
    with with_mysql_cursor() as cur:
        cur.execute("SELECT app_key FROM app_users WHERE telegram_user_id=%s AND username=%s", (tg_id, u))
        row = cur.fetchone()
        if row:
            return row["app_key"]
        k = secrets.token_hex(16)
        cur.execute("INSERT INTO app_users(telegram_user_id,username,app_key)VALUES(%s,%s,%s)", (tg_id, u, k))
        return k

def get_app_key(tg_id: int, u: str) -> str:
    tg_id = resolve_owner_id(tg_id)
    with with_mysql_cursor() as cur:
        cur.execute("SELECT app_key FROM app_users WHERE telegram_user_id=%s AND username=%s", (tg_id, u))
        row = cur.fetchone()
    return row["app_key"] if row else upsert_app_user(tg_id, u)

def upsert_local_user(owner_id: int, username: str, limit_bytes: int, duration_days: int):
    owner_id = resolve_owner_id(owner_id)
    exp = datetime.utcnow() + timedelta(days=duration_days) if duration_days > 0 else None
    with with_mysql_cursor() as cur:
        cur.execute(
            """INSERT INTO local_users(owner_id,username,plan_limit_bytes,expire_at,disabled_pushed)
               VALUES(%s,%s,%s,%s,0)
               ON DUPLICATE KEY UPDATE
                   plan_limit_bytes=VALUES(plan_limit_bytes),
                   expire_at=VALUES(expire_at),
                   disabled_pushed=0""",
            (owner_id, username, int(limit_bytes), exp)
        )

def save_link(owner_id: int, local_username: str, panel_id: int, remote_username: str):
    owner_id = resolve_owner_id(owner_id)
    with with_mysql_cursor() as cur:
        cur.execute(
            """INSERT INTO local_user_panel_links(owner_id,local_username,panel_id,remote_username)
               VALUES(%s,%s,%s,%s)
               ON DUPLICATE KEY UPDATE remote_username=VALUES(remote_username)""",
            (owner_id, local_username, panel_id, remote_username)
        )

def remove_link(owner_id: int, local_username: str, panel_id: int):
    owner_id = resolve_owner_id(owner_id)
    with with_mysql_cursor() as cur:
        cur.execute(
            "DELETE FROM local_user_panel_links WHERE owner_id=%s AND local_username=%s AND panel_id=%s",
            (owner_id, local_username, panel_id)
        )

def list_linked_panel_ids(owner_id: int, local_username: str):
    owner_id = resolve_owner_id(owner_id)
    with with_mysql_cursor() as cur:
        cur.execute(
            "SELECT panel_id FROM local_user_panel_links WHERE owner_id=%s AND local_username=%s",
            (owner_id, local_username)
        )
        return {int(r["panel_id"]) for r in cur.fetchall()}

def map_linked_remote_usernames(owner_id: int, local_username: str):
    owner_id = resolve_owner_id(owner_id)
    with with_mysql_cursor() as cur:
        cur.execute(
            "SELECT panel_id, remote_username FROM local_user_panel_links WHERE owner_id=%s AND local_username=%s",
            (owner_id, local_username)
        )
        return {int(r["panel_id"]): r["remote_username"] for r in cur.fetchall()}

def get_local_user(owner_id: int, username: str):
    owner_id = resolve_owner_id(owner_id)
    with with_mysql_cursor() as cur:
        cur.execute(
            "SELECT username,plan_limit_bytes,used_bytes,expire_at,disabled_pushed FROM local_users "
            "WHERE owner_id=%s AND username=%s LIMIT 1",
            (owner_id, username)
        )
        return cur.fetchone()

def search_local_users(owner_id: int, q: str):
    owner_id = resolve_owner_id(owner_id)
    with with_mysql_cursor() as cur:
        cur.execute(
            "SELECT username FROM local_users WHERE owner_id=%s AND username LIKE %s ORDER BY username ASC LIMIT 50",
            (owner_id, f"%{q}%")
        )
        return [r["username"] for r in cur.fetchall()]

def list_all_local_users(owner_id: int, offset: int = 0, limit: int = 25):
    owner_id = resolve_owner_id(owner_id)
    with with_mysql_cursor() as cur:
        cur.execute(
            "SELECT username FROM local_users WHERE owner_id=%s ORDER BY username ASC LIMIT %s OFFSET %s",
            (owner_id, limit, offset)
        )
        return [r["username"] for r in cur.fetchall()]

def count_local_users(owner_id: int) -> int:
    owner_id = resolve_owner_id(owner_id)
    with with_mysql_cursor() as cur:
        cur.execute("SELECT COUNT(*) c FROM local_users WHERE owner_id=%s", (owner_id,))
        row = cur.fetchone()
        return int(row["c"] if row and row.get("c") is not None else 0)

def update_limit(owner_id: int, username: str, new_limit_bytes: int):
    owner_id = resolve_owner_id(owner_id)
    with with_mysql_cursor() as cur:
        cur.execute(
            "UPDATE local_users SET plan_limit_bytes=%s WHERE owner_id=%s AND username=%s",
            (int(new_limit_bytes), owner_id, username)
        )

def reset_used(owner_id: int, username: str):
    owner_id = resolve_owner_id(owner_id)
    with with_mysql_cursor() as cur:
        cur.execute(
            "UPDATE local_users SET used_bytes=0 WHERE owner_id=%s AND username=%s",
            (owner_id, username)
        )

def renew_user(owner_id: int, username: str, add_days: int):
    owner_id = resolve_owner_id(owner_id)
    with with_mysql_cursor() as cur:
        cur.execute(
            """UPDATE local_users
               SET expire_at = IF(expire_at IS NULL, UTC_TIMESTAMP() + INTERVAL %s DAY,
                                    expire_at + INTERVAL %s DAY)
               WHERE owner_id=%s AND username=%s""",
            (add_days, add_days, owner_id, username)
        )

# panels extra
def set_panel_sub_url(owner_id: int, panel_id: int, sub_url: str | None):
    owner_id = resolve_owner_id(owner_id)
    with with_mysql_cursor() as cur:
        cur.execute("UPDATE panels SET sub_url=%s WHERE id=%s AND telegram_user_id=%s",
                    (sub_url, int(panel_id), owner_id))

def get_panel(owner_id: int, panel_id: int):
    owner_id = resolve_owner_id(owner_id)
    with with_mysql_cursor() as cur:
        cur.execute("SELECT * FROM panels WHERE id=%s AND telegram_user_id=%s", (int(panel_id), owner_id))
        return cur.fetchone()

def canonicalize_name(name: str) -> str:
    """Normalize a config name by removing user-specific fragments."""
    try:
        nm = unquote(name or "").strip()
        nm = re.sub(r"\s*\d+(?:\.\d+)?\s*[KMGT]?B/\d+(?:\.\d+)?\s*[KMGT]?B", "", nm, flags=re.I)
        nm = re.sub(r"\s*👤.*", "", nm)
        nm = re.sub(r"\s*\([a-zA-Z0-9_-]{3,}\)", "", nm)
        nm = re.sub(r"\s+", " ", nm)
        return nm.strip()[:255]
    except Exception:
        return ""

def get_panel_disabled_names(panel_id: int):
    with with_mysql_cursor() as cur:
        cur.execute(
            "SELECT config_name FROM panel_disabled_configs WHERE panel_id=%s",
            (int(panel_id),),
        )
        # Return normalized, unique names so callers can match reliably
        return sorted(
            {
                cn
                for r in cur.fetchall()
                for cn in [canonicalize_name(r["config_name"])]
                if (r["config_name"] or "").strip() and cn
            }
        )

def set_panel_disabled_names(owner_id: int, panel_id: int, names):
    owner_id = resolve_owner_id(owner_id)
    # Normalize and dedupe names so dynamic parts don't cause mismatches
    clean = [
        c
        for c in sorted({canonicalize_name(n) for n in names if n and n.strip()})
        if c
    ]
    with with_mysql_cursor() as cur:
        cur.execute("DELETE FROM panel_disabled_configs WHERE panel_id=%s", (int(panel_id),))
        if clean:
            cur.executemany(
                """
                INSERT INTO panel_disabled_configs(telegram_user_id,panel_id,config_name)
                VALUES(%s,%s,%s)
                """,
                [(owner_id, int(panel_id), n) for n in clean],
            )

def get_panel_disabled_nums(panel_id: int):
    with with_mysql_cursor() as cur:
        cur.execute(
            "SELECT config_index FROM panel_disabled_numbers WHERE panel_id=%s",
            (int(panel_id),),
        )
        return [int(r["config_index"]) for r in cur.fetchall() if r["config_index"]]

def set_panel_disabled_nums(owner_id: int, panel_id: int, nums):
    owner_id = resolve_owner_id(owner_id)
    clean = sorted({int(n) for n in nums if str(n).isdigit() and int(n) > 0})
    with with_mysql_cursor() as cur:
        cur.execute("DELETE FROM panel_disabled_numbers WHERE panel_id=%s", (int(panel_id),))
        if clean:
            cur.executemany(
                """
                INSERT INTO panel_disabled_numbers(telegram_user_id,panel_id,config_index)
                VALUES(%s,%s,%s)
                """,
                [(owner_id, int(panel_id), n) for n in clean],
            )

def list_panel_links(panel_id: int):
    with with_mysql_cursor() as cur:
        cur.execute("""
            SELECT lup.owner_id, lup.local_username, lup.remote_username,
                   p.panel_url, p.access_token, p.panel_type
            FROM local_user_panel_links lup
            JOIN panels p ON p.id = lup.panel_id
            WHERE lup.panel_id=%s
        """, (int(panel_id),))
        return cur.fetchall()

def delete_panel_and_cleanup(owner_id: int, panel_id: int):
    owner_id = resolve_owner_id(owner_id)
    # 1) disable all mapped remote users on that panel
    rows = list_panel_links(panel_id)
    for r in rows:
        try:
            api = get_api(r.get("panel_type"))
            remotes = (
                r["remote_username"].split(",")
                if r.get("panel_type") == "sanaei"
                else [r["remote_username"]]
            )
            for rn in remotes:
                ok, err = api.disable_remote_user(r["panel_url"], r["access_token"], rn)
                if not ok:
                    log.warning("disable before delete failed on %s: %s", r["panel_url"], err or "unknown")
        except Exception as e:
            log.warning("disable before delete exception: %s", e)
    # 2) delete mappings + panel
    with with_mysql_cursor() as cur:
        cur.execute("DELETE FROM local_user_panel_links WHERE panel_id=%s", (int(panel_id),))
        cur.execute("DELETE FROM panel_disabled_configs WHERE panel_id=%s", (int(panel_id),))
        cur.execute("DELETE FROM panel_disabled_numbers WHERE panel_id=%s", (int(panel_id),))
        cur.execute("DELETE FROM panels WHERE id=%s AND telegram_user_id=%s", (int(panel_id), owner_id))

# ---------- agents ----------
def upsert_agent(tg_id: int, name: str):
    with with_mysql_cursor() as cur:
        cur.execute("SELECT id FROM agents WHERE telegram_user_id=%s", (tg_id,))
        row = cur.fetchone()
        if row:
            cur.execute("UPDATE agents SET name=%s, active=1 WHERE telegram_user_id=%s", (name, tg_id))
        else:
            cur.execute(
                "INSERT INTO agents(telegram_user_id,name,plan_limit_bytes,expire_at,active,user_limit,max_user_bytes) "
                "VALUES(%s,%s,0,NULL,1,0,0)",
                (tg_id, name)
            )

def get_agent(tg_id: int):
    with with_mysql_cursor() as cur:
        cur.execute("SELECT * FROM agents WHERE telegram_user_id=%s", (tg_id,))
        return cur.fetchone()

def set_agent_quota(tg_id: int, limit_bytes: int):
    with with_mysql_cursor() as cur:
        cur.execute("UPDATE agents SET plan_limit_bytes=%s WHERE telegram_user_id=%s",
                    (int(limit_bytes), tg_id))
    try:
        usage_sync.sync_agent_now(tg_id)
    except Exception as e:
        log.warning("sync_agent_now failed for %s: %s", tg_id, e)

def set_agent_user_limit(tg_id: int, max_users: int):
    with with_mysql_cursor() as cur:
        cur.execute(
            "UPDATE agents SET user_limit=%s WHERE telegram_user_id=%s",
            (int(max_users), tg_id),
        )

def set_agent_max_user_bytes(tg_id: int, max_bytes: int):
    with with_mysql_cursor() as cur:
        cur.execute(
            "UPDATE agents SET max_user_bytes=%s WHERE telegram_user_id=%s",
            (int(max_bytes), tg_id),
        )

def renew_agent_days(tg_id: int, add_days: int):
    # if no expire_at -> set now + days; else add days
    with with_mysql_cursor() as cur:
        cur.execute("SELECT expire_at FROM agents WHERE telegram_user_id=%s", (tg_id,))
        row = cur.fetchone()
        if row and row.get("expire_at"):
            cur.execute("UPDATE agents SET expire_at = expire_at + INTERVAL %s DAY WHERE telegram_user_id=%s",
                        (add_days, tg_id))
        else:
            cur.execute("UPDATE agents SET expire_at = UTC_TIMESTAMP() + INTERVAL %s DAY WHERE telegram_user_id=%s",
                        (add_days, tg_id))

def list_agents():
    with with_mysql_cursor() as cur:
        cur.execute("SELECT * FROM agents ORDER BY created_at DESC")
        return cur.fetchall()

def set_agent_active(tg_id: int, active: bool):
    with with_mysql_cursor() as cur:
        cur.execute("UPDATE agents SET active=%s WHERE telegram_user_id=%s", (1 if active else 0, tg_id))

def list_agent_panel_ids(agent_tg_id: int):
    with with_mysql_cursor() as cur:
        cur.execute("SELECT panel_id FROM agent_panels WHERE agent_tg_id=%s", (agent_tg_id,))
        return {int(r["panel_id"]) for r in cur.fetchall()}

def set_agent_panels(agent_tg_id: int, panel_ids: set[int]):
    with with_mysql_cursor() as cur:
        cur.execute("DELETE FROM agent_panels WHERE agent_tg_id=%s", (agent_tg_id,))
        if panel_ids:
            cur.executemany("INSERT INTO agent_panels(agent_tg_id,panel_id) VALUES(%s,%s)",
                            [(agent_tg_id, int(pid)) for pid in panel_ids])

# ---------- UI ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    header = ""
    if not is_admin(uid):
        ag = get_agent(uid)
        if ag:
            limit_b = int(ag.get("plan_limit_bytes") or 0)
            max_users = int(ag.get("user_limit") or 0)
            max_user_b = int(ag.get("max_user_bytes") or 0)
            user_cnt = count_local_users(uid)
            exp = ag.get("expire_at")
            parts = [f"👤 <b>{ag['name']}</b>", f"👥 Users: {user_cnt}/{('∞' if max_users==0 else max_users)}"]
            if limit_b:
                parts.append(f"📦 Quota: {fmt_bytes_short(limit_b)}")
            if max_user_b:
                parts.append(f"📛 Max/User: {fmt_bytes_short(max_user_b)}")
            if exp:
                parts.append(f"⏳ Expire: {exp.strftime('%Y-%m-%d')}")
            header = "\n".join(parts) + "\n\n"
    if is_admin(uid):
        kb = [
            [InlineKeyboardButton("🧬 New Local User", callback_data="new_user")],
            [InlineKeyboardButton("🔍 Search User", callback_data="search_user")],
            [InlineKeyboardButton("👥 List Users", callback_data="list_users:0")],
            [InlineKeyboardButton("⚙️ Admin Panel", callback_data="admin_panel")],
        ]
    else:
        kb = [
            [InlineKeyboardButton("🧬 New Local User", callback_data="new_user")],
            [InlineKeyboardButton("🔍 Search User", callback_data="search_user")],
            [InlineKeyboardButton("👥 List Users", callback_data="list_users:0")],
        ]
    text = header + "Choose an option:"
    if update.message:
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode="HTML")
    else:
        await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode="HTML")

def _panel_select_kb(panels, selected: set, mode: str):
    rows = []
    for p in panels:
        pid = int(p["id"])
        mark = "✅" if pid in selected else "⬜"
        title = f"{mark} {p['name']} ({p.get('panel_type', 'marzneshin')})"
        if mode == "assign":
            cb = f"ap:toggle:{pid}"
        else:
            cb = f"selpanel:toggle:{pid}"
        rows.append([InlineKeyboardButton(title[:64], callback_data=cb)])

    if mode == "assign":
        prefix = "ap"
        apply_cb = "ap:apply"
        cancel_cb = "ap:cancel"
    else:
        prefix = "selpanel"
        apply_cb = f"selpanel:apply:{mode}"
        cancel_cb = "selpanel:cancel"

    rows.append([
        InlineKeyboardButton("☑️ All", callback_data=f"{prefix}:all"),
        InlineKeyboardButton("🔲 None", callback_data=f"{prefix}:none"),
    ])
    rows.append([
        InlineKeyboardButton("✅ Apply", callback_data=apply_cb),
        InlineKeyboardButton("❌ Cancel", callback_data=cancel_cb),
    ])
    return InlineKeyboardMarkup(rows)

async def show_panel_select(update_or_q, context, owner_id: int, mode: str, username: str = None):
    panels = list_panels_for_agent(owner_id) if not is_admin(owner_id) else list_my_panels_admin(owner_id)
    if not panels:
        msg = "❌ هیچ پنلی ثبت نشده."
        if hasattr(update_or_q, "edit_message_text"):
            await update_or_q.edit_message_text(msg)
        else:
            await update_or_q.message.reply_text(msg)
        return ConversationHandler.END

    if mode == "create":
        panels = [
            p for p in panels
            if not ((p.get("panel_type") in ("marzneshin", "sanaei")) and not p.get("template_username"))
        ]
        if not panels:
            txt = "⚠️ هیچ پنلی template/inbound ندارد. از 🛠️ Manage Panels تنظیم کن."
            if hasattr(update_or_q, "edit_message_text"):
                await update_or_q.edit_message_text(txt)
            else:
                await update_or_q.message.reply_text(txt)
            return ConversationHandler.END
        selected = {int(p["id"]) for p in panels}
    else:
        linked = list_linked_panel_ids(owner_id, username)
        selected = set(linked)

    context.user_data["panel_select_mode"] = mode
    context.user_data["panel_select_username"] = username
    context.user_data["panel_select_list"] = panels
    context.user_data["panel_selected"] = selected

    text = ("پنل‌های فعال برای ساخت یوزر جدید را انتخاب کن:"
            if mode == "create"
            else f"پنل‌های فعال برای <b>{username}</b> را انتخاب/غیرفعال کن:")
    kb = _panel_select_kb(panels, selected, mode)
    if hasattr(update_or_q, "edit_message_text"):
        await update_or_q.edit_message_text(text, reply_markup=kb, parse_mode="HTML")
    else:
        await update_or_q.message.reply_text(text, reply_markup=kb, parse_mode="HTML")
    return ASK_SELECT_PANELS

# ---------- buttons ----------
async def on_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data
    uid = update.effective_user.id

    if data == "admin_panel":
        if not is_admin(uid):
            await q.edit_message_text("دسترسی ندارید.")
            return ConversationHandler.END
        kb = [
            [InlineKeyboardButton("➕ Add Panel", callback_data="add_panel")],
            [InlineKeyboardButton("🛠️ Manage Panels", callback_data="manage_panels")],
            [InlineKeyboardButton("👑 Manage Agents", callback_data="manage_agents")],
            [InlineKeyboardButton("⬅️ Back", callback_data="back_home")],
        ]
        await q.edit_message_text("پنل ادمین:", reply_markup=InlineKeyboardMarkup(kb))
        return ConversationHandler.END

    # --- admin/agent shared
    if data == "add_panel":
        if not is_admin(uid):
            await q.edit_message_text("فقط ادمین می‌تواند پنل اضافه کند.")
            return ConversationHandler.END
        await q.edit_message_text("🧾 اسم پنل را بفرست:")
        return ASK_PANEL_NAME

    if data == "manage_panels":
        if not is_admin(uid):
            await q.edit_message_text("دسترسی ندارید.")
            return ConversationHandler.END
        rows = list_my_panels_admin(uid)
        if not rows:
            await q.edit_message_text("هیچ پنلی ثبت نشده. ابتدا ➕ Add Panel.")
            return ConversationHandler.END
        kb = [[InlineKeyboardButton(f"{r['name']}"[:64],
                                    callback_data=f"panel_sel:{r['id']}")] for r in rows]
        kb.append([InlineKeyboardButton("⬅️ Back", callback_data="back_home")])
        await q.edit_message_text("یک پنل را انتخاب کن:", reply_markup=InlineKeyboardMarkup(kb))
        return ConversationHandler.END

    if data.startswith("panel_sel:"):
        if not is_admin(uid):
            await q.edit_message_text("دسترسی ندارید.")
            return ConversationHandler.END
        pid = int(data.split(":", 1)[1])
        context.user_data["edit_panel_id"] = pid
        return await show_panel_card(q, context, uid, pid)

    if data == "p_set_template":
        if not is_admin(uid): return ConversationHandler.END
        pid = context.user_data.get("edit_panel_id")
        info = get_panel(uid, pid) if pid else None
        prompt = (
            "ID اینباندها (با کاما جدا کن)" if info and info.get("panel_type") == "sanaei" else "نام تمپلیت"
        )
        await q.edit_message_text(f"{prompt} را بفرست (برای حذف، '-'):") ; return ASK_PANEL_TEMPLATE
    if data == "p_rename":
        if not is_admin(uid): return ConversationHandler.END
        await q.edit_message_text("اسم جدید پنل را بفرست:") ; return ASK_EDIT_PANEL_NAME
    if data == "p_change_creds":
        if not is_admin(uid): return ConversationHandler.END
        await q.edit_message_text("یوزرنیم ادمین جدید را بفرست:") ; return ASK_EDIT_PANEL_USER
    if data == "p_set_sub":
        if not is_admin(uid): return ConversationHandler.END
        pid = context.user_data.get("edit_panel_id")
        info = get_panel(uid, pid) if pid else None
        if info and info.get("panel_type") == "sanaei":
            await q.edit_message_text("این پنل از لینک سابسکریپشن پشتیبانی نمی‌کند.")
            return ConversationHandler.END
        await q.edit_message_text("لینک سابسکریپشن پنل را بفرست (برای حذف، '-'):") ; return ASK_PANEL_SUB_URL
    if data == "p_filter_cfgs":
        if not is_admin(uid): return ConversationHandler.END
        pid = context.user_data.get("edit_panel_id")
        info = get_panel(uid, pid)
        if not info:
            await q.edit_message_text("پنل پیدا نشد.")
            return ConversationHandler.END
        if info.get("panel_type") == "sanaei":
            await q.edit_message_text("این پنل از فیلتر کانفیگ‌ها پشتیبانی نمی‌کند.")
            return ConversationHandler.END
        if not info.get("sub_url"):
            await q.edit_message_text("اول لینک سابسکریپشن پنل را تنظیم کن (Set/Clear Sub URL).")
            return ConversationHandler.END
        return await show_panel_cfg_selector(q, context, uid, pid, page=0)
    if data == "p_filter_cfgnums":
        if not is_admin(uid): return ConversationHandler.END
        pid = context.user_data.get("edit_panel_id")
        info = get_panel(uid, pid)
        if not info:
            await q.edit_message_text("پنل پیدا نشد.")
            return ConversationHandler.END
        if info.get("panel_type") == "sanaei":
            await q.edit_message_text("این پنل از فیلتر کانفیگ‌ها پشتیبانی نمی‌کند.")
            return ConversationHandler.END
        if not info.get("sub_url"):
            await q.edit_message_text("اول لینک سابسکریپشن پنل را تنظیم کن (Set/Clear Sub URL).")
            return ConversationHandler.END
        return await show_panel_cfgnum_selector(q, context, uid, pid, page=0)
    if data == "p_remove":
        if not is_admin(uid): return ConversationHandler.END
        pid = context.user_data.get("edit_panel_id")
        if not pid:
            await q.edit_message_text("پنل انتخاب نشده.")
            return ConversationHandler.END
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🗑️ بله، حذف کن", callback_data="p_remove_yes")],
            [InlineKeyboardButton("⬅️ انصراف", callback_data=f"panel_sel:{pid}")],
        ])
        await q.edit_message_text("⚠️ قبل از حذف، تمام یوزرهای مپ‌شده روی این پنل دیزیبل می‌شوند. مطمئنی؟", reply_markup=kb)
        return ASK_PANEL_REMOVE_CONFIRM
    if data == "p_remove_yes":
        if not is_admin(uid): return ConversationHandler.END
        pid = context.user_data.get("edit_panel_id")
        delete_panel_and_cleanup(uid, pid)
        await q.edit_message_text("✅ پنل حذف شد و همهٔ کانفیگ‌های مرتبط دیزیبل شدند.")
        return ConversationHandler.END

    if data == "new_user":
        await q.edit_message_text("نام یوزر جدید (local/unified) را بفرست:") ; return ASK_NEWUSER_NAME

    if data == "search_user":
        await q.edit_message_text("اسم یوزر برای جستجو (partial مجاز):") ; return ASK_SEARCH_USER

    if data.startswith("list_users:"):
        page = int(data.split(":", 1)[1])
        page = max(0, page)
        total = count_local_users(uid)
        per = 25
        off = page * per
        rows = list_all_local_users(uid, offset=off, limit=per) or []
        if not rows and page > 0:
            page = 0 ; off = 0
            rows = list_all_local_users(uid, offset=0, limit=per)
        kb = [[InlineKeyboardButton(r["username"], callback_data=f"user_sel:{r['username']}")] for r in rows]
        nav = []
        if page > 0: nav.append(InlineKeyboardButton("⬅️ قبلی", callback_data=f"list_users:{page-1}"))
        if off + per < total: nav.append(InlineKeyboardButton("بعدی ➡️", callback_data=f"list_users:{page+1}"))
        if nav: kb.append(nav)
        kb.append([InlineKeyboardButton("⬅️ Back", callback_data="back_home")])
        await q.edit_message_text(f"👥 کاربران (صفحه {page+1})", reply_markup=InlineKeyboardMarkup(kb))
        return ConversationHandler.END

    if data.startswith("user_sel:"):
        uname = data.split(":", 1)[1]
        context.user_data["manage_username"] = uname
        return await show_user_card(q, uid, uname)

    if data == "act_edit_limit":
        await q.edit_message_text("لیمیت جدید: 0/unlimited یا 500MB / 10GB / 1.5TB") ; return ASK_EDIT_LIMIT

    if data == "act_reset_used":
        uname = context.user_data.get("manage_username")
        if not uname:
            await q.edit_message_text("یوزر انتخاب نشده.")
            return ConversationHandler.END
        reset_used(uid, uname)
        return await show_user_card(q, uid, uname, notice="✅ مصرف صفر شد.")

    if data == "act_renew":
        await q.edit_message_text("چند روز اضافه شود؟ (مثلا 30)") ; return ASK_RENEW_DAYS

    if data == "act_user_panels":
        uname = context.user_data.get("manage_username")
        if not uname:
            await q.edit_message_text("یوزر انتخاب نشده.")
            return ConversationHandler.END
        return await show_panel_select(q, context, uid, mode="edit", username=uname)

    # ----- agent mgmt (admin) -----
    if data == "manage_agents":
        if not is_admin(uid):
            await q.edit_message_text("دسترسی ندارید.")
            return ConversationHandler.END
        rows = list_agents()
        kb = [[InlineKeyboardButton(f"{r['name']} - {r['telegram_user_id']}", callback_data=f"agent_sel:{r['telegram_user_id']}")] for r in rows[:50]]
        kb.append([InlineKeyboardButton("➕ Add Agent", callback_data="agent_add")])
        kb.append([InlineKeyboardButton("⬅️ Back", callback_data="back_home")])
        await q.edit_message_text("نماینده‌ها:", reply_markup=InlineKeyboardMarkup(kb))
        return ConversationHandler.END

    if data == "agent_add":
        if not is_admin(uid): return ConversationHandler.END
        await q.edit_message_text("نام نماینده:")
        return ASK_AGENT_NAME

    if data.startswith("agent_sel:"):
        if not is_admin(uid): return ConversationHandler.END
        aid = int(data.split(":",1)[1])
        context.user_data["agent_tg_id"] = aid
        return await show_agent_card(q, context, aid)

    if data == "agent_set_quota":
        if not is_admin(uid): return ConversationHandler.END
        await q.edit_message_text("حجم کل نماینده (مثلا 200GB یا 0=نامحدود):")
        return ASK_AGENT_LIMIT

    if data == "agent_set_user_limit":
        if not is_admin(uid): return ConversationHandler.END
        await q.edit_message_text("حداکثر تعداد یوزر (0=نامحدود):")
        return ASK_AGENT_MAX_USERS

    if data == "agent_set_max_user":
        if not is_admin(uid): return ConversationHandler.END
        await q.edit_message_text("حداکثر حجم هر یوزر (مثلا 50GB یا 0=نامحدود):")
        return ASK_AGENT_MAX_USER_GB

    if data == "agent_renew_days":
        if not is_admin(uid): return ConversationHandler.END
        await q.edit_message_text("چند روز به انقضا اضافه شود؟ (مثلا 30)")
        return ASK_AGENT_RENEW_DAYS

    if data == "agent_toggle_active":
        if not is_admin(uid): return ConversationHandler.END
        a = context.user_data.get("agent_tg_id")
        info = get_agent(a)
        set_agent_active(a, not bool(info and info.get("active")))
        return await show_agent_card(q, context, a)

    if data == "agent_assign_panels":
        if not is_admin(uid): return ConversationHandler.END
        a = context.user_data.get("agent_tg_id")
        return await show_assign_panels(q, context, a)

    if data.startswith("ap:"):
        if not is_admin(uid): return ConversationHandler.END
        a = context.user_data.get("agent_tg_id")
        cmd = data.split(":",1)[1]
        panels = list_my_panels_admin(uid)
        selected = context.user_data.get("ap_selected") or set(list_agent_panel_ids(a))
        if cmd == "all":
            selected = {int(p["id"]) for p in panels}
        elif cmd == "none":
            selected = set()
        elif cmd.startswith("toggle:"):
            pid = int(cmd.split(":",1)[1])
            if pid in selected: selected.remove(pid)
            else: selected.add(pid)
        elif cmd == "apply":
            set_agent_panels(a, selected)
            return await show_agent_card(q, context, a, notice="✅ پنل‌های نماینده ذخیره شد.")
        elif cmd == "cancel":
            return await show_agent_card(q, context, a)
        context.user_data["ap_selected"] = selected
        kb = _panel_select_kb(panels, selected, mode="assign")
        await q.edit_message_text("پنل‌های این نماینده:", reply_markup=kb)
        return ConversationHandler.END

    if data == "back_home":
        await start(update, context)
        return ConversationHandler.END

    # ---- panel multi-select handlers ----
    if data.startswith("selpanel:"):
        mode = context.user_data.get("panel_select_mode")
        panels = context.user_data.get("panel_select_list") or []
        selected = context.user_data.get("panel_selected") or set()

        if data == "selpanel:all":
            selected = {int(p["id"]) for p in panels}
        elif data == "selpanel:none":
            selected = set()
        elif data.startswith("selpanel:toggle:"):
            pid = int(data.split(":", 2)[2])
            if pid in selected: selected.remove(pid)
            else: selected.add(pid)
        elif data.startswith("selpanel:apply:"):
            which = data.split(":", 2)[2]
            if which != mode:
                pass
            if mode == "create":
                await q.edit_message_text("⏳ در حال ساخت روی پنل‌های انتخابی ...")
                await finalize_create_on_selected(q, context, uid, selected)
            else:
                uname = context.user_data.get("panel_select_username")
                await q.edit_message_text("⏳ در حال اعمال تغییرات پنل‌های کاربر ...")
                await apply_edit_user_panels(q, uid, uname, selected)
            return ConversationHandler.END
        elif data == "selpanel:cancel":
            if mode == "create":
                await q.edit_message_text("لغو شد.")
                return ConversationHandler.END
            else:
                uname = context.user_data.get("panel_select_username")
                return await show_user_card(q, uid, uname)

        context.user_data["panel_selected"] = selected
        await q.edit_message_text(
            ("پنل‌های فعال برای ساخت یوزر جدید را انتخاب کن:" if mode == "create"
             else f"پنل‌های فعال را برای <b>{context.user_data.get('panel_select_username')}</b> انتخاب/غیرفعال کن:"),
            reply_markup=_panel_select_kb(panels, selected, mode), parse_mode="HTML"
        )
        return ASK_SELECT_PANELS

    # ---------- panel cfg selector actions ----------
    if data.startswith("pcfg:"):
        pid = context.user_data.get("cfg_panel_id")
        if not pid:
            await q.edit_message_text("جلسه تنظیمات معتبر نیست.")
            return ConversationHandler.END

        cmd = data.split(":",1)[1]
        names = context.user_data.get("cfg_names") or []
        enabled = set(context.user_data.get("cfg_enabled") or set())
        page = int(context.user_data.get("cfg_page", 0))
        per = 20

        if cmd == "all":
            enabled = set(names)
        elif cmd == "none":
            enabled = set()
        elif cmd.startswith("toggle:"):
            idx = int(cmd.split(":",1)[1])
            if 0 <= idx < len(names):
                n = names[idx]
                if n in enabled: enabled.remove(n)
                else: enabled.add(n)
        elif cmd.startswith("page:"):
            np = int(cmd.split(":",1)[1])
            if np >= 0:
                page = np
        elif cmd == "apply":
            disabled = set(names) - set(enabled)
            set_panel_disabled_names(uid, pid, disabled)
            return await show_panel_cfg_selector(q, context, uid, pid, page=page, notice="✅ ذخیره شد.")
        elif cmd == "refresh":
            return await show_panel_cfg_selector(q, context, uid, pid, page=page)
        elif cmd == "cancel":
            return await show_panel_card(q, context, uid, pid)

        context.user_data["cfg_enabled"] = list(enabled)
        context.user_data["cfg_page"] = page
        kb, text = build_panel_cfg_kb(names, enabled, page, per)
        await q.edit_message_text(text, reply_markup=kb)
        return ConversationHandler.END

    if data.startswith("pcnum:"):
        pid = context.user_data.get("cfg_panel_id")
        if not pid:
            await q.edit_message_text("جلسه تنظیمات معتبر نیست.")
            return ConversationHandler.END

        cmd = data.split(":",1)[1]
        titles = context.user_data.get("cfgnum_titles") or []
        enabled = set(context.user_data.get("cfgnums_enabled") or set())
        page = int(context.user_data.get("cfgnum_page", 0))
        total = len(titles)
        per = 20

        if cmd == "all":
            enabled = set(range(1, total+1))
        elif cmd == "none":
            enabled = set()
        elif cmd.startswith("toggle:"):
            idx = int(cmd.split(":",1)[1])
            if 1 <= idx <= total:
                if idx in enabled: enabled.remove(idx)
                else: enabled.add(idx)
        elif cmd.startswith("page:"):
            np = int(cmd.split(":",1)[1])
            if np >= 0:
                page = np
        elif cmd == "apply":
            disabled = set(range(1, total+1)) - set(enabled)
            set_panel_disabled_nums(uid, pid, disabled)
            return await show_panel_cfgnum_selector(q, context, uid, pid, page=page, notice="✅ ذخیره شد.")
        elif cmd == "refresh":
            return await show_panel_cfgnum_selector(q, context, uid, pid, page=page)
        elif cmd == "cancel":
            return await show_panel_card(q, context, uid, pid)

        context.user_data["cfgnums_enabled"] = list(enabled)
        context.user_data["cfgnum_page"] = page
        kb, text = build_panel_cfgnum_kb(titles, enabled, page, per)
        await q.edit_message_text(text, reply_markup=kb)
        return ConversationHandler.END

    return ConversationHandler.END

# ---------- panel cfg selector UI ----------
def build_panel_cfg_kb(names, enabled_set, page: int, per: int):
    total = len(names)
    start = page * per
    end = min(start + per, total)
    page_names = names[start:end]
    rows = []
    for idx, nm in enumerate(page_names, start=start):
        mark = "✅" if nm in enabled_set else "⬜"
        title = f"{mark} {nm}"
        rows.append([InlineKeyboardButton(title[:64], callback_data=f"pcfg:toggle:{idx}")])
    controls = [
        InlineKeyboardButton("☑️ All", callback_data="pcfg:all"),
        InlineKeyboardButton("🔲 None", callback_data="pcfg:none"),
    ]
    rows.append(controls)
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"pcfg:page:{page-1}"))
    if end < total:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"pcfg:page:{page+1}"))
    if nav:
        rows.append(nav)
    rows.append([
        InlineKeyboardButton("✅ Apply", callback_data="pcfg:apply"),
        InlineKeyboardButton("❌ Cancel", callback_data="pcfg:cancel"),
        InlineKeyboardButton("🔄 Refresh", callback_data="pcfg:refresh"),
    ])
    text = f"فهرست کانفیگ‌های پنل (صفحه {page+1})"
    return InlineKeyboardMarkup(rows), text

def build_panel_cfgnum_kb(titles, enabled_set, page: int, per: int):
    total = len(titles)
    start = page * per
    end = min(start + per, total)
    page_titles = titles[start:end]
    rows = []
    for idx, nm in enumerate(page_titles, start=start+1):
        mark = "✅" if idx in enabled_set else "⬜"
        title = f"{mark} {idx}. {nm}"
        rows.append([InlineKeyboardButton(title[:64], callback_data=f"pcnum:toggle:{idx}")])
    controls = [
        InlineKeyboardButton("☑️ All", callback_data="pcnum:all"),
        InlineKeyboardButton("🔲 None", callback_data="pcnum:none"),
    ]
    rows.append(controls)
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"pcnum:page:{page-1}"))
    if end < total:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"pcnum:page:{page+1}"))
    if nav:
        rows.append(nav)
    rows.append([
        InlineKeyboardButton("✅ Apply", callback_data="pcnum:apply"),
        InlineKeyboardButton("❌ Cancel", callback_data="pcnum:cancel"),
        InlineKeyboardButton("🔄 Refresh", callback_data="pcnum:refresh"),
    ])
    text = f"فهرست کانفیگ‌ها بر اساس شماره (صفحه {page+1})"
    return InlineKeyboardMarkup(rows), text

def extract_name(link: str) -> str:
    try:
        i = link.find("#")
        if i == -1:
            return ""
        nm = unquote(link[i+1:]).strip()
        return nm[:255]
    except Exception:
        return ""

async def show_panel_cfg_selector(q, context: ContextTypes.DEFAULT_TYPE, owner_id: int, panel_id: int, page: int = 0, notice: str = None):
    info = get_panel(owner_id, panel_id)
    if not info:
        await q.edit_message_text("پنل پیدا نشد.")
        return ConversationHandler.END

    api = get_api(info.get("panel_type"))
    links = []
    if info.get("template_username"):
        u, e = api.get_user(info["panel_url"], info["access_token"], info["template_username"])
        if u and u.get("key"):
            links = api.fetch_links_from_panel(info["panel_url"], info["template_username"], u["key"])
    elif info.get("sub_url"):
        links = api.fetch_subscription_links(info["sub_url"])
    if not links:
        await q.edit_message_text("ابتدا template یا لینک سابسکریپشن را تنظیم کن.")
        return ConversationHandler.END

    seen, names = set(), []
    for s in links:
        nm = extract_name(s) or "(بدون‌نام)"
        if nm not in seen:
            seen.add(nm)
            names.append(nm)

    disabled = set(get_panel_disabled_names(panel_id))
    enabled = set(names) - disabled

    context.user_data["cfg_names"] = names
    context.user_data["cfg_enabled"] = list(enabled)
    context.user_data["cfg_page"] = page
    context.user_data["cfg_panel_id"] = panel_id

    kb, txt = build_panel_cfg_kb(names, enabled, page, 20)
    if notice:
        txt = f"{notice}\n{txt}"
    await q.edit_message_text(txt, reply_markup=kb)
    return ConversationHandler.END

async def show_panel_cfgnum_selector(q, context: ContextTypes.DEFAULT_TYPE, owner_id: int, panel_id: int, page: int = 0, notice: str = None):
    info = get_panel(owner_id, panel_id)
    if not info:
        await q.edit_message_text("پنل پیدا نشد.")
        return ConversationHandler.END

    api = get_api(info.get("panel_type"))
    links = []
    if info.get("template_username"):
        u, e = api.get_user(info["panel_url"], info["access_token"], info["template_username"])
        if u and u.get("key"):
            links = api.fetch_links_from_panel(info["panel_url"], info["template_username"], u["key"])
    elif info.get("sub_url"):
        links = api.fetch_subscription_links(info["sub_url"])
    if not links:
        await q.edit_message_text("ابتدا template یا لینک سابسکریپشن را تنظیم کن.")
        return ConversationHandler.END

    titles = [extract_name(s) or f"کانفیگ {i+1}" for i, s in enumerate(links)]
    disabled = set(get_panel_disabled_nums(panel_id))
    enabled = set(range(1, len(titles)+1)) - disabled

    context.user_data["cfgnum_titles"] = titles
    context.user_data["cfgnums_enabled"] = list(enabled)
    context.user_data["cfgnum_page"] = page
    context.user_data["cfg_panel_id"] = panel_id

    kb, txt = build_panel_cfgnum_kb(titles, enabled, page, 20)
    if notice:
        txt = f"{notice}\n{txt}"
    await q.edit_message_text(txt, reply_markup=kb)
    return ConversationHandler.END

# ---------- cards ----------
async def show_panel_card(q, context: ContextTypes.DEFAULT_TYPE, owner_id: int, panel_id: int):
    p = get_panel(owner_id, panel_id)
    if not p:
        await q.edit_message_text("پنل پیدا نشد.")
        return ConversationHandler.END

    is_sanaei = p.get('panel_type') == 'sanaei'
    label = "Inbound" if is_sanaei else "Template"
    lines = [
        f"🧩 <b>{p['name']}</b>",
        f"📦 Type: <b>{p.get('panel_type', 'marzneshin')}</b>",
        f"🌐 URL: <code>{p['panel_url']}</code>",
        f"👤 Admin: <code>{p['admin_username']}</code>",
        f"🧬 {label}: <b>{p.get('template_username') or '-'}</b>",
    ]
    if not is_sanaei:
        lines.append(f"🔗 Sub URL: <code>{p.get('sub_url') or '-'}</code>")
    lines += [
        "",
        "چه کاری انجام بدهم؟",
    ]
    kb = [
        [InlineKeyboardButton(f"🧬 Set/Clear {label}", callback_data="p_set_template")],
        [InlineKeyboardButton("🔑 Change Admin Credentials", callback_data="p_change_creds")],
        [InlineKeyboardButton("✏️ Rename Panel", callback_data="p_rename")],
    ]
    if not is_sanaei:
        kb.append([InlineKeyboardButton("🔗 Set/Clear Sub URL", callback_data="p_set_sub")])
        kb.append([InlineKeyboardButton("🧷 فیلتر کانفیگ‌های پنل", callback_data="p_filter_cfgs")])
        kb.append([InlineKeyboardButton("🔢 فیلتر بر اساس شماره", callback_data="p_filter_cfgnums")])
    kb.append([InlineKeyboardButton("🗑️ Remove Panel", callback_data="p_remove")])
    kb.append([InlineKeyboardButton("⬅️ Back", callback_data="manage_panels")])
    await q.edit_message_text("\n".join(lines), reply_markup=InlineKeyboardMarkup(kb), parse_mode="HTML")
    return ConversationHandler.END

async def show_user_card(q, owner_id: int, uname: str, notice: str = None):
    row = get_local_user(owner_id, uname)
    if not row:
        await q.edit_message_text("کاربر پیدا نشد.")
        return ConversationHandler.END

    limit_b = int(row["plan_limit_bytes"] or 0)
    used_b  = int(row["used_bytes"] or 0)
    exp     = row["expire_at"]
    pushed  = int(row.get("disabled_pushed", 0) or 0)

    app_key = get_app_key(owner_id, uname)
    public_base = os.getenv("PUBLIC_BASE_URL", "http://localhost:5000").rstrip("/")
    unified_link = f"{public_base}/sub/{uname}/{app_key}/links"

    lines = []
    if notice:
        lines.append(notice)
    lines += [
        f"👤 <b>{uname}</b>",
        f"🔗 Sub: <code>{unified_link}</code>",
        f"📦 Limit: <b>{'Unlimited' if limit_b==0 else fmt_bytes_short(limit_b)}</b>",
        f"📊 Used: <b>{fmt_bytes_short(used_b)}</b>",
        f"🧮 Remaining: <b>{'Unlimited' if limit_b==0 else fmt_bytes_short(max(0, limit_b-used_b))}</b>",
        f"⏳ Expires: <b>{(exp.strftime('%Y-%m-%d %H:%M:%S UTC') if exp else '—')}</b>",
        f"🚫 Disabled pushed: <b>{'Yes' if pushed else 'No'}</b>",
        "",
        "Choose an action:",
    ]
    kb = [
        [InlineKeyboardButton("✏️ Edit Limit", callback_data="act_edit_limit")],
        [InlineKeyboardButton("🧹 Reset Used", callback_data="act_reset_used")],
        [InlineKeyboardButton("🔁 Renew (add days)", callback_data="act_renew")],
        [InlineKeyboardButton("🧩 Panels", callback_data="act_user_panels")],
        [InlineKeyboardButton("⬅️ Back", callback_data="list_users:0")],
    ]
    await q.edit_message_text("\n".join(lines), reply_markup=InlineKeyboardMarkup(kb), parse_mode="HTML")
    return ConversationHandler.END

async def show_agent_card(q, context: ContextTypes.DEFAULT_TYPE, agent_tg_id: int, notice: str = None):
    a = get_agent(agent_tg_id)
    if not a:
        await q.edit_message_text("نماینده پیدا نشد.")
        return ConversationHandler.END
    context.user_data["agent_tg_id"] = agent_tg_id

    limit_b = int(a.get("plan_limit_bytes") or 0)
    exp = a.get("expire_at")
    active = bool(a.get("active", 1))
    max_users = int(a.get("user_limit") or 0)
    max_user_b = int(a.get("max_user_bytes") or 0)
    user_cnt = count_local_users(agent_tg_id)
    lines = []
    if notice: lines.append(notice)
    lines += [
        f"👤 <b>{a['name']}</b> (TG: <code>{a['telegram_user_id']}</code>)",
        f"📦 Agent Quota: <b>{'Unlimited' if limit_b==0 else fmt_bytes_short(limit_b)}</b>",
        f"👥 Users: <b>{user_cnt}</b> / <b>{'Unlimited' if max_users==0 else max_users}</b>",
        f"📛 Max/User: <b>{'Unlimited' if max_user_b==0 else fmt_bytes_short(max_user_b)}</b>",
        f"⏳ Agent Expire: <b>{(exp.strftime('%Y-%m-%d %H:%M:%S UTC') if exp else '—')}</b>",
        f"✅ Active: <b>{'Yes' if active else 'No'}</b>",
        "",
        "Choose:",
    ]
    kb = [
        [InlineKeyboardButton("✏️ Set Quota", callback_data="agent_set_quota")],
        [InlineKeyboardButton("👥 Set User Limit", callback_data="agent_set_user_limit")],
        [InlineKeyboardButton("📛 Set Max/User", callback_data="agent_set_max_user")],
        [InlineKeyboardButton("🔁 Renew (days)", callback_data="agent_renew_days")],
        [InlineKeyboardButton("🧩 Assign Panels", callback_data="agent_assign_panels")],
        [InlineKeyboardButton("🔘 Toggle Active", callback_data="agent_toggle_active")],
        [InlineKeyboardButton("⬅️ Back", callback_data="manage_agents")],
    ]
    await q.edit_message_text("\n".join(lines), reply_markup=InlineKeyboardMarkup(kb), parse_mode="HTML")
    return ConversationHandler.END

async def show_assign_panels(q, context: ContextTypes.DEFAULT_TYPE, agent_tg_id: int):
    panels = list_my_panels_admin(q.from_user.id)
    selected = set(list_agent_panel_ids(agent_tg_id))
    context.user_data["agent_tg_id"] = agent_tg_id
    context.user_data["ap_selected"] = selected
    kb = _panel_select_kb(panels, selected, mode="assign")
    await q.edit_message_text("پنل‌های این نماینده:", reply_markup=kb)
    return ConversationHandler.END

# ---------- add/edit panels (admin only) ----------
async def got_panel_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("دسترسی ندارید.")
        return ConversationHandler.END
    name = (update.message.text or "").strip()
    if not name:
        await update.message.reply_text("❌ اسم معتبر بفرست:")
        return ASK_PANEL_NAME
    context.user_data["panel_name"] = name
    await update.message.reply_text("نوع پنل را مشخص کن (marzneshin/marzban/sanaei):")
    return ASK_PANEL_TYPE

async def got_panel_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    t = (update.message.text or "").strip().lower()
    if t not in ("marzneshin", "marzban", "sanaei"):
        await update.message.reply_text("❌ نوع پنل نامعتبر. یکی از marzneshin/marzban/sanaei بفرست:")
        return ASK_PANEL_TYPE
    context.user_data["panel_type"] = t
    await update.message.reply_text("🌐 URL پنل (مثال https://panel.example.com):")
    return ASK_PANEL_URL

async def got_panel_url(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    url = (update.message.text or "").strip().rstrip("/")
    if not (url.startswith("http://") or url.startswith("https://")):
        await update.message.reply_text("❌ URL نامعتبر. دوباره بفرست:")
        return ASK_PANEL_URL
    context.user_data["panel_url"] = url
    await update.message.reply_text("👤 یوزرنیم ادمین:")
    return ASK_PANEL_USER

async def got_panel_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    u = (update.message.text or "").strip()
    if not u:
        await update.message.reply_text("❌ خالیه. دوباره بفرست:")
        return ASK_PANEL_USER
    context.user_data["panel_user"] = u
    await update.message.reply_text("🔒 پسورد ادمین:")
    return ASK_PANEL_PASS

async def got_panel_pass(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    panel_url = context.user_data.get("panel_url")
    panel_user = context.user_data.get("panel_user")
    panel_name = context.user_data.get("panel_name") or make_panel_name(panel_url, panel_user)
    panel_type = context.user_data.get("panel_type", "marzneshin")
    password = (update.message.text or "").strip()
    try:
        api = get_api(panel_type)
        tok, err = api.get_admin_token(panel_url, panel_user, password)
        if not tok:
            await update.message.reply_text(f"❌ لاگین ناموفق: {err}")
            return ConversationHandler.END
        with with_mysql_cursor() as cur:
            cur.execute(
                "INSERT INTO panels(telegram_user_id,panel_url,name,panel_type,admin_username,access_token)VALUES(%s,%s,%s,%s,%s,%s)",
                (resolve_owner_id(update.effective_user.id), panel_url, panel_name, panel_type, panel_user, tok)
            )
        msg = f"✅ پنل اضافه شد: {panel_name}"
        if panel_type == "sanaei":
            msg += "\nنکته: از 🛠️ Manage Panels می‌تونی Inbound ID را ست کنی."
        else:
            msg += "\nنکته: از 🛠️ Manage Panels می‌تونی Template و Sub URL را ست کنی."
        await update.message.reply_text(msg)
    except MySQLError as e:
        await update.message.reply_text(f"❌ خطای DB: {e}")
    except Exception as e:
        log.exception("add panel")
        await update.message.reply_text(f"❌ خطا: {e}")
    finally:
        for k in ("panel_name", "panel_url", "panel_user", "panel_type"):
            context.user_data.pop(k, None)
    return ConversationHandler.END

async def got_panel_template(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    pid = context.user_data.get("edit_panel_id")
    if not pid:
        await update.message.reply_text("❌ پنل انتخاب نشده.")
        return ConversationHandler.END
    txt = (update.message.text or "").strip()
    val = None if txt == "-" else txt
    info = get_panel(update.effective_user.id, pid)
    if val and info and info.get("panel_type") == "sanaei":
        parts = [p.strip() for p in val.split(",") if p.strip().isdigit()]
        if not parts:
            await update.message.reply_text("❌ شناسه‌های اینباند نامعتبر است.")
            return ASK_PANEL_TEMPLATE
        val = ",".join(parts)
    try:
        with with_mysql_cursor() as cur:
            cur.execute("UPDATE panels SET template_username=%s WHERE id=%s AND telegram_user_id=%s",
                        (val, pid, resolve_owner_id(update.effective_user.id)))
        class FakeCQ:
            async def edit_message_text(self, *args, **kwargs):
                await update.message.reply_text(*args, **kwargs)
        return await show_panel_card(FakeCQ(), context, update.effective_user.id, pid)
    except Exception as e:
        await update.message.reply_text(f"❌ خطا: {e}")
        return ConversationHandler.END

async def got_edit_panel_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    pid = context.user_data.get("edit_panel_id")
    new = (update.message.text or "").strip()
    if not pid or not new:
        await update.message.reply_text("❌ ورودی نامعتبر.")
        return ConversationHandler.END
    try:
        with with_mysql_cursor() as cur:
            cur.execute("UPDATE panels SET name=%s WHERE id=%s AND telegram_user_id=%s", (new, pid, resolve_owner_id(update.effective_user.id)))
        class FakeCQ:
            async def edit_message_text(self, *args, **kwargs):
                await update.message.reply_text(*args, **kwargs)
        return await show_panel_card(FakeCQ(), context, update.effective_user.id, pid)
    except Exception as e:
        await update.message.reply_text(f"❌ خطا: {e}")
        return ConversationHandler.END

async def got_edit_panel_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    context.user_data["new_admin_user"] = (update.message.text or "").strip()
    if not context.user_data["new_admin_user"]:
        await update.message.reply_text("❌ خالیه. دوباره بفرست:")
        return ASK_EDIT_PANEL_USER
    await update.message.reply_text("پسورد ادمین جدید را بفرست:")
    return ASK_EDIT_PANEL_PASS

async def got_edit_panel_pass(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    pid = context.user_data.get("edit_panel_id")
    new_user = context.user_data.get("new_admin_user")
    new_pass = (update.message.text or "").strip()
    if not pid or not new_user or not new_pass:
        await update.message.reply_text("❌ ورودی نامعتبر.")
        return ConversationHandler.END
    try:
        with with_mysql_cursor() as cur:
            cur.execute(
                "SELECT panel_url, panel_type FROM panels WHERE id=%s AND telegram_user_id=%s",
                (pid, resolve_owner_id(update.effective_user.id)),
            )
            row = cur.fetchone()
        if not row:
            raise RuntimeError("panel not found")
        api = get_api(row.get("panel_type"))
        tok, err = api.get_admin_token(row["panel_url"], new_user, new_pass)
        if not tok:
            raise RuntimeError(f"login failed: {err}")
        with with_mysql_cursor() as cur:
            cur.execute("UPDATE panels SET admin_username=%s, access_token=%s WHERE id=%s AND telegram_user_id=%s",
                        (new_user, tok, pid, resolve_owner_id(update.effective_user.id)))
        context.user_data.pop("new_admin_user", None)
        class FakeCQ:
            async def edit_message_text(self, *args, **kwargs):
                await update.message.reply_text(*args, **kwargs)
        return await show_panel_card(FakeCQ(), context, update.effective_user.id, pid)
    except Exception as e:
        await update.message.reply_text(f"❌ خطا در بروزرسانی دسترسی: {e}")
        return ConversationHandler.END

async def got_panel_sub_url(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    pid = context.user_data.get("edit_panel_id")
    if not pid:
        await update.message.reply_text("❌ پنل انتخاب نشده.")
        return ConversationHandler.END
    txt = (update.message.text or "").strip()
    val = None if txt == "-" else txt
    if val and not (val.startswith("http://") or val.startswith("https://")):
        await update.message.reply_text("❌ لینک نامعتبر. دوباره بفرست (یا '-' برای حذف):")
        return ASK_PANEL_SUB_URL
    try:
        set_panel_sub_url(update.effective_user.id, pid, val)
        class FakeCQ:
            async def edit_message_text(self, *args, **kwargs):
                await update.message.reply_text(*args, **kwargs)
        return await show_panel_card(FakeCQ(), context, update.effective_user.id, pid)
    except Exception as e:
        await update.message.reply_text(f"❌ خطا: {e}")
        return ConversationHandler.END

# ---------- agent mgmt ----------
async def got_agent_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    name = (update.message.text or "").strip()
    if not name:
        await update.message.reply_text("❌ نام معتبر بفرست:")
        return ASK_AGENT_NAME
    context.user_data["new_agent_name"] = name
    await update.message.reply_text("Telegram User ID نماینده را بفرست:")
    return ASK_AGENT_TGID

async def got_agent_tgid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    try:
        aid = int((update.message.text or "").strip())
    except:
        await update.message.reply_text("❌ عدد معتبر بفرست:")
        return ASK_AGENT_TGID
    upsert_agent(aid, context.user_data.get("new_agent_name") or "agent")
    context.user_data.pop("new_agent_name", None)
    await update.message.reply_text("✅ نماینده اضافه شد.")
    class Fake:
        async def edit_message_text(self, *a, **k):
            await update.message.reply_text(*a, **k)
    return await show_agent_card(Fake(), context, aid)

async def got_agent_limit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    a = context.user_data.get("agent_tg_id") or 0
    limit_b = parse_human_size(update.message.text or "0")
    set_agent_quota(a, limit_b)
    class Fake:
        async def edit_message_text(self, *a, **k):
            await update.message.reply_text(*a, **k)
    return await show_agent_card(Fake(), context, a, notice="✅ حجم کل ذخیره شد.")

async def got_agent_renew_days(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    a = context.user_data.get("agent_tg_id") or 0
    try:
        days = int(float((update.message.text or "0").strip()))
        assert days > 0
    except Exception:
        await update.message.reply_text("❌ یک عدد مثبت بفرست (مثلا 30).")
        return ASK_AGENT_RENEW_DAYS
    renew_agent_days(a, days)
    class Fake:
        async def edit_message_text(self, *a, **k):
            await update.message.reply_text(*a, **k)
    return await show_agent_card(Fake(), context, a, notice=f"✅ {days} روز به انقضا اضافه شد.")

async def got_agent_user_limit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    a = context.user_data.get("agent_tg_id") or 0
    try:
        num = int((update.message.text or "0").strip())
        assert num >= 0
    except Exception:
        await update.message.reply_text("❌ یک عدد صحیح بفرست (مثلا 100 یا 0).")
        return ASK_AGENT_MAX_USERS
    set_agent_user_limit(a, num)
    class Fake:
        async def edit_message_text(self, *a, **k):
            await update.message.reply_text(*a, **k)
    return await show_agent_card(Fake(), context, a, notice="✅ محدودیت تعداد ذخیره شد.")

async def got_agent_max_user_gb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    a = context.user_data.get("agent_tg_id") or 0
    limit_b = parse_human_size(update.message.text or "0")
    set_agent_max_user_bytes(a, limit_b)
    class Fake:
        async def edit_message_text(self, *a, **k):
            await update.message.reply_text(*a, **k)
    return await show_agent_card(Fake(), context, a, notice="✅ حداکثر حجم هر یوزر ذخیره شد.")

# ---------- new user flow ----------
async def got_newuser_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["new_username"] = (update.message.text or "").strip()
    if not context.user_data["new_username"]:
        await update.message.reply_text("❌ خالیه. دوباره بفرست:")
        return ASK_NEWUSER_NAME
    uid = update.effective_user.id
    if not is_admin(uid):
        ag = get_agent(uid) or {}
        limit = int(ag.get("user_limit") or 0)
        max_user_bytes = int(ag.get("max_user_bytes") or 0)
        context.user_data["agent_max_user_bytes"] = max_user_bytes
        if limit > 0:
            total = count_local_users(uid)
            exists = get_local_user(uid, context.user_data["new_username"])
            if not exists and total >= limit:
                await update.message.reply_text("❌ به حد مجاز تعداد کاربران رسیده‌اید.")
                return ConversationHandler.END
    else:
        context.user_data["agent_max_user_bytes"] = 0
    await update.message.reply_text("حجم در GB (0=نامحدود):")
    return ASK_LIMIT_GB

async def got_limit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    limit_b = gb_to_bytes(update.message.text or "0")
    max_b = int(context.user_data.get("agent_max_user_bytes") or 0)
    if max_b > 0 and limit_b > max_b:
        await update.message.reply_text(
            f"❌ حداکثر حجم مجاز {fmt_bytes_short(max_b)} است. دوباره بفرست:")
        return ASK_LIMIT_GB
    context.user_data["limit_bytes"] = limit_b
    await update.message.reply_text("مدت استفاده به روز (مثلا 30):")
    return ASK_DURATION

async def got_duration(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        days = int(float((update.message.text or "0").strip()))
        assert days > 0
    except Exception:
        await update.message.reply_text("❌ یک عدد مثبت بفرست (مثلا 30).")
        return ASK_DURATION
    context.user_data["duration_days"] = days
    class FakeMsg:
        async def edit_message_text(self, *args, **kwargs):
            await update.message.reply_text(*args, **kwargs)
    return await show_panel_select(FakeMsg(), context, update.effective_user.id, mode="create")

async def got_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = (update.message.text or "").strip()
    uid = update.effective_user.id
    rows = search_local_users(uid, q)
    if not rows:
        await update.message.reply_text("کاربری یافت نشد.")
        return ConversationHandler.END
    kb = [[InlineKeyboardButton(r["username"], callback_data=f"user_sel:{r['username']}")] for r in rows[:25]]
    kb.append([InlineKeyboardButton("⬅️ Back", callback_data="back_home")])
    await update.message.reply_text("نتایج:", reply_markup=InlineKeyboardMarkup(kb))
    return ConversationHandler.END

async def handle_edit_limit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uname = context.user_data.get("manage_username")
    if not uname:
        await update.message.reply_text("یوزر انتخاب نشده.")
        return ConversationHandler.END
    new_bytes = parse_human_size(update.message.text or "")
    update_limit(update.effective_user.id, uname, new_bytes)
    class FakeCQ:
        async def edit_message_text(self, *args, **kwargs):
            await update.message.reply_text(*args, **kwargs)
    return await show_user_card(FakeCQ(), update.effective_user.id, uname, notice="✅ لیمیت بروزرسانی شد.")

async def handle_renew_days(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uname = context.user_data.get("manage_username")
    if not uname:
        await update.message.reply_text("یوزر انتخاب نشده.")
        return ConversationHandler.END
    try:
        days = int(float((update.message.text or "0").strip()))
        assert days > 0
    except Exception:
        await update.message.reply_text("❌ یک عدد مثبت بفرست (مثلا 30).")
        return ASK_RENEW_DAYS
    renew_user(update.effective_user.id, uname, days)
    class FakeCQ:
        async def edit_message_text(self, *args, **kwargs):
            await update.message.reply_text(*args, **kwargs)
    return await show_user_card(FakeCQ(), update.effective_user.id, uname, notice=f"✅ {days} روز تمدید شد.")

# ---------- cancel ----------
async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("لغو شد.")
    return ConversationHandler.END

# ---------- finalize create / apply edit ----------
async def finalize_create_on_selected(q, context, owner_id: int, selected_ids: set):
    app_username = context.user_data["new_username"]
    limit_bytes = context.user_data["limit_bytes"]
    days = context.user_data["duration_days"]
    usage_sec = days * 86400

    app_key = upsert_app_user(owner_id, app_username)
    upsert_local_user(owner_id, app_username, limit_bytes, days)

    panels = list_panels_for_agent(owner_id) if not is_admin(owner_id) else list_my_panels_admin(owner_id)
    rows = [p for p in panels if int(p["id"]) in selected_ids]
    missing = [
        f"{r['name']}"
        for r in rows
        if (r.get("panel_type") in ("marzneshin", "sanaei")) and not r.get("template_username")
    ]
    if missing:
        await q.edit_message_text(
            "⚠️ این پنل‌ها template/inbound ندارند:\n" + "\n".join(f"• {m}" for m in missing)
        )
        return

    per_panel, errs = {}, []
    for r in rows:
        api = get_api(r.get("panel_type"))
        if r.get("panel_type") == "marzneshin":
            svc, e = api.fetch_user_services(
                r["panel_url"], r["access_token"], r.get("template_username")
            )
            if e:
                errs.append(
                    f"{r['panel_url']} (template '{r['template_username']}'): {e}"
                )
            per_panel[r["id"]] = {"service_ids": svc or []}
        elif r.get("panel_type") == "sanaei":
            ids = [x.strip() for x in (r.get("template_username") or "").split(",") if x.strip().isdigit()]
            per_panel[r["id"]] = {"inbound_ids": ids}
        else:
            tmpl = r.get("template_username")
            if not tmpl:
                errs.append(f"{r['panel_url']}: template missing")
                per_panel[r["id"]] = {"proxies": {}, "inbounds": {}}
                continue
            obj, e = api.get_user(r["panel_url"], r["access_token"], tmpl)
            if not obj:
                errs.append(
                    f"{r['panel_url']} (template '{tmpl}'): {e or 'not found'}"
                )
                per_panel[r["id"]] = {"proxies": {}, "inbounds": {}}
                continue
            per_panel[r["id"]] = {
                "proxies": obj.get("proxies") or {},
                "inbounds": obj.get("inbounds") or {},
            }
    if errs:
        await q.edit_message_text(
            "❌ خطا در خواندن سرویس بعضی پنل‌ها:\n" +
            "\n".join(f"• {e}" for e in errs[:10])
        )
        return

    ok, failed = 0, []
    for r in rows:
        api = get_api(r.get("panel_type"))
        remote_name = app_username
        if r.get("panel_type") == "marzneshin":
            payload = {
                "username": app_username,
                "expire_strategy": "start_on_first_use",
                "usage_duration": usage_sec,
                "data_limit": limit_bytes,
                "data_limit_reset_strategy": "no_reset",
                "note": "created_by_bot",
                "service_ids": per_panel.get(r["id"], {}).get("service_ids", []),
            }
        elif r.get("panel_type") == "sanaei":
            expire_ts = 0 if usage_sec <= 0 else int(datetime.now(timezone.utc).timestamp()) + usage_sec
            inbound_ids = per_panel.get(r["id"], {}).get("inbound_ids", [])
            remote_names = []
            for inb in inbound_ids:
                rn = f"{app_username}_{secrets.token_hex(3)}"
                client = {
                    "id": str(uuid.uuid4()),
                    "email": rn,
                    "enable": True,
                }
                if limit_bytes > 0:
                    client["totalGB"] = limit_bytes
                if expire_ts > 0:
                    client["expiryTime"] = expire_ts * 1000
                payload = {
                    "id": int(inb),
                    "settings": json.dumps({"clients": [client]}, separators=(",", ":")),
                }
                obj, e = api.create_user(r["panel_url"], r["access_token"], payload)
                if not obj:
                    obj, g = api.get_user(r["panel_url"], r["access_token"], rn)
                    if not obj:
                        failed.append(f"{r['panel_url']} (inb {inb}): {e or g or 'unknown error'}")
                        continue
                if not obj.get("enabled", True):
                    ok_en, err_en = api.enable_remote_user(r["panel_url"], r["access_token"], rn)
                    if not ok_en:
                        failed.append(f"{r['panel_url']} (inb {inb}): enable failed - {err_en or 'unknown'}")
                        continue
                remote_names.append(rn)
            if remote_names:
                remote_name = ",".join(remote_names)
                save_link(owner_id, app_username, r["id"], remote_name)
                ok += 1
            continue
        else:
            expire_ts = 0 if usage_sec <= 0 else int(datetime.now(timezone.utc).timestamp()) + usage_sec
            tmpl_info = per_panel.get(r["id"], {})
            payload = {
                "username": app_username,
                "expire": expire_ts,
                "data_limit": limit_bytes,
                "data_limit_reset_strategy": "no_reset",
                "note": "created_by_bot",
                "proxies": tmpl_info.get("proxies", {}),
                "inbounds": tmpl_info.get("inbounds", {}),
            }
        obj, e = api.create_user(r["panel_url"], r["access_token"], payload)
        if not obj:
            obj, g = api.get_user(r["panel_url"], r["access_token"], remote_name)
            if not obj:
                failed.append(f"{r['panel_url']}: {e or g or 'unknown error'}")
                continue
        if not obj.get("enabled", True):
            ok_en, err_en = api.enable_remote_user(r["panel_url"], r["access_token"], remote_name)
            if not ok_en:
                failed.append(f"{r['panel_url']}: enable failed - {err_en or 'unknown'}")
        save_link(owner_id, app_username, r["id"], remote_name)
        ok += 1

    base = os.getenv("PUBLIC_BASE_URL", "http://localhost:5000").rstrip("/")
    link = f"{base}/sub/{app_username}/{app_key}/links"
    txt = f"✅ یوزر '{app_username}' روی {ok}/{len(rows)} پنل انتخابی ساخته/فعال شد.\n🔗 {link}"
    if failed:
        txt += "\n⚠️ خطاها:\n" + "\n".join(f"• {e}" for e in failed[:8])
    await q.edit_message_text(txt)

async def apply_edit_user_panels(q, owner_id: int, username: str, selected_ids: set):
    links_map = map_linked_remote_usernames(owner_id, username)
    current = set(links_map.keys())
    to_add = selected_ids - current
    to_remove = current - selected_ids

    added_errs = []
    removed = 0
    added_ok = 0
    enabled_ok = 0

    panels = list_panels_for_agent(owner_id) if not is_admin(owner_id) else list_my_panels_admin(owner_id)
    panels_map = {int(p["id"]): p for p in panels}

    lu = get_local_user(owner_id, username)
    if lu:
        limit_bytes_default = int(lu["plan_limit_bytes"] or 0)
        exp = lu["expire_at"]
        usage_duration_default = max(86400, int((exp - datetime.utcnow()).total_seconds())) if exp else 3650*86400
    else:
        limit_bytes_default = 0
        usage_duration_default = 3650*86400

    if to_add:
        expire_ts_default = (
            0 if usage_duration_default <= 0 else int(datetime.now(timezone.utc).timestamp()) + usage_duration_default
        )
        for pid in to_add:
            p = panels_map.get(int(pid))
            if not p:
                continue
            api = get_api(p.get("panel_type"))
            tmpl = p.get("template_username")
            if p.get("panel_type") == "marzneshin":
                if not tmpl:
                    obj, g = api.get_user(p["panel_url"], p["access_token"], username)
                    if obj:
                        if not obj.get("enabled", True):
                            ok_en, err_en = api.enable_remote_user(p["panel_url"], p["access_token"], username)
                            if not ok_en:
                                added_errs.append(f"{p['panel_url']}: enable failed - {err_en or 'unknown'}")
                        save_link(owner_id, username, int(pid), username)
                        links_map[int(pid)] = username
                        added_ok += 1
                    else:
                        added_errs.append(f"{p['panel_url']}: no template & user not found")
                    continue

                svc, e = api.fetch_user_services(p["panel_url"], p["access_token"], tmpl)
                if e:
                    obj, g = api.get_user(p["panel_url"], p["access_token"], username)
                    if obj:
                        if not obj.get("enabled", True):
                            ok_en, err_en = api.enable_remote_user(p["panel_url"], p["access_token"], username)
                            if not ok_en:
                                added_errs.append(f"{p['panel_url']}: enable failed - {err_en or 'unknown'}")
                        save_link(owner_id, username, int(pid), username)
                        links_map[int(pid)] = username
                        added_ok += 1
                    else:
                        added_errs.append(f"{p['panel_url']}: {e}")
                    continue

                payload = {
                    "username": username,
                    "expire_strategy": "start_on_first_use",
                    "usage_duration": usage_duration_default,
                    "data_limit": limit_bytes_default,
                    "data_limit_reset_strategy": "no_reset",
                    "note": "user_edit_add_panel",
                    "service_ids": svc or [],
                }
                obj, e2 = api.create_user(p["panel_url"], p["access_token"], payload)
                if not obj:
                    obj, g = api.get_user(p["panel_url"], p["access_token"], username)
                    if not obj:
                        added_errs.append(f"{p['panel_url']}: {e2 or g or 'unknown error'}")
                        continue

                if not obj.get("enabled", True):
                    ok_en, err_en = api.enable_remote_user(p["panel_url"], p["access_token"], username)
                    if not ok_en:
                        added_errs.append(f"{p['panel_url']}: enable failed - {err_en or 'unknown'}")

                save_link(owner_id, username, int(pid), username)
                links_map[int(pid)] = username
                added_ok += 1
            elif p.get("panel_type") == "sanaei":
                if not tmpl:
                    added_errs.append(f"{p['panel_url']}: inbound missing")
                    continue
                inb_ids = [x.strip() for x in tmpl.split(",") if x.strip().isdigit()]
                if not inb_ids:
                    added_errs.append(f"{p['panel_url']}: inbound missing")
                    continue
                remote_names = []
                for inb in inb_ids:
                    remote_name = f"{username}_{secrets.token_hex(3)}"
                    client = {
                        "id": str(uuid.uuid4()),
                        "email": remote_name,
                        "enable": True,
                    }
                    if limit_bytes_default > 0:
                        client["totalGB"] = limit_bytes_default
                    if expire_ts_default > 0:
                        client["expiryTime"] = expire_ts_default * 1000
                    payload = {
                        "id": int(inb),
                        "settings": json.dumps({"clients": [client]}, separators=(",", ":")),
                    }
                    obj, e2 = api.create_user(p["panel_url"], p["access_token"], payload)
                    if not obj:
                        added_errs.append(f"{p['panel_url']} (inb {inb}): {e2 or 'unknown error'}")
                        continue
                    if not obj.get("enabled", True):
                        ok_en, err_en = api.enable_remote_user(p["panel_url"], p["access_token"], remote_name)
                        if not ok_en:
                            added_errs.append(f"{p['panel_url']} (inb {inb}): enable failed - {err_en or 'unknown'}")
                            continue
                    remote_names.append(remote_name)
                if remote_names:
                    joined = ",".join(remote_names)
                    save_link(owner_id, username, int(pid), joined)
                    links_map[int(pid)] = joined
                    added_ok += 1
                continue
            else:
                obj, g = api.get_user(p["panel_url"], p["access_token"], username)
                if not obj:
                    if tmpl:
                        tmpl_obj, t_err = api.get_user(
                            p["panel_url"], p["access_token"], tmpl
                        )
                        if not tmpl_obj:
                            added_errs.append(
                                f"{p['panel_url']} (template '{tmpl}'): {t_err or 'not found'}"
                            )
                            continue
                        payload = {
                            "username": username,
                            "expire": expire_ts_default,
                            "data_limit": limit_bytes_default,
                            "data_limit_reset_strategy": "no_reset",
                            "note": "user_edit_add_panel",
                            "proxies": tmpl_obj.get("proxies") or {},
                            "inbounds": tmpl_obj.get("inbounds") or {},
                        }
                        obj, e2 = api.create_user(
                            p["panel_url"], p["access_token"], payload
                        )
                        if not obj:
                            added_errs.append(
                                f"{p['panel_url']}: {e2 or 'unknown error'}"
                            )
                            continue
                    else:
                        added_errs.append(
                            f"{p['panel_url']}: no template & user not found"
                        )
                        continue
                if not obj.get("enabled", True):
                    ok_en, err_en = api.enable_remote_user(
                        p["panel_url"], p["access_token"], username
                    )
                    if not ok_en:
                        added_errs.append(
                            f"{p['panel_url']}: enable failed - {err_en or 'unknown'}"
                        )
                save_link(owner_id, username, int(pid), username)
                links_map[int(pid)] = username
                added_ok += 1

    if to_remove:
        for pid in to_remove:
            p = panels_map.get(int(pid))
            remote = links_map.get(int(pid), username)
            remove_link(owner_id, username, int(pid))
            links_map.pop(int(pid), None)
            removed += 1
            if p:
                api = get_api(p.get("panel_type"))
                remotes = remote.split(",") if p.get("panel_type") == "sanaei" else [remote]
                for rn in remotes:
                    ok, err = api.disable_remote_user(p["panel_url"], p["access_token"], rn)
                    if not ok:
                        added_errs.append(f"disable on {p['panel_url']}: {err or 'unknown error'}")

    for pid in selected_ids:
        p = panels_map.get(int(pid))
        if not p:
            continue
        api = get_api(p.get("panel_type"))
        remote = links_map.get(int(pid), username)
        remotes = remote.split(",") if p.get("panel_type") == "sanaei" else [remote]
        for rn in remotes:
            obj, g = api.get_user(p["panel_url"], p["access_token"], rn)
            if obj and not obj.get("enabled", True):
                ok_en, err_en = api.enable_remote_user(p["panel_url"], p["access_token"], rn)
                if ok_en:
                    enabled_ok += 1
                else:
                    added_errs.append(f"{p['panel_url']}: enable failed - {err_en or 'unknown'}")
        if int(pid) not in links_map:
            save_link(owner_id, username, int(pid), remote)
            links_map[int(pid)] = remote

    note = f"✅ اعمال شد. اضافه/ایجاد: {added_ok} | حذف مپ/دیسیبل: {removed} | فعال‌شده‌ها: {enabled_ok}"
    if added_errs:
        note += "\n⚠️ خطاها:\n" + "\n".join(f"• {e}" for e in added_errs[:10])
    await show_user_card(q, owner_id, username, notice=note)

# ---------- wiring ----------
def build_app():
    load_dotenv()
    tok = os.getenv("BOT_TOKEN", "").strip()
    if not tok:
        raise RuntimeError("BOT_TOKEN missing in .env")
    init_mysql_pool()
    ensure_schema()
    app = Application.builder().token(tok).build()

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start), CallbackQueryHandler(on_button)],
        states={
            # add panel (admin)
            ASK_PANEL_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, got_panel_name)],
            ASK_PANEL_TYPE: [MessageHandler(filters.TEXT & ~filters.COMMAND, got_panel_type)],
            ASK_PANEL_URL:  [MessageHandler(filters.TEXT & ~filters.COMMAND, got_panel_url)],
            ASK_PANEL_USER: [MessageHandler(filters.TEXT & ~filters.COMMAND, got_panel_user)],
            ASK_PANEL_PASS: [MessageHandler(filters.TEXT & ~filters.COMMAND, got_panel_pass)],

            # panel edits (admin)
            ASK_PANEL_TEMPLATE:  [MessageHandler(filters.TEXT & ~filters.COMMAND, got_panel_template)],
            ASK_EDIT_PANEL_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, got_edit_panel_name)],
            ASK_EDIT_PANEL_USER: [MessageHandler(filters.TEXT & ~filters.COMMAND, got_edit_panel_user)],
            ASK_EDIT_PANEL_PASS: [MessageHandler(filters.TEXT & ~filters.COMMAND, got_edit_panel_pass)],
            ASK_PANEL_SUB_URL:  [MessageHandler(filters.TEXT & ~filters.COMMAND, got_panel_sub_url)],
            ASK_PANEL_REMOVE_CONFIRM: [CallbackQueryHandler(on_button)],

            # agent mgmt (admin)
            ASK_AGENT_NAME:        [MessageHandler(filters.TEXT & ~filters.COMMAND, got_agent_name)],
            ASK_AGENT_TGID:        [MessageHandler(filters.TEXT & ~filters.COMMAND, got_agent_tgid)],
            ASK_AGENT_LIMIT:       [MessageHandler(filters.TEXT & ~filters.COMMAND, got_agent_limit)],
            ASK_AGENT_RENEW_DAYS:  [MessageHandler(filters.TEXT & ~filters.COMMAND, got_agent_renew_days)],
            ASK_AGENT_MAX_USERS:   [MessageHandler(filters.TEXT & ~filters.COMMAND, got_agent_user_limit)],
            ASK_AGENT_MAX_USER_GB: [MessageHandler(filters.TEXT & ~filters.COMMAND, got_agent_max_user_gb)],

            # user creation
            ASK_NEWUSER_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, got_newuser_name)],
            ASK_LIMIT_GB:     [MessageHandler(filters.TEXT & ~filters.COMMAND, got_limit)],
            ASK_DURATION:     [MessageHandler(filters.TEXT & ~filters.COMMAND, got_duration)],

            # panel multi-select (create/edit)
            ASK_SELECT_PANELS: [CallbackQueryHandler(on_button)],

            # search/manage
            ASK_SEARCH_USER:  [MessageHandler(filters.TEXT & ~filters.COMMAND, got_search)],
            ASK_EDIT_LIMIT:   [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_edit_limit)],
            ASK_RENEW_DAYS:   [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_renew_days)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        name="bot_flow",
        allow_reentry=True,
    )
    app.add_handler(conv)
    return app

if __name__ == "__main__":
    build_app().run_polling(drop_pending_updates=True)
