"""Service management utilities.

This module manages a higher level grouping called *service* which bundles
multiple panels together. Agents receive access to services instead of direct
panel access. Functions here operate purely on the database layer; the Telegram
bot wires them to user interactions.
"""

from __future__ import annotations

from typing import List, Callable
from mysql.connector import Error

MYSQL_POOL = None
_disable_remote: Callable | None = None
_list_mapped_links: Callable | None = None


def set_pool(pool):
    """Assign MySQL connection pool used by the bot."""
    global MYSQL_POOL
    MYSQL_POOL = pool


def set_helpers(disable_remote_fn: Callable, list_links_fn: Callable) -> None:
    """Inject helper callbacks from the bot module."""
    global _disable_remote, _list_mapped_links
    _disable_remote = disable_remote_fn
    _list_mapped_links = list_links_fn


def _ctx():
    class _Ctx:
        def __enter__(self):
            self.conn = MYSQL_POOL.get_connection()
            self.cur = self.conn.cursor(dictionary=True)
            return self.cur
        def __exit__(self, exc_type, exc, tb):
            if exc_type is None:
                self.conn.commit()
            else:
                self.conn.rollback()
            self.cur.close()
            self.conn.close()
    return _Ctx()


def create_service(name: str, owner_id: int) -> int:
    """Create a service and return its id."""
    with _ctx() as cur:
        cur.execute(
            "INSERT INTO services (name, owner_id) VALUES (%s, %s)",
            (name, owner_id),
        )
        return int(cur.lastrowid)


def add_panel_to_service(service_id: int, panel_id: int) -> None:
    with _ctx() as cur:
        cur.execute(
            "INSERT IGNORE INTO service_panels (service_id, panel_id) VALUES (%s, %s)",
            (service_id, panel_id),
        )
    propagate_service_users_to_panel(service_id, panel_id)


def remove_panel_from_service(service_id: int, panel_id: int) -> None:
    with _ctx() as cur:
        cur.execute(
            "DELETE FROM service_panels WHERE service_id=%s AND panel_id=%s",
            (service_id, panel_id),
        )
    disable_users_on_panel(service_id, panel_id)


def service_usernames(service_id: int) -> List[str]:
    with _ctx() as cur:
        cur.execute(
            "SELECT local_username FROM service_users WHERE service_id=%s",
            (service_id,),
        )
        return [r["local_username"] for r in cur.fetchall()]


def assign_user_to_service(service_id: int, local_username: str) -> None:
    with _ctx() as cur:
        cur.execute(
            "INSERT IGNORE INTO service_users (service_id, local_username) VALUES (%s, %s)",
            (service_id, local_username),
        )


def _owner_id(service_id: int) -> int | None:
    with _ctx() as cur:
        cur.execute("SELECT owner_id FROM services WHERE id=%s LIMIT 1", (service_id,))
        row = cur.fetchone()
    return int(row["owner_id"]) if row else None


def propagate_service_users_to_panel(service_id: int, panel_id: int) -> None:
    owner_id = _owner_id(service_id)
    if owner_id is None or _list_mapped_links is None:
        return
    for username in service_usernames(service_id):
        links = _list_mapped_links(owner_id, username)
        if any(l["panel_id"] == panel_id for l in links):
            continue
        with _ctx() as cur:
            cur.execute(
                "INSERT INTO local_user_panel_links (owner_id, panel_id, local_username, remote_username)"
                " VALUES (%s, %s, %s, %s)",
                (owner_id, panel_id, username, username),
            )


def disable_users_on_panel(service_id: int, panel_id: int) -> None:
    owner_id = _owner_id(service_id)
    if owner_id is None or _disable_remote is None or _list_mapped_links is None:
        return
    for username in service_usernames(service_id):
        links = [l for l in _list_mapped_links(owner_id, username) if l["panel_id"] == panel_id]
        for l in links:
            try:
                _disable_remote(l["panel_type"], l["panel_url"], l["access_token"], l["remote_username"])
            except Error:
                pass
            with _ctx() as cur:
                cur.execute(
                    "DELETE FROM local_user_panel_links WHERE owner_id=%s AND panel_id=%s AND local_username=%s",
                    (owner_id, panel_id, username),
                )
