#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Helpers for managing service-to-panel mappings."""

import logging
from typing import Tuple

from usage_sync import CurCtx, get_api, init_db, POOL

log = logging.getLogger("service_manager")

if POOL is None:
    init_db()


def switch_service_panel(service_id: int, new_panel_id: int) -> Tuple[bool, str]:
    """Move a service to a new panel and migrate linked users.

    Returns ``(ok, message)`` where ``ok`` indicates success.
    """
    with CurCtx() as cur:
        cur.execute(
            """
            SELECT s.panel_id, p.panel_url, p.access_token, p.panel_type
            FROM services s
            JOIN panels p ON p.id = s.panel_id
            WHERE s.id=%s
            """,
            (service_id,),
        )
        old = cur.fetchone()
        if not old:
            return False, "service not found"

        cur.execute(
            "SELECT id, panel_url, access_token, panel_type FROM panels WHERE id=%s",
            (new_panel_id,),
        )
        newp = cur.fetchone()
        if not newp:
            return False, "panel not found"

        # fetch remote usernames linked to this service
        cur.execute(
            "SELECT remote_username FROM local_user_panel_links WHERE service_id=%s",
            (service_id,),
        )
        usernames = [r["remote_username"] for r in cur.fetchall()]

        api_old = get_api(old["panel_type"])
        api_new = get_api(newp["panel_type"])
        for rn in usernames:
            try:
                ok, _ = api_old.remove_remote_user(
                    old["panel_url"], old["access_token"], rn
                )
                if not ok:
                    api_old.disable_remote_user(
                        old["panel_url"], old["access_token"], rn
                    )
                api_new.enable_remote_user(
                    newp["panel_url"], newp["access_token"], rn
                )
            except Exception as e:  # pragma: no cover - network errors
                log.warning("migrate %s: %s", rn, e)

        # update mappings
        cur.execute(
            "UPDATE services SET panel_id=%s WHERE id=%s",
            (new_panel_id, service_id),
        )
        cur.execute(
            "UPDATE local_user_panel_links SET panel_id=%s WHERE service_id=%s",
            (new_panel_id, service_id),
        )
        cur.execute(
            "UPDATE agent_panels SET panel_id=%s WHERE service_id=%s",
            (new_panel_id, service_id),
        )
    return True, ""

