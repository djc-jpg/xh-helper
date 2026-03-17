from __future__ import annotations

from typing import Any

from .config import settings
from .db import fetchall, fetchone

ROLE_LEVEL = {"user": 1, "operator": 2, "owner": 3}


def has_min_role(role: str, min_role: str) -> bool:
    return ROLE_LEVEL.get(role, 0) >= ROLE_LEVEL.get(min_role, 0)


def is_tool_write_action(tool_id: str, payload: dict[str, Any]) -> bool:
    if tool_id == "internal_rest_api":
        return str(payload.get("method", "GET")).upper() != "GET"
    if tool_id in {"email_ticketing", "object_storage"}:
        return True
    return False


def check_tool_policy(
    *,
    user: dict[str, Any],
    task_type: str,
    tool_id: str,
    is_write_action: bool,
    approval_id: str | None,
    task_id: str,
    run_id: str,
    environment: str | None = None,
) -> tuple[bool, str]:
    env = environment or settings.environment
    role = user["role"]

    if is_write_action and not has_min_role(role, "operator"):
        return False, "write_requires_operator"

    policies = fetchall(
        """
        SELECT effect, role_min, task_type, tool_id, environment, is_write_action, requires_approval
        FROM policies
        WHERE tenant_id = %s
          AND environment = %s
          AND (task_type IS NULL OR task_type = %s)
          AND (tool_id IS NULL OR tool_id = %s)
        """,
        (user["tenant_id"], env, task_type, tool_id),
    )

    # Deny policies first.
    for pol in policies:
        if pol["effect"] != "deny":
            continue
        # deny policies are exact-role to avoid over-blocking higher roles
        if role != pol["role_min"]:
            continue
        if not pol["is_write_action"] or is_write_action:
            return False, "policy_deny"

    requires_approval = bool(is_write_action)
    allowed = False
    for pol in policies:
        if pol["effect"] != "allow":
            continue
        if not has_min_role(role, pol["role_min"]):
            continue
        if pol["is_write_action"] and not is_write_action:
            continue
        if pol["requires_approval"]:
            requires_approval = True
            if not approval_id:
                continue
        allowed = True

    if requires_approval:
        if not approval_id:
            return False, "write_requires_approval"
        if not task_id or not run_id:
            return False, "approval_context_invalid"
        approval = fetchone(
            """
            SELECT id, status
            FROM approvals
            WHERE tenant_id = %s
              AND id = %s
              AND task_id = %s
              AND run_id = %s
            """,
            (user["tenant_id"], approval_id, task_id, run_id),
        )
        if not approval:
            return False, "approval_invalid"
        if str(approval["status"]) not in {"APPROVED", "EDITED"}:
            return False, "approval_not_approved"

    if allowed or role == "owner":
        return True, "ok"

    return False, "policy_default_deny"
