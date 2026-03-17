from __future__ import annotations

import hashlib
import json
from typing import Any


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def plan_hash(plan_payload: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(plan_payload).encode("utf-8")).hexdigest()


def build_tool_call_id(
    *,
    tenant_id: str,
    run_id: str,
    step_key: str,
    tool_id: str,
    call_seq: int,
    plan_payload: dict[str, Any],
) -> str:
    digest = hashlib.sha256(
        f"{tenant_id}:{run_id}:{step_key}:{tool_id}:{call_seq}:{plan_hash(plan_payload)}".encode("utf-8")
    ).hexdigest()
    return digest[:32]
