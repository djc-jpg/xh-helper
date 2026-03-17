from __future__ import annotations

import re
from typing import Any

from ..repositories import ToolRepository

TOKEN_PATTERN = re.compile(r"[a-z0-9_]{2,}")


def _tokenize(text: str) -> list[str]:
    return TOKEN_PATTERN.findall(text.lower())


def _tool_score(message_tokens: list[str], tool: dict[str, Any]) -> float:
    score = 0.0
    bag = " ".join(
        [
            str(tool.get("tool_name") or ""),
            str(tool.get("description") or ""),
            " ".join(str(x) for x in list(tool.get("supported_use_cases") or [])),
        ]
    ).lower()
    for token in message_tokens:
        if token in bag:
            score += 1.0
    if str(tool.get("risk_level") or "low") == "low":
        score += 0.1
    return score


class ToolRegistryService:
    def __init__(self, repo: ToolRepository) -> None:
        self._repo = repo

    def list_tools(
        self,
        *,
        tenant_id: str,
        enabled_only: bool = True,
        use_case: str | None = None,
    ) -> list[dict[str, Any]]:
        return self._repo.list_assistant_registry(
            tenant_id=tenant_id,
            enabled_only=enabled_only,
            use_case=use_case,
        )

    def upsert_tool(
        self,
        *,
        tenant_id: str,
        actor_user_id: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        self._repo.upsert_assistant_registry(
            tenant_id=tenant_id,
            actor_user_id=actor_user_id,
            tool_name=str(payload["tool_name"]),
            version=str(payload.get("version") or "v1"),
            description=str(payload["description"]),
            input_schema=dict(payload.get("input_schema") or {}),
            risk_level=str(payload.get("risk_level") or "low"),
            requires_approval=bool(payload.get("requires_approval")),
            supported_use_cases=list(payload.get("supported_use_cases") or []),
            enabled=bool(payload.get("enabled", True)),
        )
        row = self._repo.get_assistant_registry_item(
            tenant_id=tenant_id,
            tool_name=str(payload["tool_name"]),
            version=str(payload.get("version") or "v1"),
        )
        if not row:
            raise RuntimeError("tool profile upsert failed")
        return row

    def select_candidates(
        self,
        *,
        message: str,
        tools: list[dict[str, Any]],
        limit: int = 3,
    ) -> list[dict[str, Any]]:
        message_tokens = _tokenize(message)
        scored: list[tuple[float, dict[str, Any]]] = []
        for tool in tools:
            scored.append((_tool_score(message_tokens, tool), tool))
        scored.sort(key=lambda item: item[0], reverse=True)
        candidates = [tool for score, tool in scored if score > 0]
        return candidates[: max(1, int(limit))]
