from __future__ import annotations

import re
from typing import Any

from ..repositories import ToolRepository

TOKEN_PATTERN = re.compile(r"[a-z0-9_]{2,}")
CHINESE_TOOL_HINTS: tuple[tuple[tuple[str, ...], tuple[str, ...], float], ...] = (
    (
        ("\u5de5\u5355", "\u53d1\u5de5\u5355", "\u503c\u73ed", "\u90ae\u4ef6", "\u53d1\u90ae\u4ef6"),
        ("email_ticketing", "ticket", "email"),
        4.0,
    ),
    (
        ("\u641c\u7d22", "\u67e5\u627e", "\u67e5\u8be2", "\u6587\u6863", "\u8d44\u6599"),
        ("web_search", "search"),
        3.0,
    ),
)


def _tokenize(text: str) -> list[str]:
    return TOKEN_PATTERN.findall(text.lower())


def _contains_cjk(text: str) -> bool:
    return any("\u4e00" <= char <= "\u9fff" for char in text)


def _chinese_hint_score(message: str, tool: dict[str, Any]) -> float:
    if not _contains_cjk(message):
        return 0.0
    lowered_message = message.lower()
    tool_bag = " ".join(
        [
            str(tool.get("tool_name") or ""),
            str(tool.get("description") or ""),
            " ".join(str(x) for x in list(tool.get("supported_use_cases") or [])),
        ]
    ).lower()
    score = 0.0
    for message_markers, tool_markers, bonus in CHINESE_TOOL_HINTS:
        if any(marker in lowered_message for marker in message_markers) and any(marker in tool_bag for marker in tool_markers):
            score += bonus
    return score


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
            score = _tool_score(message_tokens, tool)
            score += _chinese_hint_score(message, tool)
            scored.append((score, tool))
        scored.sort(key=lambda item: item[0], reverse=True)
        candidates = [tool for score, tool in scored if score > 0]
        return candidates[: max(1, int(limit))]
