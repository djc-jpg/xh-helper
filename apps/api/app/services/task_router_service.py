from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any

ALLOWED_WEB_SEARCH_DOMAINS = {
    "example.com",
    "docs.python.org",
    "developer.mozilla.org",
}
DIRECT_HINTS = {"hello", "hi", "hey", "help", "who are you", "what can you do"}
TOOL_HINTS = {"search", "find", "lookup", "look up", "records", "internal api"}
WORKFLOW_HINTS = {"ticket", "email", "workflow", "report", "research", "summarize", "summary", "analyze"}
DOMAIN_PATTERN = re.compile(r"\b([a-z0-9][a-z0-9.-]+\.[a-z]{2,})\b")


@dataclass(frozen=True)
class TaskRouteDecision:
    route: str
    reason: str
    task_type: str | None = None
    tool_id: str | None = None
    tool_payload: dict[str, Any] | None = None


class TaskRouterService:
    def route(
        self,
        *,
        message: str,
        mode: str | None = None,
        metadata: dict[str, Any] | None = None,
        history: list[dict[str, Any]] | None = None,
    ) -> TaskRouteDecision:
        del history  # keep the interface ready for future history-aware routing
        normalized = message.strip().lower()
        forced = (mode or "auto").strip().lower()
        if forced == "direct_answer":
            return TaskRouteDecision(route="direct_answer", reason="forced_by_mode")
        if forced == "tool_task":
            return self._tool_task_decision(normalized, metadata or {}, reason="forced_by_mode")
        if forced == "workflow_task":
            return TaskRouteDecision(
                route="workflow_task",
                reason="forced_by_mode",
                task_type=self._workflow_task_type(normalized),
            )

        if self._is_direct_answer(normalized):
            return TaskRouteDecision(route="direct_answer", reason="short_qna_rule")
        if self._is_tool_task(normalized):
            return self._tool_task_decision(normalized, metadata or {}, reason="tool_rule")
        return TaskRouteDecision(
            route="workflow_task",
            reason="default_workflow_rule",
            task_type=self._workflow_task_type(normalized),
        )

    def _is_direct_answer(self, normalized: str) -> bool:
        if len(normalized) <= 40 and "?" in normalized:
            return True
        if normalized in DIRECT_HINTS:
            return True
        for hint in DIRECT_HINTS:
            if " " in hint and hint in normalized:
                return True
            if " " not in hint and re.search(rf"\b{re.escape(hint)}\b", normalized):
                return True
        return False

    def _is_tool_task(self, normalized: str) -> bool:
        if normalized.startswith("/search"):
            return True
        return any(hint in normalized for hint in TOOL_HINTS)

    def _tool_task_decision(self, normalized: str, metadata: dict[str, Any], *, reason: str) -> TaskRouteDecision:
        if "record" in normalized or "internal api" in normalized:
            query = self._extract_query(normalized)
            return TaskRouteDecision(
                route="tool_task",
                reason=reason,
                tool_id="internal_rest_api",
                tool_payload={"method": "GET", "path": "/records", "params": {"q": query}},
            )

        domain = self._extract_domain(normalized) or str(metadata.get("domain") or "example.com").lower().strip()
        if domain not in ALLOWED_WEB_SEARCH_DOMAINS:
            domain = "example.com"
        return TaskRouteDecision(
            route="tool_task",
            reason=reason,
            tool_id="web_search",
            tool_payload={
                "query": self._extract_query(normalized),
                "domain": domain,
                "top_k": int(metadata.get("top_k") or 3),
            },
        )

    def _workflow_task_type(self, normalized: str) -> str:
        if "email" in normalized or "ticket" in normalized:
            return "ticket_email"
        if "research" in normalized or "summary" in normalized or "summarize" in normalized or "report" in normalized:
            return "research_summary"
        if "tool flow" in normalized or "workflow" in normalized:
            return "tool_flow"
        return "rag_qa"

    def _extract_query(self, normalized: str) -> str:
        cleaned = normalized.replace("/search", " ").replace("search", " ").replace("lookup", " ").replace("find", " ")
        collapsed = " ".join(cleaned.split())
        return collapsed[:200] or "general query"

    def _extract_domain(self, normalized: str) -> str | None:
        match = DOMAIN_PATTERN.search(normalized)
        return str(match.group(1)).lower() if match else None
