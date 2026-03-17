from __future__ import annotations

from typing import Any

from .langgraph_graph import run_closed_loop_graph
from .state import REQUIRED_PROTOCOL_FIELDS, validate_protocol_message


class ClosedLoopCoordinator:
    """Compatibility wrapper over the LangGraph-native closed loop orchestrator."""

    def __init__(self, *, default_retry_budget: int = 1, default_latency_budget_ms: int = 20000) -> None:
        self.default_retry_budget = max(0, int(default_retry_budget))
        self.default_latency_budget_ms = max(1000, int(default_latency_budget_ms))

    async def run(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await run_closed_loop_graph(
            payload=payload,
            default_retry_budget=self.default_retry_budget,
            default_latency_budget_ms=self.default_latency_budget_ms,
        )
