from __future__ import annotations

from typing import Any, TypedDict

REQUIRED_PROTOCOL_FIELDS = {
    "type",
    "agent",
    "task_id",
    "turn",
    "inputs",
    "outputs",
    "status",
    "rationale_brief",
    "next",
}


class RetryBudgetState(TypedDict):
    remaining: int
    max: int


class LatencyBudgetState(TypedDict):
    remaining_ms: int
    max_ms: int


class MASState(TypedDict, total=False):
    task_id: str
    run_id: str
    trace_id: str
    task_type: str
    input_payload: dict[str, Any]
    budget: float

    turn: int
    phase: str
    status: str
    verdict: str
    stop_reason: str
    failure_type: str
    failure_semantic: str
    next_route: str
    agent_runtime: dict[str, Any]

    task_state: str
    plan_state: str
    evidence: list[dict[str, Any]]
    risk_level: str
    retry_budget: RetryBudgetState
    latency_budget: LatencyBudgetState

    msgs: list[dict[str, Any]]
    metrics: dict[str, Any]
    start_time_ms: int

    task_spec: dict[str, Any]
    plan: list[dict[str, Any]]
    plan_meta: dict[str, Any]
    approval_decision: str
    knowledge_pack: dict[str, Any]
    research_pack: dict[str, Any]
    weather_data: dict[str, Any]
    draft_response: str
    execution_result: dict[str, Any]
    fix_instructions: list[str]

    perceptor_output: dict[str, Any]
    planner_output: dict[str, Any]
    scheduler1_output: dict[str, Any]
    approval_output: dict[str, Any]
    scheduler2_output: dict[str, Any]
    knowledge_output: dict[str, Any]
    researcher_output: dict[str, Any]
    weather_output: dict[str, Any]
    writer_output: dict[str, Any]
    execution_output: dict[str, Any]
    execution_error_output: dict[str, Any]
    critic_output: dict[str, Any]


def validate_protocol_message(message: dict[str, Any]) -> None:
    missing = [field for field in REQUIRED_PROTOCOL_FIELDS if field not in message]
    if missing:
        raise ValueError(f"protocol_error missing_fields={missing}")
    if not isinstance(message.get("inputs"), dict):
        raise TypeError("protocol_error inputs must be object")
    if not isinstance(message.get("outputs"), dict):
        raise TypeError("protocol_error outputs must be object")
    turn = message.get("turn")
    if not isinstance(turn, int) or turn <= 0:
        raise ValueError("protocol_error turn must be positive integer")
