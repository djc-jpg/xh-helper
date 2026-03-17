from __future__ import annotations

from typing import Any, Awaitable, Callable

from ..repositories import (
    AssistantConversationRepository,
    AssistantEpisodeRepository,
    AssistantTurnRepository,
    GoalRepository,
    PolicyMemoryRepository,
    TaskRepository,
    ToolRepository,
)
from ..schemas import AssistantChatRequest
from ..tool_gateway import ToolGateway
from .assistant_orchestration_service import orchestrate_assistant_chat


async def assistant_chat(
    *,
    conversation_repo: AssistantConversationRepository,
    episode_repo: AssistantEpisodeRepository,
    turn_repo: AssistantTurnRepository,
    task_repo: TaskRepository,
    tool_repo: ToolRepository,
    gateway: ToolGateway,
    req: AssistantChatRequest,
    tenant_id: str,
    user: dict[str, Any],
    trace_id: str,
    start_workflow: Callable[[str, dict[str, Any]], Awaitable[None]],
    policy_repo: PolicyMemoryRepository | None = None,
    goal_repo: GoalRepository | None = None,
) -> dict[str, Any]:
    return await orchestrate_assistant_chat(
        conversation_repo=conversation_repo,
        episode_repo=episode_repo,
        turn_repo=turn_repo,
        task_repo=task_repo,
        tool_repo=tool_repo,
        policy_repo=policy_repo,
        goal_repo=goal_repo,
        gateway=gateway,
        req=req,
        tenant_id=tenant_id,
        user=user,
        trace_id=trace_id,
        start_workflow=start_workflow,
    )
