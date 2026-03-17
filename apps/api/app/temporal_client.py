from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

from temporalio.client import Client

from .config import settings

_client: Client | None = None
_lock = asyncio.Lock()
_forced_signal_failures: set[str] = set()
_forced_signal_lock = asyncio.Lock()


async def get_temporal_client() -> Client:
    global _client
    async with _lock:
        if _client is None:
            _client = await Client.connect(
                settings.temporal_target,
                namespace=settings.temporal_namespace,
            )
    assert _client is not None
    return _client


async def start_task_workflow(workflow_id: str, payload: dict[str, Any]) -> None:
    client = await get_temporal_client()
    await client.start_workflow(
        "TaskWorkflow",
        payload,
        id=workflow_id,
        task_queue=settings.temporal_task_queue,
        execution_timeout=timedelta(minutes=10),
    )


async def signal_approval(workflow_id: str, signal_payload: dict[str, Any]) -> None:
    if settings.temporal_signal_fail_once:
        key = str(signal_payload.get("approval_id") or workflow_id)
        async with _forced_signal_lock:
            if key not in _forced_signal_failures:
                _forced_signal_failures.add(key)
                raise RuntimeError("forced approval signal failure once")
    client = await get_temporal_client()
    handle = client.get_workflow_handle(workflow_id)
    await handle.signal("approval_signal", signal_payload)


async def cancel_workflow(workflow_id: str) -> None:
    client = await get_temporal_client()
    handle = client.get_workflow_handle(workflow_id)
    await handle.cancel()
