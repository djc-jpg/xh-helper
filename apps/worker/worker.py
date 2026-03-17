from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
import time

from prometheus_client import Gauge, REGISTRY, start_http_server
from temporalio.client import Client
from temporalio.worker import Worker

from activities import (
    create_approval_activity,
    execute_tools_activity,
    mas_orchestrate_activity,
    plan_activity,
    review_activity,
    set_status_activity,
    shadow_compare_activity,
    validate_activity,
)
from config import settings
from db import close_pool, init_pool
from graph import close_graph_resources
from otel import setup_otel
from workflows import TaskWorkflow

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def _get_or_create_gauge(name: str, documentation: str) -> Gauge:
    existing = getattr(REGISTRY, "_names_to_collectors", {}).get(name)
    if isinstance(existing, Gauge):
        return existing
    try:
        return Gauge(name, documentation)
    except ValueError:
        existing = getattr(REGISTRY, "_names_to_collectors", {}).get(name)
        if isinstance(existing, Gauge):
            return existing
        raise


worker_temporal_connected = _get_or_create_gauge(
    "worker_temporal_connected",
    "1 when worker is connected to Temporal and polling task queue",
)
ACTIVITY_EXECUTOR_WORKERS = 8


def _next_retry_sleep(previous_delay_s: float, elapsed_s: float) -> tuple[float, float]:
    next_delay = min(previous_delay_s * 2, 10.0)
    if elapsed_s >= 60.0:
        return 10.0, next_delay
    return min(previous_delay_s, 10.0), next_delay


async def _connect_temporal_with_retry() -> Client:
    start = time.monotonic()
    delay_s = 1.0
    attempt = 0
    while True:
        attempt += 1
        try:
            client = await Client.connect(settings.temporal_target, namespace=settings.temporal_namespace)
            elapsed = time.monotonic() - start
            worker_temporal_connected.set(1)
            logging.info(
                "Connected to Temporal target=%s namespace=%s attempts=%d elapsed=%.1fs",
                settings.temporal_target,
                settings.temporal_namespace,
                attempt,
                elapsed,
            )
            return client
        except Exception as exc:
            elapsed = time.monotonic() - start
            worker_temporal_connected.set(0)
            sleep_s, delay_s = _next_retry_sleep(delay_s, elapsed)
            logging.warning(
                "Temporal connect failed attempt=%d elapsed=%.1fs target=%s sleep=%.1fs error=%s",
                attempt,
                elapsed,
                settings.temporal_target,
                sleep_s,
                exc,
            )
            await asyncio.sleep(sleep_s)


async def main() -> None:
    setup_otel()
    start_http_server(9001)
    init_pool()
    worker_temporal_connected.set(0)
    activity_executor = ThreadPoolExecutor(max_workers=ACTIVITY_EXECUTOR_WORKERS)

    try:
        while True:
            client = await _connect_temporal_with_retry()
            try:
                worker = Worker(
                    client,
                    task_queue=settings.temporal_task_queue,
                    workflows=[TaskWorkflow],
                    activities=[
                        set_status_activity,
                        validate_activity,
                        mas_orchestrate_activity,
                        plan_activity,
                        execute_tools_activity,
                        create_approval_activity,
                        review_activity,
                        shadow_compare_activity,
                    ],
                    activity_executor=activity_executor,
                )
                logging.info(
                    "Worker connected and polling task_queue=%s namespace=%s",
                    settings.temporal_task_queue,
                    settings.temporal_namespace,
                )
                await worker.run()
                worker_temporal_connected.set(0)
                logging.warning("Worker.run returned unexpectedly; reconnecting in 3s")
                await asyncio.sleep(3)
            except Exception:
                worker_temporal_connected.set(0)
                logging.exception("Worker process loop failed; reconnecting in 3s")
                await asyncio.sleep(3)
    finally:
        activity_executor.shutdown(wait=False, cancel_futures=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        close_graph_resources()
        close_pool()
