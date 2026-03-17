from __future__ import annotations

import json
import os
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import psycopg
import pytest

TERMINAL_STATUSES = {"SUCCEEDED", "FAILED_FINAL", "FAILED_RETRYABLE", "CANCELLED", "TIMED_OUT"}


def _bool_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _diagnose_task_detail(detail: dict[str, Any]) -> str:
    task = detail.get("task") or {}
    runs = detail.get("runs") or []
    steps = detail.get("steps") or []
    tool_calls = detail.get("tool_calls") or []

    last_run = runs[-1] if runs else {}
    workflow_error = next((s for s in reversed(steps) if s.get("step_key") == "workflow_error"), None)
    last_tool = tool_calls[-1] if tool_calls else {}
    payload = {
        "task_id": task.get("id"),
        "task_status": task.get("status"),
        "run_id": last_run.get("id"),
        "run_status": last_run.get("status"),
        "workflow_error": workflow_error.get("payload_masked") if workflow_error else None,
        "tool_call": {
            "status": last_tool.get("status"),
            "reason_code": last_tool.get("reason_code"),
            "status_code": (last_tool.get("response_masked") or {}).get("status_code"),
            "retryable": (last_tool.get("response_masked") or {}).get("retryable"),
        }
        if last_tool
        else None,
    }
    return json.dumps(payload, ensure_ascii=True)


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


@pytest.fixture(scope="session")
def base_url() -> str:
    return os.getenv("INTEGRATION_BASE_URL", "http://localhost:18000").rstrip("/")


@pytest.fixture(scope="session")
def worker_metrics_url() -> str:
    return os.getenv("INTEGRATION_WORKER_METRICS_URL", "http://localhost:9001/metrics")


@pytest.fixture(scope="session")
def db_dsn() -> str:
    return os.getenv("INTEGRATION_DATABASE_URL", "postgresql://platform:platform@localhost:5432/platform")


@pytest.fixture(scope="session")
def require_signal_fail_once() -> bool:
    return _bool_env("INTEGRATION_REQUIRE_SIGNAL_FAIL_ONCE", default=True)


@pytest.fixture(scope="session")
def expect_rerun_409() -> bool:
    return _bool_env("INTEGRATION_EXPECT_RERUN_409", default=True)


@pytest.fixture(scope="session")
def http_client() -> Any:
    with httpx.Client(timeout=20.0) as client:
        yield client


@pytest.fixture(scope="session")
def pg_conn(db_dsn: str) -> Any:
    with psycopg.connect(db_dsn, autocommit=True) as conn:
        yield conn


def _wait_for_api_ready(client: httpx.Client, base_url: str, timeout_sec: int = 90) -> None:
    # Route choices are discovered from current API routes: /healthz and /openapi.json.
    candidates = ("/healthz", "/openapi.json")
    deadline = time.time() + timeout_sec
    last_error = "not started"
    while time.time() < deadline:
        for path in candidates:
            try:
                resp = client.get(f"{base_url}{path}")
            except Exception as exc:  # pragma: no cover - integration-only network handling
                last_error = f"{path}:{exc}"
                continue
            if resp.status_code == 200:
                return
            last_error = f"{path}:status={resp.status_code}"
        time.sleep(1)
    raise AssertionError(f"API not ready at {base_url}; last_error={last_error}")


def _wait_for_worker_ready(client: httpx.Client, worker_metrics_url: str, timeout_sec: int = 90) -> None:
    deadline = time.time() + timeout_sec
    last_error = "not started"
    while time.time() < deadline:
        try:
            resp = client.get(worker_metrics_url)
        except Exception as exc:  # pragma: no cover - integration-only network handling
            last_error = str(exc)
            time.sleep(1)
            continue
        if resp.status_code == 200 and "worker_temporal_connected 1.0" in resp.text:
            return
        last_error = f"status={resp.status_code}"
        time.sleep(1)
    raise AssertionError(f"Worker not ready at {worker_metrics_url}; last_error={last_error}")


def _login(client: httpx.Client, base_url: str, email: str, password: str) -> str:
    resp = client.post(
        f"{base_url}/auth/login",
        json={"email": email, "password": password},
    )
    if resp.status_code != 200:
        raise AssertionError(
            f"login failed for {email}: status={resp.status_code} body={resp.text[:200]} "
            "run seed first: docker compose exec -T api python -m app.seed"
        )
    return str(resp.json()["access_token"])


@pytest.fixture(scope="session", autouse=True)
def wait_for_service_ready(http_client: httpx.Client, base_url: str, worker_metrics_url: str) -> None:
    _wait_for_api_ready(http_client, base_url)
    _wait_for_worker_ready(http_client, worker_metrics_url)


@pytest.fixture(scope="session")
def tokens(http_client: httpx.Client, base_url: str) -> dict[str, str]:
    return {
        "owner": _login(
            http_client,
            base_url,
            os.getenv("SEED_OWNER_EMAIL", "owner@example.com"),
            os.getenv("SEED_OWNER_PASSWORD", "ChangeMe123!"),
        ),
        "operator": _login(
            http_client,
            base_url,
            os.getenv("SEED_OPERATOR_EMAIL", "operator@example.com"),
            os.getenv("SEED_OPERATOR_PASSWORD", "ChangeMe123!"),
        ),
        "user": _login(
            http_client,
            base_url,
            os.getenv("SEED_USER_EMAIL", "user@example.com"),
            os.getenv("SEED_USER_PASSWORD", "ChangeMe123!"),
        ),
    }


@pytest.fixture(scope="session")
def auth_headers(tokens: dict[str, str]) -> Callable[[str], dict[str, str]]:
    def _build(role: str = "operator") -> dict[str, str]:
        return {"Authorization": f"Bearer {tokens[role]}"}

    return _build


@pytest.fixture
def submit_task(
    http_client: httpx.Client,
    base_url: str,
    auth_headers: Callable[[str], dict[str, str]],
) -> Callable[[str, str, str, dict[str, Any], float], str]:
    def _submit(
        *,
        role: str,
        case_id: str,
        task_type: str,
        input_payload: dict[str, Any],
        budget: float = 1.5,
    ) -> str:
        client_request_id = f"{case_id}-{uuid.uuid4().hex[:10]}"
        task_input = dict(input_payload)
        task_input.setdefault("correlation_id", client_request_id)
        resp = http_client.post(
            f"{base_url}/tasks",
            headers=auth_headers(role),
            json={
                "client_request_id": client_request_id,
                "task_type": task_type,
                "input": task_input,
                "budget": budget,
            },
        )
        if resp.status_code != 200:
            raise AssertionError(
                f"submit task failed case={case_id} status={resp.status_code} body={resp.text[:300]}"
            )
        return str(resp.json()["task_id"])

    return _submit


@pytest.fixture
def get_task_detail(
    http_client: httpx.Client,
    base_url: str,
    auth_headers: Callable[[str], dict[str, str]],
) -> Callable[[str, str], dict[str, Any]]:
    def _get(task_id: str, role: str = "operator") -> dict[str, Any]:
        resp = http_client.get(f"{base_url}/tasks/{task_id}", headers=auth_headers(role))
        if resp.status_code != 200:
            raise AssertionError(f"get task failed task_id={task_id} status={resp.status_code} body={resp.text[:300]}")
        return resp.json()

    return _get


@pytest.fixture
def wait_for_task_terminal(get_task_detail: Callable[[str, str], dict[str, Any]]) -> Callable[[str, str, int], dict[str, Any]]:
    def _wait(task_id: str, role: str = "operator", timeout_sec: int = 180) -> dict[str, Any]:
        deadline = time.time() + timeout_sec
        last_detail: dict[str, Any] | None = None
        while time.time() < deadline:
            detail = get_task_detail(task_id, role=role)
            last_detail = detail
            status = str((detail.get("task") or {}).get("status") or "")
            if status in TERMINAL_STATUSES:
                return detail
            time.sleep(1)
        diag = _diagnose_task_detail(last_detail or {})
        raise AssertionError(f"task timeout task_id={task_id} timeout_sec={timeout_sec} last={diag}")

    return _wait


@pytest.fixture
def wait_for_status(
    get_task_detail: Callable[[str, str], dict[str, Any]]
) -> Callable[[str, str, str, int], dict[str, Any]]:
    def _wait(task_id: str, status_text: str, role: str = "operator", timeout_sec: int = 120) -> dict[str, Any]:
        deadline = time.time() + timeout_sec
        last_detail: dict[str, Any] | None = None
        while time.time() < deadline:
            detail = get_task_detail(task_id, role=role)
            last_detail = detail
            current = str((detail.get("task") or {}).get("status") or "")
            if current == status_text:
                return detail
            time.sleep(1)
        diag = _diagnose_task_detail(last_detail or {})
        raise AssertionError(f"status timeout task_id={task_id} expected={status_text} last={diag}")

    return _wait

