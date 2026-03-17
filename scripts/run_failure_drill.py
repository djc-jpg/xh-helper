from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib import error, request

TERMINAL_STATUSES = {"SUCCEEDED", "FAILED_RETRYABLE", "FAILED_FINAL", "CANCELLED", "TIMED_OUT"}


class DrillError(RuntimeError):
    pass


@dataclass
class ScenarioResult:
    name: str
    passed: bool
    expected_behavior: str
    expected_recovery_path: str
    observations: dict[str, Any]


class ApiClient:
    def __init__(self, base_url: str, *, timeout_s: float = 30.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_s = float(timeout_s)
        self.token: str | None = None

    def _call(self, method: str, path: str, payload: dict[str, Any] | None = None, *, auth: bool = True) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        body = None
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if auth:
            if not self.token:
                raise DrillError("missing bearer token")
            headers["Authorization"] = f"Bearer {self.token}"
        if payload is not None:
            body = json.dumps(payload, ensure_ascii=True).encode("utf-8")

        req = request.Request(url=url, data=body, headers=headers, method=method.upper())
        try:
            with request.urlopen(req, timeout=self.timeout_s) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw) if raw.strip() else {}
        except error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore")
            raise DrillError(f"{method} {path} failed status={exc.code} detail={detail}") from exc
        except error.URLError as exc:
            raise DrillError(f"{method} {path} url_error={exc}") from exc

    def login(self, *, email: str, password: str) -> None:
        out = self._call("POST", "/auth/login", {"email": email, "password": password}, auth=False)
        token = str(out.get("access_token") or "")
        if not token:
            raise DrillError("login response missing access_token")
        self.token = token

    def create_task(self, *, task_type: str, input_payload: dict[str, Any], budget: float = 1.0) -> dict[str, Any]:
        client_request_id = f"drill-{task_type}-{uuid.uuid4()}"
        return self._call(
            "POST",
            "/tasks",
            {
                "client_request_id": client_request_id,
                "task_type": task_type,
                "input": input_payload,
                "budget": float(budget),
            },
        )

    def get_task(self, task_id: str) -> dict[str, Any]:
        return self._call("GET", f"/tasks/{task_id}")

    def rerun_task(self, task_id: str) -> dict[str, Any]:
        return self._call("POST", f"/tasks/{task_id}/rerun", {})

    def reject_approval(self, approval_id: str, *, reason: str) -> dict[str, Any]:
        return self._call("POST", f"/approvals/{approval_id}/reject", {"reason": reason})

    def approve_approval(self, approval_id: str, *, reason: str) -> dict[str, Any]:
        return self._call("POST", f"/approvals/{approval_id}/approve", {"reason": reason})


def wait_terminal_status(api: ApiClient, task_id: str, *, timeout_s: float = 240.0, poll_s: float = 2.0) -> dict[str, Any]:
    deadline = time.time() + timeout_s
    last = {}
    while time.time() < deadline:
        last = api.get_task(task_id)
        status = str((last.get("task") or {}).get("status") or "")
        if status in TERMINAL_STATUSES:
            return last
        time.sleep(poll_s)
    raise DrillError(f"task {task_id} not terminal within {timeout_s}s")


def wait_approval(
    api: ApiClient,
    task_id: str,
    *,
    timeout_s: float = 180.0,
    poll_s: float = 2.0,
    exclude_ids: set[str] | None = None,
) -> tuple[str, dict[str, Any]]:
    seen = exclude_ids or set()
    deadline = time.time() + timeout_s
    last = {}
    while time.time() < deadline:
        last = api.get_task(task_id)
        approvals = list(last.get("approvals") or [])
        for row in approvals:
            approval_id = str(row.get("id") or "")
            status = str(row.get("status") or "")
            if approval_id and approval_id not in seen and status == "WAITING_HUMAN":
                return approval_id, last
        time.sleep(poll_s)
    raise DrillError(f"task {task_id} has no WAITING_HUMAN approval within {timeout_s}s")


def run_cmd(args: list[str], *, input_text: str | None = None, timeout_s: float = 120.0) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        input=input_text,
        text=True,
        capture_output=True,
        timeout=timeout_s,
        check=False,
    )


def scenario_network_timeout(api: ApiClient) -> ScenarioResult:
    expected_behavior = "首次执行出现 timeout 并落到 FAILED_RETRYABLE。"
    expected_recovery_path = "清除瞬时故障后通过 rerun 进入 SUCCEEDED。"

    created = api.create_task(task_type="tool_flow", input_payload={"action": "query", "query": "force_timeout_once"})
    task_id = str(created.get("task_id") or "")
    first_detail = wait_terminal_status(api, task_id)
    first_status = str((first_detail.get("task") or {}).get("status") or "")
    first_reasons = sorted({str(x.get("reason_code") or "") for x in (first_detail.get("tool_calls") or [])})

    api.rerun_task(task_id)
    second_detail = wait_terminal_status(api, task_id)
    second_status = str((second_detail.get("task") or {}).get("status") or "")

    passed = first_status == "FAILED_RETRYABLE" and "timeout" in first_reasons and second_status == "SUCCEEDED"
    return ScenarioResult(
        name="network_timeout",
        passed=passed,
        expected_behavior=expected_behavior,
        expected_recovery_path=expected_recovery_path,
        observations={
            "task_id": task_id,
            "first_status": first_status,
            "first_reason_codes": first_reasons,
            "recovery_status": second_status,
        },
    )


def scenario_downstream_503(api: ApiClient) -> ScenarioResult:
    expected_behavior = "下游 503 被识别为 adapter_http_5xx，并落到 FAILED_RETRYABLE。"
    expected_recovery_path = "下游恢复后通过 rerun 成功。"

    created = api.create_task(task_type="tool_flow", input_payload={"action": "query", "query": "force_503_once"})
    task_id = str(created.get("task_id") or "")
    first_detail = wait_terminal_status(api, task_id)
    first_status = str((first_detail.get("task") or {}).get("status") or "")
    first_reasons = sorted({str(x.get("reason_code") or "") for x in (first_detail.get("tool_calls") or [])})

    api.rerun_task(task_id)
    second_detail = wait_terminal_status(api, task_id)
    second_status = str((second_detail.get("task") or {}).get("status") or "")

    passed = first_status == "FAILED_RETRYABLE" and "adapter_http_5xx" in first_reasons and second_status == "SUCCEEDED"
    return ScenarioResult(
        name="downstream_503",
        passed=passed,
        expected_behavior=expected_behavior,
        expected_recovery_path=expected_recovery_path,
        observations={
            "task_id": task_id,
            "first_status": first_status,
            "first_reason_codes": first_reasons,
            "recovery_status": second_status,
        },
    )


def scenario_redis_unavailable() -> ScenarioResult:
    expected_behavior = "Redis 不可用时 MAS 运行时自动降级到 InMemory backend。"
    expected_recovery_path = "恢复 Redis 后服务可继续运行，不影响任务链路。"

    stop = run_cmd(["docker", "compose", "stop", "redis"], timeout_s=180.0)
    if stop.returncode != 0:
        raise DrillError(f"failed to stop redis: {stop.stderr.strip()}")

    snippet = """
import asyncio
from types import SimpleNamespace
from mas.runtime import build_mas_runtime

async def _task_handler(task):
    return {"ok": True, "task_id": task.get("task_id")}

async def main():
    settings = SimpleNamespace(
        mas_message_backend="redis",
        redis_url="redis://redis:6379/0",
        mas_cache_ttl_s=120,
        mas_rate_limit_requests=30,
        mas_rate_limit_window_s=60,
        mas_retry_max_attempts=3,
        mas_retry_base_delay_s=1.0,
        mas_retry_max_delay_s=10.0,
    )
    coordinator = await build_mas_runtime(settings=settings, task_handler=_task_handler)
    print(f"queue_backend={type(coordinator.event_bus._queue_backend).__name__}")
    print(f"rate_limiter={type(coordinator.execution_agent.rate_limiter).__name__}")

asyncio.run(main())
""".strip()

    try:
        probe = run_cmd(
            ["docker", "compose", "exec", "-T", "worker", "python", "-"],
            input_text=snippet,
            timeout_s=120.0,
        )
        if probe.returncode != 0:
            raise DrillError(f"redis fallback probe failed: {probe.stderr.strip()}")
        output = (probe.stdout or "").strip()
        fallback_ok = "queue_backend=InMemoryMessageQueue" in output and "rate_limiter=InMemoryRateLimiter" in output
    finally:
        start = run_cmd(["docker", "compose", "start", "redis"], timeout_s=180.0)
        if start.returncode != 0:
            raise DrillError(f"failed to restart redis: {start.stderr.strip()}")

    ping = run_cmd(["docker", "compose", "exec", "-T", "redis", "redis-cli", "ping"], timeout_s=60.0)
    redis_recovered = ping.returncode == 0 and "PONG" in (ping.stdout or "")
    passed = fallback_ok and redis_recovered
    return ScenarioResult(
        name="redis_unavailable",
        passed=passed,
        expected_behavior=expected_behavior,
        expected_recovery_path=expected_recovery_path,
        observations={
            "fallback_probe_output": output,
            "redis_recovered": redis_recovered,
            "redis_ping": (ping.stdout or "").strip(),
        },
    )


def scenario_approval_reject(api: ApiClient) -> ScenarioResult:
    expected_behavior = "审批拒绝后任务进入 FAILED_FINAL（approval_rejected）。"
    expected_recovery_path = "operator rerun 后重新审批通过，任务进入 SUCCEEDED。"

    created = api.create_task(
        task_type="ticket_email",
        input_payload={
            "content": "Customer reported production incident and error traces.",
            "target": "ops@example.com",
            "subject": "INC-DRILL",
        },
    )
    task_id = str(created.get("task_id") or "")
    first_approval_id, detail_at_approval = wait_approval(api, task_id)
    _ = detail_at_approval
    api.reject_approval(first_approval_id, reason="failure drill: reject branch")
    rejected_detail = wait_terminal_status(api, task_id)
    rejected_status = str((rejected_detail.get("task") or {}).get("status") or "")
    step_keys = [str(x.get("step_key") or "") for x in (rejected_detail.get("steps") or [])]

    api.rerun_task(task_id)
    seen_ids = {first_approval_id}
    second_approval_id, _detail = wait_approval(api, task_id, exclude_ids=seen_ids)
    api.approve_approval(second_approval_id, reason="failure drill: recovery approve")
    recovered_detail = wait_terminal_status(api, task_id)
    recovered_status = str((recovered_detail.get("task") or {}).get("status") or "")

    passed = rejected_status == "FAILED_FINAL" and "approval_rejected" in step_keys and recovered_status == "SUCCEEDED"
    return ScenarioResult(
        name="approval_reject",
        passed=passed,
        expected_behavior=expected_behavior,
        expected_recovery_path=expected_recovery_path,
        observations={
            "task_id": task_id,
            "first_approval_id": first_approval_id,
            "rejected_status": rejected_status,
            "has_approval_rejected_step": "approval_rejected" in step_keys,
            "recovery_approval_id": second_approval_id,
            "recovery_status": recovered_status,
        },
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Run MAS failure drill scenarios.")
    parser.add_argument("--base-url", default=os.getenv("API_BASE_URL", "http://localhost:18000"))
    parser.add_argument("--operator-email", default=os.getenv("SEED_OPERATOR_EMAIL", "operator@example.com"))
    parser.add_argument("--operator-password", default=os.getenv("SEED_OPERATOR_PASSWORD", "ChangeMe123!"))
    parser.add_argument("--output", default="artifacts/drills/failure_drill_report.json")
    args = parser.parse_args()

    api = ApiClient(args.base_url)
    api.login(email=args.operator_email, password=args.operator_password)

    scenarios: list[ScenarioResult] = []
    scenarios.append(scenario_network_timeout(api))
    scenarios.append(scenario_downstream_503(api))
    scenarios.append(scenario_redis_unavailable())
    scenarios.append(scenario_approval_reject(api))

    report = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "base_url": args.base_url,
        "results": [
            {
                "name": s.name,
                "passed": s.passed,
                "expected_behavior": s.expected_behavior,
                "expected_recovery_path": s.expected_recovery_path,
                "observations": s.observations,
            }
            for s in scenarios
        ],
    }
    report["all_passed"] = all(x["passed"] for x in report["results"])

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=True, indent=2), encoding="utf-8")

    print(json.dumps(report, ensure_ascii=True, indent=2))
    return 0 if bool(report["all_passed"]) else 2


if __name__ == "__main__":
    sys.exit(main())
