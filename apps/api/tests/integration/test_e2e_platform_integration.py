from __future__ import annotations

import concurrent.futures
import json
import time
import uuid
from pathlib import Path
from typing import Any

import httpx
import pytest
from psycopg.rows import dict_row

pytestmark = pytest.mark.integration


def _latest_approval_id(detail: dict[str, Any]) -> str:
    approvals = detail.get("approvals") or []
    if not approvals:
        raise AssertionError(f"approval missing detail={json.dumps(detail, ensure_ascii=True)[:500]}")
    return str(approvals[0]["id"])


def _wait_for_approval_id(
    *,
    get_task_detail,
    task_id: str,
    timeout_sec: int = 90,
) -> str:
    deadline = time.time() + timeout_sec
    last_detail: dict[str, Any] | None = None
    while time.time() < deadline:
        detail = get_task_detail(task_id, role="operator")
        last_detail = detail
        if str((detail.get("task") or {}).get("status") or "") == "WAITING_HUMAN":
            return _latest_approval_id(detail)
        time.sleep(1)
    raise AssertionError(
        f"approval wait timeout task_id={task_id} last={json.dumps(last_detail or {}, ensure_ascii=True)[:600]}"
    )


def _fetch_outbox_row(pg_conn, approval_id: str) -> dict[str, Any] | None:
    with pg_conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT id, status, attempt_count, last_error, updated_at
            FROM approval_signal_outbox
            WHERE approval_id = %s::uuid
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (approval_id,),
        )
        return cur.fetchone()


def _poll_outbox_status_history(pg_conn, approval_id: str, timeout_sec: int = 90) -> list[str]:
    deadline = time.time() + timeout_sec
    seen: list[str] = []
    while time.time() < deadline:
        row = _fetch_outbox_row(pg_conn, approval_id)
        if row:
            status = str(row["status"])
            if not seen or seen[-1] != status:
                seen.append(status)
            if status == "SENT":
                return seen
        time.sleep(1)
    row = _fetch_outbox_row(pg_conn, approval_id)
    raise AssertionError(f"outbox not sent approval_id={approval_id} seen={seen} row={row}")


def test_it_01_happy_path_succeeds_and_artifact_exists(
    submit_task,
    wait_for_task_terminal,
    repo_root: Path,
) -> None:
    task_id = submit_task(
        role="operator",
        case_id="it01-happy",
        task_type="tool_flow",
        input_payload={"action": "query", "query": "all"},
    )
    detail = wait_for_task_terminal(task_id, role="operator")
    assert detail["task"]["status"] == "SUCCEEDED", detail["task"]
    assert len(detail.get("tool_calls") or []) >= 1, detail
    artifacts = detail.get("artifacts") or []
    assert len(artifacts) >= 1, detail

    artifact_uri = str(artifacts[0]["uri"])
    artifact_path = Path(artifact_uri)
    if not artifact_path.is_absolute():
        artifact_path = repo_root / artifact_uri
    assert artifact_path.exists(), f"artifact missing path={artifact_path} task_id={task_id}"


def test_it_02_write_tool_without_approved_status_denied_failed_final(
    submit_task,
    get_task_detail,
    wait_for_task_terminal,
    wait_for_status,
    auth_headers,
    http_client: httpx.Client,
    base_url: str,
    pg_conn,
    require_signal_fail_once: bool,
) -> None:
    if not require_signal_fail_once:
        pytest.skip("requires fail-once signal hook; set INTEGRATION_REQUIRE_SIGNAL_FAIL_ONCE=1")

    task_id = submit_task(
        role="operator",
        case_id="it02-no-approved",
        task_type="tool_flow",
        input_payload={
            "action": "create",
            "name": "integration-it02",
            "value": "v1",
            "idempotency_key": f"it02-{uuid.uuid4().hex[:8]}",
        },
    )
    wait_for_status(task_id, "WAITING_HUMAN", role="operator")
    approval_id = _wait_for_approval_id(get_task_detail=get_task_detail, task_id=task_id)

    approve_resp = http_client.post(
        f"{base_url}/approvals/{approval_id}/approve",
        headers=auth_headers("operator"),
        json={"reason": "it02 approve then tamper"},
    )
    assert approve_resp.status_code == 200, approve_resp.text

    deadline = time.time() + 30
    outbox_row = None
    while time.time() < deadline:
        outbox_row = _fetch_outbox_row(pg_conn, approval_id)
        if outbox_row and str(outbox_row["status"]) in {"PENDING", "FAILED", "SENDING"}:
            break
        time.sleep(0.5)
    assert outbox_row is not None, f"outbox row missing approval_id={approval_id}"

    # Integration-only tamper to assert policy hard-gate on non-approved status.
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            UPDATE approvals
            SET status = 'REJECTED',
                reason = 'it02 tampered to rejected',
                updated_at = NOW()
            WHERE id = %s::uuid
            """,
            (approval_id,),
        )

    detail = wait_for_task_terminal(task_id, role="operator")
    assert detail["task"]["status"] == "FAILED_FINAL", detail
    tool_calls = detail.get("tool_calls") or []
    assert tool_calls, detail
    assert tool_calls[-1]["status"] == "DENIED", tool_calls[-1]
    assert tool_calls[-1]["reason_code"] == "approval_not_approved", tool_calls[-1]


def test_it_03_outbox_eventual_delivery_after_signal_fail_once(
    submit_task,
    get_task_detail,
    wait_for_task_terminal,
    wait_for_status,
    auth_headers,
    http_client: httpx.Client,
    base_url: str,
    pg_conn,
    require_signal_fail_once: bool,
) -> None:
    if not require_signal_fail_once:
        pytest.skip("requires fail-once signal hook; set INTEGRATION_REQUIRE_SIGNAL_FAIL_ONCE=1")

    task_id = submit_task(
        role="operator",
        case_id="it03-outbox",
        task_type="ticket_email",
        input_payload={
            "content": "Customer reports checkout incident.",
            "target": "support@example.com",
            "subject": "Incident follow-up",
        },
    )
    wait_for_status(task_id, "WAITING_HUMAN", role="operator")
    approval_id = _wait_for_approval_id(get_task_detail=get_task_detail, task_id=task_id)

    approve_resp = http_client.post(
        f"{base_url}/approvals/{approval_id}/approve",
        headers=auth_headers("operator"),
        json={"reason": "it03 approval with fail-once retry"},
    )
    assert approve_resp.status_code == 200, approve_resp.text

    status_history = _poll_outbox_status_history(pg_conn, approval_id, timeout_sec=120)
    assert "PENDING" in status_history, status_history
    assert status_history[-1] == "SENT", status_history

    row = _fetch_outbox_row(pg_conn, approval_id)
    assert row is not None
    assert int(row["attempt_count"]) >= 1, row
    assert str(row["status"]) == "SENT", row

    detail = wait_for_task_terminal(task_id, role="operator")
    assert detail["task"]["status"] == "SUCCEEDED", detail


def test_it_04_force_500_retryable_failed_retryable(
    submit_task,
    wait_for_task_terminal,
) -> None:
    task_id = submit_task(
        role="operator",
        case_id="it04-force500",
        task_type="tool_flow",
        input_payload={"action": "query", "query": "force_500"},
    )
    detail = wait_for_task_terminal(task_id, role="operator")
    assert detail["task"]["status"] == "FAILED_RETRYABLE", detail
    tool_calls = detail.get("tool_calls") or []
    assert tool_calls, detail
    last = tool_calls[-1]
    assert last["reason_code"] == "adapter_http_5xx", last
    assert (last.get("response_masked") or {}).get("retryable") is True, last
    assert (last.get("response_masked") or {}).get("status_code") == 500, last


def test_it_05_force_400_non_retryable_failed_final_single_attempt(
    submit_task,
    wait_for_task_terminal,
) -> None:
    task_id = submit_task(
        role="operator",
        case_id="it05-force400",
        task_type="tool_flow",
        input_payload={"action": "query", "query": "force_400"},
    )
    detail = wait_for_task_terminal(task_id, role="operator")
    assert detail["task"]["status"] == "FAILED_FINAL", detail
    tool_calls = detail.get("tool_calls") or []
    assert len(tool_calls) == 1, tool_calls
    last = tool_calls[-1]
    assert last["reason_code"] == "adapter_http_4xx", last
    attempts = [int(step.get("attempt") or 1) for step in (detail.get("steps") or [])]
    assert max(attempts or [1]) == 1, attempts


def test_it_06_rerun_concurrency_conflict_and_run_no_unique(
    submit_task,
    wait_for_task_terminal,
    get_task_detail,
    auth_headers,
    base_url: str,
    expect_rerun_409: bool,
) -> None:
    task_id = submit_task(
        role="operator",
        case_id="it06-rerun-base",
        task_type="rag_qa",
        input_payload={"question": "What is the current support policy?"},
    )
    wait_for_task_terminal(task_id, role="operator")

    headers = auth_headers("operator")

    def _rerun_once() -> tuple[int, str]:
        with httpx.Client(timeout=20.0) as client:
            resp = client.post(f"{base_url}/tasks/{task_id}/rerun", headers=headers)
            return resp.status_code, resp.text[:200]

    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
        results = list(pool.map(lambda _: _rerun_once(), range(24)))

    code_counts: dict[int, int] = {}
    for code, _ in results:
        code_counts[code] = code_counts.get(code, 0) + 1

    detail = get_task_detail(task_id, role="operator")
    run_nos = [int(run["run_no"]) for run in detail.get("runs") or []]
    assert len(run_nos) == len(set(run_nos)), {"run_nos": run_nos, "codes": code_counts}

    if expect_rerun_409:
        assert 409 in code_counts, {"codes": code_counts, "results": results[:8], "run_nos": run_nos}
    else:
        assert 500 not in code_counts, {"codes": code_counts, "results": results[:8], "run_nos": run_nos}
