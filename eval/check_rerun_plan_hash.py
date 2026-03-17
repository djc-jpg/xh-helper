from __future__ import annotations

import argparse
import json
import os
import time
import uuid
from typing import Any

import requests

FINAL_STATES = {"SUCCEEDED", "FAILED_FINAL", "FAILED_RETRYABLE", "CANCELLED", "TIMED_OUT"}

ACCOUNTS = {
    "owner": (os.getenv("SEED_OWNER_EMAIL", "owner@example.com"), os.getenv("SEED_OWNER_PASSWORD", "")),
    "operator": (os.getenv("SEED_OPERATOR_EMAIL", "operator@example.com"), os.getenv("SEED_OPERATOR_PASSWORD", "")),
}


def login(base_url: str, role: str) -> str:
    email, password = ACCOUNTS[role]
    if not password:
        raise RuntimeError(f"Missing seed password for role={role}; set SEED_*_PASSWORD env vars.")
    resp = requests.post(
        f"{base_url}/auth/login",
        json={"email": email, "password": password},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def auth_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def wait_run(
    *,
    base_url: str,
    token: str,
    task_id: str,
    run_id: str,
    timeout_sec: int = 180,
) -> dict[str, Any]:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        detail = requests.get(f"{base_url}/tasks/{task_id}", headers=auth_headers(token), timeout=20)
        detail.raise_for_status()
        payload = detail.json()
        run = next((r for r in payload["runs"] if str(r["id"]) == run_id), None)
        if run and run["status"] in FINAL_STATES:
            return payload
        time.sleep(2)
    raise TimeoutError(f"run timeout: task_id={task_id} run_id={run_id}")


def extract_plan_hash(detail: dict[str, Any], run_id: str) -> str:
    candidate = ""
    for step in detail.get("steps", []):
        if str(step.get("run_id")) != run_id:
            continue
        if step.get("status") != "SUCCEEDED":
            continue
        payload = step.get("payload_masked") or {}
        if isinstance(payload, dict):
            plan_hash = str(payload.get("plan_hash") or "")
            if plan_hash:
                candidate = plan_hash
    return candidate


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:18000")
    args = parser.parse_args()
    base_url = args.base_url.rstrip("/")

    owner_token = login(base_url, "owner")
    operator_token = login(base_url, "operator")

    client_req_id = f"rerun-hash-{uuid.uuid4().hex[:10]}"
    submit = requests.post(
        f"{base_url}/tasks",
        headers=auth_headers(owner_token),
        json={
            "client_request_id": client_req_id,
            "task_type": "rag_qa",
            "input": {"question": "What are incident response steps?"},
            "budget": 1.0,
        },
        timeout=20,
    )
    submit.raise_for_status()
    task_id = str(submit.json()["task_id"])
    run1_id = str(submit.json()["run_id"])

    first_detail = wait_run(base_url=base_url, token=owner_token, task_id=task_id, run_id=run1_id)
    run1_hash = extract_plan_hash(first_detail, run1_id)
    if not run1_hash:
        print(json.dumps({"task_id": task_id, "run_id": run1_id, "error": "missing run1 plan_hash"}, ensure_ascii=True))
        return 1

    rerun = requests.post(
        f"{base_url}/tasks/{task_id}/rerun",
        headers=auth_headers(operator_token),
        timeout=20,
    )
    rerun.raise_for_status()
    run2_id = str(rerun.json()["run_id"])
    second_detail = wait_run(base_url=base_url, token=owner_token, task_id=task_id, run_id=run2_id)
    run2_hash = extract_plan_hash(second_detail, run2_id)
    if not run2_hash:
        print(json.dumps({"task_id": task_id, "run_id": run2_id, "error": "missing run2 plan_hash"}, ensure_ascii=True))
        return 1

    summary = {
        "task_id": task_id,
        "run1_id": run1_id,
        "run2_id": run2_id,
        "run1_plan_hash": run1_hash,
        "run2_plan_hash": run2_hash,
        "plan_hash_equal": run1_hash == run2_hash,
    }
    print(json.dumps(summary, ensure_ascii=True, indent=2))
    return 0 if summary["plan_hash_equal"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
