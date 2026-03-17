from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


class DemoError(RuntimeError):
    pass


def _env(name: str, default: str = "") -> str:
    return str(os.getenv(name, default))


def _default_base_url() -> str:
    return _env("DEMO_BASE_URL", _env("API_BASE_URL", "http://localhost:18000"))


def _default_tenant_id() -> str:
    return _env("DEMO_TENANT_ID", _env("DEFAULT_TENANT_ID", "default"))


def _default_user_email() -> str:
    return _env("DEMO_USER_EMAIL", _env("SEED_USER_EMAIL", "user@example.com"))


def _default_user_password() -> str:
    return _env("DEMO_USER_PASSWORD", _env("SEED_USER_PASSWORD", "ChangeMe123!"))


def _default_operator_email() -> str:
    return _env("DEMO_OPERATOR_EMAIL", _env("SEED_OPERATOR_EMAIL", "operator@example.com"))


def _default_operator_password() -> str:
    return _env("DEMO_OPERATOR_PASSWORD", _env("SEED_OPERATOR_PASSWORD", "ChangeMe123!"))


def _http_json(
    *,
    method: str,
    base_url: str,
    path: str,
    token: str = "",
    tenant_id: str = "",
    body: dict[str, Any] | None = None,
    timeout_s: int = 20,
) -> Any:
    url = f"{base_url.rstrip('/')}{path}"
    headers: dict[str, str] = {"Accept": "application/json"}
    payload_bytes = None
    if body is not None:
        headers["Content-Type"] = "application/json"
        payload_bytes = json.dumps(body).encode("utf-8")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if tenant_id:
        headers["X-Tenant-Id"] = tenant_id

    request = urllib.request.Request(url=url, data=payload_bytes, headers=headers, method=method.upper())
    try:
        with urllib.request.urlopen(request, timeout=timeout_s) as response:
            raw = response.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise DemoError(f"HTTP {exc.code} {method.upper()} {path}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise DemoError(f"request failed {method.upper()} {path}: {exc.reason}") from exc


def _login(*, base_url: str, email: str, password: str) -> str:
    data = _http_json(
        method="POST",
        base_url=base_url,
        path="/auth/login",
        body={"email": email, "password": password},
    )
    token = str(data.get("access_token") or "")
    if not token:
        raise DemoError("login succeeded but access_token is empty")
    return token


def _print_json(data: Any) -> None:
    print(json.dumps(data, ensure_ascii=False, indent=2))


def cmd_create(args: argparse.Namespace) -> int:
    token = _login(base_url=args.base_url, email=args.user_email, password=args.user_password)
    client_request_id = args.client_request_id or f"demo-{int(time.time())}"
    payload = {
        "client_request_id": client_request_id,
        "task_type": args.task_type,
        "input": {
            "content": args.content,
            "target": args.target,
            "subject": args.subject,
            "reply_draft": args.reply_draft,
        },
        "budget": args.budget,
    }
    data = _http_json(
        method="POST",
        base_url=args.base_url,
        path="/tasks",
        token=token,
        tenant_id=args.tenant_id,
        body=payload,
    )

    task_id = str(data.get("task_id") or "")
    run_id = str(data.get("run_id") or "")
    print("=== Demo Create ===")
    print(f"TASK_ID={task_id}")
    print(f"RUN_ID={run_id}")
    print(f"STATUS={data.get('status')}")
    print(f"IDEMPOTENT={bool(data.get('idempotent'))}")
    print(f"TRACE_ID={data.get('trace_id')}")
    print(f"NEXT=make demo-status TASK_ID={task_id}")
    if args.json:
        _print_json(data)
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    if not args.task_id:
        raise DemoError("task_id is required, usage: make demo-status TASK_ID=<task_id>")
    token = _login(base_url=args.base_url, email=args.user_email, password=args.user_password)
    data = _http_json(
        method="GET",
        base_url=args.base_url,
        path=f"/tasks/{urllib.parse.quote(args.task_id)}",
        token=token,
        tenant_id=args.tenant_id,
    )

    task = data.get("task") or {}
    runs = data.get("runs") or []
    steps = data.get("steps") or []
    approvals = data.get("approvals") or []
    tool_calls = data.get("tool_calls") or []
    artifacts = data.get("artifacts") or []

    pending = [a for a in approvals if str(a.get("status")) == "WAITING_HUMAN"]
    print("=== Demo Status ===")
    print(f"TASK_ID={task.get('id')}")
    print(f"TASK_STATUS={task.get('status')}")
    print(f"TASK_TYPE={task.get('task_type')}")
    print(f"RUN_COUNT={len(runs)}")
    print(f"STEP_COUNT={len(steps)}")
    print(f"TOOL_CALL_COUNT={len(tool_calls)}")
    print(f"APPROVAL_COUNT={len(approvals)}")
    print(f"ARTIFACT_COUNT={len(artifacts)}")
    if runs:
        latest = runs[0]
        print(f"LATEST_RUN_ID={latest.get('id')}")
        print(f"LATEST_RUN_STATUS={latest.get('status')}")
    if pending:
        pending_ids = ",".join(str(a.get("id")) for a in pending)
        print(f"PENDING_APPROVAL_IDS={pending_ids}")
        print(f"NEXT=make demo-approve APPROVAL_ID={pending[0].get('id')}")
    if args.json:
        _print_json(data)
    return 0


def cmd_approve(args: argparse.Namespace) -> int:
    operator_token = _login(
        base_url=args.base_url,
        email=args.operator_email,
        password=args.operator_password,
    )
    approval_id = str(args.approval_id or "").strip()
    if not approval_id:
        rows = _http_json(
            method="GET",
            base_url=args.base_url,
            path="/approvals?status=WAITING_HUMAN",
            token=operator_token,
            tenant_id=args.tenant_id,
        )
        if not isinstance(rows, list):
            raise DemoError("unexpected /approvals response shape, expected list")
        waiting = rows
        if args.task_id:
            waiting = [r for r in waiting if str(r.get("task_id")) == str(args.task_id)]
        if not waiting:
            raise DemoError("no WAITING_HUMAN approvals found")
        approval_id = str(waiting[0].get("id") or "")
    if not approval_id:
        raise DemoError("approval_id is empty")

    data = _http_json(
        method="POST",
        base_url=args.base_url,
        path=f"/approvals/{urllib.parse.quote(approval_id)}/approve",
        token=operator_token,
        tenant_id=args.tenant_id,
        body={"reason": args.reason},
    )
    print("=== Demo Approve ===")
    print(f"APPROVAL_ID={data.get('approval_id', approval_id)}")
    print(f"STATUS={data.get('status')}")
    print(f"IDEMPOTENT={bool(data.get('idempotent'))}")
    if args.json:
        _print_json(data)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Interview demo helper for XH Helper.")
    parser.add_argument("--base-url", default=_default_base_url(), help="API base url")
    parser.add_argument("--tenant-id", default=_default_tenant_id(), help="tenant id header")

    subparsers = parser.add_subparsers(dest="command", required=True)

    create_parser = subparsers.add_parser("create", help="Create a demo task")
    create_parser.add_argument("--user-email", default=_default_user_email())
    create_parser.add_argument("--user-password", default=_default_user_password())
    create_parser.add_argument("--client-request-id", default="")
    create_parser.add_argument("--task-type", default="ticket_email")
    create_parser.add_argument("--budget", type=float, default=1.0)
    create_parser.add_argument("--content", default="incident: api returns 500 on /orders")
    create_parser.add_argument("--target", default="ops@example.com")
    create_parser.add_argument("--subject", default="Incident follow-up")
    create_parser.add_argument("--reply-draft", default="We are investigating and will update shortly.")
    create_parser.add_argument("--json", action="store_true", help="print full JSON payload")
    create_parser.set_defaults(func=cmd_create)

    status_parser = subparsers.add_parser("status", help="Show task/runs/steps summary")
    status_parser.add_argument("--user-email", default=_default_user_email())
    status_parser.add_argument("--user-password", default=_default_user_password())
    status_parser.add_argument("--task-id", default=os.getenv("TASK_ID", ""))
    status_parser.add_argument("--json", action="store_true", help="print full JSON payload")
    status_parser.set_defaults(func=cmd_status)

    approve_parser = subparsers.add_parser("approve", help="Approve a WAITING_HUMAN approval")
    approve_parser.add_argument("--operator-email", default=_default_operator_email())
    approve_parser.add_argument("--operator-password", default=_default_operator_password())
    approve_parser.add_argument("--approval-id", default=os.getenv("APPROVAL_ID", ""))
    approve_parser.add_argument("--task-id", default=os.getenv("TASK_ID", ""))
    approve_parser.add_argument("--reason", default="approved in demo")
    approve_parser.add_argument("--json", action="store_true", help="print full JSON payload")
    approve_parser.set_defaults(func=cmd_approve)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return int(args.func(args))
    except DemoError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
