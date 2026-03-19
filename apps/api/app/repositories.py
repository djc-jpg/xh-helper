from __future__ import annotations

from datetime import datetime
import re
from typing import Any

from psycopg.types.json import Jsonb

from .db import execute, fetchall, fetchone, transaction_cursor
from .masking import mask_payload, summarize_payload

ERROR_MESSAGE_MAX_LEN = 2048
_CODE_LIKE_RE = re.compile(r"^[a-z0-9][a-z0-9_:\.-]{0,63}$")
_CODE_SANITIZE_RE = re.compile(r"[^a-z0-9_]")
_ERROR_HINTS: tuple[tuple[str, str], ...] = (
    ("workflow_start_failed", "workflow_start_failed"),
    ("adapter_http_5xx", "adapter_http_5xx"),
    ("adapter_http_4xx", "adapter_http_4xx"),
    ("adapter_http_429", "adapter_http_429"),
    ("adapter_http_408", "adapter_http_408"),
    ("policy_default_deny", "tool_denied"),
    ("policy_deny", "tool_denied"),
    ("tool_denied", "tool_denied"),
    ("write_requires_approval", "tool_denied"),
    ("write_requires_operator", "tool_denied"),
    ("approval_not_approved", "tool_denied"),
    ("approval_invalid", "tool_denied"),
    ("approval_context_invalid", "tool_denied"),
    ("timed_out", "timed_out"),
    ("timeout", "timed_out"),
)
_SENSITIVE_TEXT_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"(?i)(authorization\s*[:=]\s*)([^\r\n,;]+)"),
    re.compile(r"(?i)(password\s*[:=]\s*)([^\s,;]+)"),
    re.compile(r"(?i)(token\s*[:=]\s*)([^\s,;]+)"),
    re.compile(r"(?i)(secret\s*[:=]\s*)([^\s,;]+)"),
)


def _runtime_state_seed(input_masked: dict[str, Any]) -> dict[str, Any]:
    nested_runtime = input_masked.get("runtime_state")
    runtime: dict[str, Any] = dict(nested_runtime) if isinstance(nested_runtime, dict) else {}
    for key in (
        "planner",
        "goal",
        "unified_task",
        "task_state",
        "current_action",
        "policy",
        "episodes",
        "retrieval_hits",
        "memory",
        "observations",
        "decision",
        "reflection",
        "steps",
        "final_output",
    ):
        value = input_masked.get(key)
        if value in (None, "", [], {}):
            continue
        runtime[key] = value
    task_state = input_masked.get("task_state")
    current_phase = str(task_state.get("current_phase") or "") if isinstance(task_state, dict) else ""
    if current_phase:
        runtime["current_phase"] = current_phase
    if runtime:
        runtime["status"] = "RECEIVED"
    return runtime


def _truncate_text(value: str, max_len: int = ERROR_MESSAGE_MAX_LEN) -> str:
    txt = value or ""
    if len(txt) <= max_len:
        return txt
    return txt[: max_len - 3] + "..."


def _conversation_title_seed(message: str, max_len: int = 80) -> str | None:
    collapsed = " ".join(str(message or "").split())
    if not collapsed:
        return None
    if len(collapsed) <= max_len:
        return collapsed
    return collapsed[: max_len - 3] + "..."


def _redact_sensitive_text(value: str) -> str:
    txt = value
    for pattern in _SENSITIVE_TEXT_PATTERNS:
        txt = pattern.sub(r"\1***", txt)
    return txt


def _normalize_error_message(error_message: Any, status_text: str) -> str:
    source: Any
    if error_message is None or error_message == "":
        source = {"status": status_text}
    else:
        source = error_message
    if isinstance(source, str):
        return _truncate_text(_redact_sensitive_text(source))
    masked = mask_payload(source)
    summary = str(summarize_payload(masked, max_len=ERROR_MESSAGE_MAX_LEN).get("summary", ""))
    return _truncate_text(_redact_sensitive_text(summary))


def _normalize_code_like(value: str) -> str:
    lowered = value.strip().lower().replace("-", "_").replace(".", "_").replace(":", "_")
    normalized = _CODE_SANITIZE_RE.sub("_", lowered).strip("_")
    return normalized[:64]


def _error_code_from_hints(value: str) -> str | None:
    lowered = value.lower()
    for token, mapped in _ERROR_HINTS:
        if token in lowered:
            return mapped
    return None


def _normalize_error_code(status_text: str, error_code: str | None, error_message: Any) -> str:
    if status_text == "TIMED_OUT":
        return "timed_out"
    raw_code = str(error_code or "").strip()
    if raw_code:
        hinted = _error_code_from_hints(raw_code)
        if hinted:
            return hinted
        if _CODE_LIKE_RE.fullmatch(raw_code.lower()):
            code_like = _normalize_code_like(raw_code)
            if code_like:
                return code_like
    raw_message = str(error_message or "")
    hinted = _error_code_from_hints(raw_message)
    if hinted:
        return hinted
    return "unknown_error"


def normalize_task_failure_fields(
    *,
    status_text: str,
    error_code: str | None,
    error_message: Any,
) -> tuple[str, str]:
    normalized_code = _normalize_error_code(status_text, error_code, error_message)
    normalized_message = _normalize_error_message(error_message, status_text)
    return normalized_code, normalized_message


class AuthRepository:
    def get_user_by_email(self, tenant_id: str, email: str) -> dict[str, Any] | None:
        return fetchone(
            """
            SELECT id, tenant_id, email, role, password_hash, is_active
            FROM users
            WHERE tenant_id = %s AND email = %s
            """,
            (tenant_id, email),
        )

    def get_user_by_id(self, tenant_id: str, user_id: str) -> dict[str, Any] | None:
        return fetchone(
            """
            SELECT id, tenant_id, email, role, is_active
            FROM users
            WHERE tenant_id = %s AND id = %s
            """,
            (tenant_id, user_id),
        )

    def user_exists(self, tenant_id: str, email: str) -> bool:
        row = fetchone(
            "SELECT id FROM users WHERE tenant_id = %s AND email = %s",
            (tenant_id, email),
        )
        return row is not None

    def count_users(self, tenant_id: str) -> int:
        row = fetchone("SELECT COUNT(*)::int AS c FROM users WHERE tenant_id = %s", (tenant_id,))
        return int((row or {}).get("c") or 0)

    def create_user(self, tenant_id: str, email: str, password_hash: str, role: str) -> dict[str, Any]:
        row = fetchone(
            """
            INSERT INTO users (tenant_id, email, password_hash, role)
            VALUES (%s, %s, %s, %s::user_role)
            RETURNING id, tenant_id, email, role
            """,
            (tenant_id, email, password_hash, role),
        )
        assert row is not None
        return row

    def store_refresh_token(self, tenant_id: str, user_id: str, token_hash: str, expires_at: datetime) -> None:
        execute(
            """
            INSERT INTO refresh_tokens (tenant_id, user_id, token_hash, expires_at)
            VALUES (%s, %s, %s, %s)
            """,
            (tenant_id, user_id, token_hash, expires_at),
        )

    def get_refresh_token(self, tenant_id: str, token_hash: str) -> dict[str, Any] | None:
        return fetchone(
            """
            SELECT id, user_id, expires_at, revoked_at
            FROM refresh_tokens
            WHERE tenant_id = %s
              AND token_hash = %s
            """,
            (tenant_id, token_hash),
        )

    def consume_refresh_token(self, tenant_id: str, token_hash: str) -> dict[str, Any] | None:
        # Atomic one-time consume: exactly one concurrent request can succeed.
        return fetchone(
            """
            UPDATE refresh_tokens
            SET revoked_at = NOW()
            WHERE tenant_id = %s
              AND token_hash = %s
              AND revoked_at IS NULL
              AND expires_at >= NOW()
            RETURNING id, user_id, expires_at, revoked_at
            """,
            (tenant_id, token_hash),
        )

    def revoke_refresh_token(self, token_id: str) -> None:
        execute("UPDATE refresh_tokens SET revoked_at = NOW() WHERE id = %s", (token_id,))

    def revoke_refresh_token_for_user(self, tenant_id: str, token_hash: str, user_id: str) -> None:
        execute(
            """
            UPDATE refresh_tokens
            SET revoked_at = NOW()
            WHERE tenant_id = %s
              AND token_hash = %s
              AND user_id = %s
            """,
            (tenant_id, token_hash, user_id),
        )


class TaskRepository:
    def get_task_by_id(self, tenant_id: str, task_id: str, *, include_sensitive: bool = False) -> dict[str, Any] | None:
        select_cols = (
            "*"
            if include_sensitive
            else """
            id, tenant_id, client_request_id, task_type, status, created_by,
            input_masked, output_masked, error_code, trace_id, cost_total, task_cost_usd,
            conversation_id, assistant_turn_id, goal_id, origin, runtime_state,
            budget, requires_hitl, created_at, updated_at
            """
        )
        return fetchone(
            f"""
            SELECT {select_cols}
            FROM tasks
            WHERE tenant_id = %s
              AND id = %s
            """,
            (tenant_id, task_id),
        )

    def get_task_by_client_request_id(self, tenant_id: str, client_request_id: str) -> dict[str, Any] | None:
        return fetchone(
            """
            SELECT id, status, trace_id
            FROM tasks
            WHERE tenant_id = %s
              AND client_request_id = %s
            """,
            (tenant_id, client_request_id),
        )

    def create_task(
        self,
        *,
        tenant_id: str,
        client_request_id: str,
        task_type: str,
        created_by: str,
        input_masked: dict[str, Any],
        input_raw_encrypted: str,
        trace_id: str,
        budget: float,
        requires_hitl: bool,
        conversation_id: str | None = None,
        assistant_turn_id: str | None = None,
        goal_id: str | None = None,
        origin: str = "task_api",
    ) -> dict[str, Any]:
        row = fetchone(
            """
            INSERT INTO tasks (
              tenant_id, client_request_id, task_type, status, created_by, input_masked, input_raw_encrypted, trace_id,
              budget, requires_hitl, conversation_id, assistant_turn_id, goal_id, origin, runtime_state
            )
            VALUES (%s, %s, %s, 'RECEIVED', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, trace_id, budget, conversation_id, assistant_turn_id, goal_id, origin, runtime_state
            """,
            (
                tenant_id,
                client_request_id,
                task_type,
                created_by,
                Jsonb(input_masked),
                input_raw_encrypted,
                trace_id,
                budget,
                requires_hitl,
                conversation_id,
                assistant_turn_id,
                goal_id,
                origin,
                Jsonb(_runtime_state_seed(input_masked)),
            ),
        )
        assert row is not None
        return row

    def create_run(
        self,
        *,
        tenant_id: str,
        task_id: str,
        run_no: int,
        workflow_id: str,
        trace_id: str,
        assigned_worker: str,
    ) -> dict[str, Any]:
        row = fetchone(
            """
            INSERT INTO runs (tenant_id, task_id, run_no, status, workflow_id, trace_id, assigned_worker, started_at)
            VALUES (%s, %s, %s, 'QUEUED', %s, %s, %s, NOW())
            RETURNING id
            """,
            (tenant_id, task_id, run_no, workflow_id, trace_id, assigned_worker),
        )
        assert row is not None
        return row

    def update_task_status(self, tenant_id: str, task_id: str, status_text: str) -> None:
        execute(
            """
            UPDATE tasks
            SET status = %s::task_status, updated_at = NOW()
            WHERE tenant_id = %s
              AND id = %s
            """,
            (status_text, tenant_id, task_id),
        )

    def update_task_runtime_state(self, tenant_id: str, task_id: str, runtime_state: dict[str, Any]) -> None:
        execute(
            """
            UPDATE tasks
            SET runtime_state = %s,
                updated_at = NOW()
            WHERE tenant_id = %s
              AND id = %s
            """,
            (Jsonb(runtime_state), tenant_id, task_id),
        )

    def mark_task_failed(
        self,
        tenant_id: str,
        task_id: str,
        status_text: str,
        error_code: str | None,
        error_message: Any = None,
    ) -> None:
        normalized_code, normalized_message = normalize_task_failure_fields(
            status_text=status_text,
            error_code=error_code,
            error_message=error_message,
        )
        execute(
            """
            UPDATE tasks
            SET status = %s::task_status, error_code = %s, error_message = %s, updated_at = NOW()
            WHERE tenant_id = %s
              AND id = %s
            """,
            (status_text, normalized_code, normalized_message, tenant_id, task_id),
        )

    def mark_task_succeeded(self, tenant_id: str, task_id: str, payload_masked: dict[str, Any]) -> None:
        execute(
            """
            UPDATE tasks
            SET status = 'SUCCEEDED', output_masked = %s, updated_at = NOW()
            WHERE tenant_id = %s
              AND id = %s
            """,
            (Jsonb(payload_masked), tenant_id, task_id),
        )

    def add_task_cost(self, tenant_id: str, task_id: str, run_id: str, amount: float) -> None:
        execute(
            """
            UPDATE tasks
            SET cost_total = COALESCE(cost_total, 0) + %s,
                task_cost_usd = COALESCE(task_cost_usd, 0) + %s
            WHERE tenant_id = %s
              AND id = %s
            """,
            (amount, amount, tenant_id, task_id),
        )
        execute(
            """
            UPDATE runs
            SET run_cost_usd = COALESCE(run_cost_usd, 0) + %s
            WHERE tenant_id = %s
              AND id = %s
            """,
            (amount, tenant_id, run_id),
        )

    def get_task_cost(self, tenant_id: str, task_id: str) -> float:
        row = fetchone("SELECT task_cost_usd FROM tasks WHERE tenant_id = %s AND id = %s", (tenant_id, task_id))
        return float((row or {}).get("task_cost_usd") or 0.0)

    def update_run_status(self, tenant_id: str, run_id: str, status_text: str) -> None:
        execute(
            """
            UPDATE runs
            SET status = %s::task_status,
                ended_at = CASE
                  WHEN %s = ANY(ARRAY['SUCCEEDED','FAILED_RETRYABLE','FAILED_FINAL','CANCELLED','TIMED_OUT'])
                  THEN NOW()
                  ELSE ended_at
                END
            WHERE tenant_id = %s
              AND id = %s
            """,
            (status_text, status_text, tenant_id, run_id),
        )

    def append_step(
        self,
        *,
        tenant_id: str,
        run_id: str,
        status_text: str,
        step_key: str,
        payload_masked: dict[str, Any],
        trace_id: str,
        span_id: str | None = None,
        attempt: int = 1,
        status_event_id: str | None = None,
    ) -> bool:
        rowcount = execute(
            """
            INSERT INTO steps (tenant_id, run_id, step_key, status, payload_masked, trace_id, span_id, attempt, status_event_id)
            VALUES (%s, %s, %s, %s::task_status, %s, %s, %s, %s, %s)
            ON CONFLICT (tenant_id, run_id, status_event_id) DO NOTHING
            """,
            (
                tenant_id,
                run_id,
                step_key,
                status_text,
                Jsonb(payload_masked),
                trace_id,
                span_id,
                attempt,
                status_event_id,
            ),
        )
        return rowcount == 1

    def has_status_event(self, tenant_id: str, run_id: str, status_event_id: str) -> bool:
        row = fetchone(
            """
            SELECT 1
            FROM steps
            WHERE tenant_id = %s
              AND run_id = %s
              AND status_event_id = %s
            """,
            (tenant_id, run_id, status_event_id),
        )
        return row is not None

    def get_latest_run_for_task(self, tenant_id: str, task_id: str) -> dict[str, Any] | None:
        return fetchone(
            """
            SELECT *
            FROM runs
            WHERE tenant_id = %s
              AND task_id = %s
            ORDER BY run_no DESC
            LIMIT 1
            """,
            (tenant_id, task_id),
        )

    def get_max_run_no(self, tenant_id: str, task_id: str) -> int:
        row = fetchone(
            """
            SELECT COALESCE(MAX(run_no), 0)::int AS max_run_no
            FROM runs
            WHERE tenant_id = %s
              AND task_id = %s
            """,
            (tenant_id, task_id),
        )
        return int((row or {}).get("max_run_no") or 0)

    def get_run_by_id(self, tenant_id: str, run_id: str) -> dict[str, Any] | None:
        return fetchone(
            """
            SELECT *
            FROM runs
            WHERE tenant_id = %s
              AND id = %s
            """,
            (tenant_id, run_id),
        )

    def list_tasks(
        self,
        *,
        tenant_id: str,
        status_filter: str,
        task_type: str,
        from_ts: str,
        to_ts: str,
        created_by: str | None,
    ) -> list[dict[str, Any]]:
        query = """
        SELECT id, client_request_id, task_type, status, created_by, trace_id, cost_total, requires_hitl, created_at, updated_at
        FROM tasks
        WHERE tenant_id = %s
          AND (%s = '' OR status::text = %s)
          AND (%s = '' OR task_type = %s)
          AND (%s = '' OR created_at >= NULLIF(%s, '')::timestamptz)
          AND (%s = '' OR created_at <= NULLIF(%s, '')::timestamptz)
        """
        params: list[Any] = [
            tenant_id,
            status_filter,
            status_filter,
            task_type,
            task_type,
            from_ts,
            from_ts,
            to_ts,
            to_ts,
        ]
        if created_by:
            query += " AND created_by = %s"
            params.append(created_by)
        query += " ORDER BY created_at DESC LIMIT 200"
        rows = fetchall(query, tuple(params))
        for row in rows:
            row["id"] = str(row["id"])
            row["created_by"] = str(row["created_by"])
        return rows

    def list_assistant_tasks_for_conversation(
        self,
        *,
        tenant_id: str,
        user_id: str,
        conversation_id: str,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        rows = fetchall(
            """
            SELECT
              t.id,
              t.task_type,
              t.status,
              t.trace_id,
              t.input_masked,
              t.output_masked,
              t.error_code,
              t.error_message,
              t.conversation_id,
              t.assistant_turn_id,
              t.origin,
              t.created_at,
              t.updated_at,
              COALESCE(step_latest.step_key, '') AS latest_step_key,
              COALESCE(step_latest.status::text, t.status::text) AS latest_step_status,
              step_latest.created_at AS latest_step_at,
              COALESCE(tool_stats.tool_call_count, 0)::int AS tool_call_count,
              COALESCE(tool_stats.failed_tool_call_count, 0)::int AS failed_tool_call_count,
              COALESCE(approval_stats.approval_count, 0)::int AS approval_count,
              COALESCE(approval_stats.waiting_approval_count, 0)::int AS waiting_approval_count
            FROM tasks t
            LEFT JOIN LATERAL (
              SELECT s.step_key, s.status, s.created_at
              FROM steps s
              JOIN runs r ON r.id = s.run_id
              WHERE r.tenant_id = t.tenant_id
                AND r.task_id = t.id
              ORDER BY s.created_at DESC
              LIMIT 1
            ) step_latest ON TRUE
            LEFT JOIN LATERAL (
              SELECT
                COUNT(*)::int AS tool_call_count,
                COUNT(*) FILTER (WHERE status <> 'SUCCEEDED')::int AS failed_tool_call_count
              FROM tool_calls tc
              WHERE tc.tenant_id = t.tenant_id
                AND tc.task_id = t.id
            ) tool_stats ON TRUE
            LEFT JOIN LATERAL (
              SELECT
                COUNT(*)::int AS approval_count,
                COUNT(*) FILTER (WHERE status = 'WAITING_HUMAN')::int AS waiting_approval_count
              FROM approvals a
              WHERE a.tenant_id = t.tenant_id
                AND a.task_id = t.id
            ) approval_stats ON TRUE
            WHERE t.tenant_id = %s
              AND t.created_by = %s
              AND COALESCE(t.conversation_id, t.input_masked ->> 'conversation_id', '') = %s
            ORDER BY t.created_at DESC
            LIMIT %s
            """,
            (tenant_id, user_id, conversation_id, max(1, int(limit))),
        )
        for row in rows:
            row["id"] = str(row["id"])
        return rows

    def list_runs_for_task(self, tenant_id: str, task_id: str) -> list[dict[str, Any]]:
        return fetchall(
            """
            SELECT *
            FROM runs
            WHERE tenant_id = %s
              AND task_id = %s
            ORDER BY run_no
            """,
            (tenant_id, task_id),
        )

    def list_steps_for_run_ids(self, tenant_id: str, run_ids: list[Any]) -> list[dict[str, Any]]:
        return fetchall(
            """
            SELECT s.*
            FROM steps s
            JOIN runs r ON r.id = s.run_id
            WHERE r.tenant_id = %s
              AND s.run_id = ANY(%s)
            ORDER BY s.created_at
            """,
            (tenant_id, run_ids or ["00000000-0000-0000-0000-000000000000"]),
        )

    def list_steps_for_run(self, tenant_id: str, run_id: str) -> list[dict[str, Any]]:
        return fetchall(
            """
            SELECT s.*
            FROM steps s
            JOIN runs r ON r.id = s.run_id
            WHERE r.tenant_id = %s
              AND s.run_id = %s
            ORDER BY s.created_at
            """,
            (tenant_id, run_id),
        )

    def list_tool_calls_for_task(self, tenant_id: str, task_id: str) -> list[dict[str, Any]]:
        return fetchall(
            """
            SELECT *
            FROM tool_calls
            WHERE tenant_id = %s
              AND task_id = %s
            ORDER BY created_at
            """,
            (tenant_id, task_id),
        )

    def list_tool_calls_for_run(self, tenant_id: str, run_id: str) -> list[dict[str, Any]]:
        return fetchall(
            """
            SELECT tc.*
            FROM tool_calls tc
            JOIN runs r ON r.id = tc.run_id
            WHERE r.tenant_id = %s
              AND tc.run_id = %s
            ORDER BY tc.created_at
            """,
            (tenant_id, run_id),
        )

    def list_approvals_for_task(self, tenant_id: str, task_id: str) -> list[dict[str, Any]]:
        return fetchall(
            """
            SELECT *
            FROM approvals
            WHERE tenant_id = %s
              AND task_id = %s
            ORDER BY created_at DESC
            """,
            (tenant_id, task_id),
        )

    def list_artifacts_for_task(self, tenant_id: str, task_id: str) -> list[dict[str, Any]]:
        return fetchall(
            """
            SELECT *
            FROM artifacts
            WHERE tenant_id = %s
              AND task_id = %s
            ORDER BY created_at DESC
            """,
            (tenant_id, task_id),
        )

    def list_cost_for_task(self, tenant_id: str, task_id: str) -> list[dict[str, Any]]:
        return fetchall(
            """
            SELECT *
            FROM cost_ledger
            WHERE tenant_id = %s
              AND task_id = %s
            ORDER BY created_at DESC
            """,
            (tenant_id, task_id),
        )

    def list_cost_for_run(self, tenant_id: str, run_id: str) -> list[dict[str, Any]]:
        return fetchall(
            """
            SELECT cl.*
            FROM cost_ledger cl
            JOIN runs r ON r.id = cl.run_id
            WHERE r.tenant_id = %s
              AND cl.run_id = %s
            ORDER BY cl.created_at DESC
            """,
            (tenant_id, run_id),
        )

    def list_approvals(self, tenant_id: str, status_filter: str) -> list[dict[str, Any]]:
        return fetchall(
            """
            SELECT *
            FROM approvals
            WHERE tenant_id = %s
              AND (%s = '' OR status::text = %s)
            ORDER BY created_at DESC
            LIMIT 200
            """,
            (tenant_id, status_filter, status_filter),
        )

    def get_approval_by_id(self, tenant_id: str, approval_id: str) -> dict[str, Any] | None:
        return fetchone(
            """
            SELECT *
            FROM approvals
            WHERE tenant_id = %s
              AND id = %s
            """,
            (tenant_id, approval_id),
        )

    def get_approval_binding_for_policy(
        self,
        *,
        tenant_id: str,
        approval_id: str,
        task_id: str,
        run_id: str,
    ) -> dict[str, Any] | None:
        return fetchone(
            """
            SELECT id, status, task_id, run_id
            FROM approvals
            WHERE tenant_id = %s
              AND id = %s
              AND task_id = %s
              AND run_id = %s
            """,
            (tenant_id, approval_id, task_id, run_id),
        )

    def set_approval_decision(
        self,
        *,
        tenant_id: str,
        approval_id: str,
        status_text: str,
        decided_by: str,
        reason: str | None,
        edited_output: str | None = None,
    ) -> None:
        if status_text == "EDITED":
            execute(
                """
                UPDATE approvals
                SET status = 'EDITED',
                    decided_by = %s,
                    edited_output = %s,
                    reason = %s,
                    updated_at = NOW()
                WHERE tenant_id = %s
                  AND id = %s
                """,
                (decided_by, edited_output, reason, tenant_id, approval_id),
            )
            return
        execute(
            """
            UPDATE approvals
            SET status = %s::approval_status,
                decided_by = %s,
                reason = %s,
                updated_at = NOW()
            WHERE tenant_id = %s
              AND id = %s
            """,
            (status_text, decided_by, reason, tenant_id, approval_id),
        )

    def apply_approval_decision_with_outbox(
        self,
        *,
        tenant_id: str,
        approval_id: str,
        status_text: str,
        decided_by: str,
        reason: str | None,
        edited_output: str | None,
        signal_payload: dict[str, Any],
    ) -> dict[str, Any]:
        with transaction_cursor() as cur:
            cur.execute(
                """
                SELECT id, run_id, status, edited_output
                FROM approvals
                WHERE tenant_id = %s
                  AND id = %s
                FOR UPDATE
                """,
                (tenant_id, approval_id),
            )
            approval = cur.fetchone()
            if not approval:
                raise LookupError("approval_not_found")

            current_status = str(approval["status"])
            current_edited = str(approval.get("edited_output") or "")
            next_edited = str(edited_output or "")
            edit_decision = status_text == "EDITED" or edited_output is not None
            idempotent = current_status == status_text and (not edit_decision or current_edited == next_edited)

            if not idempotent:
                if current_status != "WAITING_HUMAN":
                    raise ValueError("approval_already_decided")
                if status_text == "EDITED" or (status_text == "APPROVED" and edited_output is not None):
                    cur.execute(
                        """
                        UPDATE approvals
                        SET status = %s::approval_status,
                            decided_by = %s,
                            edited_output = %s,
                            reason = %s,
                            updated_at = NOW()
                        WHERE tenant_id = %s
                          AND id = %s
                        """,
                        (status_text, decided_by, edited_output, reason, tenant_id, approval_id),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE approvals
                        SET status = %s::approval_status,
                            decided_by = %s,
                            reason = %s,
                            updated_at = NOW()
                        WHERE tenant_id = %s
                          AND id = %s
                        """,
                        (status_text, decided_by, reason, tenant_id, approval_id),
                    )

            run_id = str(approval["run_id"])
            cur.execute(
                """
                SELECT workflow_id
                FROM runs
                WHERE tenant_id = %s
                  AND id = %s
                """,
                (tenant_id, run_id),
            )
            run = cur.fetchone()
            workflow_id = str((run or {}).get("workflow_id") or "")
            outbox_id = ""
            outbox_status = "SKIPPED"

            if workflow_id:
                cur.execute(
                    """
                    INSERT INTO approval_signal_outbox (
                      tenant_id, approval_id, run_id, workflow_id, signal_name, signal_payload, status,
                      attempt_count, next_attempt_at, last_error, created_at, updated_at
                    )
                    VALUES (%s, %s, %s, %s, 'approval_signal', %s, 'PENDING', 0, NOW(), NULL, NOW(), NOW())
                    ON CONFLICT (tenant_id, approval_id) DO UPDATE SET
                      run_id = EXCLUDED.run_id,
                      workflow_id = EXCLUDED.workflow_id,
                      signal_name = EXCLUDED.signal_name,
                      signal_payload = EXCLUDED.signal_payload,
                      status = CASE
                        WHEN approval_signal_outbox.status = 'SENT' THEN 'SENT'
                        ELSE 'PENDING'
                      END,
                      attempt_count = CASE
                        WHEN approval_signal_outbox.status = 'SENT' THEN approval_signal_outbox.attempt_count
                        ELSE 0
                      END,
                      next_attempt_at = CASE
                        WHEN approval_signal_outbox.status = 'SENT' THEN approval_signal_outbox.next_attempt_at
                        ELSE NOW()
                      END,
                      last_error = CASE
                        WHEN approval_signal_outbox.status = 'SENT' THEN approval_signal_outbox.last_error
                        ELSE NULL
                      END,
                      updated_at = NOW()
                    RETURNING id, status
                    """,
                    (
                        tenant_id,
                        approval_id,
                        run_id,
                        workflow_id,
                        Jsonb(signal_payload),
                    ),
                )
                outbox = cur.fetchone()
                if outbox:
                    outbox_id = str(outbox["id"])
                    outbox_status = str(outbox["status"])

            return {
                "idempotent": idempotent,
                "run_id": run_id,
                "workflow_id": workflow_id,
                "outbox_id": outbox_id,
                "outbox_status": outbox_status,
            }

    def claim_next_approval_signal_outbox(self) -> dict[str, Any] | None:
        with transaction_cursor() as cur:
            # FAILED is a terminal observation state; only PENDING rows are eligible for dispatch.
            cur.execute(
                """
                SELECT id, tenant_id, approval_id, run_id, workflow_id, signal_payload, attempt_count
                FROM approval_signal_outbox
                WHERE status = 'PENDING'
                  AND next_attempt_at <= NOW()
                ORDER BY next_attempt_at ASC, created_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
                """
            )
            row = cur.fetchone()
            if not row:
                return None
            cur.execute(
                """
                UPDATE approval_signal_outbox
                SET status = 'SENDING',
                    updated_at = NOW()
                WHERE id = %s
                RETURNING id, tenant_id, approval_id, run_id, workflow_id, signal_payload, attempt_count
                """,
                (str(row["id"]),),
            )
            return cur.fetchone()

    def mark_approval_signal_sent(self, outbox_id: str) -> None:
        execute(
            """
            UPDATE approval_signal_outbox
            SET status = 'SENT',
                sent_at = NOW(),
                last_error = NULL,
                updated_at = NOW()
            WHERE id = %s
            """,
            (outbox_id,),
        )

    def mark_approval_signal_failure(
        self,
        *,
        outbox_id: str,
        error_message: str,
        retry_delay_s: int,
        max_attempts: int,
    ) -> dict[str, Any]:
        row = fetchone(
            """
            UPDATE approval_signal_outbox
            SET attempt_count = attempt_count + 1,
                status = CASE
                  WHEN attempt_count + 1 >= %s THEN 'FAILED'
                  ELSE 'PENDING'
                END,
                next_attempt_at = CASE
                  WHEN attempt_count + 1 >= %s THEN NOW()
                  ELSE NOW() + make_interval(secs => %s)
                END,
                last_error = %s,
                updated_at = NOW()
            WHERE id = %s
            RETURNING status, attempt_count, next_attempt_at
            """,
            (max_attempts, max_attempts, retry_delay_s, error_message[:500], outbox_id),
        )
        return row or {"status": "FAILED", "attempt_count": max_attempts, "next_attempt_at": None}

    def get_approval_signal_outbox(
        self,
        *,
        tenant_id: str,
        approval_id: str,
    ) -> dict[str, Any] | None:
        return fetchone(
            """
            SELECT *
            FROM approval_signal_outbox
            WHERE tenant_id = %s
              AND approval_id = %s
            """,
            (tenant_id, approval_id),
        )

    def get_run_workflow(self, tenant_id: str, run_id: str) -> dict[str, Any] | None:
        return fetchone(
            """
            SELECT workflow_id
            FROM runs
            WHERE tenant_id = %s
              AND id = %s
            """,
            (tenant_id, run_id),
        )

    def insert_audit_log(
        self,
        *,
        tenant_id: str,
        actor_user_id: str | None,
        action: str,
        target_type: str,
        target_id: str,
        detail_masked: dict[str, Any],
        trace_id: str,
    ) -> None:
        execute(
            """
            INSERT INTO audit_log (tenant_id, actor_user_id, action, target_type, target_id, detail_masked, trace_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (tenant_id, actor_user_id, action, target_type, target_id, Jsonb(detail_masked), trace_id),
        )

    def list_audit_logs(self, tenant_id: str, limit: int) -> list[dict[str, Any]]:
        return fetchall(
            """
            SELECT *
            FROM audit_log
            WHERE tenant_id = %s
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (tenant_id, limit),
        )

    def list_audit_tool_calls(self, tenant_id: str, limit: int) -> list[dict[str, Any]]:
        return fetchall(
            """
            SELECT tool_call_id, run_id, task_id, tool_id, caller_user_id, status, reason_code, trace_id, duration_ms, created_at
            FROM tool_calls
            WHERE tenant_id = %s
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (tenant_id, limit),
        )

    def metrics_summary(self, tenant_id: str) -> dict[str, Any]:
        summary = fetchone(
            """
            SELECT
              COUNT(*)::int AS total,
              COUNT(*) FILTER (WHERE status = 'SUCCEEDED')::int AS succeeded,
              COUNT(*) FILTER (WHERE status IN ('FAILED_FINAL','FAILED_RETRYABLE','TIMED_OUT','CANCELLED'))::int AS failed,
              COALESCE(
                percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (COALESCE(ended_at, NOW()) - started_at))),
                0
              ) AS p95_latency_seconds
            FROM runs
            WHERE tenant_id = %s
              AND created_at >= NOW() - INTERVAL '24 hours'
            """,
            (tenant_id,),
        ) or {}
        retries = fetchone(
            """
            SELECT
              COUNT(*) FILTER (WHERE s.attempt > 1)::int AS retry_events,
              COUNT(*)::int AS total_steps
            FROM steps s
            JOIN runs r ON r.id = s.run_id
            WHERE r.tenant_id = %s
              AND r.created_at >= NOW() - INTERVAL '24 hours'
            """,
            (tenant_id,),
        ) or {}
        cost = fetchone(
            """
            SELECT
              COALESCE(SUM(amount), 0) AS total_cost,
              COALESCE(SUM(token_in), 0)::int AS token_in,
              COALESCE(SUM(token_out), 0)::int AS token_out
            FROM cost_ledger
            WHERE tenant_id = %s
              AND created_at >= NOW() - INTERVAL '24 hours'
            """,
            (tenant_id,),
        ) or {}
        total = float(summary.get("total") or 0)
        success = float(summary.get("succeeded") or 0)
        failure = float(summary.get("failed") or 0)
        retry_events = float(retries.get("retry_events") or 0)
        total_steps = float(retries.get("total_steps") or 0)
        return {
            "p95_latency_seconds": float(summary.get("p95_latency_seconds") or 0),
            "success_rate": (success / total) if total else 0.0,
            "failure_rate": (failure / total) if total else 0.0,
            "retry_rate": (retry_events / total_steps) if total_steps else 0.0,
            "token_in": int(cost.get("token_in") or 0),
            "token_out": int(cost.get("token_out") or 0),
            "total_cost": float(cost.get("total_cost") or 0),
        }

    def metrics_cost_rows(self, tenant_id: str) -> list[dict[str, Any]]:
        return fetchall(
            """
            SELECT date_trunc('day', created_at) AS day, category, SUM(amount) AS amount
            FROM cost_ledger
            WHERE tenant_id = %s
            GROUP BY 1, 2
            ORDER BY 1 DESC
            LIMIT 100
            """,
            (tenant_id,),
        )

    def list_new_steps_for_sse(self, tenant_id: str, task_id: str, last_step_id: int) -> list[dict[str, Any]]:
        return fetchall(
            """
            SELECT s.id, s.run_id, s.step_key, s.status, s.created_at
            FROM steps s
            JOIN runs r ON r.id = s.run_id
            WHERE r.tenant_id = %s
              AND r.task_id = %s
              AND s.id > %s
            ORDER BY s.id
            """,
            (tenant_id, task_id, last_step_id),
        )

    def get_run_binding(self, tenant_id: str, task_id: str, run_id: str) -> dict[str, Any] | None:
        return fetchone(
            """
            SELECT r.id, r.task_id, r.status, r.assigned_worker
            FROM runs r
            JOIN tasks t ON t.id = r.task_id AND t.tenant_id = r.tenant_id
            WHERE r.tenant_id = %s
              AND r.id = %s
              AND t.id = %s
            """,
            (tenant_id, run_id, task_id),
        )

    def get_run_binding_any_tenant(self, task_id: str, run_id: str) -> dict[str, Any] | None:
        return fetchone(
            """
            SELECT r.id, r.task_id, r.tenant_id, r.status, r.assigned_worker
            FROM runs r
            JOIN tasks t ON t.id = r.task_id AND t.tenant_id = r.tenant_id
            WHERE r.id = %s
              AND t.id = %s
            """,
            (run_id, task_id),
        )

    def get_run_by_workflow_id(self, tenant_id: str, workflow_id: str) -> dict[str, Any] | None:
        return fetchone(
            """
            SELECT *
            FROM runs
            WHERE tenant_id = %s
              AND workflow_id = %s
            """,
            (tenant_id, workflow_id),
        )


class AssistantConversationRepository:
    def list_conversations_for_user(
        self,
        *,
        tenant_id: str,
        user_id: str,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        rows = fetchall(
            """
            SELECT
              c.conversation_id,
              c.tenant_id,
              c.user_id,
              c.title,
              c.message_history,
              c.last_task_result,
              c.last_tool_result,
              c.user_preferences,
              c.created_at,
              c.updated_at,
              COALESCE(task_stats.task_count, 0)::int AS task_count,
              COALESCE(task_stats.running_task_count, 0)::int AS running_task_count,
              COALESCE(task_stats.waiting_approval_count, 0)::int AS waiting_approval_count
            FROM assistant_conversations c
            LEFT JOIN LATERAL (
              SELECT
                COUNT(*)::int AS task_count,
                COUNT(*) FILTER (
                  WHERE t.status NOT IN ('SUCCEEDED', 'FAILED_RETRYABLE', 'FAILED_FINAL', 'CANCELLED', 'TIMED_OUT')
                )::int AS running_task_count,
                COUNT(*) FILTER (
                  WHERE EXISTS (
                    SELECT 1
                    FROM approvals a
                    WHERE a.tenant_id = t.tenant_id
                      AND a.task_id = t.id
                      AND a.status = 'WAITING_HUMAN'
                  )
                )::int AS waiting_approval_count
              FROM tasks t
              WHERE t.tenant_id = c.tenant_id
                AND t.created_by = c.user_id
                AND COALESCE(t.conversation_id, t.input_masked ->> 'conversation_id', '') = c.conversation_id
            ) task_stats ON TRUE
            WHERE c.tenant_id = %s
              AND c.user_id = %s
            ORDER BY c.updated_at DESC
            LIMIT %s
            """,
            (tenant_id, user_id, max(1, int(limit))),
        )
        for row in rows:
            row["user_id"] = str(row["user_id"])
        return rows

    def get_conversation(self, *, tenant_id: str, conversation_id: str) -> dict[str, Any] | None:
        return fetchone(
            """
            SELECT
              conversation_id, tenant_id, user_id, title, message_history,
              last_task_result, last_tool_result, user_preferences,
              created_at, updated_at
            FROM assistant_conversations
            WHERE tenant_id = %s
              AND conversation_id = %s
            """,
            (tenant_id, conversation_id),
        )

    def get_or_create_conversation(
        self,
        *,
        tenant_id: str,
        user_id: str,
        conversation_id: str,
    ) -> dict[str, Any]:
        with transaction_cursor() as cur:
            cur.execute(
                """
                SELECT
                  conversation_id, tenant_id, user_id, title, message_history,
                  last_task_result, last_tool_result, user_preferences,
                  created_at, updated_at
                FROM assistant_conversations
                WHERE tenant_id = %s
                  AND conversation_id = %s
                FOR UPDATE
                """,
                (tenant_id, conversation_id),
            )
            row = cur.fetchone()
            if row:
                if str(row["user_id"]) != str(user_id):
                    raise PermissionError("conversation ownership mismatch")
                return row
            cur.execute(
                """
                INSERT INTO assistant_conversations (
                  conversation_id, tenant_id, user_id, title, message_history,
                  last_task_result, last_tool_result, user_preferences,
                  created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                RETURNING
                  conversation_id, tenant_id, user_id, title, message_history,
                  last_task_result, last_tool_result, user_preferences,
                  created_at, updated_at
                """,
                (
                    conversation_id,
                    tenant_id,
                    user_id,
                    None,
                    Jsonb([]),
                    Jsonb({}),
                    Jsonb({}),
                    Jsonb({}),
                ),
            )
            inserted = cur.fetchone()
            assert inserted is not None
            return inserted

    def append_message(
        self,
        *,
        tenant_id: str,
        user_id: str,
        conversation_id: str,
        role: str,
        message: str,
        route: str,
        metadata: dict[str, Any] | None,
        created_at: str,
        max_messages: int,
    ) -> list[dict[str, Any]]:
        with transaction_cursor() as cur:
            cur.execute(
                """
                SELECT user_id, title, message_history
                FROM assistant_conversations
                WHERE tenant_id = %s
                  AND conversation_id = %s
                FOR UPDATE
                """,
                (tenant_id, conversation_id),
            )
            row = cur.fetchone()
            if not row:
                raise LookupError("conversation not found")
            if str(row["user_id"]) != str(user_id):
                raise PermissionError("conversation ownership mismatch")

            history = list(row.get("message_history") or [])
            next_title = str(row.get("title") or "").strip() or None
            item: dict[str, Any] = {
                "role": role,
                "message": message,
                "route": route,
                "created_at": created_at,
            }
            if metadata:
                item["metadata"] = mask_payload(metadata)
            if role == "user" and not next_title:
                has_prior_user_message = any(str(existing.get("role") or "") == "user" for existing in history)
                if not has_prior_user_message:
                    next_title = _conversation_title_seed(message)
            history.append(item)
            if max_messages > 0:
                history = history[-max_messages:]

            cur.execute(
                """
                UPDATE assistant_conversations
                SET message_history = %s,
                    title = %s,
                    updated_at = NOW()
                WHERE tenant_id = %s
                  AND conversation_id = %s
                """,
                (Jsonb(history), next_title, tenant_id, conversation_id),
            )
            return history

    def update_title(
        self,
        *,
        tenant_id: str,
        user_id: str,
        conversation_id: str,
        title: str | None,
    ) -> dict[str, Any]:
        normalized_title = _conversation_title_seed(title or "", max_len=120) if title else None
        with transaction_cursor() as cur:
            cur.execute(
                """
                SELECT user_id
                FROM assistant_conversations
                WHERE tenant_id = %s
                  AND conversation_id = %s
                FOR UPDATE
                """,
                (tenant_id, conversation_id),
            )
            row = cur.fetchone()
            if not row:
                raise LookupError("conversation not found")
            if str(row["user_id"]) != str(user_id):
                raise PermissionError("conversation ownership mismatch")

            cur.execute(
                """
                UPDATE assistant_conversations
                SET title = %s
                WHERE tenant_id = %s
                  AND conversation_id = %s
                RETURNING
                  conversation_id, tenant_id, user_id, title, message_history,
                  last_task_result, last_tool_result, user_preferences,
                  created_at, updated_at
                """,
                (normalized_title, tenant_id, conversation_id),
            )
            updated = cur.fetchone()
            assert updated is not None
            return updated

    def delete_conversation(
        self,
        *,
        tenant_id: str,
        user_id: str,
        conversation_id: str,
    ) -> None:
        with transaction_cursor() as cur:
            cur.execute(
                """
                SELECT
                  c.user_id,
                  COALESCE(active_tasks.active_task_count, 0)::int AS active_task_count
                FROM assistant_conversations c
                LEFT JOIN LATERAL (
                  SELECT COUNT(*)::int AS active_task_count
                  FROM tasks t
                  WHERE t.tenant_id = c.tenant_id
                    AND t.created_by = c.user_id
                    AND COALESCE(t.conversation_id, t.input_masked ->> 'conversation_id', '') = c.conversation_id
                    AND t.status NOT IN ('SUCCEEDED', 'FAILED_RETRYABLE', 'FAILED_FINAL', 'CANCELLED', 'TIMED_OUT')
                ) active_tasks ON TRUE
                WHERE c.tenant_id = %s
                  AND c.conversation_id = %s
                FOR UPDATE
                """,
                (tenant_id, conversation_id),
            )
            row = cur.fetchone()
            if not row:
                raise LookupError("conversation not found")
            if str(row["user_id"]) != str(user_id):
                raise PermissionError("conversation ownership mismatch")
            if int(row.get("active_task_count") or 0) > 0:
                raise RuntimeError("conversation has active tasks")

            cur.execute(
                """
                DELETE FROM agent_subgoals
                WHERE tenant_id = %s
                  AND goal_id IN (
                    SELECT goal_id
                    FROM agent_goals
                    WHERE tenant_id = %s
                      AND user_id = %s
                      AND COALESCE(conversation_id, '') = %s
                  )
                """,
                (tenant_id, tenant_id, user_id, conversation_id),
            )
            cur.execute(
                """
                DELETE FROM agent_goals
                WHERE tenant_id = %s
                  AND user_id = %s
                  AND COALESCE(conversation_id, '') = %s
                """,
                (tenant_id, user_id, conversation_id),
            )
            cur.execute(
                """
                DELETE FROM assistant_episodes
                WHERE tenant_id = %s
                  AND user_id = %s
                  AND COALESCE(conversation_id, '') = %s
                """,
                (tenant_id, user_id, conversation_id),
            )
            cur.execute(
                """
                DELETE FROM assistant_turns
                WHERE tenant_id = %s
                  AND user_id = %s
                  AND conversation_id = %s
                """,
                (tenant_id, user_id, conversation_id),
            )
            cur.execute(
                """
                DELETE FROM assistant_conversations
                WHERE tenant_id = %s
                  AND user_id = %s
                  AND conversation_id = %s
                """,
                (tenant_id, user_id, conversation_id),
            )

    def update_memory(
        self,
        *,
        tenant_id: str,
        user_id: str,
        conversation_id: str,
        last_task_result: dict[str, Any] | None = None,
        last_tool_result: dict[str, Any] | None = None,
        user_preferences: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with transaction_cursor() as cur:
            cur.execute(
                """
                SELECT user_id, last_task_result, last_tool_result, user_preferences
                FROM assistant_conversations
                WHERE tenant_id = %s
                  AND conversation_id = %s
                FOR UPDATE
                """,
                (tenant_id, conversation_id),
            )
            row = cur.fetchone()
            if not row:
                raise LookupError("conversation not found")
            if str(row["user_id"]) != str(user_id):
                raise PermissionError("conversation ownership mismatch")

            next_last_task = dict(row.get("last_task_result") or {})
            next_last_tool = dict(row.get("last_tool_result") or {})
            next_prefs = dict(row.get("user_preferences") or {})

            if last_task_result is not None:
                next_last_task = mask_payload(last_task_result)
            if last_tool_result is not None:
                next_last_tool = mask_payload(last_tool_result)
            if user_preferences is not None:
                next_prefs = mask_payload(user_preferences)

            cur.execute(
                """
                UPDATE assistant_conversations
                SET last_task_result = %s,
                    last_tool_result = %s,
                    user_preferences = %s,
                    updated_at = NOW()
                WHERE tenant_id = %s
                  AND conversation_id = %s
                RETURNING last_task_result, last_tool_result, user_preferences
                """,
                (
                    Jsonb(next_last_task),
                    Jsonb(next_last_tool),
                    Jsonb(next_prefs),
                    tenant_id,
                    conversation_id,
                ),
            )
            out = cur.fetchone()
            assert out is not None
            return out


class AssistantTurnRepository:
    def create_turn(
        self,
        *,
        tenant_id: str,
        turn_id: str,
        conversation_id: str,
        user_id: str,
        route: str,
        status: str,
        current_phase: str,
        response_type: str,
        user_message: str,
        assistant_message: str | None,
        task_id: str | None,
        trace_id: str,
        runtime_state: dict[str, Any],
    ) -> dict[str, Any]:
        row = fetchone(
            """
            INSERT INTO assistant_turns (
              turn_id, tenant_id, conversation_id, user_id, route, status, current_phase,
              response_type, user_message, assistant_message, task_id, trace_id, runtime_state
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
            """,
            (
                turn_id,
                tenant_id,
                conversation_id,
                user_id,
                route,
                status,
                current_phase,
                response_type,
                user_message,
                assistant_message,
                task_id,
                trace_id,
                Jsonb(runtime_state),
            ),
        )
        assert row is not None
        row["user_id"] = str(row["user_id"])
        if row.get("task_id") is not None:
            row["task_id"] = str(row["task_id"])
        return row

    def update_turn(
        self,
        *,
        tenant_id: str,
        turn_id: str,
        route: str,
        status: str,
        current_phase: str,
        response_type: str,
        assistant_message: str | None,
        task_id: str | None,
        runtime_state: dict[str, Any],
    ) -> dict[str, Any]:
        row = fetchone(
            """
            UPDATE assistant_turns
            SET route = %s,
                status = %s,
                current_phase = %s,
                response_type = %s,
                assistant_message = %s,
                task_id = %s,
                runtime_state = %s,
                updated_at = NOW()
            WHERE tenant_id = %s
              AND turn_id = %s
            RETURNING *
            """,
            (
                route,
                status,
                current_phase,
                response_type,
                assistant_message,
                task_id,
                Jsonb(runtime_state),
                tenant_id,
                turn_id,
            ),
        )
        if not row:
            raise LookupError("assistant turn not found")
        row["user_id"] = str(row["user_id"])
        if row.get("task_id") is not None:
            row["task_id"] = str(row["task_id"])
        return row

    def get_turn(self, *, tenant_id: str, turn_id: str) -> dict[str, Any] | None:
        row = fetchone(
            """
            SELECT *
            FROM assistant_turns
            WHERE tenant_id = %s
              AND turn_id = %s
            """,
            (tenant_id, turn_id),
        )
        if not row:
            return None
        row["user_id"] = str(row["user_id"])
        if row.get("task_id") is not None:
            row["task_id"] = str(row["task_id"])
        return row

    def list_turns_for_conversation(
        self,
        *,
        tenant_id: str,
        user_id: str,
        conversation_id: str,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        rows = fetchall(
            """
            SELECT *
            FROM assistant_turns
            WHERE tenant_id = %s
              AND user_id = %s
              AND conversation_id = %s
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (tenant_id, user_id, conversation_id, max(1, int(limit))),
        )
        for row in rows:
            row["user_id"] = str(row["user_id"])
            if row.get("task_id") is not None:
                row["task_id"] = str(row["task_id"])
        return rows


class AssistantEpisodeRepository:
    def upsert_episode(
        self,
        *,
        tenant_id: str,
        user_id: str,
        conversation_id: str | None,
        turn_id: str | None,
        task_id: str | None,
        episode: dict[str, Any],
    ) -> dict[str, Any]:
        row = fetchone(
            """
            INSERT INTO assistant_episodes (
              episode_id, tenant_id, user_id, conversation_id, turn_id, task_id,
              normalized_goal, task_summary, chosen_strategy, action_types, tool_names,
              outcome_status, final_outcome, useful_lessons, episode_payload
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (tenant_id, episode_id) DO UPDATE SET
              conversation_id = EXCLUDED.conversation_id,
              turn_id = EXCLUDED.turn_id,
              task_id = EXCLUDED.task_id,
              normalized_goal = EXCLUDED.normalized_goal,
              task_summary = EXCLUDED.task_summary,
              chosen_strategy = EXCLUDED.chosen_strategy,
              action_types = EXCLUDED.action_types,
              tool_names = EXCLUDED.tool_names,
              outcome_status = EXCLUDED.outcome_status,
              final_outcome = EXCLUDED.final_outcome,
              useful_lessons = EXCLUDED.useful_lessons,
              episode_payload = EXCLUDED.episode_payload,
              updated_at = NOW()
            RETURNING *
            """,
            (
                str(episode.get("episode_id") or ""),
                tenant_id,
                user_id,
                conversation_id,
                turn_id,
                task_id,
                str(episode.get("normalized_goal") or episode.get("task_summary") or ""),
                str(episode.get("task_summary") or ""),
                str(episode.get("chosen_strategy") or ""),
                list(episode.get("action_types") or []),
                list(episode.get("tool_names") or []),
                str(episode.get("outcome_status") or ""),
                str(episode.get("final_outcome") or ""),
                list(episode.get("useful_lessons") or []),
                Jsonb(dict(episode.get("episode_payload") or {})),
            ),
        )
        assert row is not None
        row["user_id"] = str(row["user_id"])
        if row.get("task_id") is not None:
            row["task_id"] = str(row["task_id"])
        return row

    def list_recent_episodes_for_user(self, *, tenant_id: str, user_id: str, limit: int = 30) -> list[dict[str, Any]]:
        rows = fetchall(
            """
            SELECT *
            FROM assistant_episodes
            WHERE tenant_id = %s
              AND user_id = %s
            ORDER BY updated_at DESC, created_at DESC
            LIMIT %s
            """,
            (tenant_id, user_id, max(1, int(limit))),
        )
        for row in rows:
            row["user_id"] = str(row["user_id"])
            if row.get("task_id") is not None:
                row["task_id"] = str(row["task_id"])
        return rows


class GoalRepository:
    _SCHEDULABLE_WHERE_SQL = """
        g.status IN ('ACTIVE', 'WAITING')
        AND (
          COALESCE(g.goal_state -> 'portfolio' ->> 'hold_status', '') NOT IN ('HELD', 'PREEMPTING')
          OR COALESCE(NULLIF(g.goal_state -> 'portfolio' ->> 'hold_until', ''), '1970-01-01T00:00:00+00:00')::timestamptz <= NOW()
        )
        AND (
          COALESCE(g.goal_state -> 'reflection' ->> 'next_action', '') IN ('workflow_call', 'replan')
          OR COALESCE(g.goal_state -> 'current_action' ->> 'action_type', '') IN ('workflow_call', 'replan')
          OR COALESCE((g.goal_state -> 'event_timeouts' ->> 'expired_required_count')::int, 0) > 0
        )
        AND (
          COALESCE(g.goal_state -> 'current_action' ->> 'action_type', '') NOT IN ('ask_user', 'wait', 'approval_request', 'respond')
          OR COALESCE((g.goal_state -> 'event_timeouts' ->> 'expired_required_count')::int, 0) > 0
        )
        AND COALESCE(g.goal_state -> 'active_subgoal' ->> 'status', 'PENDING') IN ('ACTIVE', 'WAITING', 'PENDING')
        AND COALESCE(g.goal_state -> 'active_subgoal' -> 'dependency_status' ->> 'satisfied', 'true') = 'true'
        AND COALESCE(g.goal_state -> 'wake_condition' ->> 'kind', 'none') IN ('scheduler_cooldown', 'none')
        AND (
          g.current_task_id IS NULL
          OR t.status IN ('SUCCEEDED', 'FAILED_RETRYABLE', 'FAILED_FINAL', 'CANCELLED', 'TIMED_OUT')
        )
        AND g.last_active_at <= NOW() - make_interval(secs => %s)
    """

    def get_goal(self, *, tenant_id: str, goal_id: str) -> dict[str, Any] | None:
        return fetchone(
            """
            SELECT *
            FROM agent_goals
            WHERE tenant_id = %s
              AND goal_id = %s
            """,
            (tenant_id, goal_id),
        )

    def find_open_goal(
        self,
        *,
        tenant_id: str,
        user_id: str,
        conversation_id: str | None,
        normalized_goal: str,
    ) -> dict[str, Any] | None:
        return fetchone(
            """
            SELECT *
            FROM agent_goals
            WHERE tenant_id = %s
              AND user_id = %s
              AND COALESCE(conversation_id, '') = COALESCE(%s, '')
              AND normalized_goal = %s
              AND status IN ('ACTIVE', 'WAITING')
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (tenant_id, user_id, conversation_id, normalized_goal),
        )

    def create_goal(
        self,
        *,
        tenant_id: str,
        goal_id: str,
        user_id: str,
        conversation_id: str | None,
        normalized_goal: str,
        status: str,
        goal_state: dict[str, Any],
        current_task_id: str | None,
        last_turn_id: str | None,
        policy_version_id: str | None,
    ) -> dict[str, Any]:
        row = fetchone(
            """
            INSERT INTO agent_goals (
              tenant_id, goal_id, user_id, conversation_id, normalized_goal, status,
              goal_state, current_task_id, last_turn_id, continuation_count, policy_version_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 0, %s)
            RETURNING *
            """,
            (
                tenant_id,
                goal_id,
                user_id,
                conversation_id,
                normalized_goal,
                status,
                Jsonb(goal_state),
                current_task_id,
                last_turn_id,
                policy_version_id,
            ),
        )
        assert row is not None
        return row

    def update_goal(
        self,
        *,
        tenant_id: str,
        goal_id: str,
        status: str,
        goal_state: dict[str, Any],
        current_task_id: str | None,
        last_turn_id: str | None,
        continuation_count: int,
        policy_version_id: str | None,
    ) -> None:
        execute(
            """
            UPDATE agent_goals
            SET status = %s,
                goal_state = %s,
                current_task_id = %s,
                last_turn_id = %s,
                continuation_count = %s,
                policy_version_id = %s,
                updated_at = NOW(),
                last_active_at = NOW()
            WHERE tenant_id = %s
              AND goal_id = %s
            """,
            (
                status,
                Jsonb(goal_state),
                current_task_id,
                last_turn_id,
                continuation_count,
                policy_version_id,
                tenant_id,
                goal_id,
            ),
        )

    def list_goals_for_user(
        self,
        *,
        tenant_id: str,
        user_id: str,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        return fetchall(
            """
            SELECT *
            FROM agent_goals
            WHERE tenant_id = %s
              AND user_id = %s
            ORDER BY updated_at DESC
            LIMIT %s
            """,
            (tenant_id, user_id, max(1, int(limit))),
        )

    def list_goals_waiting_on_event(
        self,
        *,
        tenant_id: str,
        event_kind: str,
        event_key: str,
        user_id: str | None = None,
        conversation_id: str | None = None,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        query = """
        SELECT *
        FROM agent_goals
        WHERE tenant_id = %s
          AND status IN ('ACTIVE', 'WAITING')
          AND (
            (
              COALESCE(goal_state -> 'wake_condition' ->> 'kind', '') = %s
              AND COALESCE(goal_state -> 'wake_condition' ->> 'event_key', '') = %s
            )
            OR EXISTS (
              SELECT 1
              FROM jsonb_array_elements(COALESCE(goal_state -> 'event_subscriptions', '[]'::JSONB)) AS sub
              WHERE COALESCE(sub ->> 'kind', '') = %s
                AND COALESCE(sub ->> 'event_key', '') = %s
                AND COALESCE(sub ->> 'status', 'pending') = 'pending'
            )
          )
        """
        params: list[Any] = [tenant_id, event_kind, event_key, event_kind, event_key]
        if user_id:
            query += " AND user_id = %s"
            params.append(user_id)
        if conversation_id is not None:
            query += " AND COALESCE(conversation_id, '') = COALESCE(%s, '')"
            params.append(conversation_id)
        query += " ORDER BY updated_at ASC LIMIT %s"
        params.append(max(1, int(limit)))
        return fetchall(query, tuple(params))

    def claim_next_schedulable_goal(
        self,
        *,
        cooldown_s: int,
    ) -> dict[str, Any] | None:
        row = fetchone(
            """
            WITH candidate AS (
              SELECT g.id
              FROM agent_goals g
              LEFT JOIN tasks t
                ON t.tenant_id = g.tenant_id
               AND t.id = g.current_task_id
              WHERE """ + self._SCHEDULABLE_WHERE_SQL + """
              ORDER BY
                COALESCE((g.goal_state -> 'agenda' ->> 'priority_score')::DOUBLE PRECISION, 0.0) DESC,
                g.updated_at ASC
              LIMIT 1
              FOR UPDATE SKIP LOCKED
            )
            UPDATE agent_goals g
            SET updated_at = NOW(),
                last_active_at = NOW(),
                goal_state = jsonb_set(
                  COALESCE(g.goal_state, '{}'::JSONB),
                  '{scheduler}',
                  COALESCE(g.goal_state -> 'scheduler', '{}'::JSONB) || jsonb_build_object('claimed_at', NOW()::TEXT),
                  true
                )
            FROM candidate
            WHERE g.id = candidate.id
            RETURNING g.*
            """,
            (max(0, int(cooldown_s)),),
        )
        return row

    def list_schedulable_goals(
        self,
        *,
        cooldown_s: int,
        limit: int,
    ) -> list[dict[str, Any]]:
        return fetchall(
            """
            SELECT g.*
            FROM agent_goals g
            LEFT JOIN tasks t
              ON t.tenant_id = g.tenant_id
             AND t.id = g.current_task_id
            WHERE """ + self._SCHEDULABLE_WHERE_SQL + """
            ORDER BY g.updated_at ASC, g.created_at ASC
            LIMIT %s
            """,
            (max(0, int(cooldown_s)), max(1, int(limit))),
        )

    def count_goals_with_live_task(self) -> int:
        row = fetchone(
            """
            SELECT COUNT(*)::int AS c
            FROM agent_goals g
            JOIN tasks t
              ON t.tenant_id = g.tenant_id
             AND t.id = g.current_task_id
            WHERE g.status IN ('ACTIVE', 'WAITING')
              AND COALESCE(g.goal_state -> 'portfolio' ->> 'hold_status', '') NOT IN ('HELD', 'PREEMPTING')
              AND t.status NOT IN ('SUCCEEDED', 'FAILED_RETRYABLE', 'FAILED_FINAL', 'CANCELLED', 'TIMED_OUT')
            """
        )
        return int((row or {}).get("c") or 0)

    def list_live_goals(self, *, limit: int = 20) -> list[dict[str, Any]]:
        return fetchall(
            """
            SELECT g.*
            FROM agent_goals g
            JOIN tasks t
              ON t.tenant_id = g.tenant_id
             AND t.id = g.current_task_id
            WHERE g.status IN ('ACTIVE', 'WAITING')
              AND t.status NOT IN ('SUCCEEDED', 'FAILED_RETRYABLE', 'FAILED_FINAL', 'CANCELLED', 'TIMED_OUT')
            ORDER BY
              COALESCE((g.goal_state -> 'agenda' ->> 'priority_score')::DOUBLE PRECISION, 0.0) ASC,
              g.updated_at ASC
            LIMIT %s
            """,
            (max(1, int(limit)),),
        )

    def claim_goal_for_scheduler(
        self,
        *,
        tenant_id: str,
        goal_id: str,
        cooldown_s: int,
    ) -> dict[str, Any] | None:
        return fetchone(
            """
            WITH candidate AS (
              SELECT g.id
              FROM agent_goals g
              LEFT JOIN tasks t
                ON t.tenant_id = g.tenant_id
               AND t.id = g.current_task_id
              WHERE g.tenant_id = %s
                AND g.goal_id = %s
                AND """ + self._SCHEDULABLE_WHERE_SQL + """
              LIMIT 1
              FOR UPDATE SKIP LOCKED
            )
            UPDATE agent_goals g
            SET updated_at = NOW(),
                last_active_at = NOW(),
                goal_state = jsonb_set(
                  COALESCE(g.goal_state, '{}'::JSONB),
                  '{scheduler}',
                  COALESCE(g.goal_state -> 'scheduler', '{}'::JSONB) || jsonb_build_object('claimed_at', NOW()::TEXT),
                  true
                )
            FROM candidate
            WHERE g.id = candidate.id
            RETURNING g.*
            """,
            (tenant_id, goal_id, max(0, int(cooldown_s))),
        )

    def update_goal_portfolio(
        self,
        *,
        tenant_id: str,
        goal_id: str,
        portfolio: dict[str, Any],
    ) -> None:
        execute(
            """
            UPDATE agent_goals
            SET goal_state = jsonb_set(
                  COALESCE(goal_state, '{}'::JSONB),
                  '{portfolio}',
                  %s,
                  true
                ),
                updated_at = NOW()
            WHERE tenant_id = %s
              AND goal_id = %s
            """,
            (Jsonb(portfolio), tenant_id, goal_id),
        )

    def attach_task_to_goal(
        self,
        *,
        tenant_id: str,
        goal_id: str,
        task_id: str,
        goal_state: dict[str, Any],
    ) -> None:
        execute(
            """
            UPDATE agent_goals
            SET current_task_id = %s,
                status = 'ACTIVE',
                goal_state = %s,
                updated_at = NOW(),
                last_active_at = NOW()
            WHERE tenant_id = %s
              AND goal_id = %s
            """,
            (task_id, Jsonb(goal_state), tenant_id, goal_id),
        )

    def replace_subgoals(
        self,
        *,
        tenant_id: str,
        goal_id: str,
        subgoals: list[dict[str, Any]],
    ) -> None:
        with transaction_cursor() as cur:
            cur.execute(
                """
                DELETE FROM agent_subgoals
                WHERE tenant_id = %s
                  AND goal_id = %s
                """,
                (tenant_id, goal_id),
            )
            for row in subgoals:
                cur.execute(
                    """
                    INSERT INTO agent_subgoals (
                      tenant_id, subgoal_id, goal_id, sequence_no, title, status, depends_on, checkpoint_payload, wake_condition
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        tenant_id,
                        str(row.get("subgoal_id") or ""),
                        goal_id,
                        int(row.get("sequence_no") or 0),
                        str(row.get("title") or ""),
                        str(row.get("status") or "PENDING"),
                        Jsonb(list(row.get("depends_on") or [])),
                        Jsonb(dict(row.get("checkpoint_payload") or {})),
                        Jsonb(dict(row.get("wake_condition") or {})),
                    ),
                )

    def list_subgoals(self, *, tenant_id: str, goal_id: str) -> list[dict[str, Any]]:
        return fetchall(
            """
            SELECT *
            FROM agent_subgoals
            WHERE tenant_id = %s
              AND goal_id = %s
            ORDER BY sequence_no ASC, created_at ASC
            """,
            (tenant_id, goal_id),
        )


class PolicyMemoryRepository:
    def get_policy_version(self, *, tenant_id: str, version_id: str) -> dict[str, Any] | None:
        return fetchone(
            """
            SELECT *
            FROM agent_policy_versions
            WHERE tenant_id = %s
              AND version_id = %s
            """,
            (tenant_id, version_id),
        )

    def get_active_version(self, *, tenant_id: str) -> dict[str, Any] | None:
        return fetchone(
            """
            SELECT *
            FROM agent_policy_versions
            WHERE tenant_id = %s
              AND status = 'ACTIVE'
            ORDER BY activated_at DESC NULLS LAST, updated_at DESC
            LIMIT 1
            """,
            (tenant_id,),
        )

    def get_candidate_version(self, *, tenant_id: str) -> dict[str, Any] | None:
        return fetchone(
            """
            SELECT *
            FROM agent_policy_versions
            WHERE tenant_id = %s
              AND status IN ('CANDIDATE', 'CANARY')
            ORDER BY CASE status WHEN 'CANDIDATE' THEN 0 ELSE 1 END, updated_at DESC
            LIMIT 1
            """,
            (tenant_id,),
        )

    def create_policy_version(
        self,
        *,
        tenant_id: str,
        version_id: str,
        version_tag: str,
        status: str,
        base_version_id: str | None,
        source: str,
        memory_payload: dict[str, Any],
        comparison_payload: dict[str, Any],
        created_by: str | None,
    ) -> dict[str, Any]:
        row = fetchone(
            """
            INSERT INTO agent_policy_versions (
              tenant_id, version_id, version_tag, status, base_version_id, source,
              memory_payload, comparison_payload, created_by, activated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CASE WHEN %s = 'ACTIVE' THEN NOW() ELSE NULL END)
            RETURNING *
            """,
            (
                tenant_id,
                version_id,
                version_tag,
                status,
                base_version_id,
                source,
                Jsonb(memory_payload),
                Jsonb(comparison_payload),
                created_by,
                status,
            ),
        )
        assert row is not None
        return row

    def update_policy_version(
        self,
        *,
        tenant_id: str,
        version_id: str,
        memory_payload: dict[str, Any],
        comparison_payload: dict[str, Any],
    ) -> None:
        execute(
            """
            UPDATE agent_policy_versions
            SET memory_payload = %s,
                comparison_payload = %s,
                updated_at = NOW()
            WHERE tenant_id = %s
              AND version_id = %s
            """,
            (
                Jsonb(memory_payload),
                Jsonb(comparison_payload),
                tenant_id,
                version_id,
            ),
        )

    def mark_policy_version_status(
        self,
        *,
        tenant_id: str,
        version_id: str,
        status: str,
    ) -> None:
        execute(
            """
            UPDATE agent_policy_versions
            SET status = %s,
                updated_at = NOW()
            WHERE tenant_id = %s
              AND version_id = %s
            """,
            (status, tenant_id, version_id),
        )

    def activate_policy_version(
        self,
        *,
        tenant_id: str,
        version_id: str,
        actor_user_id: str | None,
        rollback: bool = False,
    ) -> None:
        del actor_user_id
        with transaction_cursor() as cur:
            cur.execute(
                """
                UPDATE agent_policy_versions
                SET status = CASE WHEN status = 'ACTIVE' THEN 'SUPERSEDED' ELSE status END,
                    updated_at = NOW()
                WHERE tenant_id = %s
                  AND status = 'ACTIVE'
                """,
                (tenant_id,),
            )
            cur.execute(
                """
                UPDATE agent_policy_versions
                SET status = 'ACTIVE',
                    activated_at = NOW(),
                    source = CASE WHEN %s THEN 'rollback' ELSE source END,
                    updated_at = NOW()
                WHERE tenant_id = %s
                  AND version_id = %s
                """,
                (rollback, tenant_id, version_id),
            )

    def create_eval_run(
        self,
        *,
        tenant_id: str,
        eval_run_id: str,
        candidate_version_id: str,
        baseline_version_id: str,
        summary: dict[str, Any],
        verdict: dict[str, Any],
        created_by: str | None,
    ) -> dict[str, Any]:
        row = fetchone(
            """
            INSERT INTO agent_policy_eval_runs (
              tenant_id, eval_run_id, candidate_version_id, baseline_version_id,
              summary, verdict, created_by
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING *
            """,
            (
                tenant_id,
                eval_run_id,
                candidate_version_id,
                baseline_version_id,
                Jsonb(summary),
                Jsonb(verdict),
                created_by,
            ),
        )
        assert row is not None
        return row

    def list_policy_versions(self, *, tenant_id: str, limit: int = 50) -> list[dict[str, Any]]:
        return fetchall(
            """
            SELECT *
            FROM agent_policy_versions
            WHERE tenant_id = %s
            ORDER BY updated_at DESC, created_at DESC
            LIMIT %s
            """,
            (tenant_id, max(1, int(limit))),
        )


class ToolRepository:
    def list_tools(self, tenant_id: str, enabled_only: bool) -> list[dict[str, Any]]:
        if enabled_only:
            return fetchall(
                """
                SELECT *
                FROM tool_registry
                WHERE tenant_id = %s
                  AND enabled = TRUE
                ORDER BY tool_id, version
                """,
                (tenant_id,),
            )
        return fetchall(
            """
            SELECT *
            FROM tool_registry
            WHERE tenant_id = %s
            ORDER BY tool_id, version
            """,
            (tenant_id,),
        )

    def upsert_tool(self, tenant_id: str, actor_user_id: str, manifest: dict[str, Any]) -> None:
        execute(
            """
            INSERT INTO tool_registry (
              tenant_id, tool_id, version, description, required_scopes, input_schema, output_schema, auth_type,
              rate_limit_rpm, run_limit, timeout_connect_s, timeout_read_s, timeout_overall_s, idempotency_strategy,
              audit_fields, masking_rules, egress_policy, risk_level, requires_approval, supported_use_cases, enabled, created_by
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (tenant_id, tool_id, version) DO UPDATE SET
              description = EXCLUDED.description,
              required_scopes = EXCLUDED.required_scopes,
              input_schema = EXCLUDED.input_schema,
              output_schema = EXCLUDED.output_schema,
              auth_type = EXCLUDED.auth_type,
              rate_limit_rpm = EXCLUDED.rate_limit_rpm,
              run_limit = EXCLUDED.run_limit,
              timeout_connect_s = EXCLUDED.timeout_connect_s,
              timeout_read_s = EXCLUDED.timeout_read_s,
              timeout_overall_s = EXCLUDED.timeout_overall_s,
              idempotency_strategy = EXCLUDED.idempotency_strategy,
              audit_fields = EXCLUDED.audit_fields,
              masking_rules = EXCLUDED.masking_rules,
              egress_policy = EXCLUDED.egress_policy,
              risk_level = EXCLUDED.risk_level,
              requires_approval = EXCLUDED.requires_approval,
              supported_use_cases = EXCLUDED.supported_use_cases,
              enabled = EXCLUDED.enabled,
              updated_at = NOW()
            """,
            (
                tenant_id,
                manifest["tool_id"],
                manifest["version"],
                manifest["description"],
                manifest["required_scopes"],
                Jsonb(manifest["input_schema"]),
                Jsonb(manifest["output_schema"]),
                manifest["auth_type"],
                manifest["rate_limit_rpm"],
                manifest["run_limit"],
                manifest["timeout_connect_s"],
                manifest["timeout_read_s"],
                manifest["timeout_overall_s"],
                manifest["idempotency_strategy"],
                manifest["audit_fields"],
                Jsonb(manifest["masking_rules"]),
                Jsonb(manifest["egress_policy"]),
                manifest.get("risk_level", "low"),
                bool(manifest.get("requires_approval", False)),
                list(manifest.get("supported_use_cases") or []),
                manifest["enabled"],
                actor_user_id,
            ),
        )

    def list_assistant_registry(
        self,
        *,
        tenant_id: str,
        enabled_only: bool = True,
        use_case: str | None = None,
    ) -> list[dict[str, Any]]:
        query = """
        SELECT
          tool_id AS tool_name,
          version,
          description,
          input_schema,
          COALESCE(risk_level, 'low') AS risk_level,
          COALESCE(requires_approval, FALSE) AS requires_approval,
          COALESCE(supported_use_cases, '{}'::TEXT[]) AS supported_use_cases,
          enabled
        FROM tool_registry
        WHERE tenant_id = %s
        """
        params: list[Any] = [tenant_id]
        if enabled_only:
            query += " AND enabled = TRUE"
        if use_case:
            query += " AND %s = ANY(COALESCE(supported_use_cases, '{}'::TEXT[]))"
            params.append(use_case)
        query += " ORDER BY tool_id, version"
        rows = fetchall(query, tuple(params))
        for row in rows:
            row["supported_use_cases"] = list(row.get("supported_use_cases") or [])
        return rows

    def get_assistant_registry_item(self, *, tenant_id: str, tool_name: str, version: str) -> dict[str, Any] | None:
        row = fetchone(
            """
            SELECT
              tool_id AS tool_name,
              version,
              description,
              input_schema,
              COALESCE(risk_level, 'low') AS risk_level,
              COALESCE(requires_approval, FALSE) AS requires_approval,
              COALESCE(supported_use_cases, '{}'::TEXT[]) AS supported_use_cases,
              enabled
            FROM tool_registry
            WHERE tenant_id = %s
              AND tool_id = %s
              AND version = %s
            """,
            (tenant_id, tool_name, version),
        )
        if row:
            row["supported_use_cases"] = list(row.get("supported_use_cases") or [])
        return row

    def upsert_assistant_registry(
        self,
        *,
        tenant_id: str,
        actor_user_id: str,
        tool_name: str,
        version: str,
        description: str,
        input_schema: dict[str, Any],
        risk_level: str,
        requires_approval: bool,
        supported_use_cases: list[str],
        enabled: bool,
    ) -> None:
        rowcount = execute(
            """
            UPDATE tool_registry
            SET description = %s,
                input_schema = %s,
                risk_level = %s,
                requires_approval = %s,
                supported_use_cases = %s,
                enabled = %s,
                created_by = COALESCE(created_by, %s),
                updated_at = NOW()
            WHERE tenant_id = %s
              AND tool_id = %s
              AND version = %s
            """,
            (
                description,
                Jsonb(input_schema),
                risk_level,
                requires_approval,
                supported_use_cases,
                enabled,
                actor_user_id,
                tenant_id,
                tool_name,
                version,
            ),
        )
        if rowcount != 1:
            raise LookupError("tool registry item not found")


class ToolGatewayRepository:
    def get_caller(self, tenant_id: str, caller_user_id: str) -> dict[str, Any] | None:
        return fetchone(
            "SELECT id, tenant_id, email, role FROM users WHERE tenant_id = %s AND id = %s",
            (tenant_id, caller_user_id),
        )

    def get_manifest(self, tenant_id: str, tool_id: str, version: str | None) -> dict[str, Any] | None:
        return fetchone(
            """
            SELECT *
            FROM tool_registry
            WHERE tenant_id = %s
              AND tool_id = %s
              AND enabled = TRUE
              AND (%s::text IS NULL OR version = %s)
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (
                tenant_id,
                tool_id,
                version,
                version,
            ),
        )

    def count_run_tool_calls(self, tenant_id: str, run_id: str, tool_id: str, current_tool_call_id: str) -> int:
        row = fetchone(
            """
            SELECT COUNT(*)::int AS c
            FROM tool_calls
            WHERE run_id = %s
              AND tenant_id = %s
              AND tool_id = %s
              AND tool_call_id <> %s
            """,
            (run_id, tenant_id, tool_id, current_tool_call_id),
        )
        return int((row or {}).get("c") or 0)

    def try_start_tool_call(
        self,
        *,
        tenant_id: str,
        tool_call_id: str,
        run_id: str,
        task_id: str,
        tool_id: str,
        caller_user_id: str,
        request_masked: dict[str, Any],
        trace_id: str,
    ) -> bool:
        rowcount = execute(
            """
            INSERT INTO tool_calls (
              tool_call_id, tenant_id, run_id, task_id, tool_id, caller_user_id,
              request_masked, response_masked, status, reason_code, trace_id, idempotency_key, duration_ms
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'STARTED', NULL, %s, %s, 0)
            ON CONFLICT (tool_call_id) DO NOTHING
            """,
            (
                tool_call_id,
                tenant_id,
                run_id,
                task_id,
                tool_id,
                caller_user_id,
                Jsonb(request_masked),
                Jsonb({}),
                trace_id,
                tool_call_id,
            ),
        )
        return rowcount == 1

    def load_tool_call(self, tenant_id: str, tool_call_id: str) -> dict[str, Any] | None:
        return fetchone(
            """
            SELECT status, response_masked, reason_code
            FROM tool_calls
            WHERE tenant_id = %s
              AND tool_call_id = %s
            """,
            (tenant_id, tool_call_id),
        )

    def finalize_tool_call(
        self,
        *,
        tenant_id: str,
        tool_call_id: str,
        run_id: str,
        task_id: str,
        tool_id: str,
        caller_user_id: str,
        request_masked: dict[str, Any],
        response_masked: dict[str, Any],
        status_text: str,
        reason_code: str | None,
        trace_id: str,
        duration_ms: int,
    ) -> None:
        execute(
            """
            INSERT INTO tool_calls (
              tool_call_id, tenant_id, run_id, task_id, tool_id, caller_user_id,
              request_masked, response_masked, status, reason_code, trace_id, idempotency_key, duration_ms
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (tool_call_id) DO UPDATE SET
              request_masked = EXCLUDED.request_masked,
              response_masked = EXCLUDED.response_masked,
              status = EXCLUDED.status,
              reason_code = EXCLUDED.reason_code,
              duration_ms = EXCLUDED.duration_ms
            """,
            (
                tool_call_id,
                tenant_id,
                run_id,
                task_id,
                tool_id,
                caller_user_id,
                Jsonb(request_masked),
                Jsonb(response_masked),
                status_text,
                reason_code,
                trace_id,
                tool_call_id,
                duration_ms,
            ),
        )

    def insert_audit_log(
        self,
        *,
        tenant_id: str,
        actor_user_id: str,
        action: str,
        target_type: str,
        target_id: str,
        detail_masked: dict[str, Any],
        trace_id: str,
    ) -> None:
        execute(
            """
            INSERT INTO audit_log (tenant_id, actor_user_id, action, target_type, target_id, detail_masked, trace_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (tenant_id, actor_user_id, action, target_type, target_id, Jsonb(detail_masked), trace_id),
        )
