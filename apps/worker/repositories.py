from __future__ import annotations

from typing import Any

from psycopg.types.json import Jsonb

from db import execute, fetchone


class WorkerRepository:
    def insert_approval(self, tenant_id: str, task_id: str, run_id: str, requested_by: str, reason: str | None = None) -> str:
        row = fetchone(
            """
            INSERT INTO approvals (tenant_id, task_id, run_id, status, requested_by, reason)
            VALUES (%s, %s, %s, 'WAITING_HUMAN', %s, %s)
            RETURNING id
            """,
            (tenant_id, task_id, run_id, requested_by, reason),
        )
        assert row is not None
        return str(row["id"])

    def insert_cost(
        self,
        *,
        tenant_id: str,
        task_id: str,
        run_id: str,
        category: str,
        amount: float,
        token_in: int = 0,
        token_out: int = 0,
    ) -> None:
        execute(
            """
            INSERT INTO cost_ledger (tenant_id, task_id, run_id, category, amount, token_in, token_out)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (tenant_id, task_id, run_id, category, amount, token_in, token_out),
        )

    def insert_artifact(
        self,
        *,
        tenant_id: str,
        task_id: str,
        run_id: str,
        artifact_type: str,
        uri: str,
        metadata: dict[str, Any],
    ) -> None:
        execute(
            """
            INSERT INTO artifacts (tenant_id, task_id, run_id, artifact_type, uri, metadata)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (tenant_id, task_id, run_id, artifact_type, uri, Jsonb(metadata)),
        )


worker_repo = WorkerRepository()
