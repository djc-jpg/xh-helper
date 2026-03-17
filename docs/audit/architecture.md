# Architecture Snapshot (v0.1.0)

## Why It Matters (Interviewer View)

This project demonstrates a production-style backend pattern for AI agent systems:
- API layer with strict auth/tenant boundaries
- workflow orchestration with Temporal
- governed tool execution
- approval + outbox reliability
- audit + metrics for incident debugging

## Key File References

- API entrypoint: `apps/api/app/main.py`
- Task lifecycle service: `apps/api/app/services/task_service.py`
- Internal status ingestion: `apps/api/app/services/internal_service.py::update_internal_task_status`
- Temporal client integration: `apps/api/app/temporal_client.py`
- Worker workflow: `apps/worker/workflows.py::TaskWorkflow.run`
- Tool governance: `apps/api/app/tool_gateway.py::ToolGateway.execute`
- Repository + persistence boundary: `apps/api/app/repositories.py`
- Status machine: `apps/api/app/state_machine.py`

## End-to-End Flow

1. User calls `POST /tasks`.
2. API writes task/run/initial step, then starts Temporal workflow.
3. Worker executes workflow nodes and reports status to internal API.
4. Internal status endpoint validates tenant/worker binding + transition legality.
5. Tool calls pass through ToolGateway policy/rate/egress/idempotency guardrails.
6. HITL approvals are persisted and signaled via outbox dispatcher.
7. Audit log and metrics provide postmortem evidence.

## How To Verify

```bash
cp .env.example .env
docker compose up -d --build
docker compose exec -T api python -m app.seed
make demo-create
make demo-status TASK_ID=<task_id>
```

Optional:

```bash
python scripts/test.py -q
```
