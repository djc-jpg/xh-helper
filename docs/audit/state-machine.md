# State Machine Audit

## Why It Matters (Interviewer View)

State machine discipline is the backbone of reliable agent execution.  
Without strict transition control, retries/cancels/failures become inconsistent and hard to reason about.

## Key File References

- Transition map + terminal states: `apps/api/app/state_machine.py`
  - `FINAL_STATES`
  - `ALLOWED_TRANSITIONS`
  - `is_valid_transition(...)`
- Guarded status ingestion: `apps/api/app/services/internal_service.py::update_internal_task_status`
- API-level cancel/rerun behavior: `apps/api/app/services/task_service.py`
- Workflow-side status emission: `apps/worker/workflows.py`

## Current Semantics

- API and worker updates converge through internal status endpoint.
- Terminal-state absorption prevents noisy errors from late worker updates.
- `status_event_id` provides idempotent replay safety for internal status writes.
- Illegal transitions return `409` while recording guardrail observability.

## How To Verify

```bash
python -m pytest -q apps/api/tests/test_internal_status_guardrails.py -p no:cacheprovider
python -m pytest -q apps/api/tests/test_cancel_task_semantics.py -p no:cacheprovider
```

Manual smoke:

```bash
make demo-create
make demo-status TASK_ID=<task_id>
```
