# Approval Outbox Reliability Audit

## Why It Matters (Interviewer View)

Human approval is business-critical.  
If approval decisions are stored but workflow signals are dropped, execution consistency breaks.  
Outbox pattern ensures durable, retryable signal delivery.

## Key File References

- Decision API path: `apps/api/app/services/approval_service.py::apply_approval_decision`
- Dispatcher loop: `apps/api/app/services/approval_service.py::run_approval_signal_dispatcher`
- Atomic decision + outbox write:
  `apps/api/app/repositories.py::apply_approval_decision_with_outbox`
- Outbox claim/ack/fail:
  - `claim_next_approval_signal_outbox`
  - `mark_approval_signal_sent`
  - `mark_approval_signal_failure`
- Schema/index definitions: `infra/postgres/init.sql` (`approval_signal_outbox`)

## Reliability Semantics

- Approval decision and outbox row are persisted in one DB transaction.
- Dispatcher claims pending rows with `FOR UPDATE SKIP LOCKED`.
- Failed sends backoff with retry delay and attempt counting.
- `FAILED` rows are terminal observation state (not claimed again).

## How To Verify

```bash
python -m pytest -q apps/api/tests/test_approval_outbox.py -p no:cacheprovider
```

Runtime check after approval action:

```bash
docker compose exec -T postgres psql -U platform -d platform -c \
\"select approval_id,status,attempt_count,next_attempt_at,last_error from approval_signal_outbox order by created_at desc limit 10;\"
```
