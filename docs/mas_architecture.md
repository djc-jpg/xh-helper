# Multi-Agent System (MAS) Architecture for XH Helper

## 1. Agent Architecture

Each agent follows a strict loop:

1. `perceive`: read task/environment state (DB snapshot, queue signals, approval status).
2. `decide`: apply policy (approve/reject/retry/reschedule).
3. `execute`: perform action (run task, trigger API, update state, call collaborator).
4. `communicate`: emit domain event through message bus.

### Core roles

- `approval_agent`: checks budget + time constraints and outputs `APPROVED` / `REJECTED`.
- `execution_agent`: runs task with adaptive retry and collaboration requests.
- `scheduler_agent` (logical role in coordinator): prioritizes tasks by status + priority.

## 2. Shared Base Class

Implemented in `apps/worker/mas/agents.py`:

- `BaseAgent.perceive()`
- `BaseAgent.decide()`
- `BaseAgent.execute()`
- `BaseAgent.communicate()`
- `BaseAgent.run_once()` orchestrates full lifecycle with telemetry spans.

## 3. Approval Agent Policy

Implemented in `ApprovalAgent`:

- approve when:
  - `estimated_cost <= budget`
  - `estimated_minutes <= deadline_minutes` (if deadline provided)
- reject otherwise
- emits:
  - `approval.granted` -> `execution_agent`
  - `approval.denied` -> `scheduler_agent`

## 4. Execution Agent Policy

Implemented in `TaskExecutionAgent`:

- success path:
  - execute task handler
  - emit `execution.succeeded`
  - return `PROCESS_NEXT_TASK`
- failure path:
  - classify failure (`network`, `timeout`, `service_unavailable`, `validation`, `permission`, `unknown`)
  - decide retry/collaboration using `RecoveryPolicy`
  - emit `execution.assistance_requested` and/or `execution.failed`

## 5. Messaging Mechanism

Implemented in `apps/worker/mas/messaging.py`:

- message model: `AgentMessage`
- APIs:
  - `send_message(...)`
  - `receive_message(...)`
- backends:
  - `InMemoryMessageQueue` (local unit tests/dev)
  - `RedisMessageQueue` (distributed runtime)

## 6. Redis Caching + Distributed Rate Limiting

Implemented in `apps/worker/mas/redis_support.py`:

- `RedisCache` / `InMemoryCache`
- `TaskCache` for task state snapshot
- `RedisRateLimiter` / `InMemoryRateLimiter`
- fixed-window distributed limiter: `INCR + EXPIRE`
- if Redis Python client is unavailable at runtime, MAS falls back to memory backend and logs warning

New worker config:

- `REDIS_URL`
- `MAS_MESSAGE_BACKEND`
- `MAS_CACHE_TTL_S`
- `MAS_RATE_LIMIT_REQUESTS`
- `MAS_RATE_LIMIT_WINDOW_S`

## 7. Adaptive Behavior

Implemented in `apps/worker/mas/adaptive.py`:

- failure classifier `classify_failure_type()`
- action planner `RecoveryPolicy.decide()`
- policy output `RecoveryAction`:
  - `should_retry`
  - `delay_s`
  - `request_collaboration`
  - `collaborator`
  - `reason`

## 8. Collaboration and Coordination

Implemented in `apps/worker/mas/orchestration.py`:

- `TaskScheduler`: queue with dynamic ordering (`status_weight`, `priority`)
- `MultiAgentCoordinator`:
  - approval -> execution handoff
  - failure -> re-approval loop
  - scheduler message pump for cross-agent coordination

## 9. Workflow Control

Coordinator aligns with existing task lifecycle (`QUEUED`, `WAITING_HUMAN`, `APPROVED`, `FAILED_RETRYABLE`, `SUCCEEDED`, etc.), enabling:

- dynamic order by priority and state
- automatic re-queue on retryable failures/throttling
- automatic halt on reject/final states

## 10. Unit Tests

Implemented in `apps/worker/tests/test_mas_agents.py`:

- approval decision tests (approve/reject)
- execution behavior tests (retry success / non-retryable failure)
- collaboration test (approval -> execution -> success end-to-end)

## 11. Fault Recovery and Retry

Recovery matrix (implemented):

- `network`, `timeout`, `service_unavailable`: exponential backoff retry
- `validation`, `permission`: no retry, request collaboration / re-approval
- attempts exhausted: escalate to approval/scheduler agent

## 12. Observability and OTel

Implemented in `apps/worker/mas/observability.py`:

- Prometheus metrics:
  - `mas_agent_decisions_total`
  - `mas_agent_execution_total`
  - `mas_agent_retries_total`
  - `mas_agent_inflight`
  - `mas_agent_step_latency_seconds`
- OTel spans:
  - `mas.<agent_id>.perceive`
  - `mas.<agent_id>.decide`
  - `mas.<agent_id>.execute`

OTel example entrypoint:

- `apps/worker/mas/otel_example.py`

## 13. Distributed Architecture and Scaling

### Baseline distributed topology

- API nodes: FastAPI replicas behind LB
- Worker nodes: Temporal workers + MAS agents
- Temporal cluster: task/workflow durability
- Postgres: system of record
- Redis: cache + distributed rate limiter + message queue backend

### Scale-out options for messaging

- Redis LIST/STREAM: low-latency operational simplicity
- Kafka: durable event log, replay, high-throughput fan-out
- NATS JetStream: lightweight pub/sub + durable stream semantics

### Recommended migration path

1. Enable MAS in worker with `MAS_MESSAGE_BACKEND=memory` and validate local behavior.
2. Switch to `MAS_MESSAGE_BACKEND=redis` for multi-instance workers.
3. Promote high-volume domains to Kafka/NATS topics while keeping Redis for hot cache + limiter.
4. Add HPA/autoscaling based on queue depth + agent failure/retry metrics.
