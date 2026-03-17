# Product Notes

The platform supports multi-agent orchestration with:
- Temporal for durable workflow execution.
- LangGraph for stateful graph planning.
- Tool Gateway for guarded tool execution.

Key reliability semantics:
- Idempotent task submit using client_request_id.
- Retry with exponential backoff.
- Human-in-the-loop approval gates.

