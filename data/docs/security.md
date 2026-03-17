# Security Baseline

Default deny strategy:
- Reject unknown tools.
- Reject schema-invalid tool payloads.
- Reject private-network egress from web_search.
- Reject write tool calls without approval if policy requires it.
- Reject write tool calls when approval is not `APPROVED` for the same task/run binding.

Runtime hardening notes:
- Prometheus scrape auth token is sourced from runtime secret/env, not hardcoded in config.
- Frontend token storage supports `memory` / `sessionStorage` / `localStorage` via `NEXT_PUBLIC_AUTH_STORAGE`.
- Prefer `memory` (or HttpOnly Secure Cookie session at the edge/gateway) for production. `localStorage` has higher XSS persistence risk.

Audit events must include:
- caller
- tool_call_id
- masked request/response summary
- reason_code when denied
