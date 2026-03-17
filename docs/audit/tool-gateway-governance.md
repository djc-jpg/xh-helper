# Tool Gateway Governance Audit

## Why It Matters (Interviewer View)

Tool execution is the highest-risk part of agent systems.  
A centralized governance gateway makes policy decisions explicit, testable, and auditable.

## Key File References

- Main pipeline: `apps/api/app/tool_gateway.py::ToolGateway.execute`
- Adapter dispatch: `apps/api/app/tool_gateway.py::_dispatch`
- Egress enforcement: `apps/api/app/tool_gateway.py::_enforce_egress`
- Idempotency replay: `apps/api/app/tool_gateway.py::_idempotent_replay`
- Deny/audit path: `apps/api/app/tool_gateway.py::_deny`
- Tool call persistence: `apps/api/app/repositories.py::ToolGatewayRepository`
- Policy resolution: `apps/api/app/policy.py`

## Governance Controls

- Policy allow/deny by tenant/task/tool context.
- Approval binding checks for write-like operations.
- User+tool rate limiting and per-run call cap.
- Domain/IP egress allowlist + private network blocking.
- Adapter timeout/error normalization to reason codes.
- Tool-call audit trail with masked details.

## How To Verify

```bash
python -m pytest -q apps/api/tests/test_tool_gateway_egress.py -p no:cacheprovider
python -m pytest -q apps/api/tests/test_tool_gateway_idempotency.py -p no:cacheprovider
python -m pytest -q apps/api/tests/test_tool_gateway_approval_binding.py -p no:cacheprovider
```

Manual:

```bash
make demo-create
make demo-status TASK_ID=<task_id>
```
