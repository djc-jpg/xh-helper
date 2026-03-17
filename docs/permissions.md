# RBAC + ABAC Matrix

| Action | user | operator | owner | ABAC condition |
|---|---|---|---|---|
| Register/Login/Create task | allow | allow | allow | tenant_id from auth token (or validated `X-Tenant-Id` for auth endpoints) |
| View own task details | allow | allow | allow | user only own tasks |
| Approve/Reject/Edit approval | deny | allow | allow | environment=`settings.environment` |
| Cancel/Rerun task | deny | allow | allow | environment=`settings.environment` |
| Tool registry create/update | deny | deny | allow | environment=`settings.environment` |
| Write tool calls (`internal_rest_api` POST/PUT, `email_ticketing`, `object_storage`) | deny | allow | allow | requires approval_id + policy allow |
| Read tool calls (`internal_rest_api` GET, `web_search`) | allow if policy allow | allow if policy allow | allow | tool allowlist + schema pass |

## Policy Check Middleware
- File: `apps/api/app/policy_middleware.py`
- Routes protected by middleware:
  - `POST|PUT /tools` -> requires `owner`
  - `POST /tasks/{id}/cancel|rerun` -> requires `operator`
  - `POST /approvals/{id}/approve|reject|edit` -> requires `operator`

## Example Policies (seeded in DDL)
- `allow_rag_for_user`: allow role>=`user`, task_type=`rag_qa`, read path.
- `allow_internal_write_with_approval`: allow role>=`operator`, tool=`internal_rest_api`, write action, approval required.
- `deny_user_email_write`: deny role>=`user`, tool=`email_ticketing`, write action.
