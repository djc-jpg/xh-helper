# Release Notes v0.1.0

## Highlights

- Interview-ready backend architecture snapshot and documentation refresh.
- Docker Compose local stack with API + Worker + Postgres + Temporal.
- Demo command set for live walkthrough:
  - `make demo-create`
  - `make demo-status TASK_ID=<task_id>`
  - `make demo-approve [TASK_ID=<task_id>|APPROVAL_ID=<approval_id>]`
- Security hygiene uplift:
  - expanded `.gitignore`
  - `docs/SECURITY.md`
  - explicit README warning against committing secrets
- Added MIT `LICENSE`.
- Added architecture audit pack:
  - `docs/audit/architecture.md`
  - `docs/audit/state-machine.md`
  - `docs/audit/approval-outbox.md`
  - `docs/audit/tool-gateway-governance.md`

## How To Run

```bash
cp .env.example .env
docker compose up -d --build
docker compose exec -T api python -m app.seed
```

## Demo Flow

```bash
make demo-create
make demo-status TASK_ID=<task_id>
make demo-approve TASK_ID=<task_id>
```

## Limitations

- Single-node local deployment focus; no production HA manifests yet.
- Tool adapters are example-grade and should be extended per business domain.
- In-process approval dispatcher is suitable for local/demo and controlled deployments.
- Frontend auth/storage is demo-focused; production should prefer SSO + secure session architecture.
