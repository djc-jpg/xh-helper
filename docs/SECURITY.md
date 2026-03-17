# Security Policy

## Scope

This repository is a demonstration-grade multi-agent execution platform.  
Do not treat default local configuration as production security posture.

## Supported Release Line

- `v0.1.x` (best-effort fixes)

## Reporting a Vulnerability

Please report security issues privately to project maintainers before public disclosure.

Suggested report content:
- affected version/commit
- impact and attack path
- minimal reproduction
- suggested fix (if available)

## Secret Management Rules

- Never commit real credentials/tokens/keys/passwords.
- Keep `.env` local only; commit only `.env.example`.
- Inject runtime secrets from environment/secret manager:
  - `JWT_SECRET`
  - `INTERNAL_API_TOKEN`
  - `WORKER_AUTH_TOKEN`
  - `FAKE_INTERNAL_SERVICE_TOKEN`
  - `INPUT_ENCRYPTION_KEY`

## Hardening Baseline

- Enable strict tenant isolation and worker binding checks.
- Keep internal endpoints protected by internal auth headers.
- Restrict egress in tool policies (`allow_domains`, private-network deny).
- Use masked/summarized payloads in audit logs.
- Rotate tokens regularly and avoid long-lived static credentials.
