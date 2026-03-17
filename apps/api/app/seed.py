from __future__ import annotations

from psycopg.types.json import Jsonb

from .config import settings
from .db import execute, init_pool
from .security import hash_password


def upsert_user(email: str, role: str, password: str) -> None:
    execute(
        """
        INSERT INTO users (tenant_id, email, password_hash, role)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (tenant_id, email)
        DO UPDATE SET password_hash = EXCLUDED.password_hash, role = EXCLUDED.role
        """,
        (settings.default_tenant_id, email, hash_password(password), role),
    )


def write_seed_audit() -> None:
    execute(
        """
        INSERT INTO audit_log (tenant_id, actor_user_id, action, target_type, target_id, detail_masked, trace_id)
        VALUES (%s, NULL, %s, %s, %s, %s, %s)
        """,
        (
            settings.default_tenant_id,
            "seed",
            "system",
            "seed",
            Jsonb({"users": ["owner@example.com", "operator@example.com", "user@example.com"]}),
            "seed-trace",
        ),
    )


def main() -> None:
    if not settings.seed_owner_password or not settings.seed_operator_password or not settings.seed_user_password:
        raise RuntimeError("Seed passwords must be provided via environment variables.")
    init_pool()
    upsert_user(settings.seed_owner_email, "owner", settings.seed_owner_password)
    upsert_user(settings.seed_operator_email, "operator", settings.seed_operator_password)
    upsert_user(settings.seed_user_email, "user", settings.seed_user_password)
    write_seed_audit()
    print("seed done")
    print(f"{settings.seed_owner_email} / <from env>")
    print(f"{settings.seed_operator_email} / <from env>")
    print(f"{settings.seed_user_email} / <from env>")


if __name__ == "__main__":
    main()
