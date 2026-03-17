from __future__ import annotations

from fastapi import Header, HTTPException, status

from .config import settings


def resolve_tenant_id(
    user_tenant_id: str,
    header_tenant_id: str | None = Header(default=None, alias="X-Tenant-Id"),
) -> str:
    """Resolve tenant from authenticated context and optionally validate request header."""
    tenant_id = str(user_tenant_id or settings.default_tenant_id)
    if header_tenant_id and str(header_tenant_id) != tenant_id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="tenant mismatch")
    return tenant_id

