from __future__ import annotations

from typing import Any

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from .db import fetchone
from .policy import has_min_role
from .security import decode_token

bearer = HTTPBearer(auto_error=False)


def get_current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer),
) -> dict[str, Any]:
    if not credentials:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing token")
    try:
        payload = decode_token(credentials.credentials)
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token") from exc
    if payload.get("type") != "access":
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token type")
    tenant_id = str(payload.get("tenant_id") or "")
    if not tenant_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing tenant claim")
    user = fetchone(
        "SELECT id, tenant_id, email, role, is_active FROM users WHERE tenant_id = %s AND id = %s",
        (tenant_id, payload["sub"]),
    )
    if not user or not user["is_active"]:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Inactive user")
    return user


def get_optional_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer),
) -> dict[str, Any] | None:
    if not credentials:
        return None
    try:
        payload = decode_token(credentials.credentials)
    except Exception:
        return None
    if payload.get("type") != "access":
        return None
    tenant_id = str(payload.get("tenant_id") or "")
    if not tenant_id:
        return None
    user = fetchone(
        "SELECT id, tenant_id, email, role, is_active FROM users WHERE tenant_id = %s AND id = %s",
        (tenant_id, payload["sub"]),
    )
    if not user or not user["is_active"]:
        return None
    return user


def require_role(min_role: str):
    def _inner(user: dict[str, Any] = Depends(get_current_user)) -> dict[str, Any]:
        if not has_min_role(user["role"], min_role):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f"role<{min_role}")
        return user

    return _inner
