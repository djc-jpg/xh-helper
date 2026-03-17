from __future__ import annotations

import re
from typing import Any

from fastapi import status
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from .config import settings
from .policy import has_min_role
from .security import decode_token


class PolicyCheckMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: Any):
        super().__init__(app)
        self.rules = [
            (re.compile(r"^/tools"), {"POST": "owner", "PUT": "owner"}),
            (re.compile(r"^/tasks/.+/(cancel|rerun)$"), {"POST": "operator"}),
            (re.compile(r"^/approvals/.+/(approve|reject|edit)$"), {"POST": "operator"}),
        ]

    async def dispatch(self, request: Request, call_next):
        request.state.policy_context = {"environment": settings.environment, "path": request.url.path}
        required_role = self._required_role(request)
        if not required_role:
            return await call_next(request)

        authz = request.headers.get("Authorization", "")
        if not authz.startswith("Bearer "):
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={"detail": "Missing token for policy-protected endpoint"},
            )
        token = authz.split(" ", 1)[1]
        try:
            payload = decode_token(token)
        except Exception:
            return JSONResponse(status_code=status.HTTP_401_UNAUTHORIZED, content={"detail": "Invalid token"})
        header_tenant = request.headers.get("X-Tenant-Id")
        token_tenant = str(payload.get("tenant_id") or "")
        if not token_tenant:
            return JSONResponse(status_code=status.HTTP_401_UNAUTHORIZED, content={"detail": "Missing tenant claim"})
        if header_tenant and header_tenant != token_tenant:
            return JSONResponse(status_code=status.HTTP_403_FORBIDDEN, content={"detail": "Tenant mismatch"})
        if not has_min_role(str(payload.get("role", "")), required_role):
            return JSONResponse(status_code=status.HTTP_403_FORBIDDEN, content={"detail": "Policy denied"})

        return await call_next(request)

    def _required_role(self, request: Request) -> str | None:
        for pattern, method_map in self.rules:
            if pattern.match(request.url.path):
                return method_map.get(request.method)
        return None
