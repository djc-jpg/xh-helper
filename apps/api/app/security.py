from __future__ import annotations

import datetime as dt
import hashlib
import uuid
from typing import Any

import jwt
from passlib.context import CryptContext

from .config import settings

pwd_context = CryptContext(schemes=["argon2"], deprecated="auto")


def utcnow() -> dt.datetime:
    return dt.datetime.now(tz=dt.timezone.utc)


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    return pwd_context.verify(password, password_hash)


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def create_access_token(user: dict[str, Any]) -> str:
    exp = utcnow() + dt.timedelta(minutes=settings.access_token_ttl_min)
    payload = {
        "sub": str(user["id"]),
        "email": user["email"],
        "role": str(user["role"]),
        "tenant_id": str(user["tenant_id"]),
        "type": "access",
        "exp": exp,
        "iat": utcnow(),
    }
    return jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_algorithm)


def create_refresh_token(user: dict[str, Any]) -> tuple[str, dt.datetime]:
    exp = utcnow() + dt.timedelta(days=settings.refresh_token_ttl_days)
    payload = {
        "sub": str(user["id"]),
        "tenant_id": str(user["tenant_id"]),
        "type": "refresh",
        "jti": str(uuid.uuid4()),
        "exp": exp,
        "iat": utcnow(),
    }
    token = jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_algorithm)
    return token, exp


def create_task_event_token(*, user_id: str, tenant_id: str, task_id: str, ttl_seconds: int = 60) -> str:
    exp = utcnow() + dt.timedelta(seconds=ttl_seconds)
    payload = {
        "sub": str(user_id),
        "tenant_id": str(tenant_id),
        "task_id": str(task_id),
        "type": "task_event",
        "exp": exp,
        "iat": utcnow(),
    }
    return jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_algorithm)


def decode_task_event_token(token: str) -> dict[str, Any]:
    payload = decode_token(token)
    if payload.get("type") != "task_event":
        raise ValueError("invalid token type")
    return payload


def decode_token(token: str) -> dict[str, Any]:
    return jwt.decode(token, settings.jwt_secret, algorithms=[settings.jwt_algorithm])
