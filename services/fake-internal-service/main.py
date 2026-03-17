from __future__ import annotations

import os
import time
import uuid
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Query, status
from prometheus_client import Counter, generate_latest
from starlette.responses import PlainTextResponse

app = FastAPI(title="Fake Internal Service", version="0.1.0")

SERVICE_TOKEN = os.getenv("INTERNAL_SERVICE_TOKEN", "")
if not SERVICE_TOKEN:
    raise RuntimeError("INTERNAL_SERVICE_TOKEN is required")

records: dict[str, dict[str, Any]] = {}
idempotency_index: dict[str, str] = {}
fault_once_hits: dict[str, int] = {}

request_total = Counter("internal_service_request_total", "Total requests", ["method"])


def require_token(x_service_token: str = Header(default="", alias="X-Service-Token")) -> None:
    if x_service_token != SERVICE_TOKEN:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid service token")


def _trigger_once(flag: str) -> bool:
    seen = int(fault_once_hits.get(flag, 0))
    if seen <= 0:
        fault_once_hits[flag] = 1
        return True
    return False


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/metrics")
def metrics() -> PlainTextResponse:
    return PlainTextResponse(generate_latest().decode("utf-8"), media_type="text/plain")


@app.get("/records")
def list_records(q: str = Query(default=""), x_service_token: str = Header(default="", alias="X-Service-Token")):
    require_token(x_service_token)
    request_total.labels(method="GET").inc()
    if q == "force_400":
        raise HTTPException(status_code=400, detail="forced bad request")
    if q == "force_500":
        raise HTTPException(status_code=500, detail="forced upstream failure")
    if q == "force_503":
        raise HTTPException(status_code=503, detail="forced service unavailable")
    if q == "force_503_once" and _trigger_once("force_503_once"):
        raise HTTPException(status_code=503, detail="forced service unavailable once")
    if q == "force_timeout":
        time.sleep(20)
    if q == "force_timeout_once" and _trigger_once("force_timeout_once"):
        time.sleep(20)
    items = list(records.values())
    if q:
        ql = q.lower()
        items = [x for x in items if ql in str(x).lower()]
    return {"items": items, "count": len(items)}


@app.get("/records/{record_id}")
def get_record(record_id: str, x_service_token: str = Header(default="", alias="X-Service-Token")):
    require_token(x_service_token)
    request_total.labels(method="GET").inc()
    if record_id not in records:
        raise HTTPException(status_code=404, detail="record not found")
    return records[record_id]


@app.post("/records")
def create_record(payload: dict[str, Any], x_service_token: str = Header(default="", alias="X-Service-Token")):
    require_token(x_service_token)
    request_total.labels(method="POST").inc()

    idem = str(payload.get("idempotency_key") or "")
    if idem and idem in idempotency_index:
        rid = idempotency_index[idem]
        return {"idempotent_hit": True, "record": records[rid]}

    record_id = str(uuid.uuid4())
    record = {"id": record_id, "name": payload.get("name", "unnamed"), "value": payload.get("value", "")}
    records[record_id] = record
    if idem:
        idempotency_index[idem] = record_id
    return {"idempotent_hit": False, "record": record}


@app.put("/records/{record_id}")
def update_record(record_id: str, payload: dict[str, Any], x_service_token: str = Header(default="", alias="X-Service-Token")):
    require_token(x_service_token)
    request_total.labels(method="PUT").inc()
    if record_id not in records:
        raise HTTPException(status_code=404, detail="record not found")
    records[record_id].update(payload)
    return {"record": records[record_id]}
