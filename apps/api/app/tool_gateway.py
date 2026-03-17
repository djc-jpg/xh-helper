from __future__ import annotations

import asyncio
import ipaddress
import json
import socket
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx
from jsonschema import ValidationError, validate
from opentelemetry import trace

from .config import settings
from .masking import mask_payload, summarize_payload
from .metrics import tool_denied_reason_total, tool_denied_total
from .policy import check_tool_policy, is_tool_write_action
from .repositories import ToolGatewayRepository

tracer = trace.get_tracer("tool-gateway")


class InMemoryRateLimiter:
    def __init__(self) -> None:
        self._buckets: dict[str, deque[float]] = defaultdict(deque)

    def allow(self, key: str, limit: int, window_sec: int = 60) -> bool:
        if limit <= 0:
            return False
        now = time.time()
        bucket = self._buckets[key]
        while bucket and (now - bucket[0]) > window_sec:
            bucket.popleft()
        if len(bucket) >= limit:
            return False
        bucket.append(now)
        return True


rate_limiter = InMemoryRateLimiter()
DNS_CACHE_TTL_SECONDS = 60.0


class EgressPolicyError(PermissionError):
    def __init__(self, reason_code: str):
        super().__init__(reason_code)
        self.reason_code = reason_code


class AdapterHTTPError(RuntimeError):
    def __init__(self, *, status_code: int, body_summary: str):
        self.status_code = status_code
        self.body_summary = body_summary
        if status_code == 408:
            self.reason_code = "adapter_http_408"
            self.retryable = True
        elif status_code == 429:
            self.reason_code = "adapter_http_429"
            self.retryable = True
        elif 500 <= status_code <= 599:
            self.reason_code = "adapter_http_5xx"
            self.retryable = True
        elif 400 <= status_code <= 499:
            self.reason_code = "adapter_http_4xx"
            self.retryable = False
        else:
            self.reason_code = "adapter_http_error"
            self.retryable = False
        super().__init__(f"{self.reason_code}:{status_code}")


class ToolGateway:
    def __init__(self, repo: ToolGatewayRepository | None = None) -> None:
        self._dns_cache: dict[str, tuple[float, list[str]]] = {}
        self._repo = repo or ToolGatewayRepository()

    @staticmethod
    def _tenant_id(req: dict[str, Any]) -> str:
        return str(req.get("tenant_id") or settings.default_tenant_id)

    @staticmethod
    def _deny_category(reason_code: str) -> str:
        if reason_code.startswith("EGRESS_"):
            return "egress"
        if reason_code in {
            "policy_denied",
            "policy_default_deny",
            "write_requires_operator",
            "write_requires_approval",
            "approval_not_approved",
            "approval_invalid",
            "approval_context_invalid",
        }:
            return "policy_denied"
        if reason_code in {"rate_limited_user_tool", "adapter_http_429"}:
            return "rate_limited"
        if reason_code == "run_limit_exceeded":
            return "run_limit_exceeded"
        if reason_code == "adapter_http_4xx":
            return "adapter_http_4xx"
        if reason_code == "adapter_http_5xx":
            return "adapter_http_5xx"
        if reason_code in {"timeout", "adapter_http_408"}:
            return "timeout"
        return "other"

    async def execute(self, req: dict[str, Any]) -> dict[str, Any]:
        with tracer.start_as_current_span("tool_call") as span:
            span.set_attribute("tool_id", req["tool_id"])
            span.set_attribute("tool_call_id", req["tool_call_id"])
            tenant_id = self._tenant_id(req)
            span.set_attribute("tenant_id", tenant_id)

            caller = self._repo.get_caller(tenant_id=tenant_id, caller_user_id=req["caller_user_id"])
            if not caller:
                return self._deny(req, "caller_not_found", {"message": "Unknown caller"}, masking_rules={})

            if not self._try_start_tool_call(req):
                return await self._idempotent_replay(req)

            manifest = self._repo.get_manifest(
                tenant_id=tenant_id,
                tool_id=req["tool_id"],
                version=req.get("version"),
            )
            if not manifest:
                return self._deny(req, "unknown_tool", {"message": "Tool is not allowlisted"}, masking_rules={})

            payload = req["payload"]
            try:
                validate(instance=payload, schema=manifest["input_schema"])
            except ValidationError as exc:
                return self._deny(req, "schema_invalid", {"error": str(exc)}, masking_rules=manifest["masking_rules"])

            write_action = is_tool_write_action(req["tool_id"], payload)
            allowed, reason = check_tool_policy(
                user=caller,
                task_type=req["task_type"],
                tool_id=req["tool_id"],
                is_write_action=write_action,
                approval_id=req.get("approval_id"),
                task_id=str(req.get("task_id") or ""),
                run_id=str(req.get("run_id") or ""),
                environment=settings.environment,
            )
            if not allowed:
                return self._deny(req, reason, {"message": "Policy denied"}, masking_rules=manifest["masking_rules"])

            user_tool_key = f"{caller['id']}:{req['tool_id']}"
            if not rate_limiter.allow(user_tool_key, int(manifest["rate_limit_rpm"]), 60):
                return self._deny(
                    req,
                    "rate_limited_user_tool",
                    {"message": "RPM exceeded"},
                    masking_rules=manifest["masking_rules"],
                )

            run_count = self._repo.count_run_tool_calls(
                tenant_id=tenant_id,
                run_id=req["run_id"],
                tool_id=req["tool_id"],
                current_tool_call_id=req["tool_call_id"],
            )
            if run_count >= int(manifest["run_limit"]):
                return self._deny(
                    req,
                    "run_limit_exceeded",
                    {"message": "run max tool calls exceeded"},
                    masking_rules=manifest["masking_rules"],
                )

            timeout_overall = int(manifest["timeout_overall_s"] or 15)
            start = time.perf_counter()

            try:
                result = await asyncio.wait_for(
                    self._dispatch(req, manifest, timeout_overall),
                    timeout=timeout_overall,
                )
            except TimeoutError:
                return self._deny(req, "timeout", {"message": "Tool timed out"}, masking_rules=manifest["masking_rules"])
            except httpx.TimeoutException as exc:
                return self._deny(
                    req,
                    "timeout",
                    {"message": "Tool timed out", "error": str(exc)},
                    masking_rules=manifest["masking_rules"],
                )
            except httpx.TransportError as exc:
                return self._deny(
                    req,
                    "adapter_network_error",
                    {"message": "Transport error", "error": str(exc)},
                    masking_rules=manifest["masking_rules"],
                )
            except EgressPolicyError as exc:
                return self._deny(
                    req,
                    exc.reason_code,
                    {"error": exc.reason_code},
                    masking_rules=manifest["masking_rules"],
                )
            except AdapterHTTPError as exc:
                return self._deny(
                    req,
                    exc.reason_code,
                    {
                        "status_code": exc.status_code,
                        "retryable": exc.retryable,
                        "body": exc.body_summary,
                    },
                    masking_rules=manifest["masking_rules"],
                )
            except PermissionError as exc:
                return self._deny(req, "policy_denied", {"error": str(exc)}, masking_rules=manifest["masking_rules"])
            except Exception as exc:
                return self._deny(req, "adapter_error", {"error": str(exc)}, masking_rules=manifest["masking_rules"])

            try:
                validate(instance=result, schema=manifest["output_schema"])
            except ValidationError as exc:
                return self._deny(
                    req,
                    "output_schema_invalid",
                    {"error": str(exc)},
                    masking_rules=manifest["masking_rules"],
                )

            duration_ms = int((time.perf_counter() - start) * 1000)
            self._finalize_tool_call(
                req=req,
                status="SUCCEEDED",
                reason_code=None,
                request_data=payload,
                response_data=result,
                duration_ms=duration_ms,
                masking_rules=manifest["masking_rules"],
            )
            return {
                "status": "SUCCEEDED",
                "tool_call_id": req["tool_call_id"],
                "reason_code": None,
                "result": result,
                "idempotent_hit": False,
            }

    async def _dispatch(self, req: dict[str, Any], manifest: dict[str, Any], timeout: int) -> dict[str, Any]:
        tool_id = req["tool_id"]
        if tool_id == "internal_rest_api":
            return await self._internal_rest(req, manifest)
        if tool_id == "web_search":
            return await self._web_search(req, manifest)
        if tool_id == "email_ticketing":
            return await self._email_ticketing(req)
        if tool_id == "object_storage":
            return await self._object_storage(req)
        raise ValueError(f"unknown adapter: {tool_id}")

    async def _internal_rest(self, req: dict[str, Any], manifest: dict[str, Any]) -> dict[str, Any]:
        payload = req["payload"]
        method = str(payload["method"]).upper()
        path = str(payload["path"])
        if not path.startswith("/records"):
            raise PermissionError("path not allowlisted")

        timeout = httpx.Timeout(
            timeout=float(manifest["timeout_overall_s"]),
            connect=float(manifest["timeout_connect_s"]),
            read=float(manifest["timeout_read_s"]),
        )
        headers = {"X-Service-Token": settings.fake_internal_service_token}
        url = f"{settings.fake_internal_base_url}{path}"
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.request(
                method=method,
                url=url,
                params=payload.get("params"),
                json=payload.get("body"),
                headers=headers,
            )
        try:
            body = resp.json()
        except Exception:
            body = {"raw": resp.text}
        if resp.status_code < 200 or resp.status_code >= 300:
            body_summary = summarize_payload(body).get("summary", "")
            raise AdapterHTTPError(status_code=int(resp.status_code), body_summary=str(body_summary))
        return {"status_code": resp.status_code, "result": body}

    async def _web_search(self, req: dict[str, Any], manifest: dict[str, Any]) -> dict[str, Any]:
        payload = req["payload"]
        domain = str(payload["domain"]).lower().strip()
        self._enforce_egress(domain, manifest.get("egress_policy") or {})
        query = str(payload["query"]).strip().lower()
        top_k = int(payload.get("top_k") or 3)

        docs_dir = Path(settings.docs_dir)
        results: list[dict[str, str]] = []
        for p in sorted(docs_dir.glob("*.md")):
            text = p.read_text(encoding="utf-8")
            score = text.lower().count(query)
            if score <= 0:
                continue
            snippet = text.replace("\n", " ")[:180]
            results.append(
                {
                    "title": p.stem,
                    "url": f"https://{domain}/{p.stem}",
                    "snippet": snippet,
                }
            )

        if not results:
            results.append(
                {
                    "title": "no-match",
                    "url": f"https://{domain}/search?q={query}",
                    "snippet": "No local corpus match; this is a controlled stub result.",
                }
            )
        return {"results": results[:top_k]}

    async def _email_ticketing(self, req: dict[str, Any]) -> dict[str, Any]:
        payload = req["payload"]
        if not req.get("approval_id"):
            raise PermissionError("approval_id required for email_ticketing write action")

        action = payload["action"]
        out_dir = Path(settings.artifact_dir) / "email_ticketing"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"{req['tool_call_id']}.json"
        out_file.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")

        if action == "create_ticket":
            return {
                "status": "queued",
                "message": "Ticket creation accepted by mock service",
                "ticket_id": f"TKT-{req['tool_call_id'][:8]}",
            }
        return {"status": "queued", "message": "Email send accepted by mock service"}

    async def _object_storage(self, req: dict[str, Any]) -> dict[str, Any]:
        payload = req["payload"]
        object_key = str(payload["object_key"]).replace("..", "_").lstrip("/")
        content = str(payload["content"])
        base = Path(settings.artifact_dir) / "objects"
        target = base / object_key
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
        return {"uri": str(target), "size": target.stat().st_size}

    def _enforce_egress(self, domain: str, policy: dict[str, Any]) -> None:
        allow_domains = [str(x).lower().strip() for x in (policy.get("allow_domains") or []) if str(x).strip()]

        host = domain
        if "://" in domain:
            host = urlparse(domain).hostname or domain
        host = host.lower().strip()
        if not host:
            raise EgressPolicyError("EGRESS_INVALID_HOST")

        if allow_domains and not self._host_is_allowlisted(host, allow_domains):
            raise EgressPolicyError("EGRESS_DOMAIN_NOT_ALLOWLISTED")

        if host == "localhost":
            raise EgressPolicyError("EGRESS_PRIVATE_HOST")

        try:
            ip = ipaddress.ip_address(host)
            self._ensure_public_ip(ip, from_dns=False)
            return
        except ValueError:
            pass

        if host.endswith(".local") or host.endswith(".internal"):
            raise EgressPolicyError("EGRESS_PRIVATE_DOMAIN")

        ips = self._resolve_host_ips(host)
        if not ips:
            raise EgressPolicyError("EGRESS_DNS_RESOLUTION_FAILED")

        for resolved in ips:
            self._ensure_public_ip(ipaddress.ip_address(resolved), from_dns=True)

    def _host_is_allowlisted(self, host: str, allow_domains: list[str]) -> bool:
        for domain in allow_domains:
            if host == domain or host.endswith(f".{domain}"):
                return True
        return False

    def _resolve_host_ips(self, host: str) -> list[str]:
        now = time.time()
        cached = self._dns_cache.get(host)
        if cached and cached[0] > now:
            return cached[1]

        try:
            infos = socket.getaddrinfo(host, None, proto=socket.IPPROTO_TCP)
        except socket.gaierror as exc:
            raise EgressPolicyError("EGRESS_DNS_RESOLUTION_FAILED") from exc

        ips = sorted({str(info[4][0]) for info in infos if info and info[4] and info[4][0]})
        self._dns_cache[host] = (now + DNS_CACHE_TTL_SECONDS, ips)
        return ips

    def _ensure_public_ip(self, ip: ipaddress.IPv4Address | ipaddress.IPv6Address, *, from_dns: bool) -> None:
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_multicast or ip.is_reserved or ip.is_unspecified:
            if from_dns:
                raise EgressPolicyError("EGRESS_DNS_PRIVATE_IP")
            raise EgressPolicyError("EGRESS_PRIVATE_IP")

    def _try_start_tool_call(self, req: dict[str, Any]) -> bool:
        tenant_id = self._tenant_id(req)
        return self._repo.try_start_tool_call(
            tenant_id=tenant_id,
            tool_call_id=req["tool_call_id"],
            run_id=req["run_id"],
            task_id=req["task_id"],
            tool_id=req["tool_id"],
            caller_user_id=req["caller_user_id"],
            request_masked=mask_payload(req.get("payload", {}), {}),
            trace_id=req["trace_id"],
        )

    def _load_tool_call(self, tenant_id: str, tool_call_id: str) -> dict[str, Any] | None:
        return self._repo.load_tool_call(tenant_id=tenant_id, tool_call_id=tool_call_id)

    async def _idempotent_replay(self, req: dict[str, Any]) -> dict[str, Any]:
        tenant_id = self._tenant_id(req)
        deadline = time.monotonic() + 5.0
        existing = self._load_tool_call(tenant_id, req["tool_call_id"])
        while existing and existing["status"] == "STARTED" and time.monotonic() < deadline:
            await asyncio.sleep(0.2)
            existing = self._load_tool_call(tenant_id, req["tool_call_id"])

        if not existing:
            return {
                "status": "DENIED",
                "tool_call_id": req["tool_call_id"],
                "reason_code": "idempotency_record_missing",
                "result": {"message": "idempotency record missing"},
                "idempotent_hit": True,
            }

        if existing["status"] == "STARTED":
            return {
                "status": "DENIED",
                "tool_call_id": req["tool_call_id"],
                "reason_code": "idempotency_in_progress",
                "result": {"message": "tool call still in progress"},
                "idempotent_hit": True,
            }

        return {
            "status": existing["status"],
            "tool_call_id": req["tool_call_id"],
            "reason_code": existing["reason_code"],
            "result": existing["response_masked"],
            "idempotent_hit": True,
        }

    def _deny(self, req: dict[str, Any], reason_code: str, detail: dict[str, Any], masking_rules: dict[str, Any]) -> dict[str, Any]:
        tool_denied_total.inc()
        tool_denied_reason_total.labels(
            reason_code=reason_code,
            category=self._deny_category(reason_code),
        ).inc()
        self._finalize_tool_call(
            req=req,
            status="DENIED",
            reason_code=reason_code,
            request_data=req.get("payload", {}),
            response_data=detail,
            duration_ms=0,
            masking_rules=masking_rules,
        )
        return {
            "status": "DENIED",
            "tool_call_id": req["tool_call_id"],
            "reason_code": reason_code,
            "result": detail,
            "idempotent_hit": False,
        }

    def _finalize_tool_call(
        self,
        *,
        req: dict[str, Any],
        status: str,
        reason_code: str | None,
        request_data: dict[str, Any],
        response_data: dict[str, Any],
        duration_ms: int,
        masking_rules: dict[str, Any],
    ) -> None:
        masked_req = mask_payload(request_data, masking_rules)
        masked_resp = mask_payload(response_data, masking_rules)

        tenant_id = self._tenant_id(req)
        self._repo.finalize_tool_call(
            tenant_id=tenant_id,
            tool_call_id=req["tool_call_id"],
            run_id=req["run_id"],
            task_id=req["task_id"],
            tool_id=req["tool_id"],
            caller_user_id=req["caller_user_id"],
            request_masked=masked_req,
            response_masked=masked_resp,
            status_text=status,
            reason_code=reason_code,
            trace_id=req["trace_id"],
            duration_ms=duration_ms,
        )

        self._repo.insert_audit_log(
            tenant_id=tenant_id,
            actor_user_id=req["caller_user_id"],
            action="tool_call",
            target_type="tool_call",
            target_id=req["tool_call_id"],
            detail_masked={
                "tool_id": req["tool_id"],
                "status": status,
                "reason_code": reason_code,
                "task_id": req.get("task_id"),
                "run_id": req.get("run_id"),
                "approval_id": req.get("approval_id"),
                "request": summarize_payload(masked_req),
                "response": summarize_payload(masked_resp),
            },
            trace_id=req["trace_id"],
        )
