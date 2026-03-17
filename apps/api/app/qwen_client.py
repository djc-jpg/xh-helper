from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx
from opentelemetry import trace

from .config import settings

logger = logging.getLogger(__name__)
tracer = trace.get_tracer("api.qwen_client")


def _extract_content(payload: dict[str, Any]) -> str:
    choices = payload.get("choices") or []
    if not choices:
        return ""
    message = dict(choices[0].get("message") or {})
    content = message.get("content")
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str) and item.strip():
                parts.append(item.strip())
                continue
            if not isinstance(item, dict):
                continue
            text = item.get("text") or item.get("content") or ""
            if isinstance(text, str) and text.strip():
                parts.append(text.strip())
        return "\n".join(parts).strip()
    return str(content or "").strip()


class QwenClient:
    def is_enabled(self) -> bool:
        return bool(settings.qwen_api_key.strip())

    async def chat_text(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        temperature: float | None = None,
        max_tokens: int | None = None,
        model: str | None = None,
        timeout_s: float | None = None,
    ) -> str:
        if not self.is_enabled():
            raise RuntimeError("qwen_not_configured")

        body = {
            "model": model or settings.qwen_model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": settings.qwen_temperature if temperature is None else float(temperature),
            "max_tokens": settings.qwen_max_tokens if max_tokens is None else int(max_tokens),
        }
        endpoint = f"{settings.qwen_base_url.rstrip('/')}/chat/completions"
        headers = {
            "Authorization": f"Bearer {settings.qwen_api_key}",
            "Content-Type": "application/json",
        }
        effective_timeout = float(timeout_s if timeout_s is not None else settings.qwen_timeout_s)
        with tracer.start_as_current_span("llm_call") as span:
            span.set_attribute("llm.provider", "qwen")
            span.set_attribute("llm.model", body["model"])
            span.set_attribute("llm.timeout_s", effective_timeout)
            timeout = httpx.Timeout(effective_timeout)
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await asyncio.wait_for(
                    client.post(endpoint, headers=headers, json=body),
                    timeout=effective_timeout + 1,
                )
                response.raise_for_status()
                payload = response.json()
            text = _extract_content(payload)
            if not text:
                raise RuntimeError("qwen_empty_response")
            return text


qwen_client = QwenClient()
