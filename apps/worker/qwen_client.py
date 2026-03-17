from __future__ import annotations

from typing import Any

import httpx

from config import settings


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

    def chat_text(
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
        request_timeout = settings.qwen_timeout_s if timeout_s is None else float(timeout_s)
        with httpx.Client(timeout=request_timeout) as client:
            response = client.post(endpoint, headers=headers, json=body)
            response.raise_for_status()
            payload = response.json()
        text = _extract_content(payload)
        if not text:
            raise RuntimeError("qwen_empty_response")
        return text


qwen_client = QwenClient()
