from __future__ import annotations

from typing import Any

DEFAULT_MASK_KEYS = {
    "password",
    "token",
    "secret",
    "authorization",
    "body",
    "content",
}


def mask_payload(data: Any, rules: dict[str, Any] | None = None) -> Any:
    mask_keys = set(DEFAULT_MASK_KEYS)
    if rules and isinstance(rules, dict):
        rule_keys = rules.get("mask_fields") or []
        mask_keys.update(str(k).lower() for k in rule_keys)
    return _mask_value(data, mask_keys)


def _mask_value(value: Any, mask_keys: set[str]) -> Any:
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for k, v in value.items():
            if str(k).lower() in mask_keys:
                out[k] = "***"
            else:
                out[k] = _mask_value(v, mask_keys)
        return out
    if isinstance(value, list):
        return [_mask_value(x, mask_keys) for x in value]
    return value


def summarize_payload(value: Any, max_len: int = 240) -> dict[str, Any]:
    txt = str(value)
    if len(txt) > max_len:
        txt = txt[: max_len - 3] + "..."
    return {"summary": txt}

