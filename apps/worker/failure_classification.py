from __future__ import annotations

from temporalio.exceptions import ApplicationError


def iter_exception_chain(exc: BaseException):
    seen: set[int] = set()
    queue: list[BaseException] = [exc]
    while queue:
        current = queue.pop(0)
        marker = id(current)
        if marker in seen:
            continue
        seen.add(marker)
        yield current

        for attr in ("cause", "__cause__", "__context__"):
            nested = getattr(current, attr, None)
            if isinstance(nested, BaseException):
                queue.append(nested)


def classify_failure_status(exc: Exception) -> str:
    reason = str(exc).lower()
    default_status = "FAILED_RETRYABLE" if ("timeout" in reason or "connection" in reason) else "FAILED_FINAL"

    for nested in iter_exception_chain(exc):
        if isinstance(nested, ApplicationError):
            if nested.type == "ToolCallRetryableError":
                return "FAILED_RETRYABLE"
            if nested.non_retryable:
                return "FAILED_FINAL"
            return default_status

    return default_status
