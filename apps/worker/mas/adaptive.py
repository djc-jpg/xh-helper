from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class FailureType(str, Enum):
    NETWORK = "network"
    SERVICE_UNAVAILABLE = "service_unavailable"
    TIMEOUT = "timeout"
    VALIDATION = "validation"
    PERMISSION = "permission"
    UNKNOWN = "unknown"


@dataclass(slots=True)
class RecoveryAction:
    should_retry: bool
    delay_s: float
    request_collaboration: bool
    collaborator: str | None
    reason: str


def classify_failure_type(exc: Exception) -> FailureType:
    message = str(exc).lower()
    if "timeout" in message:
        return FailureType.TIMEOUT
    if "connection" in message or "network" in message or "dns" in message:
        return FailureType.NETWORK
    if "503" in message or "service unavailable" in message:
        return FailureType.SERVICE_UNAVAILABLE
    if "permission" in message or "forbidden" in message or "unauthorized" in message:
        return FailureType.PERMISSION
    if "validation" in message or "schema" in message or "invalid" in message:
        return FailureType.VALIDATION
    return FailureType.UNKNOWN


class RecoveryPolicy:
    def __init__(self, *, max_attempts: int = 3, base_delay_s: float = 1.0, max_delay_s: float = 20.0) -> None:
        self.max_attempts = max(1, int(max_attempts))
        self.base_delay_s = max(0.1, float(base_delay_s))
        self.max_delay_s = max(self.base_delay_s, float(max_delay_s))

    def decide(self, *, failure_type: FailureType, attempt: int) -> RecoveryAction:
        if attempt >= self.max_attempts:
            return RecoveryAction(
                should_retry=False,
                delay_s=0.0,
                request_collaboration=True,
                collaborator="approval_agent",
                reason=f"attempt_limit_reached:{failure_type.value}",
            )

        if failure_type in {FailureType.NETWORK, FailureType.SERVICE_UNAVAILABLE, FailureType.TIMEOUT}:
            delay = min(self.base_delay_s * (2 ** max(0, attempt - 1)), self.max_delay_s)
            collaborator = "scheduler_agent" if failure_type == FailureType.SERVICE_UNAVAILABLE else None
            return RecoveryAction(
                should_retry=True,
                delay_s=delay,
                request_collaboration=collaborator is not None,
                collaborator=collaborator,
                reason=f"retryable:{failure_type.value}",
            )

        if failure_type in {FailureType.PERMISSION, FailureType.VALIDATION}:
            return RecoveryAction(
                should_retry=False,
                delay_s=0.0,
                request_collaboration=True,
                collaborator="approval_agent",
                reason=f"manual_intervention:{failure_type.value}",
            )

        return RecoveryAction(
            should_retry=False,
            delay_s=0.0,
            request_collaboration=True,
            collaborator="scheduler_agent",
            reason="unknown_error",
        )
