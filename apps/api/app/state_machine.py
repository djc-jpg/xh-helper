from __future__ import annotations

from typing import Final


FINAL_STATES: Final[set[str]] = {"SUCCEEDED", "FAILED_RETRYABLE", "FAILED_FINAL", "CANCELLED", "TIMED_OUT"}

ALLOWED_TRANSITIONS: Final[dict[str, set[str]]] = {
    "QUEUED": {"VALIDATING", "FAILED_RETRYABLE", "FAILED_FINAL", "CANCELLED"},
    "VALIDATING": {"PLANNING", "FAILED_RETRYABLE", "FAILED_FINAL", "CANCELLED"},
    "PLANNING": {"RUNNING", "FAILED_RETRYABLE", "FAILED_FINAL", "CANCELLED"},
    "RUNNING": {"WAITING_TOOL", "REVIEWING", "FAILED_RETRYABLE", "FAILED_FINAL", "CANCELLED"},
    "WAITING_TOOL": {"WAITING_HUMAN", "REVIEWING", "FAILED_RETRYABLE", "FAILED_FINAL", "CANCELLED"},
    "WAITING_HUMAN": {"REVIEWING", "FAILED_FINAL", "TIMED_OUT", "CANCELLED"},
    "REVIEWING": {"SUCCEEDED", "FAILED_RETRYABLE", "FAILED_FINAL", "CANCELLED"},
    "FAILED_RETRYABLE": {"VALIDATING", "FAILED_FINAL", "CANCELLED"},
    "FAILED_FINAL": {"FAILED_FINAL"},
    "SUCCEEDED": {"SUCCEEDED"},
    "TIMED_OUT": {"TIMED_OUT"},
    "CANCELLED": {"CANCELLED"},
    "RECEIVED": {"QUEUED", "VALIDATING", "FAILED_FINAL"},
}


def is_valid_transition(current_status: str, next_status: str) -> bool:
    if current_status == next_status:
        return True
    return next_status in ALLOWED_TRANSITIONS.get(current_status, set())
