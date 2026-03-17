from __future__ import annotations

import warnings
import secrets

from pydantic import AliasChoices, Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_env: str = "local"
    database_url: str = "postgresql://platform:platform@localhost:5432/platform"

    jwt_secret: str = ""
    jwt_algorithm: str = "HS256"
    access_token_ttl_min: int = 30
    refresh_token_ttl_days: int = 7

    temporal_target: str = "localhost:7233"
    temporal_namespace: str = "default"
    temporal_task_queue: str = "xh-task-queue"

    internal_api_token: str = ""
    worker_auth_tokens: dict[str, str] = Field(default_factory=dict)
    allowed_worker_ids: list[str] = Field(default_factory=lambda: ["worker-local"])
    default_worker_id: str = "worker-local"

    fake_internal_base_url: str = "http://localhost:8100"
    fake_internal_service_token: str = ""

    otel_service_name: str = "api"
    otel_exporter_otlp_endpoint: str = "http://localhost:4317"
    otel_startup_strict: bool = False

    artifact_dir: str = "/workspace/artifacts"
    docs_dir: str = "/workspace/data/docs"
    input_encryption_key: str = ""

    default_tenant_id: str = "default"
    environment: str = "local"

    seed_owner_email: str = "owner@example.com"
    seed_owner_password: str = ""
    seed_operator_email: str = "operator@example.com"
    seed_operator_password: str = ""
    seed_user_email: str = "user@example.com"
    seed_user_password: str = ""

    approval_signal_dispatcher_enabled: bool = True
    approval_signal_dispatch_interval_s: float = 2.0
    approval_signal_dispatch_batch_size: int = 20
    approval_signal_retry_base_delay_s: int = 2
    approval_signal_retry_max_delay_s: int = 30
    approval_signal_retry_max_attempts: int = 6
    goal_scheduler_enabled: bool = True
    goal_scheduler_interval_s: float = 5.0
    goal_scheduler_batch_size: int = 10
    goal_scheduler_cooldown_s: int = 10
    goal_scheduler_max_active_goals: int = 4
    goal_scheduler_soft_preempt_threshold: float = 0.88
    goal_scheduler_hold_s: int = 180
    goal_scheduler_active_preemption_enabled: bool = True
    goal_scheduler_starvation_score_threshold: float = 0.72
    goal_scheduler_starvation_min_age_min: float = 20.0
    goal_event_subscription_default_timeout_s: int = 900
    goal_external_source_confidence_floor: float = 0.25
    goal_external_source_low_reliability_score: float = -0.2
    goal_external_source_high_reliability_score: float = 0.2
    goal_external_source_low_timeout_multiplier: float = 0.5
    goal_external_source_high_timeout_multiplier: float = 1.5
    policy_auto_eval_enabled: bool = True
    policy_auto_eval_promote: bool = True
    policy_auto_eval_min_episode_feedback: int = 3
    policy_auto_eval_min_portfolio_feedback: int = 2
    policy_auto_eval_min_total_feedback: int = 4
    policy_auto_eval_feedback_delta: int = 4
    policy_eval_canary_on_pass_without_promote: bool = True
    policy_canary_enabled: bool = True
    policy_canary_rollout_pct: int = 20
    policy_canary_allow_high_risk: bool = False
    policy_canary_auto_rollback_on_failure: bool = True
    policy_canary_max_starvation_rate: float = 0.2
    policy_canary_starvation_rate_delta: float = 0.05
    policy_canary_max_subscription_timeout_rate: float = 0.2
    policy_canary_subscription_timeout_rate_delta: float = 0.05
    policy_canary_min_throughput_score: float = 0.35
    policy_canary_throughput_score_delta: float = 0.06
    policy_shadow_enabled: bool = True
    policy_shadow_min_probe_count: int = 3
    policy_shadow_min_outcome_count: int = 2
    policy_shadow_min_portfolio_probe_count: int = 3
    policy_shadow_min_high_risk_probe_count: int = 1
    policy_shadow_min_action_agreement_rate: float = 0.7
    policy_shadow_min_high_risk_action_agreement_rate: float = 0.85
    policy_shadow_min_portfolio_agreement_rate: float = 0.65
    policy_shadow_auto_rollback_enabled: bool = True
    policy_shadow_max_regret_signal_rate: float = 0.5
    policy_memory_max_lessons: int = 12
    policy_memory_max_tool_entries: int = 12
    policy_memory_confidence_feedback_floor: int = 12
    policy_memory_forget_after_updates: int = 18
    policy_memory_min_tool_evidence: int = 2
    policy_memory_max_retired_lessons: int = 12
    temporal_signal_fail_once: bool = False
    rerun_conflict_test_mode: bool = False
    qwen_api_key: str = Field(default="", validation_alias=AliasChoices("QWEN_API_KEY", "DASHSCOPE_API_KEY"))
    qwen_model: str = Field(default="qwen-plus", validation_alias=AliasChoices("QWEN_MODEL", "DASHSCOPE_MODEL"))
    qwen_base_url: str = Field(
        default="https://dashscope.aliyuncs.com/compatible-mode/v1",
        validation_alias=AliasChoices("QWEN_BASE_URL", "DASHSCOPE_BASE_URL"),
    )
    qwen_timeout_s: float = 20.0
    qwen_temperature: float = 0.2
    qwen_max_tokens: int = 800

    @model_validator(mode="after")
    def validate_security_fields(self) -> "Settings":
        missing = []
        for field in (
            "jwt_secret",
            "internal_api_token",
            "fake_internal_service_token",
            "input_encryption_key",
        ):
            if not getattr(self, field):
                missing.append(field)

        if not missing:
            if not self.worker_auth_tokens:
                self.worker_auth_tokens = {self.default_worker_id: self.internal_api_token}
            return self

        if self.app_env == "local":
            if not self.jwt_secret:
                self.jwt_secret = secrets.token_urlsafe(32)
            if not self.internal_api_token:
                self.internal_api_token = secrets.token_urlsafe(24)
            if not self.fake_internal_service_token:
                self.fake_internal_service_token = secrets.token_urlsafe(24)
            if not self.input_encryption_key:
                self.input_encryption_key = secrets.token_urlsafe(32)
            warnings.warn(
                "Using dev-only security defaults because required env vars are missing; do not use in production.",
                stacklevel=1,
            )
            if not self.worker_auth_tokens:
                self.worker_auth_tokens = {self.default_worker_id: self.internal_api_token}
            return self

        raise ValueError(f"Missing required security env vars: {', '.join(missing)}")


settings = Settings()
