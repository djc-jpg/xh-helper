from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_env: str = "local"
    database_url: str = "postgresql://platform:platform@localhost:5432/platform"

    api_base_url: str = "http://localhost:18000"
    internal_api_token: str = ""
    worker_id: str = "worker-local"
    worker_auth_token: str = ""

    temporal_target: str = "localhost:7233"
    temporal_namespace: str = "default"
    temporal_task_queue: str = "xh-task-queue"

    langgraph_postgres_dsn: str | None = None
    langgraph_checkpoint_fail_fast: bool = False
    docs_dir: str = "/workspace/data/docs"
    artifact_dir: str = "/workspace/artifacts"

    otel_service_name: str = "worker"
    otel_exporter_otlp_endpoint: str = "http://localhost:4317"
    default_tenant_id: str = "default"

    mas_enabled: bool = False
    mas_orchestration_mode: str = "closed_loop"
    mas_message_backend: str = "memory"
    redis_url: str = "redis://localhost:6379/0"
    mas_cache_ttl_s: int = 120
    mas_rate_limit_requests: int = 30
    mas_rate_limit_window_s: int = 60
    mas_retry_max_attempts: int = 3
    mas_retry_base_delay_s: float = 1.0
    mas_retry_max_delay_s: float = 10.0
    mas_shadow_mode: bool = False
    qwen_api_key: str = Field(default="", validation_alias=AliasChoices("QWEN_API_KEY", "DASHSCOPE_API_KEY"))
    qwen_model: str = Field(default="qwen-plus", validation_alias=AliasChoices("QWEN_MODEL", "DASHSCOPE_MODEL"))
    qwen_base_url: str = Field(
        default="https://dashscope.aliyuncs.com/compatible-mode/v1",
        validation_alias=AliasChoices("QWEN_BASE_URL", "DASHSCOPE_BASE_URL"),
    )
    qwen_timeout_s: float = 20.0
    qwen_temperature: float = 0.2
    qwen_max_tokens: int = 800


settings = Settings()
