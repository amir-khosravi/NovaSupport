"""
Application settings loaded from environment variables via pydantic-settings.
All values have defaults safe for local development.
"""

from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Absolute path to the project-root .env so nested classes can find it
# regardless of the current working directory.
_ENV_FILE = str(Path(__file__).resolve().parent.parent / ".env")

_BASE_CFG = SettingsConfigDict(
    env_file=_ENV_FILE,
    env_file_encoding="utf-8",
    extra="ignore",
    case_sensitive=False,
)


class AppSettings(BaseSettings):
    model_config = _BASE_CFG
    env: str = Field("development", alias="APP_ENV")
    name: str = Field("novasupport", alias="APP_NAME")
    version: str = Field("0.1.0", alias="APP_VERSION")
    log_level: str = Field("INFO", alias="LOG_LEVEL")


class LLMSettings(BaseSettings):
    model_config = _BASE_CFG
    provider: str = Field("anthropic", alias="LLM_PROVIDER")
    model: str = Field("claude-sonnet-4-6", alias="LLM_MODEL")
    anthropic_api_key: str = Field("", alias="ANTHROPIC_API_KEY")


class LangSmithSettings(BaseSettings):
    model_config = _BASE_CFG
    api_key: str = Field("", alias="LANGSMITH_API_KEY")
    project: str = Field("novasupport", alias="LANGSMITH_PROJECT")
    tracing: bool = Field(False, alias="LANGSMITH_TRACING")


class KafkaSettings(BaseSettings):
    model_config = _BASE_CFG
    bootstrap_servers: str = Field("localhost:9094", alias="KAFKA_BOOTSTRAP_SERVERS")
    command_topic: str = Field("agent.commands", alias="KAFKA_COMMAND_TOPIC")
    result_topic: str = Field("agent.results", alias="KAFKA_RESULT_TOPIC")
    ticket_topic: str = Field("ticket.created", alias="KAFKA_TICKET_TOPIC")
    completed_topic: str = Field("ticket.completed", alias="KAFKA_COMPLETED_TOPIC")
    retry_topic: str = Field("agent.commands.retry", alias="KAFKA_RETRY_TOPIC")
    dlq_topic: str = Field("agent.commands.dlq", alias="KAFKA_DLQ_TOPIC")
    escalated_topic: str = Field("ticket.escalated", alias="KAFKA_ESCALATED_TOPIC")
    consumer_group: str = Field("support-agent-workers", alias="KAFKA_CONSUMER_GROUP")
    command_partitions: int = Field(6, alias="KAFKA_COMMAND_PARTITIONS")
    replication_factor: int = Field(1, alias="KAFKA_REPLICATION_FACTOR")


class PostgresSettings(BaseSettings):
    model_config = _BASE_CFG
    url: str = Field(
        "postgresql+asyncpg://novasupport:novasupport_secret@localhost:5432/novasupport",
        alias="POSTGRES_URL",
    )


class QdrantSettings(BaseSettings):
    model_config = _BASE_CFG
    url: str = Field("http://localhost:6333", alias="QDRANT_URL")
    api_key: str = Field("", alias="QDRANT_API_KEY")
    collection: str = Field("novasupport_policies", alias="QDRANT_COLLECTION")


class RedisSettings(BaseSettings):
    model_config = _BASE_CFG
    url: str = Field("redis://localhost:6379/0", alias="REDIS_URL")


class APISettings(BaseSettings):
    model_config = _BASE_CFG
    host: str = Field("0.0.0.0", alias="API_HOST")
    port: int = Field(8000, alias="API_PORT")
    reload: bool = Field(True, alias="API_RELOAD")


class WorkerSettings(BaseSettings):
    model_config = _BASE_CFG
    concurrency: int = Field(3, alias="WORKER_CONCURRENCY")
    task_timeout_seconds: int = Field(60, alias="WORKER_TASK_TIMEOUT_SECONDS")
    max_retries: int = Field(3, alias="WORKER_MAX_RETRIES")


class RAGSettings(BaseSettings):
    model_config = _BASE_CFG
    chunk_size: int = Field(512, alias="RAG_CHUNK_SIZE")
    chunk_overlap: int = Field(64, alias="RAG_CHUNK_OVERLAP")
    top_k: int = Field(5, alias="RAG_TOP_K")
    embedding_model: str = Field(
        "sentence-transformers/all-MiniLM-L6-v2", alias="RAG_EMBEDDING_MODEL"
    )


class MCPSettings(BaseSettings):
    model_config = _BASE_CFG
    operational_data_url: str = Field(
        "http://localhost:8001/sse",
        alias="MCP_OPERATIONAL_DATA_URL",
    )


class SecuritySettings(BaseSettings):
    # env_ignore_empty: empty string in .env → use default instead of failing JSON parse
    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
        env_ignore_empty=True,
    )
    # Accepts JSON array: API_KEYS=["sk-nova-abc","sk-nova-xyz"]
    # OR comma-separated:  API_KEYS=sk-nova-abc,sk-nova-xyz
    # Empty / unset = open mode (no auth required — for local dev)
    api_keys: list[str] = Field(default_factory=list, alias="API_KEYS")

    @field_validator("api_keys", mode="before")
    @classmethod
    def _parse_api_keys(cls, v: object) -> object:
        if isinstance(v, str):
            v = v.strip()
            if not v:
                return []
            if v.startswith("["):
                import json
                return json.loads(v)
            return [k.strip() for k in v.split(",") if k.strip()]
        return v
    rate_limit_requests: int = Field(60, alias="RATE_LIMIT_REQUESTS")
    rate_limit_window_seconds: int = Field(60, alias="RATE_LIMIT_WINDOW_SECONDS")
    max_message_length: int = Field(2000, alias="MAX_MESSAGE_LENGTH")


class OTelSettings(BaseSettings):
    model_config = _BASE_CFG
    enabled: bool = Field(False, alias="OTEL_ENABLED")
    endpoint: str = Field("http://localhost:4317", alias="OTEL_EXPORTER_OTLP_ENDPOINT")
    service_name: str = Field("novasupport", alias="OTEL_SERVICE_NAME")


class Settings(BaseSettings):
    """Root settings object. Reads from project-root .env automatically."""

    model_config = _BASE_CFG

    app: AppSettings = Field(default_factory=AppSettings)
    llm: LLMSettings = Field(default_factory=LLMSettings)
    langsmith: LangSmithSettings = Field(default_factory=LangSmithSettings)
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    postgres: PostgresSettings = Field(default_factory=PostgresSettings)
    qdrant: QdrantSettings = Field(default_factory=QdrantSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    api: APISettings = Field(default_factory=APISettings)
    worker: WorkerSettings = Field(default_factory=WorkerSettings)
    rag: RAGSettings = Field(default_factory=RAGSettings)
    mcp: MCPSettings = Field(default_factory=MCPSettings)
    otel: OTelSettings = Field(default_factory=OTelSettings)
    security: SecuritySettings = Field(default_factory=SecuritySettings)


# Single shared instance — import this everywhere.
settings = Settings()
