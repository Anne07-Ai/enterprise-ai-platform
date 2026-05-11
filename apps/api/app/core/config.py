"""Application configuration — Pydantic Settings v2, env-driven, EAIP_ prefix.

Settings are instantiated once via ``get_settings()`` and cached. Sensitive
values use ``SecretStr`` so they don't leak to logs by default.

Nested blocks use the ``EAIP_<SECTION>_<KEY>`` convention via
``env_nested_delimiter='_'``. For example, the database URL is read from
``EAIP_DATABASE_URL`` but other DB knobs use ``EAIP_DATABASE_POOL_SIZE`` etc.
We disable nested delimiter and use explicit field-level ``alias_choices``
where needed because some keys (DATABASE_URL, REDIS_URL) are conventionally
flat.
"""

from __future__ import annotations

from enum import StrEnum
from functools import lru_cache
from pathlib import Path

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Environment(StrEnum):
    LOCAL = "local"
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    TEST = "test"


class _Base(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="EAIP_",
        env_file=None,  # Provided by docker / shell, not by .env in production.
        case_sensitive=False,
        extra="ignore",
    )


class DatabaseSettings(_Base):
    model_config = SettingsConfigDict(env_prefix="EAIP_DATABASE_", case_sensitive=False, extra="ignore")

    url: SecretStr = Field(
        default=SecretStr("postgresql+asyncpg://eaip:changeme_local_only@localhost:5432/eaip"),
        description="SQLAlchemy async URL — must use the asyncpg driver.",
    )
    pool_size: int = 10
    max_overflow: int = 20
    pool_timeout_seconds: int = 30
    pool_recycle_seconds: int = 1800
    echo: bool = False
    statement_timeout_ms: int = 30_000

    @field_validator("url")
    @classmethod
    def _must_be_async(cls, v: SecretStr) -> SecretStr:
        if "+asyncpg" not in v.get_secret_value():
            raise ValueError("EAIP_DATABASE_URL must use the asyncpg driver (postgresql+asyncpg://...)")
        return v


class RedisSettings(_Base):
    model_config = SettingsConfigDict(env_prefix="EAIP_REDIS_", case_sensitive=False, extra="ignore")

    url: SecretStr = Field(default=SecretStr("redis://localhost:6379/0"))
    pool_max_connections: int = 50
    socket_timeout_seconds: float = 5.0


class KafkaSettings(_Base):
    model_config = SettingsConfigDict(env_prefix="EAIP_KAFKA_", case_sensitive=False, extra="ignore")

    bootstrap_servers: str = "localhost:9092"
    client_id: str = "eaip-api"
    audit_topic: str = "audit.event.v1"
    request_timeout_ms: int = 10_000
    enable_idempotence: bool = True


class AuthSettings(_Base):
    model_config = SettingsConfigDict(env_prefix="EAIP_AUTH_", case_sensitive=False, extra="ignore")

    jwt_algorithm: str = "RS256"
    jwt_issuer: str = "eaip"
    jwt_audience: str = "eaip-api"
    jwt_private_key_path: Path | None = None
    jwt_public_key_path: Path | None = None
    access_token_ttl_seconds: int = 60 * 15        # 15 minutes
    refresh_token_ttl_seconds: int = 60 * 60 * 24 * 7  # 7 days
    api_key_prefix_live: str = "eaip_live_"
    api_key_prefix_test: str = "eaip_test_"
    # Argon2id parameters tuned for ~50ms on a typical server CPU.
    argon2_time_cost: int = 3
    argon2_memory_cost_kib: int = 64 * 1024
    argon2_parallelism: int = 2
    argon2_hash_length: int = 32


class RateLimitSettings(_Base):
    model_config = SettingsConfigDict(env_prefix="EAIP_RATELIMIT_", case_sensitive=False, extra="ignore")

    enabled: bool = True
    per_org_rps: int = 100
    per_org_burst: int = 200
    per_ip_rps: int = 50
    per_ip_burst: int = 100
    redis_key_prefix: str = "rl"


class IdempotencySettings(_Base):
    model_config = SettingsConfigDict(env_prefix="EAIP_IDEMPOTENCY_", case_sensitive=False, extra="ignore")

    enabled: bool = True
    ttl_seconds: int = 60 * 60 * 24
    redis_key_prefix: str = "idem"


class ObservabilitySettings(_Base):
    model_config = SettingsConfigDict(env_prefix="EAIP_OTEL_", case_sensitive=False, extra="ignore")

    exporter_otlp_endpoint: str = "http://localhost:4317"
    exporter_otlp_insecure: bool = True
    service_name: str = "eaip-api"
    service_version: str = "0.1.0"
    sample_ratio: float = 1.0




class OpenAISettings(_Base):
    model_config = SettingsConfigDict(env_prefix="EAIP_OPENAI_", case_sensitive=False, extra="ignore")

    api_key: SecretStr = Field(default=SecretStr(""), description="OpenAI API key.")
    embedding_model: str = "text-embedding-3-small"
    embedding_dimensions: int = 1536
    request_timeout_seconds: float = 30.0
    max_retries: int = 3




class MinIOSettings(_Base):
    """MinIO / S3-compatible object storage configuration."""

    model_config = SettingsConfigDict(env_prefix='EAIP_MINIO_', case_sensitive=False, extra='ignore')

    endpoint_url: str = 'http://localhost:9000'
    access_key: SecretStr = Field(default=SecretStr('minioadmin'))
    secret_key: SecretStr = Field(default=SecretStr('changeme_local_only'))
    region: str = 'us-east-1'


class Settings(_Base):
    """Top-level settings. Composed of nested blocks — each is its own ``BaseSettings``."""

    model_config = SettingsConfigDict(env_prefix="EAIP_", case_sensitive=False, extra="ignore")

    environment: Environment = Environment.LOCAL
    log_level: str = "INFO"
    log_json: bool = True
    seed_demo: bool = Field(default=False, alias="EAIP_SEED_DEMO")
    cors_allow_origins: list[str] = Field(default_factory=lambda: ["http://localhost:3000"])

    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    auth: AuthSettings = Field(default_factory=AuthSettings)
    rate_limit: RateLimitSettings = Field(default_factory=RateLimitSettings)
    idempotency: IdempotencySettings = Field(default_factory=IdempotencySettings)
    observability: ObservabilitySettings = Field(default_factory=ObservabilitySettings)
    openai: OpenAISettings = Field(default_factory=OpenAISettings)
    minio: MinIOSettings = Field(default_factory=MinIOSettings)

    @property
    def is_production(self) -> bool:
        return self.environment == Environment.PRODUCTION

    @property
    def is_test(self) -> bool:
        return self.environment == Environment.TEST


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide settings singleton.

    Tests that need to override settings should call ``get_settings.cache_clear()``
    after monkey-patching the relevant environment variables.
    """
    return Settings()
