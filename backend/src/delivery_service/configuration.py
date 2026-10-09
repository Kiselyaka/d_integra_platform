from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    database_url: str = "sqlite+pysqlite:///:memory:"
    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_outbox_poll_interval_seconds: float = 1.0
    delivery_timeout_seconds: float = 5.0
    delivery_poll_interval_seconds: float = 1.0
    delivery_stale_processing_timeout_seconds: int = 60
    log_level: str = "INFO"
