from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "sqlite+pysqlite:///:memory:"
    kafka_bootstrap_servers: str = "kafka:9092"
    crm_base_url: str = "http://crm:8000"
    crm_delivery_timeout_seconds: float = 5.0
    log_level: str = "INFO"
