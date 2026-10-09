from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "sqlite+pysqlite:///:memory:"
    kafka_bootstrap_servers: str = "kafka:9092"
    crm_base_url: str = "http://crm:8000"
    crm_delivery_timeout_seconds: float = 5.0
    warehouse_base_url: str = "http://host.docker.internal:8081/WarehouseDemo_Base"
    warehouse_delivery_timeout_seconds: float = 5.0
    mini_shop_base_url: str = "http://mini-shop:8002"
    mini_shop_delivery_timeout_seconds: float = 5.0
    delivery_max_attempts: int = 3
    delivery_retry_strategy: str = "exponential"
    delivery_retry_initial_delay_seconds: int = 5
    delivery_retry_max_delay_seconds: int = 300
    delivery_retry_multiplier: int = 2
    delivery_stale_processing_timeout_seconds: int = 60
    log_level: str = "INFO"
