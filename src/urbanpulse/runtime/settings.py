# src/urbanpulse/runtime/settings.py
from pydantic_settings import BaseSettings, SettingsConfigDict


class RuntimeSettings(BaseSettings):
    """Connection settings for the local Compose runtime"""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="URBANPULSE_",
        extra="ignore",
    )

    kafka_bootstrap_servers: str = "localhost:19092"
    postgres_dsn: str = (
        "postgresql://urbanpulse:urbanpulse-local-only@localhost:55433/urbanpulse"
    )
    object_store_endpoint_url: str = "http://localhost:18333"
    object_store_access_key_id: str = "urbanpulse"
    object_store_secret_access_key: str = "urbanpulse-local-only"
    object_store_bucket: str = "urbanpulse-raw"