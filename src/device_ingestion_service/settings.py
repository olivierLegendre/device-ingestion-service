from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "device-ingestion-service"
    app_env: str = "dev"

    persistence_backend: str = Field(default="postgres", pattern="^(in_memory|postgres)$")
    postgres_dsn: str = "postgresql://postgres:postgres@localhost:5432/device_ingestion"
    postgres_auto_init: bool = True

    dedup_window_seconds: int = Field(default=3600, ge=60)
    dead_letter_list_default_limit: int = Field(default=100, ge=1, le=1000)

    mqtt_host: str = "localhost"
    mqtt_port: int = 1883
    mqtt_topics: str = "zigbee2mqtt/#,lorawan/#"
    mqtt_client_id: str = "device-ingestion-worker"
    mqtt_qos: int = Field(default=1, ge=0, le=2)
    mqtt_default_organization_id: str = "org-default"
    mqtt_default_site_id: str = "site-default"

    model_config = SettingsConfigDict(
        env_prefix="DEVICE_INGESTION_",
        env_file=".env",
        extra="ignore",
    )
