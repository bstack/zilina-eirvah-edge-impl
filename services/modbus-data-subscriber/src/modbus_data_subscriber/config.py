"""Settings for the Modbus TCP data subscriber."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class SubscriberSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="MODBUS_DATA_SUBSCRIBER_",
        env_file=None,
        extra="ignore",
    )

    durable_inspections: bool = False
    mqtt_host: str = "mosquitto"
    mqtt_port: int = 1883
    mqtt_username: str = "eirvah"
    mqtt_password: str = "eirvah-dev-password"
    receipt_key: SecretStr = SecretStr("")
    receipt_topic: str = "uniza/zilina/factory1/line_a/inspector/inspector_01/inspection_receipt"
    source_id: str = "modbus-data-subscriber"
    position_path: Path = Path("/var/lib/eirvah/positions.sqlite3")
    nats_servers: list[str] = ["nats://nats:4222"]
    register_map_path: Path = Path("/etc/modbus-data-subscriber/modbus-register-map.yaml")
    http_port: int = Field(default=8080, ge=1024, le=65535)
    log_level: str = "INFO"
    reconnect_delay_s: float = 5.0
