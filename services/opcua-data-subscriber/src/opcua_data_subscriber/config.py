"""Settings for the OPC UA data subscriber."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class SubscriberSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="OPCUA_DATA_SUBSCRIBER_",
        env_file=None,
        extra="ignore",
    )

    nats_servers: list[str] = ["nats://nats:4222"]
    node_list_path: Path = Path("/etc/opcua-data-subscriber/opcua-node-list.yaml")
    http_port: int = Field(default=8080, ge=1024, le=65535)
    log_level: str = "INFO"
    reconnect_delay_s: float = 5.0
