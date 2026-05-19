"""Settings for the actuation signal publisher."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class SignalPublisherSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="ACTUATION_SIGNAL_PUBLISHER_",
        env_file=None,
        extra="ignore",
    )

    nats_servers: list[str] = ["nats://nats:4222"]
    mapping_path: Path = Path(
        "/etc/actuation-signal-publisher/opcua-node-to-uns-mapping.yaml"
    )
    node_list_path: Path = Path(
        "/etc/actuation-signal-publisher/opcua-node-list.yaml"
    )
    enterprise: str = "uniza"
    site: str = "zilina"
    http_port: int = Field(default=8080, ge=1024, le=65535)
    log_level: str = "INFO"
