"""Settings for the actuation signal publisher."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class SignalPublisherSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="ACTUATION_SIGNAL_PUBLISHER_",
        env_file=None,
        extra="ignore",
    )

    approval_key: SecretStr = SecretStr("")
    writer_id: str = "actuation-signal-publisher"
    ledger_path: Path = Path("/var/lib/eirvah/commands.sqlite3")
    ledger_retention_s: int = Field(default=86400, ge=1)

    nats_servers: list[str] = ["nats://nats:4222"]
    mapping_path: Path = Path("/etc/actuation-signal-publisher/opcua-node-to-uns-mapping.yaml")
    node_list_path: Path = Path("/etc/actuation-signal-publisher/opcua-node-list.yaml")
    native_targets_path: Path = Path(
        "/etc/actuation-signal-publisher/native-actuation-targets.yaml"
    )
    enterprise: str = "uniza"
    site: str = "zilina"
    http_port: int = Field(default=8080, ge=1024, le=65535)
    log_level: str = "INFO"
