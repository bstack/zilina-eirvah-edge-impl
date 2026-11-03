"""Settings for the actuation event validator."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class ValidatorSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="ACTUATION_EVENT_VALIDATOR_",
        env_file=None,
        extra="ignore",
    )

    ingress_key: SecretStr = SecretStr("")
    approval_key: SecretStr = SecretStr("")
    approved_writer: str = "actuation-signal-publisher"

    nats_servers: list[str] = ["nats://nats:4222"]
    policy_path: Path = Path("/etc/actuation-event-validator/actuation-policy.yaml")
    http_port: int = Field(default=8080, ge=1024, le=65535)
    log_level: str = "INFO"
