"""Settings for the data-converter worker."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class DataConverterSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="DATA_CONVERTER_",
        env_file=None,
        extra="ignore",
    )

    nats_servers: list[str] = ["nats://nats:4222"]
    rules_path: Path = Path("/etc/data-converter/conversion-rules.yaml")
    http_port: int = Field(default=8080, ge=1024, le=65535)
    log_level: str = "INFO"
