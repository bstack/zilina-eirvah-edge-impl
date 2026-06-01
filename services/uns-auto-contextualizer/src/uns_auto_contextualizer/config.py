"""Settings for the UNS auto-contextualizer worker."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AutoContextualizerSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="UNS_AUTO_CONTEXTUALIZER_",
        env_file=None,
        extra="ignore",
    )

    nats_servers: list[str] = ["nats://nats:4222"]
    ontology_path: Path = Path("/etc/uns-auto-contextualizer/eirvah-line-a.jsonld")
    http_port: int = Field(default=8080, ge=1024, le=65535)
    log_level: str = "INFO"
