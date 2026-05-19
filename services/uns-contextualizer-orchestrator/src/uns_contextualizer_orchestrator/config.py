"""Settings for the UNS contextualizer orchestrator."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class OrchestratorSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="UNS_CONTEXTUALIZER_ORCHESTRATOR_",
        env_file=None,
        extra="ignore",
    )

    nats_servers: list[str] = ["nats://nats:4222"]
    pipeline_config_path: Path = Path(
        "/etc/uns-contextualizer-orchestrator/uns-contextualizer.yaml"
    )
    http_port: int = Field(default=8080, ge=1024, le=65535)
    log_level: str = "INFO"
