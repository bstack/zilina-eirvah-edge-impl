"""Settings for the actuation control orchestrator."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ActuationOrchestratorSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="ACTUATION_CONTROL_ORCHESTRATOR_",
        env_file=None,
        extra="ignore",
    )

    nats_servers: list[str] = ["nats://nats:4222"]
    amqp_url: str = "amqp://eirvah:eirvah-dev-password@rabbitmq:5672/"
    amqp_results_exchange: str = "eirvah.actuation.results"
    pipeline_config_path: Path = Path(
        "/etc/actuation-control-orchestrator/actuation-control.yaml"
    )
    allow_writes: bool = False
    http_port: int = Field(default=8080, ge=1024, le=65535)
    log_level: str = "INFO"
