"""Settings for the S7 Capper+Palletizer simulator."""
from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class SimulatorSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="S7_SIMULATOR_",
        env_file=None,
        extra="ignore",
    )

    host: str = "0.0.0.0"
    tick_rate_ms: int = Field(default=500, ge=50, le=10000)
    seed: int = 0
    http_port: int = Field(default=8080, ge=1024, le=65535)
    log_level: str = "INFO"
