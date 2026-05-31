"""Settings for the Modbus TCP simulator."""
from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class SimulatorSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="MODBUS_SIMULATOR_",
        env_file=None,
        extra="ignore",
    )

    host: str = "0.0.0.0"
    port: int = Field(default=5020, ge=1024, le=65535)
    unit_id: int = Field(default=1, ge=1, le=247)
    tick_rate_ms: int = Field(default=500, ge=50, le=10000)
    seed: int = 0
    http_port: int = Field(default=8080, ge=1024, le=65535)
    log_level: str = "INFO"
