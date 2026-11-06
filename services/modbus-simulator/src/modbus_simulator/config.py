"""Settings for the Modbus TCP simulator."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class SimulatorSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="MODBUS_SIMULATOR_",
        env_file=None,
        extra="ignore",
    )

    journal_path: Path | None = None
    outbox_capacity: int = Field(default=4096, ge=1)
    host: str = "0.0.0.0"
    port: int = Field(default=5020, ge=1024, le=65535)
    unit_id: int = Field(default=1, ge=1, le=247)
    tick_rate_ms: int = Field(default=500, ge=50, le=10000)
    seed: int = 0
    jam_probability: float = Field(default=0.001, ge=0, le=1, allow_inf_nan=False)
    http_port: int = Field(default=8080, ge=1024, le=65535)
    inspection_every_n_bottles: int = Field(default=4, ge=1, le=65535)
    inspection_fail_every_n: int = Field(default=5, ge=1, le=65535)
    log_level: str = "INFO"
