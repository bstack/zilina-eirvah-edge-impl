"""Environment-driven settings for the OPC UA simulator."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class SimulatorSettings(BaseSettings):
    """All knobs the simulator exposes via env (spec §9.4 + ConfigMap §7.1)."""

    model_config = SettingsConfigDict(
        env_prefix="OPCUA_SIMULATOR_",
        env_file=None,
        extra="ignore",
    )

    endpoint: str = "opc.tcp://0.0.0.0:4840/eirvah/simulator"
    tick_rate_ms: int = Field(default=100, ge=10, le=5000)
    seed: int = 0
    address_space_path: Path = Path("/etc/opcua-simulator/opcua-address-space.yaml")
    http_port: int = Field(default=8080, ge=1024, le=65535)
    hot_spike_probability: float = Field(default=0.0, ge=0.0, le=1.0)
    motor_fault_probability: float = Field(default=0.0, ge=0.0, le=1.0)
    log_level: str = "INFO"
