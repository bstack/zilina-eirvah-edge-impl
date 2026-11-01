"""Experimental production-mode policy; defaults are study assumptions."""

from __future__ import annotations

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class DecisionAgentSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="DECISION_AGENT_STUB_",
        env_file=None,
        extra="ignore",
    )

    mqtt_host: str = "mosquitto"
    mqtt_port: int = 1883
    mqtt_username: str = "eirvah"
    mqtt_password: str = "eirvah-dev-password"
    subscribe_topic: str = "uniza/zilina/factory1/line_a/conveyor/belt_01/speed_setpoint"
    amqp_url: str = "amqp://eirvah:eirvah-dev-password@rabbitmq:5672/"
    amqp_queue: str = "eirvah.actuation.requests"
    enabled: bool = True
    normal_speed_max: float = Field(default=0.5, ge=0, le=0.8, allow_inf_nan=False)
    high_speed_min: float = Field(default=0.6, ge=0, le=0.8, allow_inf_nan=False)
    normal_setpoint: float = Field(default=22.0, ge=20, le=30, allow_inf_nan=False)
    high_setpoint: float = Field(default=26.0, ge=20, le=30, allow_inf_nan=False)
    max_observation_age_s: float = Field(default=5.0, gt=0, allow_inf_nan=False)
    target_uns_topic: str = (
        "uniza/zilina/factory1/line_a/bottler/setpoint_unit/setpoint_temperature"
    )
    log_level: str = "INFO"

    @model_validator(mode="after")
    def check_speed_bands(self) -> DecisionAgentSettings:
        if self.normal_speed_max >= self.high_speed_min:
            raise ValueError("normal and high speed bands must be separated")
        return self
