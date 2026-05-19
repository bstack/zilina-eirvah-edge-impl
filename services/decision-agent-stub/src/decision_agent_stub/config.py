"""Settings for the decision agent stub."""

from __future__ import annotations

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
    subscribe_topic: str = (
        "uniza/zilina/factory1/line_a/bottler/temperature_sensor_01/temperature"
    )
    amqp_url: str = "amqp://eirvah:eirvah-dev-password@rabbitmq:5672/"
    amqp_queue: str = "eirvah.actuation.requests"
    threshold: float = 26.0
    trigger_duration_s: float = 30.0
    setpoint_target: float = 22.0
    cooldown_s: float = 60.0
    target_uns_topic: str = (
        "uniza/zilina/factory1/line_a/bottler/setpoint_unit/setpoint_temperature"
    )
    log_level: str = "INFO"
