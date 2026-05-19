"""Settings for the MQTT UNS publisher worker."""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class MqttPublisherSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="MQTT_UNS_PUBLISHER_",
        env_file=None,
        extra="ignore",
    )

    nats_servers: list[str] = ["nats://nats:4222"]
    mqtt_host: str = "mosquitto"
    mqtt_port: int = 1883
    mqtt_username: str = "eirvah"
    mqtt_password: str = "eirvah-dev-password"
    mqtt_client_id: str = "mqtt-uns-publisher"
    qos: int = Field(default=1, ge=0, le=2)
    retain: bool = False
    http_port: int = Field(default=8080, ge=1024, le=65535)
    log_level: str = "INFO"
    reconnect_delay_s: float = 5.0
