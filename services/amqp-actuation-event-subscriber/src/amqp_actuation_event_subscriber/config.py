"""Settings for the AMQP actuation event subscriber."""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AmqpSubscriberSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="AMQP_ACTUATION_EVENT_SUBSCRIBER_",
        env_file=None,
        extra="ignore",
    )

    nats_servers: list[str] = ["nats://nats:4222"]
    amqp_url: str = "amqp://eirvah:eirvah-dev-password@rabbitmq:5672/"
    amqp_queue: str = "eirvah.actuation.requests"
    amqp_prefetch: int = 1
    http_port: int = Field(default=8080, ge=1024, le=65535)
    log_level: str = "INFO"
