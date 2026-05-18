"""Entry point for the MQTT UNS publisher pod."""

from __future__ import annotations

import asyncio

from mqtt_uns_publisher.config import MqttPublisherSettings
from mqtt_uns_publisher.service import run


def main() -> None:
    asyncio.run(run(MqttPublisherSettings()))


if __name__ == "__main__":  # pragma: no cover
    main()
