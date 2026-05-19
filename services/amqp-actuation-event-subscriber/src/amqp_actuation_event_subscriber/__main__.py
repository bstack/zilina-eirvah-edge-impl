"""Entry point for the AMQP actuation event subscriber pod."""

from __future__ import annotations

import asyncio

from amqp_actuation_event_subscriber.config import AmqpSubscriberSettings
from amqp_actuation_event_subscriber.service import run


def main() -> None:
    asyncio.run(run(AmqpSubscriberSettings()))


if __name__ == "__main__":  # pragma: no cover
    main()
