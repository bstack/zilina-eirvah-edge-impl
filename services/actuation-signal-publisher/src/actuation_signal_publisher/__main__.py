"""Entry point for the actuation signal publisher pod."""

from __future__ import annotations

import asyncio

from actuation_signal_publisher.config import SignalPublisherSettings
from actuation_signal_publisher.service import run


def main() -> None:
    asyncio.run(run(SignalPublisherSettings()))


if __name__ == "__main__":  # pragma: no cover
    main()
