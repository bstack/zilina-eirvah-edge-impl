from __future__ import annotations
import asyncio
from modbus_data_subscriber.config import SubscriberSettings
from modbus_data_subscriber.service import run


def main() -> None:
    asyncio.run(run(SubscriberSettings()))


if __name__ == "__main__":  # pragma: no cover
    main()
