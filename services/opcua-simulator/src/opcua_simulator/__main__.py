"""Entry point for the OPC UA simulator pod."""

from __future__ import annotations

import asyncio

from opcua_simulator.config import SimulatorSettings
from opcua_simulator.server import run


def main() -> None:
    settings = SimulatorSettings()
    asyncio.run(run(settings))


if __name__ == "__main__":  # pragma: no cover
    main()
