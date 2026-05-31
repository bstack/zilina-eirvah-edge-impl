from __future__ import annotations
import asyncio
from modbus_simulator.config import SimulatorSettings
from modbus_simulator.server import run

def main() -> None:
    asyncio.run(run(SimulatorSettings()))

if __name__ == "__main__":  # pragma: no cover
    main()
