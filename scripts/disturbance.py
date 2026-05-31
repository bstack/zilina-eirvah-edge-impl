"""
Injects periodic setpoint disturbances into the OPC UA simulator.

Simulates a process disturbance (heat load change, upstream override, etc.)
that pushes temperature above the decision-agent-stub threshold, triggering
the CPS actuation loop autonomously.

Timing (default config, alpha=0.05, tick=500ms):
  ~7s   — temperature crosses 26°C threshold after disturbance
  ~37s  — decision-agent-stub fires (30s sustained breach)
  ~60s  — actuation cooldown expires
  120s  — next disturbance (default interval)

Usage:
  uv run python scripts/disturbance.py
  uv run python scripts/disturbance.py --disturbance 30.0 --interval 120 --endpoint opc.tcp://localhost:14840/eirvah/simulator
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import UTC, datetime


async def run(
    *,
    endpoint: str,
    disturbance_value: float,
    interval_s: float,
    namespace_uri: str,
) -> None:
    from asyncua import Client

    print(f"connecting to {endpoint}", flush=True)

    async with Client(url=endpoint) as client:
        ns_idx = await client.get_namespace_index(namespace_uri)
        node = await client.nodes.objects.get_child(
            [f"{ns_idx}:bottler", f"{ns_idx}:SetpointTemperature"]
        )

        cycle = 0
        while True:
            cycle += 1
            current = float(await node.read_value())
            await node.write_value(disturbance_value)
            ts = datetime.now(UTC).isoformat(timespec="seconds")
            print(
                f"[{ts}] cycle={cycle}  disturbance written: {current:.2f}°C → {disturbance_value:.2f}°C",
                flush=True,
            )
            print(
                f"[{ts}]   decision-agent fires in ~37s  |  next disturbance in {interval_s:.0f}s",
                flush=True,
            )
            await asyncio.sleep(interval_s)


def main() -> None:
    parser = argparse.ArgumentParser(description="OPC UA setpoint disturbance injector")
    parser.add_argument("--endpoint", default="opc.tcp://localhost:14840/eirvah/simulator")
    parser.add_argument("--namespace", default="https://eirvah.uniza/zilina/factory1")
    parser.add_argument("--disturbance", type=float, default=30.0, help="setpoint to inject (°C)")
    parser.add_argument("--interval", type=float, default=120.0, help="seconds between disturbances")
    args = parser.parse_args()

    try:
        asyncio.run(
            run(
                endpoint=args.endpoint,
                disturbance_value=args.disturbance,
                interval_s=args.interval,
                namespace_uri=args.namespace,
            )
        )
    except KeyboardInterrupt:
        print("\nstopped", flush=True)
        sys.exit(0)


if __name__ == "__main__":
    main()
