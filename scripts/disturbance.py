"""Alternate conveyor production modes and inject a separate warmer cooling load.

This supersedes the old direct temperature-setpoint override demo. Conveyor
commands use normal AMQP validation/writing; the supervisor selects the warmer
recipe from UNS speed-setpoint observations. Stop with Ctrl+C. The final mode
remains selected; this tool does not silently restore or retry uncertain commands.
"""

from __future__ import annotations

import argparse
import asyncio
import math
import os
from datetime import UTC, datetime

from asyncua import Client
from control import build_command, send


async def apply_cycle(client: Client, *, speed: float, amqp_url: str, namespace_uri: str) -> None:
    command = build_command("conveyor", "speed", speed)
    result = await send(command, amqp_url)
    if result.get("decision") != "approve" or not result.get("written_at"):
        raise RuntimeError(f"conveyor speed write not confirmed: {result}")
    ns = await client.get_namespace_index(namespace_uri)
    bottler = await client.nodes.objects.get_child([f"{ns}:bottler"])
    await bottler.call_method(f"{ns}:TriggerCoolingLoad")
    print(
        f"{datetime.now(UTC).isoformat()} conveyor speed setpoint={speed} m/s; "
        "cooling injected without changing the warmer setpoint. "
        "Check the separate supervisory command and subsequent temperature response.",
        flush=True,
    )


async def run(*, endpoint: str, interval_s: float, namespace_uri: str, amqp_url: str) -> None:
    async with Client(url=endpoint) as client:
        high = True
        while True:
            await apply_cycle(
                client,
                speed=0.7 if high else 0.4,
                amqp_url=amqp_url,
                namespace_uri=namespace_uri,
            )
            high = not high
            await asyncio.sleep(interval_s)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", default="opc.tcp://localhost:4840/eirvah/simulator")
    parser.add_argument("--namespace", default="https://eirvah.uniza/zilina/factory1")
    parser.add_argument("--amqp-url", default=os.environ.get("EIRVAH_COMMAND_AMQP_URL"))
    parser.add_argument("--interval", type=float, default=120.0)
    args = parser.parse_args()
    if not args.amqp_url:
        parser.error("set EIRVAH_COMMAND_AMQP_URL or --amqp-url with experiment-driver credentials")
    if not math.isfinite(args.interval) or args.interval <= 0:
        parser.error("interval must be positive and finite")
    try:
        asyncio.run(
            run(
                endpoint=args.endpoint,
                interval_s=args.interval,
                namespace_uri=args.namespace,
                amqp_url=args.amqp_url,
            )
        )
    except TimeoutError:
        parser.exit(2, "Outcome UNKNOWN: inspect the command before retrying.\n")
    except KeyboardInterrupt:
        print("Stopped; the final production mode remains selected.")


if __name__ == "__main__":
    main()
