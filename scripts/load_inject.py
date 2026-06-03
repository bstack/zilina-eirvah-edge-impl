"""
NATS load injector for Experiment C — horizontal autoscaling.

Publishes NATSEnvelope messages directly to uns.work.contextualize at a
configurable rate, cycling through all 21 bottling-line node IDs. Replies
are discarded (reply-to has no subscriber) — pure load generation.

Usage:
    uv run python scripts/load_inject.py
    uv run python scripts/load_inject.py --rate 500 --duration 120
    uv run python scripts/load_inject.py --rate 1000 --duration 60 --nats-url nats://localhost:4222
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from datetime import UTC, datetime
from itertools import cycle

SUBJECT = "uns.work.contextualize"
SINK = "_load_inject.sink"  # reply-to with no subscriber — responses silently dropped

NODE_IDS = [
    "Bottler.Temperature01",
    "Bottler.ThroughputMeter01",
    "Bottler.Motor01.State",
    "Bottler.Motor01.Rpm",
    "Bottler.SetpointUnit.SetpointTemperature",
    "Filler.FillLevelSensor01",
    "Filler.Motor01.State",
    "Filler.ThroughputMeter01",
    "Conveyor.Belt01.BeltSpeed",
    "Conveyor.Belt01.JamDetected",
    "Conveyor.Belt01.BottleCount",
    "RejectStation.RejectCounter01",
    "RejectStation.ConveyorActive01",
    "Inspector.Inspector01.GoodRate",
    "Labeler.Labeler01.AlignmentScore",
    "Capper.TorqueSensor01",
    "Capper.CapSensor01",
    "Capper.RejectCounter01",
    "Palletizer.LayerCounter01",
    "Palletizer.PalletSensor01",
    "Palletizer.CycleCounter01",
]


def _build_payload(node_id: str, seq: int) -> bytes:
    now = datetime.now(UTC).isoformat()
    return json.dumps({
        "correlation_id": f"LOAD{seq:022d}",
        "payload": {
            "node_id": node_id,
            "value": 1.0,
            "value_type": "double",
            "unit": "dimensionless",
            "quality": "good",
            "source_timestamp": now,
            "received_at": now,
        },
    }).encode()


async def run(*, rate: int, duration: int, nats_url: str) -> None:
    import nats

    nc = await nats.connect(nats_url)
    print(f"connected to {nats_url}", flush=True)
    print(f"target={rate}/s  duration={duration}s  subject={SUBJECT}", flush=True)
    print("", flush=True)

    node_ids = cycle(NODE_IDS)
    sent = 0
    start = time.monotonic()
    deadline = start + duration
    report_at = start + 5.0

    # Send in batches (~20 batches/s) to reduce asyncio overhead at high rates
    batch = max(1, rate // 20)
    batch_interval = batch / rate

    while time.monotonic() < deadline:
        t0 = time.monotonic()

        await asyncio.gather(*[
            nc.publish(SUBJECT, _build_payload(next(node_ids), sent + i), reply=SINK)
            for i in range(batch)
        ])
        sent += batch

        now = time.monotonic()
        if now >= report_at:
            actual = sent / (now - start)
            remaining = max(0.0, deadline - now)
            print(f"  sent={sent:>8d}  actual={actual:>6.0f}/s  remaining={remaining:.0f}s", flush=True)
            report_at = now + 5.0

        sleep = batch_interval - (time.monotonic() - t0)
        if sleep > 0:
            await asyncio.sleep(sleep)

    await nc.drain()
    elapsed = time.monotonic() - start
    print("", flush=True)
    print(f"done  sent={sent}  actual={sent / elapsed:.0f}/s  elapsed={elapsed:.1f}s", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="NATS load injector for uns-auto-contextualizer")
    parser.add_argument("--rate", type=int, default=500, help="messages per second (default: 500)")
    parser.add_argument("--duration", type=int, default=120, help="seconds to run (default: 120)")
    parser.add_argument("--nats-url", default="nats://localhost:4222", help="NATS server URL")
    args = parser.parse_args()

    try:
        asyncio.run(run(rate=args.rate, duration=args.duration, nats_url=args.nats_url))
    except KeyboardInterrupt:
        print("\nstopped", flush=True)
        sys.exit(0)


if __name__ == "__main__":
    main()
