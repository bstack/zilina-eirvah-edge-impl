"""
NATS load injector for Experiment C — horizontal autoscaling.

Publishes NATSEnvelope messages directly to uns.work.contextualize at a
configurable rate, cycling through all 21 bottling-line node IDs. Checks
correlated contextualization replies and reports completed/error/unknown work.
This tests the contextualizer only, not the full industrial-data pipeline.
The pending-request limit applies backpressure; achieved send rate may be lower
than requested. Timeouts remain unknown outcomes, not proof of processing loss.

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
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import cycle

import nats
from eirvah_contracts.envelope import NATSEnvelope
from eirvah_contracts.pipeline import ContextualizeResult
from eirvah_contracts.ulid import generate_correlation_id
from eirvah_contracts.uns import build_uns_topic
from nats.aio.client import Client
from nats.errors import TimeoutError as RequestTimeout

SUBJECT = "uns.work.contextualize"

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


def _build_payload(node_id: str) -> bytes:
    now = datetime.now(UTC).isoformat()
    return json.dumps(
        {
            "correlation_id": generate_correlation_id(),
            "payload": {
                "node_id": node_id,
                "value": 1.0,
                "value_type": "double",
                "unit": "dimensionless",
                "quality": "good",
                "source_timestamp": now,
                "received_at": now,
            },
        }
    ).encode()


@dataclass
class LoadStats:
    sent: int = 0
    succeeded: int = 0
    errors: int = 0
    unresolved: int = 0


def _validate_reply(data: bytes, correlation_id: str) -> None:
    reply = NATSEnvelope.model_validate_json(data)
    if reply.correlation_id != correlation_id:
        raise ValueError("reply correlation ID does not match request")
    if reply.status != "ok" or reply.error is not None:
        raise ValueError(f"contextualization failed: {reply.error}")
    result = ContextualizeResult.model_validate(reply.payload)
    if result.uns_topic != build_uns_topic(result.uns_path):
        raise ValueError("reply topic does not match its UNS path")
    if not all((result.semantic_type, result.sensor_uri, result.feature_uri, result.property_uri)):
        raise ValueError("reply is missing contextualization metadata")


async def _request_one(client: Client, node_id: str, stats: LoadStats, *, timeout: float) -> None:
    payload = _build_payload(node_id)
    correlation_id = NATSEnvelope.model_validate_json(payload).correlation_id
    stats.sent += 1
    try:
        reply = await client.request(SUBJECT, payload, timeout=timeout)
        _validate_reply(reply.data, correlation_id)
    except RequestTimeout:
        # A missing reply does not establish whether processing happened.
        stats.unresolved += 1
    except Exception as exc:
        stats.errors += 1
        if stats.errors <= 5:
            print(f"request_error: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
    else:
        stats.succeeded += 1


async def run(
    *,
    rate: int,
    duration: float,
    nats_url: str,
    timeout: float = 5.0,
    max_pending: int = 1000,
) -> LoadStats:
    if rate <= 0 or duration <= 0 or timeout <= 0 or max_pending <= 0:
        raise ValueError("rate, duration, timeout and max_pending must be positive")

    nc = await nats.connect(nats_url)
    print(f"connected to {nats_url}", flush=True)
    print(f"target={rate}/s  duration={duration}s  subject={SUBJECT}", flush=True)
    print("", flush=True)

    node_ids = cycle(NODE_IDS)
    stats = LoadStats()
    pending: set[asyncio.Task[None]] = set()
    start = time.monotonic()
    deadline = start + duration
    report_at = start + 5.0

    # Send in batches (~20 batches/s) to reduce asyncio overhead at high rates
    batch = max(1, rate // 20)
    batch_interval = batch / rate

    try:
        while time.monotonic() < deadline:
            t0 = time.monotonic()
            pending = {task for task in pending if not task.done()}
            for _ in range(batch):
                if len(pending) >= max_pending:
                    _, pending = await asyncio.wait(
                        pending,
                        timeout=max(0.0, deadline - time.monotonic()),
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                if len(pending) >= max_pending or time.monotonic() >= deadline:
                    break
                pending.add(
                    asyncio.create_task(
                        _request_one(nc, next(node_ids), stats, timeout=timeout),
                    )
                )

            now = time.monotonic()
            if now >= report_at:
                print(
                    f"  sent={stats.sent} succeeded={stats.succeeded} errors={stats.errors} "
                    f"unresolved={stats.unresolved} pending={len(pending)} "
                    f"send_rate={stats.sent / (now - start):.1f}/s",
                    flush=True,
                )
                report_at = now + 5.0

            sleep = min(batch_interval - (now - t0), deadline - now)
            await asyncio.sleep(max(0.0, sleep))

        sending_elapsed = time.monotonic() - start
        if pending:
            await asyncio.gather(*pending)
    finally:
        for task in pending:
            if not task.done():
                task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        await nc.close()

    elapsed = time.monotonic() - start
    print("", flush=True)
    print(
        f"done sent={stats.sent} succeeded={stats.succeeded} errors={stats.errors} "
        f"unresolved={stats.unresolved} send_rate={stats.sent / sending_elapsed:.1f}/s "
        f"completion_rate={stats.succeeded / elapsed:.1f}/s elapsed={elapsed:.1f}s",
        flush=True,
    )
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="NATS load injector for uns-auto-contextualizer")
    parser.add_argument("--rate", type=int, default=500, help="messages per second (default: 500)")
    parser.add_argument("--duration", type=int, default=120, help="seconds to run (default: 120)")
    parser.add_argument("--nats-url", default="nats://localhost:4222", help="NATS server URL")
    parser.add_argument("--timeout", type=float, default=5.0, help="reply timeout in seconds")
    parser.add_argument(
        "--max-pending",
        type=int,
        default=1000,
        help="maximum concurrent requests; bounds load when replies lag",
    )
    args = parser.parse_args()
    if args.rate <= 0 or args.duration <= 0 or args.timeout <= 0 or args.max_pending <= 0:
        parser.error("rate, duration, timeout and max-pending must be positive")

    try:
        stats = asyncio.run(
            run(
                rate=args.rate,
                duration=args.duration,
                nats_url=args.nats_url,
                timeout=args.timeout,
                max_pending=args.max_pending,
            )
        )
    except KeyboardInterrupt:
        print("\nstopped", flush=True)
        sys.exit(130)
    if stats.sent == 0 or stats.errors or stats.unresolved:
        sys.exit(1)


if __name__ == "__main__":
    main()
