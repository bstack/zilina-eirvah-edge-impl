"""E1-only bounded processing comparison; no broker credentials, approval or device writes.

Reuses production contextualization, command policy and inspection contract validation.
The command operation deliberately is not the authenticated actuation pipeline.
"""

from __future__ import annotations

import asyncio
import multiprocessing
import os
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import uvicorn
from actuation_event_validator.service import NodePolicy, load_policy, validate_request
from eirvah_bus.client import BusClient
from eirvah_bus.consumer import subscribe_queue_group
from eirvah_contracts.actuation import ActuationRequest
from eirvah_contracts.envelope import EnvelopeError, NATSEnvelope
from eirvah_contracts.inspection import BottleInspection
from eirvah_observability.health import HealthApp
from eirvah_observability.metrics import make_counter, make_gauge, make_histogram
from nats.aio.msg import Msg
from rdflib import Graph
from uns_auto_contextualizer.service import handle_contextualize_request, load_ontology

_graph: Graph | None = None
_policies: dict[str, NodePolicy] = {}


def allocation(mode: str) -> dict[str, dict[str, int]]:
    if mode == "shared":
        return dict(shared=dict(workers=2, capacity=128))
    if mode == "separate":
        return dict(data=dict(workers=1, capacity=64), control=dict(workers=1, capacity=64))
    raise ValueError("mode must be shared or separate")


def initialize(ontology: str, policy: str) -> None:
    global _graph, _policies
    _graph, _policies = load_ontology(Path(ontology)), load_policy(Path(policy))


def compute(kind: str, body: bytes) -> bytes:
    envelope = NATSEnvelope.model_validate_json(body)
    try:
        if kind == "data":
            assert _graph is not None
            reply = handle_contextualize_request(envelope, _graph)
        elif kind == "command":
            result = validate_request(ActuationRequest.model_validate(envelope.payload), _policies)
            reply = NATSEnvelope(
                correlation_id=envelope.correlation_id, payload=result.model_dump(mode="json")
            )
        elif kind == "event":
            event = BottleInspection.model_validate(envelope.payload)
            reply = NATSEnvelope(
                correlation_id=envelope.correlation_id, payload=event.model_dump(mode="json")
            )
        else:
            raise ValueError("unknown work kind")
    except Exception as exc:
        reply = NATSEnvelope(
            correlation_id=envelope.correlation_id,
            status="error",
            error=EnvelopeError(kind=type(exc).__name__, message=str(exc)[:200]),
        )
    return reply.model_dump_json().encode()


class Processing:
    def __init__(self, mode: str) -> None:
        self.mode = mode
        self.plan = allocation(mode)
        self.queues: dict[str, asyncio.Queue[tuple[Msg, float]]] = {
            key: asyncio.Queue(maxsize=value["capacity"]) for key, value in self.plan.items()
        }
        self.ready = False
        self.count = make_counter(
            "e1_completed", "Selected comparison handlers", labelnames=["flow", "status"]
        )
        self.pending = make_gauge("e1_pending", "Application queue messages", labelnames=["pool"])
        self.duration = make_histogram(
            "e1_service_seconds", "Service time excluding queue wait", labelnames=["flow"]
        )
        self.wait = make_histogram(
            "e1_queue_seconds", "Application queue waiting time", labelnames=["flow"]
        )

    async def accept(self, message: Msg) -> None:
        flow = message.subject.rsplit(".", 1)[-1]
        pool = "shared" if self.mode == "shared" else "data" if flow == "data" else "control"
        await self.queues[pool].put((message, time.monotonic()))

    async def pump(self, pool: str, executor: ProcessPoolExecutor) -> None:
        queue = self.queues[pool]
        while True:
            message, queued = await queue.get()
            flow = message.subject.rsplit(".", 1)[-1]
            self.wait.labels(flow=flow).observe(time.monotonic() - queued)
            started = time.monotonic()
            try:
                reply = await asyncio.get_running_loop().run_in_executor(
                    executor, compute, flow, message.data
                )
                parsed = NATSEnvelope.model_validate_json(reply)
                self.count.labels(flow=flow, status=parsed.status).inc()
                message.headers = {
                    **(message.headers or {}),
                    "X-Eirvah-Worker": os.environ.get("HOSTNAME", "local"),
                }
                await message.respond(reply)
            finally:
                self.duration.labels(flow=flow).observe(time.monotonic() - started)
                queue.task_done()

    async def run(self) -> None:
        bus = BusClient(
            servers=[os.environ.get("NATS_URL", "nats://nats:4222")], name="e1-processing"
        )
        await bus.connect()
        tasks: list[asyncio.Task[None]] = []
        executors = []
        try:
            for pool, spec in self.plan.items():
                self.pending.labels(pool=pool).set_function(self.queues[pool].qsize)
                executor = ProcessPoolExecutor(
                    max_workers=spec["workers"],
                    mp_context=multiprocessing.get_context("spawn"),
                    initializer=initialize,
                    initargs=("/etc/e1/ontology.jsonld", "/etc/e1/policy.yaml"),
                )
                executors.append(executor)
                tasks.extend(
                    asyncio.create_task(self.pump(pool, executor)) for _ in range(spec["workers"])
                )
            for flow in ("data", "command", "event"):
                await subscribe_queue_group(nc=bus.nc, subject="e1." + flow, handler=self.accept)
            await bus.nc.flush()
            self.ready = True
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            for executor in executors:
                executor.shutdown(wait=False, cancel_futures=True)


async def main() -> None:
    runtime = Processing(os.environ.get("E1_MODE", "shared"))
    server = uvicorn.Server(
        uvicorn.Config(
            HealthApp(is_ready=lambda: runtime.ready).asgi,
            host="0.0.0.0",
            port=8080,
            log_level="warning",
        )
    )
    await asyncio.gather(runtime.run(), server.serve())


if __name__ == "__main__":
    asyncio.run(main())
