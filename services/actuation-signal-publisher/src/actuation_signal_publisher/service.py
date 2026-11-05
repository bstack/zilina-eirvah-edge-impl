"""Actuation signal publisher NATS req/rep worker (spec §3.2)."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import structlog
import uvicorn
import yaml
from asyncua import Client
from eirvah_bus.client import BusClient
from eirvah_bus.consumer import subscribe_queue_group
from eirvah_contracts.actuation import WriteResult
from eirvah_contracts.authorization import (
    ApprovedWriteRequest,
    command_digest,
    evidence_fingerprint,
    require_key,
    verify_evidence,
)
from eirvah_contracts.envelope import EnvelopeError, NATSEnvelope
from eirvah_observability.execution import record_command
from eirvah_observability.health import HealthApp
from eirvah_observability.logging import configure_logging
from eirvah_observability.metrics import make_counter
from nats.aio.msg import Msg
from pydantic import BaseModel

from actuation_signal_publisher.config import SignalPublisherSettings
from actuation_signal_publisher.ledger import CommandLedger
from actuation_signal_publisher.native import NativeTarget, load_native_targets, write_native_value

_log = structlog.get_logger("actuation-signal-publisher")
SUBJECT = "act.work.write_signal"


@dataclass
class WriteTarget:
    browse_names: list[str]
    endpoint: str
    namespace_uri: str


class _NodeListEntry(BaseModel):
    browse_names: list[str]
    alias: str


class _NodeListConfig(BaseModel):
    endpoint: str
    namespace_uri: str
    publishing_interval_ms: int = 500
    nodes: list[_NodeListEntry]


def load_write_targets(
    *,
    mapping_path: Path,
    node_list_path: Path,
    enterprise: str,
    site: str,
) -> dict[str, WriteTarget]:
    """Build uns_topic → WriteTarget. Fails if mapping is not bijective."""
    mapping_raw = yaml.safe_load(mapping_path.read_text())
    node_list_raw = yaml.safe_load(node_list_path.read_text())
    node_list = _NodeListConfig.model_validate(node_list_raw)

    alias_to_browse: dict[str, list[str]] = {
        entry.alias: entry.browse_names for entry in node_list.nodes
    }

    targets: dict[str, WriteTarget] = {}
    for entry in mapping_raw["mappings"]:
        alias = entry["node_id"]
        topic = (
            f"{enterprise}/{site}/{entry['area']}/{entry['line']}"
            f"/{entry['cell']}/{entry['equipment']}/{entry['measurement']}"
        )
        if topic in targets:
            raise ValueError(f"mapping not bijective: topic {topic!r} maps to multiple node_ids")
        browse_names = alias_to_browse.get(alias)
        if browse_names is None:
            continue  # node not in node-list — not writable via this service
        targets[topic] = WriteTarget(
            browse_names=browse_names,
            endpoint=node_list.endpoint,
            namespace_uri=node_list.namespace_uri,
        )

    return targets


async def write_opcua_value(
    *,
    target: WriteTarget,
    value: Any,
    before_write: Callable[[], None] | None = None,
) -> None:
    async with Client(url=target.endpoint) as client:
        ns_idx = await client.get_namespace_index(target.namespace_uri)
        path = [f"{ns_idx}:{name}" for name in target.browse_names]
        node = await client.nodes.objects.get_child(path)
        if before_write is not None:
            before_write()
        await node.write_value(value)


class SignalPublisherWorker:
    def __init__(
        self, settings: SignalPublisherSettings, *, ledger: CommandLedger | None = None
    ) -> None:
        self._settings = settings
        self._ledger = ledger
        self._targets: dict[str, WriteTarget] = {}
        self._native_targets: dict[str, NativeTarget] = {}
        self._ready = False
        self._handled = make_counter(
            "worker_handler_total",
            "Worker handler invocations",
            labelnames=["worker", "outcome"],
        )

    def is_ready(self) -> bool:
        return self._ready

    async def run(self) -> None:
        require_key(self._settings.approval_key.get_secret_value())
        self._targets = load_write_targets(
            mapping_path=self._settings.mapping_path,
            node_list_path=self._settings.node_list_path,
            enterprise=self._settings.enterprise,
            site=self._settings.site,
        )
        self._native_targets = load_native_targets(self._settings.native_targets_path)
        if self._ledger is None:
            self._ledger = CommandLedger(
                self._settings.ledger_path, retention_s=self._settings.ledger_retention_s
            )
        self._ledger.recover()
        bus = BusClient(servers=self._settings.nats_servers, name="actuation-signal-publisher")
        await bus.connect()
        await subscribe_queue_group(nc=bus.nc, subject=SUBJECT, handler=self._handle)
        self._ready = True
        _log.info("signal_publisher_ready", subject=SUBJECT, targets=len(self._targets))
        await asyncio.get_event_loop().create_future()

    async def _handle(self, msg: Msg) -> None:
        correlation_id = "UNKNOWN"
        token: str | None = None
        req = None
        try:
            envelope = NATSEnvelope.model_validate_json(msg.data)
            correlation_id = envelope.correlation_id
            approved = ApprovedWriteRequest.model_validate(envelope.payload)
            req = approved.command
            if req.correlation_id != correlation_id:
                raise ValueError("command envelope identity mismatch")
            key = self._settings.approval_key.get_secret_value()
            # Expired evidence may retrieve a retained result, never initiate another write.
            verify_evidence(
                req,
                approved.approval,
                key,
                "approval",
                self._settings.writer_id,
                allow_expired=True,
            )
            ledger = self._ledger
            if ledger is None:
                raise RuntimeError("durable command ledger is unavailable")
            digest = command_digest(req)
            try:
                result = ledger.lookup(correlation_id, digest)
            except Exception as exc:
                raise RuntimeError("command history is unavailable") from exc
            if result is None or result.retryable:
                verify_evidence(req, approved.approval, key, "approval", self._settings.writer_id)
                assert req.deadline is not None
                try:
                    claim = ledger.claim(correlation_id, digest, deadline=req.deadline.timestamp())
                except Exception as exc:
                    raise RuntimeError("command claim could not be established") from exc
                token, result = claim.token, claim.result
                if token is not None:
                    record_command(
                        "actuation-signal-publisher",
                        "executor_admitted",
                        req,
                        attempt_id=token,
                        command_digest=digest,
                        approval_fingerprint=evidence_fingerprint(approved.approval),
                        approval_verified=True,
                        approval_issued_at=approved.approval.issued_at.isoformat(),
                    )
                    initiated = False

                    def before_write() -> None:
                        nonlocal initiated
                        verify_evidence(
                            req, approved.approval, key, "approval", self._settings.writer_id
                        )
                        ledger.mark_started(correlation_id, token)
                        if not initiated:
                            record_command(
                                "actuation-signal-publisher",
                                "write_initiated",
                                req,
                                attempt_id=token,
                                command_digest=digest,
                                approval_fingerprint=evidence_fingerprint(approved.approval),
                                approval_verified=True,
                                evidence_scope="before_first_native_frame",
                            )
                            initiated = True
                        # Journal I/O must not let approval expire before the native frame.
                        verify_evidence(
                            req, approved.approval, key, "approval", self._settings.writer_id
                        )

                    try:
                        native = self._native_targets.get(req.target_uns_topic)
                        if native is not None:
                            await write_native_value(
                                native, req.requested_value, before_write=before_write
                            )
                        else:
                            target = self._targets.get(req.target_uns_topic)
                            if target is None:
                                raise ValueError("no configured write target")
                            await write_opcua_value(
                                target=target, value=req.requested_value, before_write=before_write
                            )
                    except asyncio.CancelledError:
                        cancelled = ledger.fail(correlation_id, token, reason="execution_cancelled")
                        record_command(
                            "actuation-signal-publisher",
                            "writer_result",
                            req,
                            attempt_id=token,
                            outcome=cancelled.outcome,
                            reason=cancelled.reason,
                            replayed=False,
                        )
                        raise
                    except Exception as exc:
                        result = ledger.fail(correlation_id, token, reason=type(exc).__name__)
                    else:
                        record_command(
                            "actuation-signal-publisher",
                            "device_acknowledged",
                            req,
                            attempt_id=token,
                            protocol=native.protocol if native is not None else "opcua",
                            evidence_scope="native_acknowledgement",
                            process_response="not_established",
                        )
                        result = ledger.complete(
                            correlation_id, token, written_at=datetime.now(UTC)
                        )
            assert result is not None
            self._handled.labels(worker="actuation-signal-publisher", outcome=result.outcome).inc()
            reply = NATSEnvelope(
                correlation_id=correlation_id, payload=result.model_dump(mode="json")
            )
        except Exception as exc:
            self._handled.labels(worker="actuation-signal-publisher", outcome="error").inc()
            # Missing history cannot establish that an earlier delivery did not execute.
            uncertain = token is not None or isinstance(exc, RuntimeError)
            if token is not None and self._ledger is not None:
                try:
                    self._ledger.fail(correlation_id, token, reason="ledger_failure")
                except Exception:
                    _log.error("command_ledger_recovery_required", correlation_id=correlation_id)
            result = WriteResult(
                outcome="unknown" if uncertain else "not_executed",
                reason=("ledger_failure" if token is not None else "ledger_unavailable")
                if uncertain
                else type(exc).__name__,
            )
            reply = NATSEnvelope(
                correlation_id=correlation_id,
                status="error",
                payload=result.model_dump(mode="json"),
                error=EnvelopeError(kind=type(exc).__name__, message=str(exc)[:200]),
            )
        if req is not None:
            record_command(
                "actuation-signal-publisher",
                "writer_result",
                req,
                attempt_id=token,
                outcome=result.outcome,
                reason=result.reason,
                replayed=result.replayed,
                written_at=result.written_at.isoformat() if result.written_at else None,
            )
        await msg.respond(reply.model_dump_json().encode())


async def run(settings: SignalPublisherSettings) -> None:
    configure_logging(level=settings.log_level)
    worker = SignalPublisherWorker(settings)
    health = HealthApp(is_ready=worker.is_ready)
    http_cfg = uvicorn.Config(
        health.asgi,
        host="0.0.0.0",
        port=settings.http_port,
        log_level=settings.log_level.lower(),
    )
    http = uvicorn.Server(http_cfg)
    await asyncio.gather(worker.run(), http.serve())
