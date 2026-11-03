"""Actuation event validator NATS req/rep worker (spec §3.2)."""

from __future__ import annotations

import asyncio
import math
from pathlib import Path

import structlog
import uvicorn
import yaml
from eirvah_bus.client import BusClient
from eirvah_bus.consumer import subscribe_queue_group
from eirvah_contracts.actuation import ActuationRequest, ValidationResult
from eirvah_contracts.authorization import (
    AuthenticatedRequest,
    require_key,
    sign_approval,
    verify_evidence,
)
from eirvah_contracts.envelope import EnvelopeError, NATSEnvelope
from eirvah_contracts.signals import SignalValueType
from eirvah_observability.health import HealthApp
from eirvah_observability.logging import configure_logging
from eirvah_observability.metrics import make_counter
from nats.aio.msg import Msg
from pydantic import BaseModel

from actuation_event_validator.config import ValidatorSettings

_log = structlog.get_logger("actuation-event-validator")
SUBJECT = "act.work.validate"


class NodePolicy(BaseModel):
    uns_topic: str
    allowed_range: tuple[float, float]
    allowlist: list[str]
    value_type: SignalValueType = "double"


class PolicyConfig(BaseModel):
    policies: list[NodePolicy]


def load_policy(path: Path) -> dict[str, NodePolicy]:
    raw = yaml.safe_load(path.read_text())
    cfg = PolicyConfig.model_validate(raw)
    return {p.uns_topic: p for p in cfg.policies}


def validate_request(
    req: ActuationRequest,
    policies: dict[str, NodePolicy],
) -> ValidationResult:
    policy = policies.get(req.target_uns_topic)
    if policy is None:
        return ValidationResult(
            decision="reject",
            reason=f"no policy for topic {req.target_uns_topic!r}",
            reason_code="no_policy",
        )
    if req.requester not in policy.allowlist:
        return ValidationResult(
            decision="reject",
            reason=f"requester {req.requester!r} not in allowlist",
            reason_code="not_allowlisted",
        )
    value_type_ok = req.value_type == policy.value_type and (
        (policy.value_type == "bool" and type(req.requested_value) is bool)
        or (policy.value_type == "int64" and type(req.requested_value) is int)
        or (policy.value_type == "double" and type(req.requested_value) in (int, float))
    )
    if not value_type_ok:
        return ValidationResult(
            decision="reject", reason="requested_value has wrong type", reason_code="not_numeric"
        )
    try:
        value = float(req.requested_value)
    except (TypeError, ValueError):
        return ValidationResult(
            decision="reject",
            reason="requested_value is not numeric",
            reason_code="not_numeric",
        )
    lo, hi = policy.allowed_range
    if not math.isfinite(value) or not (lo <= value <= hi):
        return ValidationResult(
            decision="reject",
            reason=f"value {value} outside policy range [{lo}, {hi}]",
            reason_code="out_of_range",
        )
    return ValidationResult(decision="approve")


class ValidatorWorker:
    def __init__(self, settings: ValidatorSettings) -> None:
        self._settings = settings
        self._policies: dict[str, NodePolicy] = {}
        self._ready = False
        self._handled = make_counter(
            "worker_handler_total",
            "Worker handler invocations",
            labelnames=["worker", "outcome"],
        )

    def is_ready(self) -> bool:
        return self._ready

    async def run(self) -> None:
        require_key(self._settings.ingress_key.get_secret_value())
        require_key(self._settings.approval_key.get_secret_value())
        self._policies = load_policy(self._settings.policy_path)
        bus = BusClient(servers=self._settings.nats_servers, name="actuation-event-validator")
        await bus.connect()
        await subscribe_queue_group(nc=bus.nc, subject=SUBJECT, handler=self._handle)
        self._ready = True
        _log.info("validator_ready", subject=SUBJECT, policies=len(self._policies))
        await asyncio.get_event_loop().create_future()

    async def _handle(self, msg: Msg) -> None:
        correlation_id = "UNKNOWN"
        try:
            envelope = NATSEnvelope.model_validate_json(msg.data)
            correlation_id = envelope.correlation_id
            authenticated = AuthenticatedRequest.model_validate(envelope.payload)
            req = authenticated.command
            if req.correlation_id != correlation_id:
                raise ValueError("command envelope identity mismatch")
            verify_evidence(
                req,
                authenticated.identity,
                self._settings.ingress_key.get_secret_value(),
                "ingress",
                "",
            )
            result = validate_request(req, self._policies)
            if result.decision == "approve":
                result.approval = sign_approval(
                    req,
                    self._settings.approval_key.get_secret_value(),
                    self._settings.approved_writer,
                )
            self._handled.labels(worker="actuation-event-validator", outcome=result.decision).inc()
            reply = NATSEnvelope(
                correlation_id=correlation_id,
                payload=result.model_dump(mode="json"),
            )
        except Exception as exc:
            self._handled.labels(worker="actuation-event-validator", outcome="error").inc()
            reply = NATSEnvelope(
                correlation_id=correlation_id,
                status="error",
                error=EnvelopeError(kind=type(exc).__name__, message=str(exc)[:200]),
            )
        await msg.respond(reply.model_dump_json().encode())


async def run(settings: ValidatorSettings) -> None:
    configure_logging(level=settings.log_level)
    worker = ValidatorWorker(settings)
    health = HealthApp(is_ready=worker.is_ready)
    http_cfg = uvicorn.Config(
        health.asgi,
        host="0.0.0.0",
        port=settings.http_port,
        log_level=settings.log_level.lower(),
    )
    http = uvicorn.Server(http_cfg)
    await asyncio.gather(worker.run(), http.serve())
