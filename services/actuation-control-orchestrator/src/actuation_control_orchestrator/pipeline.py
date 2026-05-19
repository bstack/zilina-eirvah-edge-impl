"""Actuation pipeline runner: drives validate → (conditional) write_signal."""

from __future__ import annotations

from collections.abc import Callable, Coroutine
from datetime import UTC, datetime
from typing import Any

import aio_pika
import structlog
from eirvah_bus.request_reply import RequestTimeout, request_reply
from eirvah_contracts.actuation import (
    ActuationApproveResult,
    ActuationRejectResult,
    ActuationRequest,
    ValidationResult,
)
from eirvah_contracts.envelope import NATSEnvelope
from nats.aio.client import Client as NATSClient

from actuation_control_orchestrator.metrics import ActuationMetrics
from actuation_control_orchestrator.models import ActuationPipelineConfig

_log = structlog.get_logger("actuation-control-orchestrator")
_PATH = "actuation"

RequestReplyFn = Callable[..., Coroutine[Any, Any, Any]]


async def run_actuation_pipeline(
    *,
    envelope: NATSEnvelope,
    cfg: ActuationPipelineConfig,
    nc: NATSClient,
    amqp_results_exchange: aio_pika.abc.AbstractExchange,
    metrics: ActuationMetrics,
    allow_writes: bool,
    request_reply_fn: RequestReplyFn = request_reply,
) -> None:
    ingress_at = datetime.now(UTC)

    try:
        req = ActuationRequest.model_validate(envelope.payload)
    except Exception as exc:
        _log.warning("bad_actuation_envelope", error=str(exc))
        return

    correlation_id = envelope.correlation_id

    # Deadline check
    if req.deadline and datetime.now(UTC) > req.deadline:
        await _emit_reject(nc, cfg.dlq_subject, amqp_results_exchange, req, reason="expired")
        metrics.inc_rejected(path=_PATH, reason="expired")
        return

    # Stage: validate
    validate_stage = next((s for s in cfg.stages if s.name == "validate"), None)
    if validate_stage is None:
        _log.error("missing_validate_stage")
        return

    try:
        reply_msg = await request_reply_fn(
            nc=nc,
            subject=validate_stage.subject,
            payload=NATSEnvelope(
                correlation_id=correlation_id,
                payload=req.model_dump(mode="json"),
            ).model_dump_json().encode(),
            correlation_id=correlation_id,
            timeout_s=validate_stage.timeout_s,
        )
    except RequestTimeout:
        metrics.inc_stage_timeout(path=_PATH, stage="validate")
        _log.warning("validate_timeout", correlation_id=correlation_id)
        await _emit_reject(nc, cfg.dlq_subject, amqp_results_exchange, req, reason="validate_timeout")
        metrics.inc_rejected(path=_PATH, reason="validate_timeout")
        return
    except Exception as exc:
        _log.warning("validate_error", error=str(exc), correlation_id=correlation_id)
        await _emit_reject(nc, cfg.dlq_subject, amqp_results_exchange, req, reason=type(exc).__name__)
        metrics.inc_rejected(path=_PATH, reason=type(exc).__name__)
        return

    try:
        reply_env = NATSEnvelope.model_validate_json(reply_msg.data)
        validation = ValidationResult.model_validate(reply_env.payload)
    except Exception as exc:
        _log.warning("validate_bad_reply", error=str(exc), correlation_id=correlation_id)
        await _emit_reject(nc, cfg.dlq_subject, amqp_results_exchange, req, reason="BadValidateReply")
        metrics.inc_rejected(path=_PATH, reason="BadValidateReply")
        return

    if validation.decision == "reject":
        reason = validation.reason or "rejected"
        await _emit_reject(nc, cfg.dlq_subject, amqp_results_exchange, req, reason=reason)
        metrics.inc_rejected(path=_PATH, reason=reason)
        return

    # Safety gate
    if not allow_writes:
        await _emit_reject(nc, cfg.dlq_subject, amqp_results_exchange, req, reason="writes_disabled")
        metrics.inc_rejected(path=_PATH, reason="writes_disabled")
        return

    # Stage: write_signal
    write_stage = next((s for s in cfg.stages if s.name == "write_signal"), None)
    if write_stage is None:
        _log.error("missing_write_signal_stage")
        return

    try:
        write_reply_msg = await request_reply_fn(
            nc=nc,
            subject=write_stage.subject,
            payload=NATSEnvelope(
                correlation_id=correlation_id,
                payload=req.model_dump(mode="json"),
            ).model_dump_json().encode(),
            correlation_id=correlation_id,
            timeout_s=write_stage.timeout_s,
        )
    except RequestTimeout:
        metrics.inc_stage_timeout(path=_PATH, stage="write_signal")
        await _emit_reject(nc, cfg.dlq_subject, amqp_results_exchange, req, reason="write_timeout")
        metrics.inc_rejected(path=_PATH, reason="write_timeout")
        return
    except Exception as exc:
        await _emit_reject(nc, cfg.dlq_subject, amqp_results_exchange, req, reason=type(exc).__name__)
        metrics.inc_rejected(path=_PATH, reason=type(exc).__name__)
        return

    write_env = NATSEnvelope.model_validate_json(write_reply_msg.data)
    if write_env.status == "error":
        reason = write_env.error.kind if write_env.error else "WriteError"
        await _emit_reject(nc, cfg.dlq_subject, amqp_results_exchange, req, reason=reason)
        metrics.inc_rejected(path=_PATH, reason=reason)
        return

    # Approve
    elapsed = (datetime.now(UTC) - ingress_at).total_seconds()
    await _emit_approve(amqp_results_exchange, req)
    metrics.inc_approved(path=_PATH)
    metrics.observe_e2e_latency(path=_PATH, seconds=elapsed)
    _log.info("actuation_approved", correlation_id=correlation_id, latency_s=elapsed)


async def _emit_reject(
    nc: NATSClient,
    dlq_subject: str,
    exchange: aio_pika.abc.AbstractExchange,
    req: ActuationRequest,
    *,
    reason: str,
) -> None:
    result = ActuationRejectResult(**req.model_dump(), decision="reject", rejection_reason=reason)
    body = result.model_dump_json().encode()
    await nc.publish(dlq_subject, body)
    await exchange.publish(
        aio_pika.Message(body=body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
        routing_key="actuation.result",
    )
    _log.info("actuation_rejected", correlation_id=req.correlation_id, reason=reason)


async def _emit_approve(
    exchange: aio_pika.abc.AbstractExchange,
    req: ActuationRequest,
) -> None:
    result = ActuationApproveResult(
        **req.model_dump(),
        decision="approve",
        written_at=datetime.now(UTC),
    )
    body = result.model_dump_json().encode()
    await exchange.publish(
        aio_pika.Message(body=body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
        routing_key="actuation.result",
    )
