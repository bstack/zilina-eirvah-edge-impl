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
    ActuationPendingResult,
    ActuationRejectResult,
    ActuationRequest,
    ActuationUnknownResult,
    ValidationResult,
    WriteResult,
)
from eirvah_contracts.authorization import ApprovedWriteRequest, AuthenticatedRequest
from eirvah_contracts.envelope import NATSEnvelope
from eirvah_observability.execution import record_command
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
        authenticated = AuthenticatedRequest.model_validate(envelope.payload)
        req = authenticated.command
        if req.correlation_id != envelope.correlation_id:
            raise ValueError("command envelope identity mismatch")
    except Exception as exc:
        _log.warning("bad_actuation_envelope", error=str(exc))
        return

    correlation_id = envelope.correlation_id
    record_command("actuation-control-orchestrator", "pipeline_received", req)

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
                payload=authenticated.model_dump(mode="json"),
            )
            .model_dump_json()
            .encode(),
            correlation_id=correlation_id,
            timeout_s=validate_stage.timeout_s,
        )
    except RequestTimeout:
        metrics.inc_stage_timeout(path=_PATH, stage="validate")
        _log.warning("validate_timeout", correlation_id=correlation_id)
        await _emit_reject(
            nc, cfg.dlq_subject, amqp_results_exchange, req, reason="validate_timeout"
        )
        metrics.inc_rejected(path=_PATH, reason="validate_timeout")
        return
    except Exception as exc:
        _log.warning("validate_error", error=str(exc), correlation_id=correlation_id)
        await _emit_reject(
            nc, cfg.dlq_subject, amqp_results_exchange, req, reason=type(exc).__name__
        )
        metrics.inc_rejected(path=_PATH, reason=type(exc).__name__)
        return

    try:
        reply_env = NATSEnvelope.model_validate_json(reply_msg.data)
        if reply_env.status != "ok" or reply_env.correlation_id != correlation_id:
            raise ValueError("invalid validation reply")
        validation = ValidationResult.model_validate(reply_env.payload)
    except Exception as exc:
        _log.warning("validate_bad_reply", error=str(exc), correlation_id=correlation_id)
        await _emit_reject(
            nc, cfg.dlq_subject, amqp_results_exchange, req, reason="bad_validate_reply"
        )
        metrics.inc_rejected(path=_PATH, reason="bad_validate_reply")
        return

    if validation.decision == "reject":
        reason = validation.reason or "rejected"
        await _emit_reject(nc, cfg.dlq_subject, amqp_results_exchange, req, reason=reason)
        metrics.inc_rejected(path=_PATH, reason=validation.reason_code or "policy_reject")
        return

    if validation.approval is None:
        await _emit_reject(
            nc, cfg.dlq_subject, amqp_results_exchange, req, reason="missing_approval"
        )
        metrics.inc_rejected(path=_PATH, reason="missing_approval")
        return

    # Safety gate
    if not allow_writes:
        await _emit_reject(
            nc, cfg.dlq_subject, amqp_results_exchange, req, reason="writes_disabled"
        )
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
                payload=ApprovedWriteRequest(command=req, approval=validation.approval).model_dump(
                    mode="json"
                ),
            )
            .model_dump_json()
            .encode(),
            correlation_id=correlation_id,
            timeout_s=write_stage.timeout_s,
        )
    except RequestTimeout:
        metrics.inc_stage_timeout(path=_PATH, stage="write_signal")
        await _emit_unsettled(
            amqp_results_exchange, req, metrics, state="unknown", reason="write_timeout"
        )
        return
    except Exception:
        await _emit_unsettled(
            amqp_results_exchange, req, metrics, state="unknown", reason="write_transport_error"
        )
        return

    try:
        write_env = NATSEnvelope.model_validate_json(write_reply_msg.data)
        if write_env.correlation_id != correlation_id:
            raise ValueError("writer reply identity mismatch")
        result = WriteResult.model_validate(write_env.payload)
        if result.outcome == "completed" and (
            write_env.status != "ok" or result.written_at is None
        ):
            raise ValueError("writer completion evidence missing")
    except Exception:
        await _emit_unsettled(
            amqp_results_exchange, req, metrics, state="unknown", reason="bad_write_reply"
        )
        return

    if result.replayed:
        metrics.inc_replayed(path=_PATH, outcome=result.outcome)
    if result.outcome in ("unknown", "pending"):
        await _emit_unsettled(
            amqp_results_exchange,
            req,
            metrics,
            state=result.outcome,
            reason=result.reason or result.outcome,
        )
    elif result.outcome in ("not_executed", "conflict"):
        reason = (
            "command_id_reused"
            if result.outcome == "conflict"
            else (result.reason or "writer_refused")
        )
        await _emit_reject(
            nc,
            cfg.dlq_subject,
            amqp_results_exchange,
            req,
            reason=reason,
            retryable=result.retryable,
        )
        category = "command_conflict" if result.outcome == "conflict" else "pre_write_failure"
        metrics.inc_rejected(path=_PATH, reason=category)
    else:
        assert result.written_at is not None
        await _emit_approve(
            amqp_results_exchange, req, written_at=result.written_at, replayed=result.replayed
        )
        if not result.replayed:
            metrics.inc_approved(path=_PATH)
            metrics.observe_e2e_latency(
                path=_PATH, seconds=(datetime.now(UTC) - ingress_at).total_seconds()
            )
        _log.info("actuation_approved", correlation_id=correlation_id, replayed=result.replayed)


async def _emit_unsettled(
    exchange: aio_pika.abc.AbstractExchange,
    req: ActuationRequest,
    metrics: ActuationMetrics,
    *,
    state: str,
    reason: str,
) -> None:
    result: ActuationPendingResult | ActuationUnknownResult
    if state == "pending":
        result = ActuationPendingResult(
            **req.model_dump(), decision="pending", outcome_reason=reason
        )
        metrics.inc_pending(path=_PATH)
    else:
        result = ActuationUnknownResult(
            **req.model_dump(), decision="unknown", outcome_reason=reason
        )
        metrics.inc_unknown(path=_PATH)
    await exchange.publish(
        aio_pika.Message(
            body=result.model_dump_json().encode(), delivery_mode=aio_pika.DeliveryMode.PERSISTENT
        ),
        routing_key="actuation.result",
    )
    record_command(
        "actuation-control-orchestrator", "outcome_published", req, outcome=state, reason=reason
    )
    _log.warning(
        "actuation_outcome", correlation_id=req.correlation_id, outcome=state, reason=reason
    )


async def _emit_reject(
    nc: NATSClient,
    dlq_subject: str,
    exchange: aio_pika.abc.AbstractExchange,
    req: ActuationRequest,
    *,
    reason: str,
    retryable: bool = False,
) -> None:
    result = ActuationRejectResult(
        **req.model_dump(), decision="reject", rejection_reason=reason, retryable=retryable
    )
    body = result.model_dump_json().encode()
    await nc.publish(dlq_subject, body)
    await exchange.publish(
        aio_pika.Message(body=body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
        routing_key="actuation.result",
    )
    record_command(
        "actuation-control-orchestrator",
        "outcome_published",
        req,
        outcome="reject",
        reason=reason,
        retryable=retryable,
    )
    _log.info("actuation_rejected", correlation_id=req.correlation_id, reason=reason)


async def _emit_approve(
    exchange: aio_pika.abc.AbstractExchange,
    req: ActuationRequest,
    *,
    written_at: datetime,
    replayed: bool,
) -> None:
    result = ActuationApproveResult(
        **req.model_dump(),
        decision="approve",
        written_at=written_at,
        replayed=replayed,
    )
    body = result.model_dump_json().encode()
    await exchange.publish(
        aio_pika.Message(body=body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
        routing_key="actuation.result",
    )
    record_command(
        "actuation-control-orchestrator",
        "outcome_published",
        req,
        outcome="approve",
        replayed=replayed,
        written_at=written_at.isoformat(),
    )
