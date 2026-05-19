"""Pipeline runner: drives convert → contextualize → publish for one message."""

from __future__ import annotations

from collections.abc import Callable, Coroutine
from datetime import UTC, datetime
from typing import Any

import structlog
from eirvah_bus.request_reply import RequestTimeout, request_reply
from eirvah_contracts.envelope import EnvelopeError, NATSEnvelope
from eirvah_contracts.pipeline import ContextualizeResult
from eirvah_contracts.signals import NormalizedSignalEnvelope, RawSignalEnvelope
from nats.aio.client import Client as NATSClient

from uns_contextualizer_orchestrator.metrics import PipelineMetrics
from uns_contextualizer_orchestrator.models import PipelineConfig, PipelineContext

_log = structlog.get_logger("uns-contextualizer-orchestrator")

_PATH = "telemetry"

RequestReplyFn = Callable[..., Coroutine[Any, Any, Any]]


async def run_pipeline(
    *,
    envelope: NATSEnvelope,
    cfg: PipelineConfig,
    nc: NATSClient,
    metrics: PipelineMetrics,
    request_reply_fn: RequestReplyFn = request_reply,
) -> None:
    ingress_at = datetime.now(UTC)

    try:
        raw = RawSignalEnvelope.model_validate(envelope.payload)
    except Exception as exc:
        _log.warning(
            "bad_ingress_envelope",
            error=str(exc),
            correlation_id=envelope.correlation_id,
        )
        return

    ctx = PipelineContext(
        correlation_id=envelope.correlation_id,
        raw=raw,
        ingress_at=ingress_at,
    )

    for stage in cfg.stages:
        if stage.name == "convert":
            payload_dict: dict[str, Any] = raw.model_dump(mode="json")
        elif stage.name == "contextualize":
            assert ctx.normalized is not None
            payload_dict = ctx.normalized.model_dump(mode="json")
        elif stage.name == "publish":
            payload_dict = ctx.build_publish_request().model_dump(mode="json")
        else:
            _log.error("unknown_stage", stage=stage.name)
            continue

        req_env = NATSEnvelope(
            correlation_id=ctx.correlation_id,
            payload=payload_dict,
        )

        try:
            reply_msg = await request_reply_fn(
                nc=nc,
                subject=stage.subject,
                payload=req_env.model_dump_json().encode(),
                correlation_id=ctx.correlation_id,
                timeout_s=stage.timeout_s,
            )
        except RequestTimeout:
            metrics.inc_stage_timeout(path=_PATH, stage=stage.name)
            _log.warning(
                "stage_timeout",
                stage=stage.name,
                correlation_id=ctx.correlation_id,
            )
            await _publish_dlq(
                nc, cfg.dlq_subject, ctx,
                failing_stage=stage.name, reason="timeout",
            )
            return
        except Exception as exc:
            metrics.inc_stage_error(
                path=_PATH, stage=stage.name, reason=type(exc).__name__
            )
            _log.warning(
                "stage_error",
                stage=stage.name,
                error=str(exc),
                correlation_id=ctx.correlation_id,
            )
            await _publish_dlq(
                nc, cfg.dlq_subject, ctx,
                failing_stage=stage.name, reason=type(exc).__name__,
            )
            return

        try:
            reply_env = NATSEnvelope.model_validate_json(reply_msg.data)
        except Exception:
            metrics.inc_stage_error(path=_PATH, stage=stage.name, reason="BadReply")
            await _publish_dlq(
                nc, cfg.dlq_subject, ctx,
                failing_stage=stage.name, reason="BadReply",
            )
            return

        if reply_env.status == "error":
            reason = reply_env.error.kind if reply_env.error else "unknown"
            metrics.inc_stage_error(path=_PATH, stage=stage.name, reason=reason)
            _log.warning(
                "stage_replied_error",
                stage=stage.name,
                reason=reason,
                correlation_id=ctx.correlation_id,
            )
            await _publish_dlq(
                nc, cfg.dlq_subject, ctx,
                failing_stage=stage.name, reason=reason,
            )
            return

        if stage.name == "convert":
            ctx.normalized = NormalizedSignalEnvelope.model_validate(reply_env.payload)
        elif stage.name == "contextualize":
            ctx.contextualized = ContextualizeResult.model_validate(reply_env.payload)

    elapsed = (datetime.now(UTC) - ingress_at).total_seconds()
    metrics.inc_success(path=_PATH)
    metrics.observe_e2e_latency(path=_PATH, seconds=elapsed)
    _log.info(
        "pipeline_success",
        correlation_id=ctx.correlation_id,
        latency_s=elapsed,
    )


async def _publish_dlq(
    nc: NATSClient,
    dlq_subject: str,
    ctx: PipelineContext,
    *,
    failing_stage: str,
    reason: str,
) -> None:
    dlq_payload = NATSEnvelope(
        correlation_id=ctx.correlation_id,
        status="error",
        error=EnvelopeError(
            kind="PipelineFailure",
            message=f"stage={failing_stage} reason={reason}",
        ),
        payload=ctx.raw.model_dump(mode="json"),
    )
    await nc.publish(dlq_subject, dlq_payload.model_dump_json().encode())
