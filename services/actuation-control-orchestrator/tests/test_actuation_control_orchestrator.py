"""Unit tests for actuation-control-orchestrator."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from eirvah_contracts.actuation import ActuationRequest, ValidationResult
from eirvah_contracts.envelope import NATSEnvelope
from eirvah_contracts.ulid import generate_correlation_id
from prometheus_client.registry import CollectorRegistry


# ── helpers ──────────────────────────────────────────────────────────────────

def _sample_request(*, deadline: datetime | None = None) -> ActuationRequest:
    now = datetime.now(UTC)
    return ActuationRequest(
        correlation_id=generate_correlation_id(),
        requester="decision-agent-stub",
        target_uns_topic=(
            "uniza/zilina/factory1/line_a/bottler/setpoint_unit/setpoint_temperature"
        ),
        requested_value=22.0,
        value_type="double",
        reason="test",
        requested_at=now,
        deadline=deadline,
    )


def _actuation_envelope(req: ActuationRequest) -> NATSEnvelope:
    return NATSEnvelope(
        correlation_id=req.correlation_id,
        payload=req.model_dump(mode="json"),
    )


def _make_cfg(
    *,
    validate_subject: str = "act.work.validate",
    write_subject: str = "act.work.write_signal",
    dlq_subject: str = "act.dlq.rejected",
):
    from actuation_control_orchestrator.models import (
        ActuationPipelineConfig,
        ActuationPipelineStage,
    )

    return ActuationPipelineConfig(
        stages=[
            ActuationPipelineStage(name="validate", subject=validate_subject, timeout_s=2.0),
            ActuationPipelineStage(name="write_signal", subject=write_subject, timeout_s=2.0),
        ],
        dlq_subject=dlq_subject,
    )


# ── models ──────────────────────────────────────────────────────────────────

def test_pipeline_config_loads_from_yaml(tmp_path: Path) -> None:
    from actuation_control_orchestrator.models import load_pipeline_config

    cfg_file = tmp_path / "actuation-control.yaml"
    cfg_file.write_text(
        "stages:\n"
        "  - name: validate\n"
        "    subject: act.work.validate\n"
        "    timeout_s: 3.0\n"
        "  - name: write_signal\n"
        "    subject: act.work.write_signal\n"
        "    timeout_s: 5.0\n"
        "dlq_subject: act.dlq.rejected\n"
    )
    cfg = load_pipeline_config(cfg_file)
    assert len(cfg.stages) == 2
    assert cfg.stages[0].name == "validate"
    assert cfg.stages[0].timeout_s == 3.0
    assert cfg.stages[1].name == "write_signal"
    assert cfg.stages[1].timeout_s == 5.0
    assert cfg.dlq_subject == "act.dlq.rejected"


# ── metrics ──────────────────────────────────────────────────────────────────

def test_actuation_metrics_create_without_error() -> None:
    from actuation_control_orchestrator.metrics import ActuationMetrics

    reg = CollectorRegistry()
    m = ActuationMetrics(registry=reg)
    m.inc_approved(path="actuation")
    m.inc_rejected(path="actuation", reason="writes_disabled")
    m.inc_stage_timeout(path="actuation", stage="validate")
    m.observe_e2e_latency(path="actuation", seconds=0.05)


# ── pipeline runner ──────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_run_actuation_pipeline_approve_writes_disabled() -> None:
    """Validation approves but allow_writes=False → reject with writes_disabled."""
    from actuation_control_orchestrator.metrics import ActuationMetrics
    from actuation_control_orchestrator.pipeline import run_actuation_pipeline

    req = _sample_request()
    envelope = _actuation_envelope(req)
    cfg = _make_cfg()

    validation_ok = ValidationResult(decision="approve")

    async def fake_request_reply(*, nc, subject, payload, correlation_id, timeout_s):
        msg = MagicMock()
        reply = NATSEnvelope(
            correlation_id=correlation_id,
            payload=validation_ok.model_dump(mode="json"),
        )
        msg.data = reply.model_dump_json().encode()
        return msg

    nc_mock = MagicMock()
    nc_mock.publish = AsyncMock()
    amqp_exchange_mock = MagicMock()
    amqp_exchange_mock.publish = AsyncMock()

    reg = CollectorRegistry()
    metrics = ActuationMetrics(registry=reg)

    await run_actuation_pipeline(
        envelope=envelope,
        cfg=cfg,
        nc=nc_mock,
        amqp_results_exchange=amqp_exchange_mock,
        metrics=metrics,
        allow_writes=False,
        request_reply_fn=fake_request_reply,
    )

    # NATS DLQ publish must have been called with the DLQ subject
    nc_mock.publish.assert_called_once()
    assert nc_mock.publish.call_args[0][0] == "act.dlq.rejected"

    # AMQP exchange publish must have been called once with a reject result
    amqp_exchange_mock.publish.assert_called_once()
    body = json.loads(amqp_exchange_mock.publish.call_args[0][0].body)
    assert body["decision"] == "reject"
    assert body["rejection_reason"] == "writes_disabled"


@pytest.mark.asyncio
async def test_run_actuation_pipeline_approve_writes_enabled() -> None:
    """Validation approves, allow_writes=True, write_signal succeeds → approve on AMQP."""
    from actuation_control_orchestrator.metrics import ActuationMetrics
    from actuation_control_orchestrator.pipeline import run_actuation_pipeline

    req = _sample_request()
    envelope = _actuation_envelope(req)
    cfg = _make_cfg()

    validation_ok = ValidationResult(decision="approve")

    async def fake_request_reply(*, nc, subject, payload, correlation_id, timeout_s):
        msg = MagicMock()
        if subject == "act.work.validate":
            reply = NATSEnvelope(
                correlation_id=correlation_id,
                payload=validation_ok.model_dump(mode="json"),
            )
        else:
            # write_signal returns a plain ok envelope
            reply = NATSEnvelope(correlation_id=correlation_id)
        msg.data = reply.model_dump_json().encode()
        return msg

    nc_mock = MagicMock()
    nc_mock.publish = AsyncMock()
    amqp_exchange_mock = MagicMock()
    amqp_exchange_mock.publish = AsyncMock()

    reg = CollectorRegistry()
    metrics = ActuationMetrics(registry=reg)

    await run_actuation_pipeline(
        envelope=envelope,
        cfg=cfg,
        nc=nc_mock,
        amqp_results_exchange=amqp_exchange_mock,
        metrics=metrics,
        allow_writes=True,
        request_reply_fn=fake_request_reply,
    )

    # NATS DLQ must NOT have been called (happy path)
    nc_mock.publish.assert_not_called()

    # AMQP exchange publish must have been called once with an approve result
    amqp_exchange_mock.publish.assert_called_once()
    body = json.loads(amqp_exchange_mock.publish.call_args[0][0].body)
    assert body["decision"] == "approve"


@pytest.mark.asyncio
async def test_run_actuation_pipeline_deadline_expired() -> None:
    """ActuationRequest with an expired deadline → immediate reject without calling workers."""
    from actuation_control_orchestrator.metrics import ActuationMetrics
    from actuation_control_orchestrator.pipeline import run_actuation_pipeline

    past_deadline = datetime.now(UTC) - timedelta(seconds=5)
    req = _sample_request(deadline=past_deadline)
    envelope = _actuation_envelope(req)
    cfg = _make_cfg()

    request_reply_mock = AsyncMock()

    nc_mock = MagicMock()
    nc_mock.publish = AsyncMock()
    amqp_exchange_mock = MagicMock()
    amqp_exchange_mock.publish = AsyncMock()

    reg = CollectorRegistry()
    metrics = ActuationMetrics(registry=reg)

    await run_actuation_pipeline(
        envelope=envelope,
        cfg=cfg,
        nc=nc_mock,
        amqp_results_exchange=amqp_exchange_mock,
        metrics=metrics,
        allow_writes=True,
        request_reply_fn=request_reply_mock,
    )

    # No worker should have been called
    request_reply_mock.assert_not_called()

    # AMQP exchange must have been called with decision=reject, reason=expired
    amqp_exchange_mock.publish.assert_called_once()
    body = json.loads(amqp_exchange_mock.publish.call_args[0][0].body)
    assert body["decision"] == "reject"
    assert body["rejection_reason"] == "expired"
