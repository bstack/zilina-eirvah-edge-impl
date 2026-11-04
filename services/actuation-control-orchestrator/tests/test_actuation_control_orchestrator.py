"""Unit tests for actuation-control-orchestrator."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from eirvah_contracts.actuation import ActuationRequest, ValidationResult, WriteResult
from eirvah_contracts.authorization import AuthenticatedRequest, sign_approval, sign_ingress
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
        deadline=deadline or now + timedelta(seconds=10),
    )


def _actuation_envelope(req: ActuationRequest) -> NATSEnvelope:
    return NATSEnvelope(
        correlation_id=req.correlation_id,
        payload=AuthenticatedRequest(
            command=req, identity=sign_ingress(req, "test-ingress-key-with-at-least-32-bytes")
        ).model_dump(mode="json"),
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


def test_actuation_metrics_pre_registers_known_rejection_reasons() -> None:
    """Every known reason has a 0 sample from construction — without this,
    increase()/rate() over a lone rejection reads back as 0 forever, since
    prometheus_client only creates a labeled series on its first .inc()."""
    from actuation_control_orchestrator.metrics import (
        KNOWN_REJECTION_REASONS,
        ActuationMetrics,
    )

    reg = CollectorRegistry()
    ActuationMetrics(registry=reg)

    for reason in KNOWN_REJECTION_REASONS:
        assert (
            reg.get_sample_value(
                "eirvah_actuation_rejected_total", {"path": "actuation", "reason": reason}
            )
            == 0
        )


# ── pipeline runner ──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_run_actuation_pipeline_approve_writes_disabled() -> None:
    """Validation approves but allow_writes=False → reject with writes_disabled."""
    from actuation_control_orchestrator.metrics import ActuationMetrics
    from actuation_control_orchestrator.pipeline import run_actuation_pipeline

    req = _sample_request()
    envelope = _actuation_envelope(req)
    cfg = _make_cfg()

    validation_ok = ValidationResult(
        decision="approve",
        approval=sign_approval(
            req, "test-approval-key-with-at-least-32-bytes", "actuation-signal-publisher"
        ),
    )

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
async def test_run_actuation_pipeline_policy_reject_uses_stable_metric_label() -> None:
    """Validator rejects on policy → AMQP keeps the free-text reason, but the
    metric is labeled with the bounded reason_code (not the free text) so
    increase()/rate() over per-value rejection messages doesn't read back as 0."""
    from actuation_control_orchestrator.metrics import ActuationMetrics
    from actuation_control_orchestrator.pipeline import run_actuation_pipeline

    req = _sample_request()
    envelope = _actuation_envelope(req)
    cfg = _make_cfg()

    validation_reject = ValidationResult(
        decision="reject",
        reason="value 99.0 outside policy range [20.0, 30.0]",
        reason_code="out_of_range",
    )

    async def fake_request_reply(*, nc, subject, payload, correlation_id, timeout_s):
        msg = MagicMock()
        reply = NATSEnvelope(
            correlation_id=correlation_id,
            payload=validation_reject.model_dump(mode="json"),
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
        allow_writes=True,
        request_reply_fn=fake_request_reply,
    )

    # AMQP result keeps the full, human-readable rejection reason.
    body = json.loads(amqp_exchange_mock.publish.call_args[0][0].body)
    assert body["decision"] == "reject"
    assert body["rejection_reason"] == "value 99.0 outside policy range [20.0, 30.0]"

    # The metric is labeled with the bounded reason_code, not the free text.
    sample_value = reg.get_sample_value(
        "eirvah_actuation_rejected_total", {"path": "actuation", "reason": "out_of_range"}
    )
    assert sample_value == 1
    assert (
        reg.get_sample_value(
            "eirvah_actuation_rejected_total",
            {"path": "actuation", "reason": "value 99.0 outside policy range [20.0, 30.0]"},
        )
        is None
    )


@pytest.mark.asyncio
async def test_run_actuation_pipeline_approve_writes_enabled() -> None:
    """Validation approves, allow_writes=True, write_signal succeeds → approve on AMQP."""
    from actuation_control_orchestrator.metrics import ActuationMetrics
    from actuation_control_orchestrator.pipeline import run_actuation_pipeline

    req = _sample_request()
    envelope = _actuation_envelope(req)
    cfg = _make_cfg()

    validation_ok = ValidationResult(
        decision="approve",
        approval=sign_approval(
            req, "test-approval-key-with-at-least-32-bytes", "actuation-signal-publisher"
        ),
    )

    async def fake_request_reply(*, nc, subject, payload, correlation_id, timeout_s):
        msg = MagicMock()
        if subject == "act.work.validate":
            reply = NATSEnvelope(
                correlation_id=correlation_id,
                payload=validation_ok.model_dump(mode="json"),
            )
        else:
            reply = NATSEnvelope(
                correlation_id=correlation_id,
                payload=WriteResult(outcome="completed", written_at=datetime.now(UTC)).model_dump(
                    mode="json"
                ),
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
async def test_expired_request_can_retrieve_retained_completion() -> None:
    from actuation_control_orchestrator.metrics import ActuationMetrics
    from actuation_control_orchestrator.pipeline import run_actuation_pipeline
    from eirvah_contracts.authorization import ApprovedWriteRequest

    past_deadline = datetime.now(UTC) - timedelta(seconds=5)
    req = _sample_request(deadline=past_deadline)
    envelope = NATSEnvelope(
        correlation_id=req.correlation_id,
        payload=AuthenticatedRequest(
            command=req,
            identity=sign_ingress(
                req, "test-ingress-key-with-at-least-32-bytes", allow_expired=True
            ),
        ).model_dump(mode="json"),
    )
    completed_at = past_deadline - timedelta(seconds=1)

    async def rpc(*, nc, subject, payload, correlation_id, timeout_s):
        incoming = NATSEnvelope.model_validate_json(payload)
        if subject == "act.work.validate":
            assert (
                AuthenticatedRequest.model_validate(incoming.payload).command.deadline
                == past_deadline
            )
            response = ValidationResult(
                decision="approve",
                approval=sign_approval(
                    req,
                    "test-approval-key-with-at-least-32-bytes",
                    "actuation-signal-publisher",
                    allow_expired=True,
                ),
            )
        else:
            assert (
                ApprovedWriteRequest.model_validate(incoming.payload).command.deadline
                == past_deadline
            )
            response = WriteResult(outcome="completed", written_at=completed_at, replayed=True)
        return MagicMock(
            data=NATSEnvelope(
                correlation_id=correlation_id, payload=response.model_dump(mode="json")
            )
            .model_dump_json()
            .encode()
        )

    exchange = MagicMock(publish=AsyncMock())
    nc = MagicMock(publish=AsyncMock())
    await run_actuation_pipeline(
        envelope=envelope,
        cfg=_make_cfg(),
        nc=nc,
        amqp_results_exchange=exchange,
        metrics=ActuationMetrics(registry=CollectorRegistry()),
        allow_writes=True,
        request_reply_fn=rpc,
    )
    result = json.loads(exchange.publish.call_args.args[0].body)
    assert result["decision"] == "approve" and result["replayed"]
    assert datetime.fromisoformat(result["written_at"]) == completed_at
    nc.publish.assert_not_called()
