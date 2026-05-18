from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from prometheus_client.registry import CollectorRegistry


# ── models ──────────────────────────────────────────────────────────────────

def test_pipeline_config_loads_from_yaml(tmp_path: Path) -> None:
    from uns_contextualizer_orchestrator.models import load_pipeline_config

    cfg_file = tmp_path / "pipeline.yaml"
    cfg_file.write_text(
        "stages:\n"
        "  - name: convert\n"
        "    subject: uns.work.convert\n"
        "    timeout_s: 2.0\n"
        "  - name: contextualize\n"
        "    subject: uns.work.contextualize\n"
        "    timeout_s: 2.0\n"
        "  - name: publish\n"
        "    subject: uns.work.publish\n"
        "    timeout_s: 2.0\n"
        "dlq_subject: uns.dlq.telemetry\n"
    )
    cfg = load_pipeline_config(cfg_file)
    assert len(cfg.stages) == 3
    assert cfg.stages[0].name == "convert"
    assert cfg.stages[0].timeout_s == 2.0
    assert cfg.dlq_subject == "uns.dlq.telemetry"


def test_pipeline_context_builds_publish_request() -> None:
    from uns_contextualizer_orchestrator.models import PipelineContext
    from eirvah_contracts.pipeline import ContextualizeResult
    from eirvah_contracts.signals import NormalizedSignalEnvelope, RawSignalEnvelope
    from eirvah_contracts.uns import UNSPath, build_uns_topic

    now = datetime.now(UTC)
    raw = RawSignalEnvelope(
        source_endpoint="opc.tcp://test:4840",
        node_id="Bottler.Temperature01",
        value=23.4,
        value_type="double",
        quality="good",
        source_timestamp=now,
        server_timestamp=now,
        received_at=now,
    )
    normalized = NormalizedSignalEnvelope(
        node_id="Bottler.Temperature01",
        value=23.4,
        value_type="double",
        unit="degC",
        quality="good",
        source_timestamp=now,
        received_at=now,
    )
    uns_path = UNSPath(
        enterprise="uniza", site="zilina", area="factory1",
        line="line_a", cell="bottler",
        equipment="temperature_sensor_01", measurement="temperature",
    )
    ctx_result = ContextualizeResult(
        uns_topic=build_uns_topic(uns_path),
        uns_path=uns_path,
        semantic_type="temperature.celsius",
    )
    ctx = PipelineContext(
        correlation_id="01HZXC8P9G7Q3M6V0K2T8R5W4A",
        raw=raw,
        ingress_at=now,
        normalized=normalized,
        contextualized=ctx_result,
    )
    req = ctx.build_publish_request()
    assert req.value == 23.4
    assert req.unit == "degC"
    assert req.uns_topic == build_uns_topic(uns_path)
    assert req.source_node_id == "Bottler.Temperature01"


# ── metrics ──────────────────────────────────────────────────────────────────

def test_pipeline_metrics_create_without_error() -> None:
    from uns_contextualizer_orchestrator.metrics import PipelineMetrics

    reg = CollectorRegistry()
    m = PipelineMetrics(registry=reg)
    m.inc_success(path="telemetry")
    m.inc_stage_error(path="telemetry", stage="convert", reason="timeout")
    m.observe_e2e_latency(path="telemetry", seconds=0.05)


# ── pipeline runner ──────────────────────────────────────────────────────────

def _corr_id() -> str:
    return "01HZXC8P9G7Q3M6V0K2T8R5W4A"


def _raw_envelope() -> "NATSEnvelope":  # noqa: F821
    from eirvah_contracts.envelope import NATSEnvelope
    from eirvah_contracts.signals import RawSignalEnvelope

    now = datetime.now(UTC)
    raw = RawSignalEnvelope(
        source_endpoint="opc.tcp://test:4840",
        node_id="Bottler.Temperature01",
        value=23.4,
        value_type="double",
        quality="good",
        source_timestamp=now,
        server_timestamp=now,
        received_at=now,
    )
    return NATSEnvelope(correlation_id=_corr_id(), payload=raw.model_dump(mode="json"))


@pytest.mark.asyncio
async def test_run_pipeline_success() -> None:
    from uns_contextualizer_orchestrator.models import PipelineConfig, PipelineStage
    from uns_contextualizer_orchestrator.metrics import PipelineMetrics
    from uns_contextualizer_orchestrator.pipeline import run_pipeline
    from eirvah_contracts.envelope import NATSEnvelope
    from eirvah_contracts.pipeline import ContextualizeResult
    from eirvah_contracts.signals import NormalizedSignalEnvelope
    from eirvah_contracts.uns import UNSPath, build_uns_topic

    now = datetime.now(UTC)
    uns_path = UNSPath(
        enterprise="uniza", site="zilina", area="factory1",
        line="line_a", cell="bottler",
        equipment="temperature_sensor_01", measurement="temperature",
    )
    normalized = NormalizedSignalEnvelope(
        node_id="Bottler.Temperature01", value=23.4, value_type="double",
        unit="degC", quality="good", source_timestamp=now, received_at=now,
    )
    ctx_result = ContextualizeResult(
        uns_topic=build_uns_topic(uns_path), uns_path=uns_path,
        semantic_type="temperature.celsius",
    )

    cfg = PipelineConfig(
        stages=[
            PipelineStage(name="convert", subject="uns.work.convert", timeout_s=2.0),
            PipelineStage(name="contextualize", subject="uns.work.contextualize", timeout_s=2.0),
            PipelineStage(name="publish", subject="uns.work.publish", timeout_s=2.0),
        ],
        dlq_subject="uns.dlq.telemetry",
    )

    async def fake_request_reply(*, nc, subject, payload, correlation_id, timeout_s):
        msg = MagicMock()
        if subject == "uns.work.convert":
            reply = NATSEnvelope(
                correlation_id=correlation_id,
                payload=normalized.model_dump(mode="json"),
            )
        elif subject == "uns.work.contextualize":
            reply = NATSEnvelope(
                correlation_id=correlation_id,
                payload=ctx_result.model_dump(mode="json"),
            )
        else:
            reply = NATSEnvelope(correlation_id=correlation_id)
        msg.data = reply.model_dump_json().encode()
        return msg

    nc_mock = MagicMock()
    nc_mock.publish = AsyncMock()
    reg = CollectorRegistry()
    metrics = PipelineMetrics(registry=reg)

    envelope = _raw_envelope()
    await run_pipeline(
        envelope=envelope,
        cfg=cfg,
        nc=nc_mock,
        metrics=metrics,
        request_reply_fn=fake_request_reply,
    )
    nc_mock.publish.assert_not_called()


@pytest.mark.asyncio
async def test_run_pipeline_stage_timeout_publishes_dlq() -> None:
    from uns_contextualizer_orchestrator.models import PipelineConfig, PipelineStage
    from uns_contextualizer_orchestrator.metrics import PipelineMetrics
    from uns_contextualizer_orchestrator.pipeline import run_pipeline
    from eirvah_bus.request_reply import RequestTimeout

    cfg = PipelineConfig(
        stages=[PipelineStage(name="convert", subject="uns.work.convert", timeout_s=2.0)],
        dlq_subject="uns.dlq.telemetry",
    )

    async def fake_request_reply(**kwargs):
        raise RequestTimeout("timed out")

    nc_mock = MagicMock()
    nc_mock.publish = AsyncMock()
    reg = CollectorRegistry()
    metrics = PipelineMetrics(registry=reg)

    envelope = _raw_envelope()
    await run_pipeline(
        envelope=envelope,
        cfg=cfg,
        nc=nc_mock,
        metrics=metrics,
        request_reply_fn=fake_request_reply,
    )

    nc_mock.publish.assert_called_once()
    call_args = nc_mock.publish.call_args
    assert call_args[0][0] == "uns.dlq.telemetry"
