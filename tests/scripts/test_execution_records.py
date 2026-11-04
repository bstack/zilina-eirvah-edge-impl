"""Execution evidence contracts and command-path integration checks."""

import json
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest
from control import build_command
from structlog.testing import capture_logs

from tests.scripts.test_command_recovery import invoke_writer, make_writer


def test_record_contains_command_identity_and_explicit_clock_limits():
    # Arrange
    from eirvah_observability.execution import record_command

    command = build_command("capper", "speed", 1200)

    # Act
    with capture_logs() as records:
        record_command("writer", "write_initiated", command, attempt_id="attempt-1")

    # Assert
    record = records[0]
    assert record["event"] == "command_execution"
    assert record["correlation_id"] == command.correlation_id
    assert record["target"] == command.target_uns_topic
    assert record["requested_value"] == 1200
    assert record["deadline"] == command.deadline.isoformat()
    assert record["clock_uncertainty_s"] is None
    assert record["process_instance"] and record["record_id"]
    assert record["monotonic_ns"] > 0
    assert "signature" not in record and "approval_key" not in record


async def test_writer_records_one_initiation_per_attempt_and_no_initiation_on_replay(tmp_path):
    # Arrange
    worker, key = make_writer(tmp_path / "commands.sqlite3")
    command = build_command("capper", "speed", 1200)

    async def native(target, value, *, before_write):
        before_write()
        before_write()  # Two protocol frames, one application initiation.

    # Act
    with (
        capture_logs() as records,
        patch("actuation_signal_publisher.service.write_native_value", side_effect=native),
    ):
        await invoke_writer(worker, command, key)
        await invoke_writer(worker, command, key)

    # Assert
    events = [r for r in records if r.get("event") == "command_execution"]
    actions = [r["action"] for r in events]
    assert actions.count("write_initiated") == 1
    assert actions.count("device_acknowledged") == 1
    assert actions.count("writer_result") == 2
    assert actions.index("executor_admitted") < actions.index("write_initiated")
    assert actions.index("write_initiated") < actions.index("device_acknowledged")
    assert events[-1]["replayed"] is True
    assert all(r["correlation_id"] == command.correlation_id for r in events)
    assert "process_observed" not in actions


async def test_failed_write_records_unknown_without_acknowledgement(tmp_path):
    # Arrange
    worker, key = make_writer(tmp_path / "commands.sqlite3")
    command = build_command("capper", "speed", 1200)

    async def native(target, value, *, before_write):
        before_write()
        raise TimeoutError("lost device acknowledgement")

    # Act
    with (
        capture_logs() as records,
        patch("actuation_signal_publisher.service.write_native_value", side_effect=native),
    ):
        result = await invoke_writer(worker, command, key)

    # Assert
    events = [r for r in records if r.get("event") == "command_execution"]
    assert result.outcome == "unknown"
    assert "device_acknowledged" not in [r["action"] for r in events]
    assert events[-1]["outcome"] == "unknown"


def test_confirmed_rejection_records_command_and_actual_feedback():
    # Arrange
    from decision_agent_stub.inspection import RejectionTracker
    from eirvah_contracts.inspection import BottleInspection

    now = datetime.now(UTC)
    command = build_command("reject", "reject", 20)
    tracker = RejectionTracker()
    tracker.add(
        command,
        BottleInspection(
            inspection_id="0123456789abcdef:0001", bottle_id=20, passed=False, inspected_at=now
        ),
        baseline_count=4,
    )
    tracker.result(
        {
            "correlation_id": command.correlation_id,
            "decision": "approve",
            "written_at": now.isoformat(),
        },
        now,
    )
    tracker.feedback("count", 5, now + timedelta(seconds=1), now + timedelta(seconds=1))

    # Act
    with capture_logs() as records:
        tracker.feedback("bottle", 20, now + timedelta(seconds=1), now + timedelta(seconds=1))

    # Assert
    event = next(r for r in records if r.get("action") == "process_observed")
    assert event["correlation_id"] == command.correlation_id
    assert event["inspection_id"] == "0123456789abcdef:0001"
    assert event["observed_bottle"] == 20
    assert event["observed_count"] == 5 and event["baseline_count"] == 4
    assert event["evidence_scope"] == "simulator_feedback"


def test_collector_retains_replay_and_reports_missing_stages_without_inventing_completion():
    # Arrange
    from command_trace import extract_records, summarize

    line = json.dumps(
        {
            "event": "command_execution",
            "correlation_id": "id",
            "action": "writer_result",
            "outcome": "unknown",
            "record_id": "one",
            "component": "writer",
            "recorded_at": "2026-11-04T10:00:00+00:00",
        }
    )

    # Act
    records = extract_records("not json\n" + line, "id", pod="writer-1", previous=True)
    report = summarize(records, collection_errors=["writer-2: logs unavailable"])

    # Assert
    assert len(records) == 1 and records[0]["source"]["previous"] is True
    assert report["formal_conformance"] == "not_assessed"
    assert report["collection_errors"] == ["writer-2: logs unavailable"]
    assert "write_initiated" in report["missing_actions"]
    assert report["process_response"] == "unknown"
    assert report["ordering"] == "cross_process_order_not_established"


def test_collector_does_not_merge_different_commands():
    # Arrange
    from command_trace import extract_records

    line = json.dumps({"event": "command_execution", "correlation_id": "other"})

    # Act
    records = extract_records(line, "id", pod="writer-1", previous=False)

    # Assert
    assert records == []


@pytest.mark.parametrize("outcome", ["completed", "unknown"])
async def test_pipeline_records_published_outcome_after_publishing(outcome):
    # Arrange
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, MagicMock

    from actuation_control_orchestrator.models import (
        ActuationPipelineConfig,
        ActuationPipelineStage,
    )
    from actuation_control_orchestrator.pipeline import run_actuation_pipeline
    from eirvah_contracts.actuation import ValidationResult, WriteResult
    from eirvah_contracts.authorization import AuthenticatedRequest, sign_approval, sign_ingress
    from eirvah_contracts.envelope import NATSEnvelope

    command = build_command("capper", "speed", 1200)
    envelope = NATSEnvelope(
        correlation_id=command.correlation_id,
        payload=AuthenticatedRequest(
            command=command, identity=sign_ingress(command, "i" * 32)
        ).model_dump(mode="json"),
    )
    validation = ValidationResult(
        decision="approve", approval=sign_approval(command, "a" * 32, "actuation-signal-publisher")
    )
    writer = WriteResult(
        outcome=outcome, written_at=datetime.now(UTC) if outcome == "completed" else None
    )
    responses = [
        SimpleNamespace(
            data=NATSEnvelope(
                correlation_id=command.correlation_id, payload=result.model_dump(mode="json")
            )
            .model_dump_json()
            .encode()
        )
        for result in (validation, writer)
    ]
    exchange = MagicMock(publish=AsyncMock())
    cfg = ActuationPipelineConfig(
        stages=[
            ActuationPipelineStage(name=name, subject=name, timeout_s=1)
            for name in ("validate", "write_signal")
        ],
        dlq_subject="dlq",
    )

    # Act
    with capture_logs() as records:
        await run_actuation_pipeline(
            envelope=envelope,
            cfg=cfg,
            nc=MagicMock(),
            amqp_results_exchange=exchange,
            metrics=MagicMock(),
            allow_writes=True,
            request_reply_fn=AsyncMock(side_effect=responses),
        )

    # Assert
    published = json.loads(exchange.publish.call_args.args[0].body)
    event = next(r for r in records if r.get("action") == "outcome_published")
    assert event["outcome"] == published["decision"]
    assert event["correlation_id"] == published["correlation_id"] == command.correlation_id
    assert event["outcome"] == ("approve" if outcome == "completed" else "unknown")


def test_collector_reads_every_replica_and_previous_logs_and_exposes_collection_failure():
    # Arrange
    from types import SimpleNamespace

    from command_trace import COMPONENTS, collect

    pods = [
        {
            "metadata": {"name": name, "labels": {"app.kubernetes.io/name": component}},
            "status": {"containerStatuses": [{"restartCount": restarts}]},
        }
        for name, component, restarts in [
            ("writer-a", "actuation-signal-publisher", 1),
            ("writer-b", "actuation-signal-publisher", 0),
        ]
    ]
    line = json.dumps(
        {"event": "command_execution", "correlation_id": "id", "action": "writer_result"}
    )
    replies = [
        SimpleNamespace(stdout=json.dumps({"items": pods})),
        SimpleNamespace(stdout=line, returncode=0),
        SimpleNamespace(stdout=line, returncode=0),
        SimpleNamespace(stdout="", returncode=1),
    ]

    # Act
    with patch("command_trace.subprocess.run", side_effect=replies):
        records, errors = collect("kind-eirvah-edge", "eirvah-edge", "id", "1h")

    # Assert
    assert len(records) == 2
    assert {r["source"]["previous"] for r in records} == {False, True}
    assert any("writer-b: logs unavailable" in e for e in errors)
    assert all(any(c in e for e in errors) for c in COMPONENTS if c != "actuation-signal-publisher")
