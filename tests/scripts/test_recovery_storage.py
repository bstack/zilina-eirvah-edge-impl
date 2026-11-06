from pathlib import Path

import pytest


def test_adapter_epoch_survives_restart_and_fences_the_old_process(tmp_path: Path):
    from eirvah_controls.recovery import ProducerPositions

    first = ProducerPositions(tmp_path / "positions.db", "modbus-edge")
    first.begin_epoch()
    a = first.next("Conveyor.Speed")
    b = first.next("Conveyor.Speed")
    restarted = ProducerPositions(tmp_path / "positions.db", "modbus-edge")
    restarted.begin_epoch()

    c = restarted.next("Conveyor.Speed")

    assert a["producer"] == b["producer"] == c["producer"]
    assert a["epoch"] == b["epoch"] < c["epoch"]
    assert (a["sequence"], b["sequence"], c["sequence"]) == (1, 2, 1)
    with pytest.raises(RuntimeError, match="fenced"):
        first.next("Conveyor.Speed")


def test_source_event_stays_pending_until_receipt_and_survives_reopen(tmp_path: Path):
    from eirvah_controls.recovery import SourceJournal

    path = tmp_path / "source.db"
    journal = SourceJournal(path, capacity=2)
    journal.commit({"bottles": 10}, {"inspection_id": "a:1", "bottle_id": 10})
    reopened = SourceJournal(path, capacity=2)

    assert reopened.head()["inspection_id"] == "a:1"
    assert reopened.state() == {"bottles": 10}
    assert reopened.ack("other") is False
    assert reopened.ack("a:1") is True
    assert reopened.ack("a:1") is True
    assert reopened.head() is None
    assert reopened.stats()["created"] == reopened.stats()["received"] == 1


def test_source_capacity_does_not_overwrite_unreceived_events(tmp_path: Path):
    from eirvah_controls.recovery import SourceJournal

    journal = SourceJournal(tmp_path / "source.db", capacity=1)
    journal.commit({}, {"inspection_id": "a:1", "bottle_id": 10})

    with pytest.raises(RuntimeError, match="capacity"):
        journal.commit({}, {"inspection_id": "a:2", "bottle_id": 11})
    assert journal.head()["inspection_id"] == "a:1"
    assert journal.stats()["created"] == 1


def test_inspection_and_command_identity_are_committed_before_receipt(tmp_path: Path):
    from eirvah_controls.recovery import ConsumerStore

    path = tmp_path / "consumer.db"
    event = {
        "inspection_id": "aaaaaaaaaaaaaaaa:0001",
        "bottle_id": 10,
        "passed": False,
        "inspected_at": "2026-11-06T10:00:00Z",
    }
    from datetime import datetime

    now = datetime.fromisoformat(event["inspected_at"]).timestamp()
    created = []

    def command():
        created.append(True)
        return {"correlation_id": "original-command", "deadline": "2026-11-06T10:00:02Z"}

    store = ConsumerStore(path)
    first = store.receive(event, now, 2.0, command)
    store.close()
    restarted = ConsumerStore(path)

    replay = restarted.receive(event, now + 1, 2.0, command)

    assert replay["command_id"] == first["command_id"] == "original-command"
    assert len(created) == 1
    changed = {**event, "bottle_id": 11}
    with pytest.raises(ValueError, match="identity conflict"):
        restarted.receive(changed, now + 1, 2.0, command)


def test_old_epoch_is_refused_after_consumer_reopen(tmp_path: Path):
    from eirvah_controls.recovery import ConsumerStore

    path = tmp_path / "consumer.db"
    store = ConsumerStore(path)
    assert (
        store.advance("topic", {"producer": "edge/node", "epoch": 2, "sequence": 1}, "value")
        == "accepted"
    )
    store.close()
    reopened = ConsumerStore(path)

    assert (
        reopened.advance("topic", {"producer": "edge/node", "epoch": 1, "sequence": 999}, "old")
        == "old_epoch"
    )
    assert (
        reopened.advance("topic", {"producer": "edge/node", "epoch": 2, "sequence": 1}, "changed")
        == "position_conflict"
    )
    assert (
        reopened.advance("topic", {"producer": "edge/node", "epoch": 3, "sequence": 1}, "new")
        == "accepted"
    )


def test_receipt_cannot_delete_another_event_or_use_a_forged_signature():
    from datetime import UTC, datetime

    from eirvah_contracts.inspection import BottleInspection, sign_receipt, verify_receipt

    event = BottleInspection(
        inspection_id="aaaaaaaaaaaaaaaa:0001",
        bottle_id=10,
        passed=False,
        inspected_at=datetime.now(UTC),
    )
    receipt = sign_receipt(event, "r" * 48)

    verify_receipt(receipt, event, "r" * 48)
    with pytest.raises(ValueError):
        verify_receipt(receipt, event.model_copy(update={"bottle_id": 11}), "r" * 48)
    with pytest.raises(ValueError):
        verify_receipt(receipt.model_copy(update={"signature": "0" * 64}), event, "r" * 48)


def test_position_survives_conversion_context_and_public_sosa():
    from datetime import UTC, datetime

    from data_converter.service import ConversionRule, apply_conversion
    from eirvah_contracts.pipeline import ContextualizeResult
    from eirvah_contracts.signals import RawSignalEnvelope, SourcePosition
    from eirvah_contracts.uns import parse_uns_topic
    from mqtt_uns_publisher.service import build_sosa_observation
    from uns_contextualizer_orchestrator.models import PipelineContext

    now = datetime.now(UTC)
    position = SourcePosition(producer="modbus/node", epoch=2, sequence=5)
    raw = RawSignalEnvelope(
        node_id="Node",
        source_endpoint="modbus://source",
        value=0.4,
        value_type="double",
        quality="good",
        source_timestamp=now,
        server_timestamp=now,
        received_at=now,
        position=position,
    )
    normalized = apply_conversion(
        raw, ConversionRule(node_id="Node", value_type="double", unit="m/s")
    )
    topic = "uniza/zilina/factory1/line_a/conveyor/belt_01/speed_setpoint"
    ctx = PipelineContext(
        correlation_id="01ARZ3NDEKTSV4RRFFQ69G5FAV",
        raw=raw,
        normalized=normalized,
        contextualized=ContextualizeResult(
            uns_topic=topic,
            uns_path=parse_uns_topic(topic),
            semantic_type="speed",
            sensor_uri="sensor",
            feature_uri="feature",
            property_uri="property",
        ),
        ingress_at=now,
    )

    published = build_sosa_observation(ctx.build_publish_request()).to_jsonld()

    assert normalized.position == position
    assert published["eirvah:position"] == position.model_dump()


def test_simulated_process_and_native_effect_state_survive_source_restart(tmp_path: Path):
    from unittest.mock import patch

    from modbus_simulator.config import SimulatorSettings
    from modbus_simulator.server import SimulatorRuntime

    settings = SimulatorSettings(journal_path=tmp_path / "source.db")
    with patch("modbus_simulator.server.SimulatorMetrics"):
        first = SimulatorRuntime(settings)
        first._open_recovery()
        first._block.reject.observe(10)
        assert first._block.reject.apply(1, 10) == 1
        first._acks["28"] = [7, 1]
        first._journal.commit(first._snapshot())
        first._journal.close()
        restarted = SimulatorRuntime(settings)
        restarted._open_recovery()

    assert restarted._block.reject.count == 1
    assert restarted._block.reject.apply(1, 10) == 4
    assert restarted._acks["28"] == [7, 1]
    assert restarted._journal.stats()["boots"] == 2
    restarted._journal.close()


def test_inspection_fifo_wire_roundtrip_preserves_receipt_digest():
    from datetime import UTC, datetime

    from eirvah_contracts.inspection import (
        BottleInspection,
        decode_inspection,
        encode_inspection,
        inspection_digest,
    )

    event = BottleInspection(
        inspection_id="aaaaaaaaaaaaaaaa:0001",
        bottle_id=10,
        passed=False,
        inspected_at=datetime(2026, 11, 6, 10, 0, 0, 123000, tzinfo=UTC),
    )

    copied = decode_inspection(encode_inspection(event))

    assert copied == event
    assert inspection_digest(copied) == inspection_digest(event)


def test_pending_command_survives_reopen_and_unknown_outcome_is_not_retried(tmp_path: Path):
    import json
    from datetime import UTC, datetime

    from eirvah_controls.recovery import ConsumerStore

    now = datetime(2026, 11, 6, 12, tzinfo=UTC).timestamp()
    event = {
        "inspection_id": "aaaaaaaaaaaaaaaa:0001",
        "bottle_id": 10,
        "inspected_at": datetime.fromtimestamp(now, UTC).isoformat(),
        "passed": False,
    }
    command = {
        "correlation_id": "original-id",
        "requester": "decision-agent-stub",
        "target_uns_topic": "reject",
        "requested_value": 10,
        "value_type": "int64",
        "deadline": "original-deadline",
    }
    path = tmp_path / "consumer.db"
    first = ConsumerStore(path)
    owned = first.receive(event, now, 2, lambda: command)
    first.sent(owned["id"], now)
    first.close()
    restarted = ConsumerStore(path)

    ready = restarted.ready(now + 3)
    restarted.settle({**command, "decision": "unknown"})

    assert len(ready) == 1
    assert json.loads(ready[0]["command"]) == command
    assert restarted.ready(now + 5) == []
    assert restarted.find(event["inspection_id"])["disposition"] == "unknown"


def test_completed_result_cannot_be_replaced_by_a_rejection(tmp_path: Path):
    from datetime import UTC, datetime

    from eirvah_controls.recovery import ConsumerStore

    now = datetime(2026, 11, 6, 12, tzinfo=UTC).timestamp()
    event = {
        "inspection_id": "aaaaaaaaaaaaaaaa:0001",
        "bottle_id": 10,
        "inspected_at": datetime.fromtimestamp(now, UTC).isoformat(),
        "passed": False,
    }
    command = {
        "correlation_id": "original-id",
        "requester": "decision-agent-stub",
        "target_uns_topic": "reject",
        "requested_value": 10,
        "value_type": "int64",
    }
    store = ConsumerStore(tmp_path / "consumer.db")
    store.receive(event, now, 2, lambda: command)
    store.settle({**command, "decision": "approve", "written_at": "retained-time"})

    with pytest.raises(ValueError, match="cannot be overwritten"):
        store.settle({**command, "decision": "reject"})

    assert store.find(event["inspection_id"])["disposition"] == "completed"
