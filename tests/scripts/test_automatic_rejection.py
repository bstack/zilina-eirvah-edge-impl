from datetime import UTC, datetime, timedelta

import pytest
from decision_agent_stub.config import DecisionAgentSettings
from decision_agent_stub.inspection import InspectionRule, RejectionTracker
from eirvah_controls.inspection import InspectionStream
from modbus_data_subscriber.service import decode_inspection


def sample(*, passed: bool = False, bottle_id: int = 20, now: datetime | None = None):
    from eirvah_contracts.inspection import BottleInspection

    return BottleInspection(
        inspection_id="0123456789abcdef:0001",
        bottle_id=bottle_id,
        passed=passed,
        inspected_at=now or datetime.now(UTC),
    )


def test_atomic_inspection_snapshot_identifies_real_bottle_and_repeats_pattern() -> None:
    now = datetime.now(UTC)
    a = InspectionStream(session=1, every_n_bottles=2, fail_every_n_inspections=3)
    b = InspectionStream(session=2, every_n_bottles=2, fail_every_n_inspections=3)
    results = []
    for bottle in range(1, 7):
        for stream in (a, b):
            stream.observe(bottle, now)
        if bottle % 2 == 0:
            left, right = decode_inspection(a.registers()), decode_inspection(b.registers())
            assert left is not None and right is not None
            assert left.bottle_id == right.bottle_id == bottle
            assert left.passed == right.passed
            assert left.inspection_id != right.inspection_id
            results.append(left.passed)
    assert results == [True, True, False]
    assert a.inspected == 3 and a.failed == 1


def test_good_bottle_does_not_request_rejection() -> None:
    now = datetime.now(UTC)
    assert InspectionRule(DecisionAgentSettings()).update(sample(passed=True, now=now), now) is None


def test_failed_bottle_emits_once_even_with_repeated_inspection_messages() -> None:
    now = datetime.now(UTC)
    rule = InspectionRule(DecisionAgentSettings())
    event = sample(now=now)
    req = rule.update(event, now)
    assert req is not None and req.requested_value == event.bottle_id
    assert req.target_uns_topic.endswith("rejector_01/reject_bottle")
    assert rule.update(event, now) is None
    # A contradictory replay of the same event must not generate work either.
    assert rule.update(event.model_copy(update={"passed": True}), now) is None


@pytest.mark.parametrize("seconds", [-20, 20])
def test_stale_or_future_inspection_cannot_request_rejection(seconds: int) -> None:
    now = datetime.now(UTC)
    event = sample(now=now + timedelta(seconds=seconds))
    assert InspectionRule(DecisionAgentSettings()).update(event, now) is None


def test_confirmation_requires_ack_identity_and_counter_feedback() -> None:
    now = datetime.now(UTC)
    event = sample(now=now)
    request = InspectionRule(DecisionAgentSettings()).update(event, now)
    assert request is not None
    tracker = RejectionTracker()
    tracker.add(request, event, baseline_count=0)
    assert (
        tracker.result(
            {
                "correlation_id": request.correlation_id,
                "decision": "approve",
                "written_at": now.isoformat(),
            },
            now,
        )
        == []
    )
    assert tracker.feedback("bottle", 99, now, now) == []
    assert tracker.feedback("count", 1, now, now) == []
    assert tracker.feedback("bottle", event.bottle_id, now, now) == [
        (event.inspection_id, "confirmed")
    ]
    assert tracker.feedback("bottle", event.bottle_id, now, now) == []


def test_missing_ack_is_unknown_even_with_device_feedback() -> None:
    now = datetime.now(UTC)
    event = sample(now=now)
    request = InspectionRule(DecisionAgentSettings()).update(event, now)
    assert request is not None
    tracker = RejectionTracker()
    tracker.add(request, event, baseline_count=0)
    assert tracker.feedback("bottle", event.bottle_id, now, now) == []
    assert tracker.feedback("count", 1, now, now) == []
    assert tracker.expire(now + timedelta(seconds=20)) == [(event.inspection_id, "unknown")]


def test_deduplication_fails_closed_at_capacity() -> None:
    now = datetime.now(UTC)
    rule = InspectionRule(DecisionAgentSettings(), capacity=1)
    assert rule.update(sample(now=now), now) is not None
    other = sample(now=now).model_copy(
        update={"inspection_id": "0123456789abcdef:0002", "bottle_id": 21}
    )
    assert rule.update(other, now) is None


def test_restarted_or_wrapped_inspection_stream_has_new_identity() -> None:
    now = datetime.now(UTC)
    stream = InspectionStream(session=1, every_n_bottles=1)
    stream._sequence = 65535
    stream.observe(42, now)
    event = decode_inspection(stream.registers())
    assert event is not None and event.inspection_id == "0000000000000002:0001"


def test_stale_feedback_cannot_confirm_a_new_rejection() -> None:
    now = datetime.now(UTC)
    event = sample(now=now)
    request = InspectionRule(DecisionAgentSettings()).update(event, now)
    assert request is not None
    tracker = RejectionTracker()
    tracker.add(request, event, baseline_count=0)
    tracker.result(
        {
            "correlation_id": request.correlation_id,
            "decision": "approve",
            "written_at": now.isoformat(),
        },
        now,
    )
    old = now - timedelta(seconds=1)
    assert tracker.feedback("bottle", event.bottle_id, old, now) == []
    assert tracker.feedback("count", 1, old, now) == []


async def test_inspection_through_uns_policy_and_real_modbus_rejection() -> None:
    import asyncio
    import socket
    from contextlib import suppress
    from pathlib import Path
    from unittest.mock import patch

    from actuation_event_validator.service import load_policy, validate_request
    from actuation_signal_publisher.native import load_native_targets, write_native_value
    from data_converter.service import handle_convert_request, load_rules
    from decision_agent_stub.service import DecisionAgentRuntime
    from eirvah_contracts.pipeline import ContextualizeResult, PublishRequest
    from eirvah_contracts.signals import NormalizedSignalEnvelope, RawSignalEnvelope
    from modbus_data_subscriber.service import wrap_in_nats_envelope
    from modbus_simulator.config import SimulatorSettings
    from modbus_simulator.server import SimulatorRuntime
    from mqtt_uns_publisher.service import build_sosa_observation
    from pymodbus.client import AsyncModbusTcpClient
    from pymodbus.server import ServerAsyncStop
    from uns_auto_contextualizer.service import handle_contextualize_request, load_ontology

    def unused_port() -> int:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            return int(sock.getsockname()[1])

    root = Path(__file__).resolve().parents[2]
    port = unused_port()
    with patch("modbus_simulator.server.SimulatorMetrics"):
        plant = SimulatorRuntime(SimulatorSettings(host="127.0.0.1", port=port, tick_rate_ms=50))
    # Freeze movement after presenting a real bottle for inspection.
    plant._block.conveyor.running = False
    plant._block.reject.observe(42)
    plant._inspection = InspectionStream(session=1, every_n_bottles=1, fail_every_n_inspections=1)
    now = datetime.now(UTC)
    plant._inspection.observe(42, now)
    with patch("decision_agent_stub.service.make_counter"):
        decision = DecisionAgentRuntime(DecisionAgentSettings())
    task = asyncio.create_task(plant.run())
    try:
        await asyncio.sleep(0.15)
        async with AsyncModbusTcpClient("127.0.0.1", port=port) as client:
            response = await client.read_holding_registers(32, count=10, slave=1)
            assert not response.isError()
            event = decode_inspection(response.registers)
            assert event is not None
            raw = RawSignalEnvelope(
                source_endpoint=f"modbus-tcp://127.0.0.1:{port}",
                node_id="Inspector01.BottleInspection",
                value=event.model_dump_json(),
                value_type="string",
                quality="good",
                source_timestamp=event.inspected_at,
                server_timestamp=event.inspected_at,
                received_at=now,
            )
            envelope = wrap_in_nats_envelope(raw)
            normalized_reply = handle_convert_request(
                envelope, load_rules(root / "deploy/k3s/base/data-converter/conversion-rules.yaml")
            )
            assert normalized_reply.status == "ok"
            normalized = NormalizedSignalEnvelope.model_validate(normalized_reply.payload)
            contextualized = handle_contextualize_request(
                normalized_reply,
                load_ontology(
                    root / "deploy/k3s/base/uns-auto-contextualizer/eirvah-line-a.jsonld"
                ),
            )
            assert contextualized.status == "ok"
            context = ContextualizeResult.model_validate(contextualized.payload)
            published = PublishRequest(
                **context.model_dump(),
                correlation_id=envelope.correlation_id,
                value=normalized.value,
                value_type=normalized.value_type,
                unit=normalized.unit,
                quality=normalized.quality,
                source_endpoint=raw.source_endpoint,
                source_node_id=raw.node_id,
                source_timestamp=raw.source_timestamp,
                edge_ingress=now,
            )
            observation = build_sosa_observation(published).to_jsonld()
            settings = decision._settings
            assert context.uns_topic == settings.inspection_topic
            baseline = {
                "sosa:hasSimpleResult": 0,
                "sosa:resultTime": now.isoformat(),
                "eirvah:quality": "good",
                "eirvah:unit": "dimensionless",
            }
            decision.observation(settings.reject_count_topic, baseline, now)
            request = decision.observation(context.uns_topic, observation, now)
            assert request is not None
            assert decision.observation(context.uns_topic, observation, now) is None
            policies = load_policy(
                root / "deploy/k3s/base/actuation-event-validator/actuation-policy.yaml"
            )
            assert validate_request(request, policies).decision == "approve"
            targets = load_native_targets(
                root / "deploy/k3s/base/actuation-signal-publisher/native-actuation-targets.yaml"
            )
            target = targets[request.target_uns_topic].model_copy(
                update={"host": "127.0.0.1", "port": port}
            )
            await write_native_value(target, request.requested_value)
            assert plant._block.reject.last_bottle == 42 and plant._block.reject.count == 1
            with pytest.raises(ValueError, match="status=4"):
                await write_native_value(target, request.requested_value)
            assert plant._block.reject.count == 1
            feedback = await client.read_holding_registers(6, count=6, slave=1)
            at = datetime.now(UTC)
            assert feedback.registers[0] == 1 and feedback.registers[5] == 42
            tracker = decision._rejections
            assert tracker.feedback("count", feedback.registers[0], at, at) == []
            assert tracker.feedback("bottle", feedback.registers[5], at, at) == []
            assert tracker.result(
                {
                    "correlation_id": request.correlation_id,
                    "decision": "approve",
                    "written_at": at.isoformat(),
                },
                at,
            ) == [(event.inspection_id, "confirmed")]
    finally:
        await ServerAsyncStop()
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


@pytest.mark.parametrize(
    "field,value",
    [("bottle_id", True), ("bottle_id", 0), ("passed", "false"), ("inspection_id", "bad")],
)
def test_invalid_inspection_contract_is_rejected(field: str, value: object) -> None:
    from eirvah_contracts.inspection import BottleInspection
    from pydantic import ValidationError

    doc = sample().model_dump()
    doc[field] = value
    with pytest.raises(ValidationError):
        BottleInspection.model_validate(doc)


def test_runtime_requires_good_atomic_inspection_and_fresh_counter_baseline() -> None:
    from unittest.mock import patch

    from decision_agent_stub.service import DecisionAgentRuntime

    now = datetime.now(UTC)
    event = sample(now=now)
    with patch("decision_agent_stub.service.make_counter"):
        runtime = DecisionAgentRuntime(DecisionAgentSettings())
    settings = runtime._settings
    payload = {
        "sosa:hasSimpleResult": event.model_dump_json(),
        "sosa:resultTime": now.isoformat(),
        "eirvah:quality": "good",
        "eirvah:unit": "json",
    }
    assert runtime.observation(settings.inspection_topic, payload, now) is None
    baseline = {
        "sosa:hasSimpleResult": 0,
        "sosa:resultTime": now.isoformat(),
        "eirvah:quality": "good",
        "eirvah:unit": "dimensionless",
    }
    runtime.observation(settings.reject_count_topic, baseline, now)
    for key, value in [
        ("eirvah:quality", "bad"),
        ("eirvah:unit", "m/s"),
        ("sosa:resultTime", (now - timedelta(seconds=1)).isoformat()),
    ]:
        assert runtime.observation(settings.inspection_topic, {**payload, key: value}, now) is None
    request = runtime.observation(settings.inspection_topic, payload, now)
    assert request is not None and request.requested_value == event.bottle_id
    assert runtime.observation(settings.inspection_topic, payload, now) is None


def test_tracker_handles_write_timeout_as_unconfirmed_without_retry() -> None:
    now = datetime.now(UTC)
    event = sample(now=now)
    request = InspectionRule(DecisionAgentSettings()).update(event, now)
    assert request is not None
    tracker = RejectionTracker()
    tracker.add(request, event, 0)
    assert tracker.result(
        {
            "correlation_id": request.correlation_id,
            "decision": "reject",
            "rejection_reason": "write_timeout",
        },
        now,
    ) == [(event.inspection_id, "not_confirmed")]
    assert not tracker.pending


@pytest.mark.parametrize("decision", ["pending", "unknown"])
def test_rejection_tracker_preserves_pending_and_reports_unknown(decision: str) -> None:
    now = datetime.now(UTC)
    event = sample(now=now)
    req = InspectionRule(DecisionAgentSettings()).update(event, now)
    assert req is not None
    tracker = RejectionTracker()
    tracker.add(req, event, baseline_count=0)
    result = tracker.result({"correlation_id": req.correlation_id, "decision": decision}, now)
    if decision == "pending":
        assert result == [] and req.correlation_id in tracker.pending
    else:
        assert (
            result == [(event.inspection_id, "unknown")]
            and req.correlation_id not in tracker.pending
        )
