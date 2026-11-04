"""Scenario contracts and execution behaviour with controlled transports."""

import json
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest


def plan_data():
    return {
        "name": "test",
        "steps": [
            {"name": "speed", "target": "capper", "action": "speed", "value": 1200},
            {"name": "replay", "replay_of": "speed"},
        ],
    }


def completed(command, *, replayed=False):
    return {
        **command.model_dump(mode="json"),
        "decision": "approve",
        "written_at": "2026-11-04T10:00:00Z",
        "replayed": replayed,
    }


async def test_runner_saves_command_before_send_and_replays_exact_identity(tmp_path):
    # Arrange
    from scenario import Scenario, run_scenario

    plan = Scenario.model_validate(plan_data())
    out = tmp_path / "run"
    sent = []

    async def sender(command):
        journal = [json.loads(x) for x in (out / "journal.jsonl").read_text().splitlines()]
        assert journal[-1]["event"] == "command_submitting"
        assert journal[-1]["command"]["correlation_id"] == command.correlation_id
        sent.append(command.model_dump(mode="json"))
        return completed(command, replayed=len(sent) == 2)

    # Act
    report = await run_scenario(plan, out, sender=sender, observations=AsyncMock())

    # Assert
    assert report["outcome"] == "passed"
    import hashlib

    assert (
        report["scenario_sha256"]
        == hashlib.sha256((out / "scenario.json").read_bytes()).hexdigest()
    )
    assert sent[0] == sent[1]
    assert report["formal_conformance"] == "not_assessed"
    assert json.loads((out / "summary.json").read_text())["outcome"] == "passed"


@pytest.mark.parametrize("failure", ["timeout", "unknown", "pending", "wrong_identity"])
async def test_uncertain_command_stops_remaining_steps_without_retry(tmp_path, failure):
    # Arrange
    from scenario import Scenario, run_scenario

    plan = Scenario.model_validate(plan_data())
    calls = []

    async def sender(command):
        calls.append(command.correlation_id)
        if failure == "timeout":
            raise TimeoutError("amqp://user:secret@host")
        result = completed(command)
        if failure == "wrong_identity":
            result["correlation_id"] = "01M6KKJF2RT51XG8Y6TA95R8ZH"
        else:
            result["decision"] = failure
        return result

    # Act
    report = await run_scenario(plan, tmp_path / "run", sender=sender, observations=AsyncMock())

    # Assert
    assert report["outcome"] == "unknown" and len(calls) == 1
    assert "secret" not in (tmp_path / "run" / "summary.json").read_text()
    assert len(report["steps"]) == 1


async def test_policy_refusal_must_match_expected_reason(tmp_path):
    # Arrange
    from scenario import Scenario, run_scenario

    plan = Scenario.model_validate(
        {
            "name": "policy",
            "steps": [
                {
                    "name": "reject",
                    "target": "capper",
                    "action": "speed",
                    "value": 2000,
                    "expect": "reject",
                    "reason_contains": "outside policy range",
                }
            ],
        }
    )

    async def sender(command):
        return {
            **command.model_dump(mode="json"),
            "decision": "reject",
            "rejection_reason": "not allowlisted",
        }

    # Act
    report = await run_scenario(plan, tmp_path / "run", sender=sender, observations=AsyncMock())

    # Assert
    assert report["outcome"] == "failed"


async def test_replay_requires_original_completion_time_and_replayed_marker(tmp_path):
    # Arrange
    from scenario import Scenario, run_scenario

    sender = AsyncMock(side_effect=lambda c: completed(c))

    # Act
    report = await run_scenario(
        Scenario.model_validate(plan_data()),
        tmp_path / "run",
        sender=sender,
        observations=AsyncMock(),
    )

    # Assert
    assert report["outcome"] == "failed" and sender.await_count == 2


async def test_repetitions_generate_new_ids_but_replay_within_each_iteration(tmp_path):
    # Arrange
    from scenario import Scenario, run_scenario

    sent = []

    async def sender(command):
        repeated = command.correlation_id in sent
        sent.append(command.correlation_id)
        return completed(command, replayed=repeated)

    # Act
    report = await run_scenario(
        Scenario.model_validate(plan_data()),
        tmp_path / "run",
        sender=sender,
        observations=AsyncMock(),
        repetitions=2,
    )

    # Assert
    assert report["outcome"] == "passed"
    assert sent[0] == sent[1] and sent[2] == sent[3] and sent[0] != sent[2]


async def test_fault_reset_is_skipped_only_after_fresh_healthy_observation(tmp_path):
    # Arrange
    from scenario import Scenario, run_scenario

    plan = Scenario.model_validate(
        {
            "name": "reset",
            "steps": [
                {"name": "reset", "target": "conveyor", "action": "reset", "if_faulted": True}
            ],
        }
    )
    observations = AsyncMock()
    observations.faulted.return_value = {"value": False, "observed_at": "now"}
    sender = AsyncMock()

    # Act
    report = await run_scenario(plan, tmp_path / "run", sender=sender, observations=observations)

    # Assert
    assert report["steps"][0]["outcome"] == "skipped"
    sender.assert_not_awaited()
    observations.faulted.assert_awaited_once_with("conveyor")


async def test_dynamic_bottle_is_recorded_with_inspection_identity(tmp_path):
    # Arrange
    from scenario import Scenario, run_scenario

    plan = Scenario.model_validate(
        {
            "name": "bottle",
            "steps": [
                {
                    "name": "reject",
                    "target": "reject",
                    "action": "reject",
                    "value_source": "passed_bottle",
                }
            ],
        }
    )
    observations = AsyncMock()
    observations.passed_bottle.return_value = {"value": 42, "inspection_id": "session:1"}
    sender = AsyncMock(side_effect=lambda c: completed(c))

    # Act
    report = await run_scenario(plan, tmp_path / "run", sender=sender, observations=observations)

    # Assert
    assert report["outcome"] == "passed"
    assert sender.call_args.args[0].requested_value == 42
    assert report["steps"][0]["observation"]["inspection_id"] == "session:1"


@pytest.mark.parametrize("change", ["duplicate", "forward_replay", "bad_action", "ambiguous_value"])
def test_invalid_plan_is_refused_before_execution(change):
    # Arrange
    from scenario import Scenario

    data = plan_data()
    if change == "duplicate":
        data["steps"][1]["name"] = "speed"
    elif change == "forward_replay":
        data["steps"].reverse()
    elif change == "bad_action":
        data["steps"][0]["action"] = "close"
    else:
        data["steps"][0]["value_source"] = "passed_bottle"

    # Act / Assert
    with pytest.raises(ValueError):
        Scenario.model_validate(data)


@pytest.mark.parametrize(
    "age,quality,passed",
    [(10, "good", True), (-2, "good", True), (0, "bad", True), (0, "good", False)],
)
def test_stale_bad_or_failed_inspection_cannot_supply_rejection_target(age, quality, passed):
    # Arrange
    from scenario import passed_inspection

    now = datetime.now(UTC)
    at = now - timedelta(seconds=age)
    payload = {
        "eirvah:quality": quality,
        "eirvah:unit": "json",
        "sosa:resultTime": at.isoformat(),
        "sosa:hasSimpleResult": json.dumps(
            {
                "schema_version": "1.0",
                "inspection_id": "0123456789abcdef:0001",
                "bottle_id": 42,
                "passed": passed,
                "inspected_at": at.isoformat(),
            }
        ),
    }

    # Act
    result = passed_inspection(payload, now, set())

    # Assert
    assert result is None


async def test_cancellation_retains_uncertain_command_and_interrupted_summary(tmp_path):
    # Arrange
    import asyncio

    from scenario import Scenario, run_scenario

    sender = AsyncMock(side_effect=asyncio.CancelledError())

    # Act
    with pytest.raises(asyncio.CancelledError):
        await run_scenario(
            Scenario.model_validate(plan_data()),
            tmp_path / "run",
            sender=sender,
            observations=AsyncMock(),
        )

    # Assert
    report = json.loads((tmp_path / "run/summary.json").read_text())
    assert report["outcome"] == "interrupted"
    assert report["steps"][0]["outcome"] == "unknown"
    assert sender.await_count == 1
    events = [json.loads(x) for x in (tmp_path / "run/journal.jsonl").read_text().splitlines()]
    assert events[0]["event"] == "command_submitting"


def test_fresh_passed_bottle_must_have_matching_timestamp_and_unused_inspection():
    # Arrange
    from scenario import passed_inspection

    now = datetime.now(UTC)
    event = {
        "schema_version": "1.0",
        "inspection_id": "0123456789abcdef:0001",
        "bottle_id": 42,
        "passed": True,
        "inspected_at": now.isoformat(),
    }
    payload = {
        "eirvah:quality": "good",
        "eirvah:unit": "json",
        "sosa:resultTime": now.isoformat(),
        "sosa:hasSimpleResult": json.dumps(event),
    }

    # Act
    fresh = passed_inspection(payload, now, set())
    reused = passed_inspection(payload, now, {event["inspection_id"]})
    mismatch = passed_inspection(
        {**payload, "sosa:resultTime": (now - timedelta(seconds=1)).isoformat()}, now, set()
    )

    # Assert
    assert fresh["value"] == 42 and fresh["inspection_id"] == event["inspection_id"]
    assert reused is None and mismatch is None


async def test_existing_evidence_directory_is_never_overwritten(tmp_path):
    # Arrange
    from scenario import Scenario, run_scenario

    out = tmp_path / "run"
    out.mkdir()
    (out / "summary.json").write_text("original")
    sender = AsyncMock()

    # Act
    with pytest.raises(FileExistsError):
        await run_scenario(
            Scenario.model_validate(plan_data()), out, sender=sender, observations=AsyncMock()
        )

    # Assert
    assert (out / "summary.json").read_text() == "original"
    sender.assert_not_awaited()
