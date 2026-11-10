"""A public success is insufficient without the declared cloud/edge execution route."""

import copy
from pathlib import Path

import pytest
from split_deployment import load_config
from split_rehearsal import route_ok


def records(config):
    rows = []
    for component, action, zone in [
        ("decision-agent-stub", "request_submitting", "edge"),
        ("amqp-actuation-event-subscriber", "request_received", "cloud"),
        ("actuation-control-orchestrator", "pipeline_received", "cloud"),
        ("actuation-event-validator", "validation_decided", "cloud"),
        ("actuation-signal-publisher", "write_initiated", "edge"),
        ("actuation-signal-publisher", "device_acknowledged", "edge"),
        ("actuation-signal-publisher", "writer_result", "edge"),
        ("actuation-control-orchestrator", "outcome_published", "cloud"),
    ]:
        rows.append(
            dict(
                component=component,
                action=action,
                correlation_id="selected",
                target="temperature",
                requested_value=26.0,
                requester="decision-agent-stub",
                decision="approve",
                outcome="completed" if action == "writer_result" else "approve",
                source=config[zone],
            )
        )
    return rows


def test_complete_automatic_command_uses_cloud_admission_and_edge_executor():
    config = load_config(Path("config/deployments/local-split.json"))
    assert route_ok(records(config), "selected", config)


@pytest.mark.parametrize(
    "change",
    ["wrong_namespace", "duplicate_write", "missing_validation", "wrong_value", "missing_outcome"],
)
def test_incomplete_or_misplaced_command_route_cannot_pass(change):
    config = load_config(Path("config/deployments/local-split.json"))
    rows = copy.deepcopy(records(config))
    if change == "wrong_namespace":
        rows[4]["source"] = config["cloud"]
    elif change == "duplicate_write":
        rows.append(copy.deepcopy(rows[4]))
    elif change == "missing_validation":
        rows = [r for r in rows if r["action"] != "validation_decided"]
    elif change == "wrong_value":
        rows[4]["requested_value"] = 25.0
    else:
        rows = [r for r in rows if r["action"] != "outcome_published"]

    assert not route_ok(rows, "selected", config)


def test_recovery_requires_retained_native_initiation_evidence():
    from datetime import UTC, datetime, timedelta

    from control import build_command
    from eirvah_contracts.authorization import command_digest
    from split_rehearsal import availability_ok

    old = build_command("temperature", "set", 25.0).model_copy(
        update=dict(
            requested_at=datetime.now(UTC) - timedelta(seconds=20),
            deadline=datetime.now(UTC) - timedelta(seconds=10),
        )
    )
    fresh = build_command("temperature", "set", 25.0)
    case = dict(
        command=old.model_dump(mode="json"),
        before=dict(setpoint=22.0),
        after_unavailable=dict(setpoint=22.0),
        unavailable_result=dict(decision="unknown"),
        expired_replay_sent_at=datetime.now(UTC).isoformat(),
        expired_result=dict(decision="reject"),
        expired_ledger=[],
        after_expired=dict(setpoint=22.0),
        fresh_command=fresh.model_dump(mode="json"),
        fresh_result=dict(decision="approve", correlation_id=fresh.correlation_id),
        fresh_ledger=[
            dict(command_id=fresh.correlation_id, state="completed", digest=command_digest(fresh))
        ],
        after_recovery=dict(setpoint=25.0),
        writer_records=[
            dict(correlation_id=fresh.correlation_id, action="write_initiated"),
            dict(correlation_id=fresh.correlation_id, action="device_acknowledged"),
        ],
    )

    assert availability_ok(case)
    case["writer_records"] = []
    assert not availability_ok(case)
