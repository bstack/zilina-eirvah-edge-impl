"""Independent recovery verdicts require raw bounded evidence, including clocks."""

import copy
import hashlib
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from control import build_command
from eirvah_contracts.authorization import command_digest
from eirvah_controls.recovery import canonical
from recovery_checks import NAMES, PROFILE, check_recovery


@pytest.fixture
def evidence():
    command = build_command("reject", "reject", 10)
    request = command.model_dump(mode="json")
    event = {
        "inspection_id": "aaaaaaaaaaaaaaaa:0001",
        "bottle_id": 10,
        "inspected_at": request["requested_at"],
        "passed": False,
        "schema_version": "1.0",
    }
    row = {
        "id": event["inspection_id"],
        "digest": hashlib.sha256(canonical(event).encode()).hexdigest(),
        "payload": event,
        "command_id": command.correlation_id,
        "command": request,
        "disposition": "completed",
        "result": {"written_at": "2026-11-06T12:00:00Z"},
    }

    def source(count, created, boot):
        return {
            "meta": {
                "db_id": "source-db",
                "boots": str(boot),
                "created": str(created),
                "received": str(created),
            },
            "state": {"block": {"reject": {"count": count}}},
            "last_serial": created,
            "last_effect": count,
            "pending": 0,
            "clock": {"domain": "shared-clock"},
        }

    def packet(epoch, sequence):
        return {
            "eirvah:position": {
                "producer": "adapter/signal",
                "epoch": epoch,
                "sequence": sequence,
                "boundary": "adapter",
            }
        }

    start = source(0, 0, 1)
    end = source(1, 1, 2)
    end.update(
        events=[{"serial": 1, "id": event["inspection_id"], "payload": event, "received": 1}],
        effects=[
            {
                "payload": {
                    "target": 28,
                    "value": 10,
                    "status": 1,
                    "before_count": 0,
                    "after_count": 1,
                }
            }
        ],
    )
    pending_before = copy.deepcopy(row)
    pending_before["disposition"] = "pending"
    pending_before["result"] = None
    pending_after = copy.deepcopy(pending_before)
    pending_after["disposition"] = "refused"
    return {
        "pending_recovery": {"before_row": pending_before, "after_row": pending_after},
        "schema_version": "1.0",
        "mode": "live_cluster_recovery",
        "profile_sha256": hashlib.sha256(PROFILE.read_bytes()).hexdigest(),
        "capture_errors": [],
        "positions": {r: packet(1, 1) for r in ("opcua", "modbus", "s7")},
        "producer_restart": {
            "before_uid": "p1",
            "after_uid": "p2",
            "before_volume": {"pvc": "id"},
            "after_volume": {"pvc": "id"},
            "before_packet": packet(1, 1),
            "after_packet": packet(2, 1),
        },
        "old_state_audit": {
            "decision": {"accepted": False, "reason": "old_epoch"},
            "payload": packet(1, 1),
            "snapshot": {"position": {"epoch": 2}},
        },
        "publisher_restart": {
            "before_uid": "m1",
            "after_uid": "m2",
            "before_packet": packet(2, 2),
            "after_packet": packet(2, 3),
        },
        "source_restart": {
            "before_uid": "s1",
            "after_uid": "s2",
            "before": source(0, 0, 1),
            "after": source(0, 0, 2),
        },
        "consumer_restart": {
            "before_uid": "c1",
            "after_uid": "c2",
            "before_db": "consumer-db",
            "after_db": "consumer-db",
            "before_row": copy.deepcopy(row),
            "after_row": copy.deepcopy(row),
        },
        "start": {"source": start},
        "end": {
            "source": end,
            "consumer": {"inspections": [row], "clock": {"domain": "shared-clock"}},
            "writer": [
                {
                    "command_id": command.correlation_id,
                    "state": "completed",
                    "digest": command_digest(command),
                }
            ],
            "writer_capture": {"clock": {"domain": "shared-clock"}},
        },
        "outage": {"replicas_during": 0, "source_pending_during": 1},
        "forged_receipt": {
            "before": {"id": "event", "received": 0},
            "after": {"id": "event", "received": 0},
        },
    }


def test_complete_bounded_evidence_passes_five_checks(evidence):
    checks = check_recovery(evidence)

    assert [c["check"] for c in checks] == NAMES
    assert {c["status"] for c in checks} == {"pass"}


@pytest.mark.parametrize(
    "change,expected",
    [
        ("missing_clock", "unknown"),
        ("different_clock", "unknown"),
        ("missing_records", "unknown"),
        ("capture_error", "unknown"),
        ("lost_source_event", "fail"),
        ("double_effect", "fail"),
        ("mutated_command", "fail"),
        ("new_database", "fail"),
        ("accepted_forgery", "fail"),
        ("epoch_regression", "fail"),
    ],
)
def test_incomplete_or_contradictory_evidence_never_passes(evidence, change, expected):
    if change == "missing_clock":
        del evidence["end"]["writer_capture"]
    elif change == "different_clock":
        evidence["end"]["writer_capture"]["clock"]["domain"] = "other"
    elif change == "missing_records":
        del evidence["end"]["source"]["events"]
    elif change == "capture_error":
        evidence["capture_errors"].append("build mismatch")
    elif change == "lost_source_event":
        evidence["end"]["source"]["events"] = []
    elif change == "double_effect":
        evidence["end"]["source"]["effects"] *= 2
    elif change == "mutated_command":
        evidence["consumer_restart"]["after_row"]["command"]["deadline"] = "changed"
    elif change == "new_database":
        evidence["consumer_restart"]["after_db"] = "new-db"
    elif change == "accepted_forgery":
        evidence["forged_receipt"]["after"]["received"] = 1
    elif change == "epoch_regression":
        evidence["producer_restart"]["after_packet"]["eirvah:position"]["epoch"] = 1

    checks = check_recovery(evidence)

    assert expected in {c["status"] for c in checks}
    assert any(c["status"] != "pass" for c in checks)


def test_missing_package_is_unknown():
    checks = check_recovery(None)

    assert {c["status"] for c in checks} == {"unknown"}


def test_pending_restart_does_not_allow_a_new_command_or_deadline(evidence):
    evidence["pending_recovery"]["after_row"]["command"]["deadline"] = "renewed"

    checks = check_recovery(evidence)

    assert next(c for c in checks if c["check"] == "C06.durable_recovery")["status"] == "fail"


def test_combined_report_replaces_only_the_five_supported_live_limitations(evidence):
    import json
    import tempfile

    from conformance import ROOT, evaluate
    from state_event_case import capture

    manifest = json.loads((ROOT / "config/architecture/local-control.json").read_text())
    reference = json.loads((ROOT / "config/architecture/conformance-reference.json").read_text())
    with tempfile.TemporaryDirectory() as directory:
        capture(Path(directory) / "consumer")
        consumer = json.loads((Path(directory) / "consumer/evidence.json").read_text())
        report = evaluate(
            manifest,
            reference,
            records=[],
            policies=[],
            collection_errors=[],
            data_evidence=consumer,
            recovery_evidence=evidence,
        )

    checks = [c for rule in report["rules"] for c in rule["checks"]]
    for name in NAMES:
        selected = [c for c in checks if c["check"] == name]
        assert len(selected) == 1
        assert selected[0]["status"] == "pass"
    assert report["overall"] == "unknown"
