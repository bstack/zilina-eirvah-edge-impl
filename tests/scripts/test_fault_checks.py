"""Bounded fault verdicts require independent effects and immutable recovery."""

import copy
import hashlib
from datetime import UTC, datetime, timedelta

import pytest
from control import build_command
from eirvah_contracts.authorization import command_digest
from fault_checks import EXPECTED, PROFILE, ROOT, check_fault_evidence


@pytest.fixture
def evidence():
    data = {
        "schema_version": "1.0",
        "mode": "live_command_faults",
        "profile_sha256": hashlib.sha256(PROFILE.read_bytes()).hexdigest(),
        "cases": [],
        "capture_errors": [],
        "restored": True,
    }
    for name, (count, state, decision) in EXPECTED.items():
        command = build_command("reject", "reject", 10)
        payload = command.model_dump(mode="json")
        before = {
            "meta": {"db_id": "source"},
            "clock": {"domain": "clock"},
            "last_effect": 10,
            "state": {"block": {"reject": {"count": 0}}},
        }
        after = copy.deepcopy(before)
        after["last_effect"] += count
        after["state"]["block"]["reject"]["count"] = count
        after["effects"] = (
            [
                {
                    "payload": {
                        "target": 28,
                        "operation": 1,
                        "value": 10,
                        "status": 1,
                        "sequence": 7,
                        "before_count": 0,
                        "after_count": 1,
                    }
                }
            ]
            if count
            else []
        )
        result = {**payload, "decision": decision, "replayed": True, "written_at": "original-time"}
        ledger = (
            [
                {
                    "command_id": command.correlation_id,
                    "digest": command_digest(command),
                    "state": state,
                    "result": {"written_at": "original-time"},
                }
            ]
            if state
            else []
        )
        events = {
            "caller_reply_lost": ["commit_forwarded", "ack_forwarded"],
            "before_write_disconnect": [
                "disconnected_before_write",
                "commit_forwarded",
                "ack_forwarded",
            ],
            "commit_dropped": ["commit_dropped"],
            "native_ack_lost": ["commit_forwarded", "ack_dropped"],
            "writer_restart_after_effect": ["commit_forwarded", "ack_held"],
            "delayed_expired_command": [],
        }[name]
        first = copy.deepcopy(ledger)
        if name == "before_write_disconnect":
            first[0]["state"] = "retryable"
        if name == "writer_restart_after_effect":
            first[0]["state"] = "started"
        data["cases"].append(
            {
                "id": name,
                "command": payload,
                "replay_command": copy.deepcopy(payload),
                "before": before,
                "after": after,
                "ledger": ledger,
                "replay_result": result,
                "writer_clock": {"domain": "clock"},
                "proxy": [
                    {
                        "case_id": name,
                        "command_id": command.correlation_id,
                        "event": e,
                        "status": 1,
                        "sequence": 7,
                    }
                    for e in events
                ],
                "first_ledger": first,
                "first_source": copy.deepcopy(before),
                "first_result": None
                if name == "caller_reply_lost"
                else {
                    **payload,
                    "decision": "reject" if name == "before_write_disconnect" else "unknown",
                    "retryable": True,
                },
                "reply_subscription_absent": True,
                "restart": {
                    "before": {"uid": "a", "volumes": {"pvc": "original"}},
                    "after": {"uid": "b", "volumes": {"pvc": "original"}},
                },
                "published_clock": {"wall": command.requested_at.timestamp()},
                "held_replicas": 0,
                "held_clock": {"wall": command.deadline.timestamp() + 1},
            }
        )
    for case in data["cases"]:
        command = build_command("reject", "reject", 10).model_validate(case["command"])
        rows = (
            []
            if case["id"] == "delayed_expired_command"
            else [
                {
                    "record_seq": 1,
                    "record_id": "initiation",
                    "correlation_id": command.correlation_id,
                    "action": "write_initiated",
                    "command_digest": command_digest(command),
                    "target": command.target_uns_topic,
                    "requested_value": 10,
                    "approval_verified": True,
                }
            ]
        )
        paths = [
            "libs/eirvah-observability/src/eirvah_observability/execution.py",
            "libs/eirvah-contracts/src/eirvah_contracts/actuation.py",
            "libs/eirvah-contracts/src/eirvah_contracts/authorization.py",
            "services/actuation-signal-publisher/src/actuation_signal_publisher/service.py",
        ]
        case["writer_journals"] = [
            {
                "overflow": False,
                "records": rows,
                "record_seq": len(rows),
                "source_hashes": {
                    p: hashlib.sha256((ROOT / p).read_bytes()).hexdigest() for p in paths
                },
            }
        ]
    return data


def test_complete_finite_fault_package_passes(evidence):
    checks = check_fault_evidence(evidence)

    assert len(checks) == 6
    assert {c["status"] for c in checks} == {"pass"}


@pytest.mark.parametrize(
    "mutation",
    [
        "new_id",
        "new_deadline",
        "extra_effect",
        "wrong_bottle",
        "lost_record",
        "false_refusal",
        "new_volume",
        "missing_clock",
        "cleanup_failed",
        "lost_case",
    ],
)
def test_fault_evidence_cannot_silently_pass_incomplete_or_contradictory_data(evidence, mutation):
    case = evidence["cases"][3]
    if mutation == "new_id":
        case["replay_command"]["correlation_id"] = build_command(
            "reject", "reject", 10
        ).correlation_id
    elif mutation == "new_deadline":
        case["replay_command"]["deadline"] = (datetime.now(UTC) + timedelta(days=1)).isoformat()
    elif mutation == "extra_effect":
        case["after"]["effects"] *= 2
    elif mutation == "wrong_bottle":
        case["after"]["effects"][0]["payload"]["value"] = 11
    elif mutation == "lost_record":
        del case["after"]["effects"]
    elif mutation == "false_refusal":
        case["first_result"]["decision"] = "reject"
    elif mutation == "new_volume":
        evidence["cases"][4]["restart"]["after"]["volumes"] = {"pvc": "new"}
    elif mutation == "missing_clock":
        del case["writer_clock"]
    elif mutation == "cleanup_failed":
        evidence["restored"] = False
    elif mutation == "lost_case":
        evidence["cases"].pop()

    checks = check_fault_evidence(evidence)

    assert any(c["status"] != "pass" for c in checks)
    if mutation not in ("lost_record", "missing_clock", "cleanup_failed", "lost_case"):
        assert any(c["status"] == "fail" for c in checks)


def test_absent_fault_package_is_unknown():
    checks = check_fault_evidence(None)

    assert {c["status"] for c in checks} == {"unknown"}


@pytest.mark.parametrize(
    "violation", ["duplicate_initiation", "missing_sequence", "overflow", "wrong_source"]
)
def test_writer_trace_coverage_and_attribution_are_required(evidence, violation):
    case = evidence["cases"][0]
    journal = case["writer_journals"][0]
    if violation == "duplicate_initiation":
        journal["records"].append(
            {**journal["records"][0], "record_seq": 2, "record_id": "second-initiation"}
        )
        journal["record_seq"] = 2
    elif violation == "missing_sequence":
        journal["record_seq"] = 2
    elif violation == "overflow":
        journal["overflow"] = True
    else:
        journal["source_hashes"][next(iter(journal["source_hashes"]))] = "different"

    checks = check_fault_evidence(evidence)

    assert checks[0]["status"] == ("fail" if violation == "duplicate_initiation" else "unknown")


def test_combined_report_preserves_mixed_traffic_obligation(evidence):
    import json

    from conformance import ROOT, evaluate

    manifest = json.loads((ROOT / "config/architecture/local-control.json").read_text())
    reference = json.loads((ROOT / "config/architecture/conformance-reference.json").read_text())

    combined = evaluate(
        manifest, reference, records=[], policies=[], collection_errors=[], fault_evidence=evidence
    )

    c10 = next(r for r in combined["rules"] if r["id"] == "C10")
    assert (
        len(
            [
                c
                for c in c10["checks"]
                if c["check"].startswith("C10.fault.") and c["status"] == "pass"
            ]
        )
        == 6
    )
    assert not any(c["check"] == "C10.experiment" for c in c10["checks"])
    assert next(r for r in combined["rules"] if r["id"] == "C11")["status"] == "unknown"
    assert combined["overall"] == "unknown"
