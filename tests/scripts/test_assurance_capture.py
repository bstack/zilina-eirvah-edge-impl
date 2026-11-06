"""Synthetic, explicitly labelled capture fixtures exercise proof completeness."""

import copy
import hashlib
import json
from datetime import UTC, datetime

import pytest
from assurance_checks import check_assurance
from assurance_components import ROOT, capture
from control import build_command
from e0_capture import calibrate_record


@pytest.fixture
def proof(tmp_path):
    component = capture(tmp_path / "components")
    command = build_command("capper", "speed", 2000).model_dump(mode="json")
    actors = [
        "amqp-actuation-event-subscriber",
        "actuation-control-orchestrator",
        "actuation-event-validator",
        "actuation-signal-publisher",
    ]
    origins = {a: "fixture-" + str(i) for i, a in enumerate(actors)}
    base = int(datetime.fromisoformat(command["requested_at"]).timestamp() * 1e9)

    def clock(tick):
        return {
            "monotonic_ns": tick,
            "monotonic_before_ns": tick - 1,
            "monotonic_after_ns": tick + 1,
            "wall_time_ns": base + tick,
            "clock_domain": "fixture-clock",
            "timestamp_resolution_ns": 1,
        }

    def row(role, action, cid, **extra):
        return {
            "event": "command_execution",
            "execution_schema": "1.0",
            "record_id": role + "-" + action + "-" + cid,
            "component": role,
            "action": action,
            "correlation_id": cid,
            "process_instance": origins[role],
            "monotonic_ns": 3000,
            "monotonic_before_ns": 2999,
            "monotonic_after_ns": 3001,
            "clock_domain": "fixture-clock",
            "recorded_at": datetime.fromtimestamp((base + 3000) / 1e9, UTC).isoformat(),
            "clock_uncertainty_s": None,
            **extra,
        }

    raw = [
        row(actors[0], "request_received", command["correlation_id"]),
        row(actors[1], "pipeline_received", command["correlation_id"]),
        row(actors[2], "validation_decided", command["correlation_id"], decision="reject"),
        row(
            actors[1],
            "outcome_published",
            command["correlation_id"],
            outcome="reject",
            reason="policy",
        ),
    ]
    bypass = []
    for mode in ("unsigned", "tampered", "wrong_writer", "expired", "wrong_envelope"):
        request = build_command("capper", "speed", 1200).model_dump(mode="json")
        bypass.append(
            {
                "mode": mode,
                "command": request,
                "native_before": {"fixture_mailbox": 0},
                "native_after": {"fixture_mailbox": 0},
                "reply": {"status": "error", "payload": {"outcome": "not_executed"}},
                "approval_claims": {"writer": "another-writer", "command_digest": "different"},
            }
        )
        raw.append(
            row(actors[3], "writer_result", request["correlation_id"], outcome="not_executed")
        )
    starts = {}
    ends = {}
    all_records = []
    for actor in actors:
        paths = [
            "libs/eirvah-observability/src/eirvah_observability/execution.py",
            "libs/eirvah-contracts/src/eirvah_contracts/actuation.py",
            "libs/eirvah-contracts/src/eirvah_contracts/authorization.py",
            "services/" + actor + "/src/" + actor.replace("-", "_") + "/service.py",
        ]
        if actor == actors[3]:
            paths.append(
                "services/actuation-signal-publisher/src/actuation_signal_publisher/ledger.py"
            )
        hashes = {p: hashlib.sha256((ROOT / p).read_bytes()).hexdigest() for p in paths}
        selected = [
            {**r, "record_seq": i + 1}
            for i, r in enumerate(x for x in raw if x["component"] == actor)
        ]
        remote = {
            "process_instance": origins[actor],
            "record_seq": 0,
            "overflow": False,
            "clock": clock(1000),
            "source_hashes": hashes,
            "records": [],
            "writer_policy": {
                "retention_s": 86400,
                "ledger_path": "/var/lib/eirvah/commands.sqlite3",
            },
        }
        starts[actor] = {"client_before": clock(990), "client_after": clock(1020), "remote": remote}
        ends[actor] = {
            "client_before": clock(9990),
            "client_after": clock(10020),
            "remote": {
                **copy.deepcopy(remote),
                "record_seq": len(selected),
                "records": selected,
                "clock": clock(10000),
            },
        }
        all_records.extend(calibrate_record(r, starts[actor], ends[actor], []) for r in selected)
    records = [r for r in all_records if r["correlation_id"] == command["correlation_id"]]
    inventory = {
        "pods": {
            a: {
                "component": a,
                "uid": origins[a],
                "pod": a,
                "node": "fixture-node",
                "context": "fixture-context",
                "namespace": "fixture-namespace",
                "containers": [{"imageID": "fixture-image"}],
            }
            for a in actors
        },
        "ledger_volume": {"uid": "fixture-pvc"},
    }
    topics = {
        "uniza/zilina/factory1/line_a/" + p
        for p in (
            "conveyor/belt_01/speed_setpoint",
            "reject_station/reject_counter_01/reject_count",
            "reject_station/rejector_01/last_rejected_bottle",
            "inspector/inspector_01/bottle_inspection",
        )
    }
    hashes = component["source_hashes"]
    data = {
        "fixture_only": True,
        "schema_version": "1.0",
        "profile_sha256": component["profile_sha256"],
        "components": component,
        "starts": starts,
        "ends": ends,
        "all_records": all_records,
        "records": records,
        "host_samples": [],
        "capture_errors": [],
        "inventory_start": inventory,
        "inventory_end": copy.deepcopy(inventory),
        "controller_state": {"restored": True},
        "source_case_state": {"restored": True},
        "scenario": {"outcome": "passed"},
        "submissions": [
            {
                "command": command,
                "result": {**command, "decision": "reject", "rejection_reason": "policy"},
                "ledger_before": [],
            }
        ],
        "ledger": [],
        "bypass": bypass,
        "live_samples": {
            r["topic"]: r["packet"]
            for r in component["contracts"]["observations"]
            if r["topic"] in topics
        },
        "receiver_snapshot": {
            "guard_path": "/etc/decision-agent-stub/eirvah-line-a.jsonld",
            "ontology": hashes["config/eirvah-line-a.jsonld"],
            "hashes": {
                p: hashes[p]
                for p in (
                    "services/decision-agent-stub/src/decision_agent_stub/service.py",
                    "libs/eirvah-contracts/src/eirvah_contracts/observation.py",
                    "libs/eirvah-contracts/src/eirvah_contracts/sosa.py",
                )
            },
        },
    }
    return data


def test_complete_synthetic_proof_can_close_only_its_refused_command(proof):
    checks, closed = check_assurance(
        proof, proof["records"], [r["command"] for r in proof["submissions"]]
    )

    assert {c["status"] for c in checks} == {"pass"}
    assert closed == {proof["submissions"][0]["command"]["correlation_id"]}


@pytest.mark.parametrize(
    "change",
    [
        "missing_record",
        "changed_record",
        "overflow",
        "restart",
        "wrong_build",
        "replaced_volume",
        "bad_clock",
        "not_restored",
        "unrelated_run",
    ],
)
def test_incomplete_capture_never_establishes_absence_of_execution(proof, change):
    rows = proof["records"]
    if change == "missing_record":
        proof["all_records"].pop()
    elif change == "changed_record":
        proof["all_records"][0]["action"] = "changed"
    elif change == "overflow":
        next(iter(proof["ends"].values()))["remote"]["overflow"] = True
    elif change == "restart":
        next(iter(proof["ends"].values()))["remote"]["process_instance"] = "other"
    elif change == "wrong_build":
        next(iter(proof["starts"].values()))["remote"]["source_hashes"][
            "libs/eirvah-contracts/src/eirvah_contracts/actuation.py"
        ] = "wrong"
    elif change == "replaced_volume":
        proof["inventory_end"]["ledger_volume"]["uid"] = "new"
    elif change == "bad_clock":
        proof["all_records"][0]["clock_uncertainty_s"] = 0.0
    elif change == "not_restored":
        proof["controller_state"]["restored"] = False
    else:
        rows = []

    checks, closed = check_assurance(proof, rows, [r["command"] for r in proof["submissions"]])

    assert next(c for c in checks if c["check"] == "C12.collection_coverage")["status"] != "pass"
    assert not closed


def test_missing_proof_leaves_execution_only_rules_unknown(proof):
    from conformance import evaluate

    manifest = json.loads((ROOT / "config/architecture/local-control.json").read_text())
    reference = json.loads((ROOT / "config/architecture/conformance-reference.json").read_text())
    commands = [r["command"] for r in proof["submissions"]]
    without = evaluate(
        manifest,
        reference,
        records=proof["records"],
        policies=[],
        collection_errors=[],
        commands=commands,
    )
    with_proof = evaluate(
        manifest,
        reference,
        records=proof["records"],
        policies=[],
        collection_errors=[],
        commands=commands,
        assurance_evidence=proof,
    )

    for rule in ("C07", "C08"):
        assert next(r for r in without["rules"] if r["id"] == rule)["status"] == "unknown"
        assert next(r for r in with_proof["rules"] if r["id"] == rule)["status"] == "pass"
