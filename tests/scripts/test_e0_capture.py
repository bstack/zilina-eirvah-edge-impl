"""Measurement/capture checks must reject gaps rather than manufacture certainty."""

from copy import deepcopy


def clock(mono, wall):
    return {
        "monotonic_ns": mono,
        "monotonic_before_ns": mono - 1,
        "monotonic_after_ns": mono + 1,
        "wall_time_ns": wall,
        "clock_domain": "domain",
        "timestamp_resolution_ns": 1,
    }


def probe(seq, rows, mono=100):
    return {
        "client_before": clock(mono - 10, mono + 990),
        "client_after": clock(mono + 10, mono + 1010),
        "remote": {
            "schema_version": "1.0",
            "process_instance": "proc",
            "record_seq": seq,
            "overflow": False,
            "clock": clock(mono - 50, mono + 500),
            "records": rows,
        },
    }


def test_sequence_fences_require_every_committed_record_and_same_process():
    from e0_capture import validate_fence

    start = probe(3, [])
    end = probe(
        5,
        [
            {"record_seq": 4, "process_instance": "proc"},
            {"record_seq": 5, "process_instance": "proc"},
        ],
    )

    assert validate_fence(start, end) == []
    missing = deepcopy(end)
    missing["remote"]["records"].pop()
    assert validate_fence(start, missing)
    restart = deepcopy(end)
    restart["remote"]["process_instance"] = "new"
    assert validate_fence(start, restart)


def test_overflow_is_an_unknown_capture_even_when_rows_look_contiguous():
    from e0_capture import validate_fence

    start = probe(0, [])
    end = probe(1, [{"record_seq": 1, "process_instance": "proc"}])
    end["remote"]["overflow"] = True

    assert "journal capacity exceeded" in validate_fence(start, end)


def test_calibration_retains_measured_round_trip_bounds():
    from e0_capture import calibrate_record

    start = probe(0, [], mono=100)
    end = probe(1, [], mono=200)
    row = {
        "recorded_at": "old",
        "monotonic_ns": 100,
        "monotonic_before_ns": 99,
        "monotonic_after_ns": 101,
        "clock_domain": "domain",
        "clock_uncertainty_s": None,
    }

    measured = calibrate_record(row, start, end)

    assert measured["raw_recorded_at"] == "old"
    assert measured["clock_uncertainty_s"] > 0
    assert measured["calibration_basis"] == "bracketing_causal_envelope"
    assert row["clock_uncertainty_s"] is None


def test_calibration_refuses_extrapolation_or_changed_clock_domain():
    import pytest
    from e0_capture import calibrate_record

    start = probe(0, [], mono=100)
    end = probe(1, [], mono=200)
    row = {
        "recorded_at": "old",
        "monotonic_ns": 1000,
        "monotonic_before_ns": 999,
        "monotonic_after_ns": 1001,
        "clock_domain": "domain",
    }

    with pytest.raises(ValueError):
        calibrate_record(row, start, end)
    row.update(
        monotonic_ns=100, monotonic_before_ns=99, monotonic_after_ns=101, clock_domain="other"
    )
    with pytest.raises(ValueError):
        calibrate_record(row, start, end)


def test_shared_clock_domain_proves_cross_component_order_without_fake_utc_bound():
    from conformance import before

    first = {
        "component": "validator",
        "process_instance": "a",
        "clock_domain": "shared",
        "monotonic_ns": 10,
        "monotonic_after_ns": 11,
        "monotonic_before_ns": 9,
        "recorded_at": "2026-11-04T10:00:00Z",
        "clock_uncertainty_s": None,
    }
    second = {
        **first,
        "component": "writer",
        "process_instance": "b",
        "monotonic_ns": 20,
        "monotonic_before_ns": 19,
        "monotonic_after_ns": 21,
    }

    assert before(first, second) is True
    second["clock_domain"] = "other"
    assert before(first, second) is None


def test_verified_ticket_causality_does_not_require_shared_cloud_edge_clock():
    from conformance import check_execution

    from tests.scripts.test_conformance import finding, inputs, policy, trace

    _, reference = inputs()
    records = trace()
    for r in records:
        r.update(clock_domain=r["component"], clock_uncertainty_s=None)
        if r["action"] in ("validation_decided", "executor_admitted", "write_initiated"):
            r.update(
                approval_fingerprint="f" * 64,
                command_digest="d" * 64,
                approval_verified=True,
                ingress_verified=True,
            )

    checks = check_execution(records, reference, policy())

    assert finding(checks, "C07.admission")["status"] == "pass"


def test_ticket_fingerprint_survives_wire_round_trip_and_changes_with_ticket():
    from control import build_command
    from eirvah_contracts.actuation import SignedEvidence
    from eirvah_contracts.authorization import evidence_fingerprint, sign_approval

    ticket = sign_approval(build_command("capper", "speed", 1200), "x" * 32, "writer")

    round_trip = SignedEvidence.model_validate_json(ticket.model_dump_json())
    changed = ticket.model_copy(update={"writer": "other"})

    assert evidence_fingerprint(ticket) == evidence_fingerprint(round_trip)
    assert evidence_fingerprint(ticket) != evidence_fingerprint(changed)


def test_targets_route_cloud_admission_and_edge_writer_without_provider_specific_logic():
    from e0_capture import Cluster

    cluster = Cluster(
        "default",
        "eirvah",
        {
            "actuation-event-validator": {"context": "public", "namespace": "cloud"},
            "actuation-signal-publisher": {"context": "private", "namespace": "edge"},
            "rabbitmq": {
                "context": "public",
                "namespace": "cloud",
                "service": "commands",
                "port": 5672,
            },
        },
    )

    validator = cluster.target("actuation-event-validator")
    writer = cluster.target("actuation-signal-publisher")
    transport = cluster.transport("rabbitmq", 5672)

    assert validator.prefix == ["kubectl", "--context", "public", "-n", "cloud"]
    assert writer.prefix == ["kubectl", "--context", "private", "-n", "edge"]
    assert cluster.target("s7-simulator").context == "default"
    assert transport == (["kubectl", "--context", "public", "-n", "cloud"], "commands", 5672)


def test_native_transport_and_ledger_use_their_selected_cluster(monkeypatch):
    from e0_capture import Cluster

    calls = []

    def read(self, *args, **kwargs):
        calls.append((self.context, self.namespace, args))
        return "[]" if "-i" in args else '{"feedback": []}'

    monkeypatch.setattr(Cluster, "run", read)
    cluster = Cluster(
        "default",
        "base",
        {
            "s7-simulator": {"context": "edge", "namespace": "plant"},
            "actuation-signal-publisher": {"context": "writer", "namespace": "controls"},
        },
    )

    native = cluster.transport("s7-simulator", 102)
    cluster.ledger(["command"])

    assert native == (["kubectl", "--context", "edge", "-n", "plant"], "s7-simulator", 102)
    assert calls[0][:2] == ("writer", "controls")


def test_custom_manifest_is_snapshotted_with_its_own_provenance(tmp_path):
    import hashlib
    import json

    from e0_capture import snapshot_manifest

    source = tmp_path / "split.json"
    source.write_text(json.dumps(dict(format_version="1.0", id="custom-split")))
    output = tmp_path / "capture"
    output.mkdir()

    selected = snapshot_manifest(source, output)

    assert selected == output / "architecture-manifest.json"
    assert json.loads(selected.read_text())["id"] == "custom-split"
    provenance = json.loads((output / "manifest-source.json").read_text())
    assert provenance["sha256"] == hashlib.sha256(source.read_bytes()).hexdigest()
