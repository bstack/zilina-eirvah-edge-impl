"""Scoped E0 checks: deliberate contradictions fail; missing evidence stays unknown."""

import copy
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def inputs():
    return (
        json.loads((ROOT / "config/architecture/local-control.json").read_text()),
        json.loads((ROOT / "config/architecture/conformance-reference.json").read_text()),
    )


def finding(checks, name):
    return next(c for c in checks if c["check"] == name)


def record(action, *, component="actuation-signal-publisher", tick=20, **changes):
    at = datetime(2026, 11, 4, 12, 0, 0, tzinfo=UTC)
    result = {
        "event": "command_execution",
        "execution_schema": "1.0",
        "record_id": action,
        "component": component,
        "action": action,
        "correlation_id": "01M6MAWHBB5Z3ZJDJXK24ACPFX",
        "process_instance": component + "-process",
        "monotonic_ns": tick * 1_000_000_000,
        "recorded_at": (at + timedelta(seconds=tick)).isoformat(),
        "clock_uncertainty_s": 0.001,
        "requester": "experiment-driver",
        "target": "uniza/zilina/factory1/line_a/capper/motor_01/speed_setpoint",
        "requested_value": 1200,
        "value_type": "int64",
        "requested_at": at.isoformat(),
        "deadline": (at + timedelta(seconds=30)).isoformat(),
        "attempt_id": "attempt-1",
    }
    result.update(changes)
    return result


def trace():
    return [
        record(
            "validation_decided",
            component="actuation-event-validator",
            tick=10,
            decision="approve",
            approved_writer="actuation-signal-publisher",
            approval_issued_at="2026-11-04T12:00:10+00:00",
        ),
        record("executor_admitted", tick=15, approval_issued_at="2026-11-04T12:00:10+00:00"),
        record("write_initiated", tick=20),
        record("device_acknowledged", tick=21),
        record(
            "writer_result",
            tick=22,
            outcome="completed",
            replayed=False,
            written_at="2026-11-04T12:00:21+00:00",
        ),
    ]


def policy():
    import yaml

    return yaml.safe_load(
        (ROOT / "deploy/k3s/base/actuation-event-validator/actuation-policy.yaml").read_text()
    )["policies"]


def test_declared_graph_satisfies_typed_and_required_route_checks():
    # Arrange
    from conformance import check_structure

    manifest, reference = inputs()

    # Act
    checks = check_structure(manifest, reference)

    # Assert
    assert finding(checks, "C01.typed_graph")["status"] == "pass"
    assert finding(checks, "C02.required_routes")["status"] == "pass"


@pytest.mark.parametrize(
    "mutation", ["endpoint", "contract_kind", "role", "duplicate_edge", "self_authorized_profile"]
)
def test_malformed_graph_is_detected_against_pinned_reference(mutation):
    # Arrange
    from conformance import check_structure

    manifest, reference = inputs()
    if mutation == "endpoint":
        manifest["edges"][0]["target"] = "missing"
    elif mutation == "contract_kind":
        manifest["contracts"][0]["kind"] = "command"
    elif mutation == "role":
        manifest["vertices"][0]["roles"] = ["invented"]
    elif mutation == "duplicate_edge":
        manifest["edges"].append(copy.deepcopy(manifest["edges"][0]))
    else:
        manifest["profile"]["permitted_interactions"].append(["decision", "command", "physical"])

    # Act
    checks = check_structure(manifest, reference)

    # Assert
    assert finding(checks, "C01.typed_graph")["status"] == "fail"


@pytest.mark.parametrize("mutation", ["bypass", "missing_route", "deleted_requirement"])
def test_missing_or_bypassed_required_route_cannot_pass(mutation):
    # Arrange
    from conformance import check_structure

    manifest, reference = inputs()
    if mutation == "bypass":
        manifest["edges"].append(
            {
                "id": "bypass",
                "source": "manual-decision",
                "target": "s7-physical",
                "flow": "command",
                "contract": "native-write",
                "channels": ["s7-write"],
            }
        )
    elif mutation == "deleted_requirement":
        manifest["required_routes"] = []
    else:
        manifest["edges"] = [
            e for e in manifest["edges"] if not (e["flow"] == "command" and e["source"] == "writer")
        ]

    # Act
    checks = check_structure(manifest, reference)

    # Assert
    assert finding(checks, "C02.required_routes")["status"] == "fail"


def test_empty_mapping_is_unknown_not_vacuous_success():
    # Arrange
    from conformance import check_structure

    manifest, reference = inputs()
    manifest["vertices"] = []

    # Act
    checks = check_structure(manifest, reference)

    # Assert
    assert finding(checks, "C01.typed_graph")["status"] == "unknown"


def test_bounded_clock_and_matching_admission_support_observed_write():
    # Arrange
    from conformance import check_execution

    _, reference = inputs()

    # Act
    checks = check_execution(trace(), reference, policy())

    # Assert
    assert finding(checks, "C07.admission")["status"] == "pass"
    assert finding(checks, "C08.deadline")["status"] == "pass"
    assert finding(checks, "C09.initiations")["status"] == "pass"


@pytest.mark.parametrize(
    "offset,uncertainty,expected", [(31, 0.001, "fail"), (29, 2, "unknown"), (20, None, "unknown")]
)
def test_deadline_verdict_respects_clock_uncertainty(offset, uncertainty, expected):
    # Arrange
    from conformance import check_execution

    _, reference = inputs()
    records = trace()
    records[2] = record("write_initiated", tick=offset, clock_uncertainty_s=uncertainty)

    # Act
    checks = check_execution(records, reference, policy())

    # Assert
    assert finding(checks, "C08.deadline")["status"] == expected


def test_missing_admission_is_unknown_not_assumed_bypass():
    # Arrange
    from conformance import check_execution

    _, reference = inputs()
    records = [r for r in trace() if r["action"] != "executor_admitted"]

    # Act
    checks = check_execution(records, reference, policy())

    # Assert
    assert finding(checks, "C07.admission")["status"] == "unknown"


def test_duplicate_initiations_and_changed_admitted_payload_are_counterexamples():
    # Arrange
    from conformance import check_execution

    _, reference = inputs()
    records = [
        *trace(),
        record(
            "executor_admitted",
            tick=23,
            record_id="second-admit",
            attempt_id="second",
            requested_value=1300,
        ),
        record(
            "write_initiated",
            tick=24,
            record_id="second-write",
            attempt_id="second",
            requested_value=1300,
        ),
    ]

    # Act
    checks = check_execution(records, reference, policy())

    # Assert
    assert finding(checks, "C09.initiations")["status"] == "fail"
    assert finding(checks, "C09.binding")["status"] == "fail"


def test_identical_collected_record_does_not_count_as_second_execution():
    # Arrange
    from conformance import check_execution

    _, reference = inputs()
    records = trace()
    records.append({**records[2], "source": {"previous": True}})

    # Act
    checks = check_execution(records, reference, policy())

    # Assert
    assert finding(checks, "C09.initiations")["status"] == "pass"


def test_retention_expiry_does_not_automatically_violate_duplicate_policy():
    # Arrange
    from conformance import check_execution

    _, reference = inputs()
    records = [
        *trace(),
        record("write_initiated", tick=90000, record_id="later", attempt_id="later"),
    ]

    # Act
    checks = check_execution(records, reference, policy())

    # Assert
    assert finding(checks, "C09.initiations")["status"] != "fail"


def test_write_timeout_reported_as_refusal_is_a_visible_outcome_violation():
    # Arrange
    from conformance import check_execution

    _, reference = inputs()
    records = [
        *trace(),
        record(
            "outcome_published",
            component="actuation-control-orchestrator",
            tick=25,
            outcome="reject",
            reason="write_timeout",
        ),
    ]

    # Act
    checks = check_execution(records, reference, policy())

    # Assert
    assert finding(checks, "C10.outcome_meaning")["status"] == "fail"


def test_unknown_outcome_is_not_itself_a_conformance_failure():
    # Arrange
    from conformance import check_execution

    _, reference = inputs()
    records = [
        record("writer_result", outcome="unknown"),
        record("outcome_published", component="actuation-control-orchestrator", outcome="unknown"),
    ]

    # Act
    checks = check_execution(records, reference, policy())

    # Assert
    assert finding(checks, "C10.outcome_meaning")["status"] == "pass"


def test_no_trace_and_unexecuted_study_areas_are_explicit_in_report():
    # Arrange
    from conformance import evaluate

    manifest, reference = inputs()

    # Act
    report = evaluate(
        manifest, reference, records=[], policies=policy(), collection_errors=["missing trace"]
    )

    # Assert
    assert report["overall"] == "unknown"
    assert {r["id"] for r in report["rules"]} == {f"C{i:02}" for i in range(1, 14)}
    assert next(r for r in report["rules"] if r["id"] == "C04")["execution"] == "not_run"
    assert report["universal_conformance"] == "not_established"


def test_missing_placement_for_component_is_detected():
    # Arrange
    from conformance import check_structure

    manifest, reference = inputs()
    manifest["placement"]["component_hosts"].pop("actuation-signal-publisher")

    # Act
    checks = check_structure(manifest, reference)

    # Assert
    assert finding(checks, "C13.declared_placement")["status"] == "fail"


def test_missing_approval_identity_cannot_satisfy_bound_admission():
    # Arrange
    from conformance import check_execution

    _, reference = inputs()
    records = trace()
    records[0].pop("approval_issued_at")
    records[1].pop("approval_issued_at")

    # Act
    checks = check_execution(records, reference, policy())

    # Assert
    assert finding(checks, "C07.admission")["status"] == "unknown"


def test_dropped_initiation_is_named_as_a_coverage_gap():
    # Arrange
    from conformance import check_execution

    _, reference = inputs()
    records = [r for r in trace() if r["action"] != "write_initiated"]

    # Act
    checks = check_execution(records, reference, policy())

    # Assert
    gap = finding(checks, "C12.stage_coverage")
    assert gap["status"] == "unknown"
    assert "write_initiated" in gap["evidence"][1]["missing_actions"]


def test_conflicting_copies_of_the_same_record_are_not_silently_deduplicated():
    # Arrange
    from conformance import check_execution

    _, reference = inputs()
    records = trace()
    records.append({**records[2], "requested_value": 1300})

    # Act
    checks = check_execution(records, reference, policy())

    # Assert
    assert finding(checks, "C12.record_integrity")["status"] == "fail"


def test_rejected_conflicting_payload_is_not_an_admitted_identity_violation():
    # Arrange
    from conformance import check_execution

    _, reference = inputs()
    records = [
        *trace(),
        record("writer_result", record_id="declined", outcome="conflict", requested_value=1300),
    ]

    # Act
    checks = check_execution(records, reference, policy())

    # Assert
    assert finding(checks, "C09.binding")["status"] == "pass"


def test_missing_trace_files_remain_unknown_without_crashing(tmp_path):
    # Arrange
    from conformance import evaluate, read_run

    manifest, reference = inputs()
    command_id = "01M6MAWHBB5Z3ZJDJXK24ACPFX"
    (tmp_path / "summary.json").write_text(json.dumps({"outcome": "passed"}))
    (tmp_path / "journal.jsonl").write_text(
        json.dumps({"event": "command_submitting", "command": {"correlation_id": command_id}})
        + "\n"
    )

    # Act
    records, commands, errors, files = read_run(tmp_path)
    report = evaluate(
        manifest,
        reference,
        records=records,
        commands=commands,
        policies=policy(),
        collection_errors=errors,
    )

    # Assert
    assert len(files) == 2
    assert any("missing records.jsonl" in e for e in errors)
    assert report["overall"] == "unknown"


@pytest.mark.parametrize(
    "change", [{"requested_value": 2000}, {"requested_value": True}, {"requester": "untrusted"}]
)
def test_observed_invalid_write_fails_pinned_target_policy(change):
    # Arrange
    from conformance import check_execution

    _, reference = inputs()
    records = trace()
    records[2].update(change)

    # Act
    checks = check_execution(records, reference, policy())

    # Assert
    assert finding(checks, "C07.write_policy")["status"] == "fail"


@pytest.mark.parametrize("value", [None, -1, True, float("inf")])
def test_invalid_clock_bounds_do_not_become_zero(value):
    # Arrange
    from conformance import uncertainty

    # Act
    bound = uncertainty({"clock_uncertainty_s": value})

    # Assert
    assert bound is None


def test_malformed_journal_command_is_a_collection_gap(tmp_path):
    # Arrange
    from conformance import read_run

    (tmp_path / "summary.json").write_text("[]")
    (tmp_path / "journal.jsonl").write_text(
        json.dumps({"event": "command_submitting", "command": "bad"}) + "\n"
    )

    # Act
    records, commands, errors, _ = read_run(tmp_path)

    # Assert
    assert records == [] and commands == []
    assert "invalid summary.json" in errors and "invalid journal.jsonl" in errors


def test_missing_policy_is_unknown_instead_of_a_claimed_authority_violation():
    # Arrange
    from conformance import check_execution

    _, reference = inputs()

    # Act
    checks = check_execution(trace(), reference, [])

    # Assert
    assert finding(checks, "C07.write_policy")["status"] == "unknown"


def test_a_changed_contract_version_is_detected_for_selected_commands():
    # Arrange
    from conformance import check_bindings

    manifest, reference = inputs()
    next(c for c in manifest["contracts"] if c["id"] == "actuation-request")["wire_version"] = (
        "99.0"
    )

    # Act
    checks = check_bindings(manifest, reference, policy())

    # Assert
    assert finding(checks, "C03.selected_commands")["status"] == "fail"


@pytest.mark.parametrize(
    "observed_bottle,observed_count,expected", [(20, 5, "pass"), (19, 5, "fail"), (20, 4, "fail")]
)
def test_process_observation_requires_matching_bottle_and_increased_count(
    observed_bottle, observed_count, expected
):
    # Arrange
    from conformance import check_execution

    _, reference = inputs()
    records = [
        record(
            "process_observed",
            component="decision-agent-stub",
            target="uniza/zilina/factory1/line_a/reject_station/rejector_01/reject_bottle",
            requested_value=20,
            observed_bottle=observed_bottle,
            observed_count=observed_count,
            baseline_count=4,
            inspection_id="0123456789abcdef:0001",
            evidence_scope="simulator_feedback",
            bottle_observed_at="2026-11-04T12:00:10Z",
            count_observed_at="2026-11-04T12:00:10Z",
        )
    ]

    # Act
    checks = check_execution(records, reference, policy())

    # Assert
    assert finding(checks, "C06.feedback")["status"] == expected


@pytest.mark.parametrize("changed_payload,expected", [(False, "fail"), (True, "pass")])
def test_later_refusal_only_applies_to_same_immutable_command(changed_payload, expected):
    # Arrange
    from conformance import check_execution

    _, reference = inputs()
    records = trace()
    records.append(
        record(
            "validation_decided",
            record_id="later-refusal",
            component="actuation-event-validator",
            tick=17,
            decision="reject",
            requested_value=1300 if changed_payload else 1200,
        )
    )

    # Act
    checks = check_execution(records, reference, policy())

    # Assert
    assert finding(checks, "C07.admission")["status"] == expected


def test_valid_reported_outcomes_do_not_claim_fault_reconciliation_was_tested():
    # Arrange
    from conformance import evaluate

    manifest, reference = inputs()

    # Act
    report = evaluate(manifest, reference, records=trace(), policies=policy(), collection_errors=[])

    # Assert
    outcomes = next(r for r in report["rules"] if r["id"] == "C10")
    assert outcomes["status"] == "unknown"
    assert any(
        c["check"] == "C10.outcome_meaning" and c["status"] == "pass" for c in outcomes["checks"]
    )
    assert any(
        c["check"] == "C10.experiment" and c["execution"] == "not_run" for c in outcomes["checks"]
    )


@pytest.mark.parametrize("value_type,expected", [("double", True), ("int64", False)])
def test_legacy_warmer_policy_uses_declared_default_type(value_type, expected):
    # Arrange
    from conformance import valid_policy

    sample = record(
        "write_initiated",
        target="uniza/zilina/factory1/line_a/bottler/setpoint_unit/setpoint_temperature",
        value_type=value_type,
        requested_value=25.0,
    )

    # Act
    accepted = valid_policy(sample, policy())

    # Assert
    assert accepted is expected
