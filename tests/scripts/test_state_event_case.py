from datetime import UTC, datetime, timedelta

from decision_agent_stub.config import DecisionAgentSettings
from decision_agent_stub.inspection import InspectionRule
from eirvah_contracts.inspection import BottleInspection


def event(sequence, at, bottle=10, session="aaaaaaaaaaaaaaaa"):
    return BottleInspection(
        inspection_id=f"{session}:{sequence:04x}", bottle_id=bottle, passed=False, inspected_at=at
    )


def test_inspection_identity_mutation_is_a_refusal_with_explicit_diagnostic():
    now = datetime.now(UTC)
    rule = InspectionRule(DecisionAgentSettings())
    original = rule.update(event(1, now), now)

    changed = rule.update(event(1, now, bottle=11), now)

    assert original is not None
    assert changed is None
    assert rule.last_decision["reason"] == "identity_conflict"


def test_missing_snapshots_and_sequence_wrap_are_not_silent_complete_coverage():
    now = datetime.now(UTC)
    rule = InspectionRule(DecisionAgentSettings())
    rule.update(event(1, now), now)
    later = now + timedelta(milliseconds=100)

    command = rule.update(event(3, later, bottle=11), later)
    gap = dict(rule.last_decision)
    regressed_at = later + timedelta(milliseconds=100)
    wrapped = rule.update(event(2, regressed_at, bottle=12), regressed_at)

    assert command is not None and gap["missing_sequences"] == 1
    assert wrapped is None and rule.last_decision["reason"] == "sequence_regression"


def test_missing_or_empty_data_evidence_never_passes():
    from data_conformance import check_data_evidence

    checks = check_data_evidence({})

    assert checks and all(c["status"] == "unknown" for c in checks)


def test_production_replay_is_pinned_and_counterexamples_are_detected(tmp_path):
    from copy import deepcopy

    from data_conformance import check_data_evidence
    from state_event_case import capture

    evidence = capture(tmp_path / "case")
    checks = check_data_evidence(evidence)
    changed = deepcopy(evidence)
    case = next(c for c in changed["cases"] if c["id"] == "stale-state")
    case["records"][1]["decision"]["accepted"] = True
    altered_checks = check_data_evidence(changed)

    assert all(c["status"] == "pass" for c in checks if c["execution"] == "executed")
    assert any(c["status"] == "fail" for c in altered_checks)
    assert any(c["status"] == "unknown" for c in checks if c["execution"] == "not_run")


def test_omitted_restart_case_and_altered_input_are_not_accepted(tmp_path):
    from copy import deepcopy

    from data_conformance import check_data_evidence
    from state_event_case import capture

    evidence = capture(tmp_path / "case")
    missing = deepcopy(evidence)
    missing["cases"] = [c for c in missing["cases"] if c["id"] != "inspection-consumer-restart"]
    modified = deepcopy(evidence)
    modified["cases"][0]["records"][0]["input"]["value"] = 0.8

    assert any(
        c["status"] == "unknown" and "inspection-consumer-restart" in c["check"]
        for c in check_data_evidence(missing)
    )
    assert any(c["status"] == "fail" for c in check_data_evidence(modified))


def test_missing_observation_is_unknown_and_duplicate_case_is_a_counterexample(tmp_path):
    from copy import deepcopy

    from data_conformance import check_data_evidence
    from state_event_case import capture

    evidence = capture(tmp_path / "case")
    missing = deepcopy(evidence)
    del missing["cases"][0]["records"][0]["decision"]
    duplicate = deepcopy(evidence)
    duplicate["cases"].append(deepcopy(duplicate["cases"][0]))

    assert (
        next(c for c in check_data_evidence(missing) if c["check"] == "C05.ordered-speed")["status"]
        == "unknown"
    )
    assert any(c["status"] == "fail" for c in check_data_evidence(duplicate))


def test_whole_study_consumes_data_cases_without_claiming_physical_effects(tmp_path):
    from conformance import evaluate
    from state_event_case import capture

    from tests.scripts.test_conformance import inputs, policy

    manifest, reference = inputs()
    evidence = capture(tmp_path / "case")

    report = evaluate(
        manifest,
        reference,
        records=[],
        policies=policy(),
        collection_errors=[],
        data_evidence=evidence,
    )

    state = next(r for r in report["rules"] if r["id"] == "C05")
    events = next(r for r in report["rules"] if r["id"] == "C06")
    assert any(c["check"] == "C05.ordered-speed" and c["status"] == "pass" for c in state["checks"])
    assert any(
        c["check"] == "C06.business_effects" and c["status"] == "unknown" for c in events["checks"]
    )
    assert report["overall"] == "unknown"


def test_observed_bottle_refusal_does_not_become_a_false_snapshot_gap():
    now = datetime.now(UTC)
    rule = InspectionRule(DecisionAgentSettings())
    rule.update(event(1, now), now)
    rule.update(event(2, now + timedelta(milliseconds=100)), now + timedelta(milliseconds=100))

    accepted = rule.update(
        event(3, now + timedelta(milliseconds=200), bottle=11), now + timedelta(milliseconds=200)
    )

    assert accepted is not None
    assert rule.last_decision["missing_sequences"] == 0


def test_event_command_cannot_be_rebound_to_another_namespace(tmp_path):
    from copy import deepcopy

    from data_conformance import check_data_evidence
    from state_event_case import capture

    evidence = capture(tmp_path / "case")
    changed = deepcopy(evidence)
    case = next(c for c in changed["cases"] if c["id"] == "inspection-duplicate")
    case["records"][0]["command"]["target_uns_topic"] = case["records"][0]["command"][
        "target_uns_topic"
    ].replace("uniza/", "other/")

    assert any(c["status"] == "fail" for c in check_data_evidence(changed))


def test_missing_code_fingerprints_cannot_claim_attributable_evidence(tmp_path):
    from data_conformance import check_data_evidence
    from state_event_case import capture

    evidence = capture(tmp_path / "case")
    evidence["source_sha256"] = {"fake.py": "bad"}

    assert all(c["status"] == "unknown" for c in check_data_evidence(evidence))


def test_missing_initial_state_decision_does_not_make_later_freshness_a_false_failure(tmp_path):
    from data_conformance import check_data_evidence
    from state_event_case import capture

    evidence = capture(tmp_path / "case")
    case = next(c for c in evidence["cases"] if c["id"] == "missing-speed")
    del case["records"][0]["decision"]

    check = next(c for c in check_data_evidence(evidence) if c["check"] == "C05.missing-speed")

    assert check["status"] == "unknown"


def test_fresh_unique_late_inspection_is_not_discarded_like_old_state():
    now = datetime.now(UTC)
    rule = InspectionRule(DecisionAgentSettings())
    rule.update(event(3, now, bottle=12), now)

    late = rule.update(event(2, now - timedelta(milliseconds=100), bottle=11), now)
    repeated = rule.update(event(2, now - timedelta(milliseconds=100), bottle=11), now)

    assert late is not None and late.requested_value == 11
    assert repeated is None
