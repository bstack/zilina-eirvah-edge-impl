"""Golden E2 preparation cannot be promoted into a native-transport result."""

import copy

import pytest
from interoperability import PROFILE, evaluate, load_profile, precision_probe, run_case


@pytest.fixture
def evidence():
    return run_case(load_profile(PROFILE))


def test_three_protocols_and_both_variants_match_independent_golden_cases(evidence):
    report = evaluate(evidence)

    assert report["preparation"] == "pass"
    assert report["C04"] == "unknown"
    assert len(evidence["rows"]) == 102
    assert {r["variant"] for r in evidence["rows"]} == {"canonical", "source_specific"}
    assert {r["protocol"] for r in evidence["rows"]} == {"opcua", "modbus", "s7"}
    assert evidence["native_transport"] == "not_exercised"
    assert evidence["physical_effects"] == "not_exercised"


@pytest.mark.parametrize("field", ["unit", "sensor", "property", "topic", "value_type", "value"])
def test_semantic_oracle_detects_wrong_output(evidence, field):
    row = evidence["rows"][0]
    row["meaning"][field] = 999 if field == "value" else "wrong"

    assert evaluate(evidence)["preparation"] == "fail"


def test_wrong_controller_output_is_detected(evidence):
    evidence["rows"][0]["command"]["requested_value"] = 29.0

    assert evaluate(evidence)["preparation"] == "fail"


@pytest.mark.parametrize("change", ["missing", "duplicate", "unattributed", "wrong_mode"])
def test_incomplete_or_unattributed_capture_cannot_pass(evidence, change):
    if change == "missing":
        evidence["rows"].pop()
    elif change == "duplicate":
        evidence["rows"][-1] = copy.deepcopy(evidence["rows"][0])
    elif change == "unattributed":
        evidence["source_hashes"] = {}
    else:
        evidence["native_transport"] = "passed"

    assert evaluate(evidence)["preparation"] != "pass"


def test_native_encoding_error_is_detected_without_redefining_golden_expectation():
    profile = load_profile(PROFILE)
    profile["samples"][0]["modbus"] = 70
    evidence = run_case(profile)

    assert evaluate(evidence)["preparation"] != "pass"


def test_float32_tolerance_does_not_hide_changed_units(evidence):
    row = next(r for r in evidence["rows"] if r["protocol"] == "s7" and r["sample"] == "speed-high")
    assert row["source_value"] != 0.7
    assert row["meaning"]["value"] == 0.7
    assert abs(row["meaning"]["value"] - 0.7) < 1e-6
    assert evaluate(evidence)["preparation"] == "pass"


def test_compare_hysteresis_and_same_mode_without_extra_commands(evidence):
    rows = [r for r in evidence["rows"] if r["variant"] == "canonical" and r["protocol"] == "opcua"]
    commands = {r["sample"]: r["command"] for r in rows if "command" in r}

    assert commands["speed-hysteresis"] is None
    assert commands["speed-high-repeat"] is None
    assert commands["speed-return"]["requested_value"] == 22.0


def test_upper_bound_probe_resolves_difference_and_retains_unrounded_counterexample():
    probe = precision_probe(load_profile(PROFILE))

    assert probe["status"] == "pass"
    assert probe["policy_decision"] == "declared_0.01_m_per_s"
    assert all(c["status"] == "pass" for c in probe["cases"] if c["protocol"] != "s7")
    assert all(c["normalized_value"] == 0.8 for c in probe["cases"])
    assert all(c["unrounded_command"] is None for c in probe["cases"] if c["protocol"] == "s7")
    assert all(c["decoded_value"] > 0.8 for c in probe["cases"] if c["protocol"] == "s7")


def test_actual_wire_payload_corruption_is_not_hidden_by_summary(evidence):
    evidence["rows"][0]["public_payload"]["eirvah:unit"] = "km/h"

    assert evaluate(evidence)["preparation"] == "fail"


def test_all_protocols_reject_over_range_before_rounding(evidence):
    rows = [r for r in evidence["rows"] if r["sample"] == "speed-over-range"]

    assert len(rows) == 6
    assert all(r.get("error") == "ValueError" for r in rows)
    assert all("command" not in r for r in rows)
