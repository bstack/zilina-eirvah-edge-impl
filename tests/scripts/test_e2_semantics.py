"""E2 quality and error checks report their actual policy boundary."""

import copy

import pytest
from e2_semantics import capture_quality, evaluate_quality


@pytest.fixture
def evidence():
    return capture_quality()


def test_selected_quality_and_error_matrix_passes_without_inventing_native_quality(evidence):
    report = evaluate_quality(evidence)

    assert report["quality_case"] == "pass"
    assert len(evidence["rows"]) == 33
    assert evidence["capabilities"]["modbus"]["source_quality"] == "not_carried"
    assert evidence["capabilities"]["s7"]["source_quality"] == "not_carried"
    assert report["mode"] == "in_process_policy_checks"


@pytest.mark.parametrize(
    "change", ["missing", "command_on_bad", "wrong_quality", "changed_sources", "invented_quality"]
)
def test_bad_or_incomplete_policy_evidence_cannot_pass(evidence, change):
    data = copy.deepcopy(evidence)
    if change == "missing":
        data["rows"].pop()
    elif change == "command_on_bad":
        next(r for r in data["rows"] if r["case"] == "opcua/bad")["command"] = {
            "requested_value": 26
        }
    elif change == "wrong_quality":
        next(r for r in data["rows"] if r["case"] == "opcua/bad")["raw_quality"] = "good"
    elif change == "invented_quality":
        data["capabilities"]["modbus"]["source_quality"] = "status_code"
    else:
        data["source_hashes"] = {}

    assert evaluate_quality(data)["quality_case"] != "pass"


def test_stale_and_future_data_are_refused_for_every_selected_representation(evidence):
    rows = [r for r in evidence["rows"] if r["case"].endswith(("/stale", "/future"))]

    assert len(rows) == 6
    assert all(r["command"] is None for r in rows)
    assert {r["state_reason"] for r in rows} == {"stale", "future"}
