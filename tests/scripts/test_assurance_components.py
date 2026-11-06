"""Component retention is labelled virtual time; altered or absent proof cannot pass."""


import pytest
from assurance_checks import check_assurance, retention_valid
from assurance_components import capture, retention_capture


def test_production_ledger_boundaries_pass_without_claiming_native_effects(tmp_path):
    data = retention_capture(tmp_path / "retention")

    assert retention_valid(data)
    assert data["mode"] == "isolated_production_ledger_virtual_time"
    assert data["native_effects"] == "not_exercised"


@pytest.mark.parametrize(
    "change",
    [
        "too_short",
        "missing_boundary",
        "early_prune",
        "retained_after_window",
        "lost_identity",
        "unfenced_old_token",
        "blind_started_retry",
    ],
)
def test_retention_oracle_detects_policy_contradictions(tmp_path, change):
    data = retention_capture(tmp_path / "retention")
    if change == "too_short":
        data["initial"][0]["keep_until"] = 1
    elif change == "missing_boundary":
        data["points"][1]["label"] = "different"
    elif change == "early_prune":
        data["points"][0]["result"] = None
    elif change == "retained_after_window":
        data["points"][2]["result"] = data["completion"]
    elif change == "lost_identity":
        data["initial"][0]["digest"] = "different"
    elif change == "unfenced_old_token":
        data["restarts"][0]["old_token_refused"] = False
    else:
        data["restarts"][1]["token"] = "unsafe-new-attempt"

    try:
        verdict = retention_valid(data)
    except (TypeError, KeyError):
        verdict = False

    assert not verdict


@pytest.fixture
def component_package(tmp_path):
    data = capture(tmp_path / "components")
    contract = data["contracts"]
    selected = {
        "uniza/zilina/factory1/line_a/" + p
        for p in (
            "conveyor/belt_01/speed_setpoint",
            "reject_station/reject_counter_01/reject_count",
            "reject_station/rejector_01/last_rejected_bottle",
            "inspector/inspector_01/bottle_inspection",
        )
    }
    hashes = data["source_hashes"]
    return {
        "schema_version": "1.0",
        "profile_sha256": data["profile_sha256"],
        "components": data,
        "live_samples": {
            r["topic"]: r["packet"] for r in contract["observations"] if r["topic"] in selected
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


def test_component_contracts_can_pass_while_missing_live_capture_stays_unknown(component_package):
    checks, closed = check_assurance(component_package, [], [])

    assert next(c for c in checks if c["check"] == "C03.full_semantics")["status"] == "pass"
    assert next(c for c in checks if c["check"] == "C12.collection_coverage")["status"] == "unknown"
    assert closed == set()


@pytest.mark.parametrize(
    "change",
    [
        "accept_future_version",
        "accept_wrong_meaning",
        "wrong_public_unit",
        "receiver_not_deployed",
        "wrong_catalogue",
        "changed_source",
    ],
)
def test_contract_evidence_requires_actual_compatibility_and_receiver_attribution(
    component_package, change
):
    data = component_package
    if change == "accept_future_version":
        next(c for c in data["components"]["contracts"]["cases"] if c["mode"] == "unsupported")[
            "accepted"
        ] = True
    elif change == "accept_wrong_meaning":
        next(
            c for c in data["components"]["contracts"]["public_cases"] if c["mode"] == "wrong_unit"
        )["accepted"] = True
    elif change == "wrong_public_unit":
        next(iter(data["live_samples"].values()))["eirvah:unit"] = "wrong"
    elif change == "receiver_not_deployed":
        data["receiver_snapshot"]["guard_path"] = None
    elif change == "wrong_catalogue":
        data["components"]["contracts"]["catalogue"]["contracts"][0]["version"] = "2.0"
    else:
        data["components"]["source_hashes"][
            "libs/eirvah-contracts/src/eirvah_contracts/sosa.py"
        ] = "changed"

    checks, _ = check_assurance(data, [], [])

    assert next(c for c in checks if c["check"] == "C03.full_semantics")["status"] != "pass"
