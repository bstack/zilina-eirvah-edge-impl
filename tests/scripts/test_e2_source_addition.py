"""Add a gateway alias without changing the logical signal or consumer."""

import json

import pytest
from e2_source_addition import prepare_addition, validate_bundle
from interoperability import PROFILE, load_profile


def test_gateway_task_preserves_public_contract_and_existing_alias(tmp_path):
    bundle = prepare_addition(tmp_path, load_profile(PROFILE), "opcua")

    assert validate_bundle(bundle)
    assert bundle["consumer_before"] == bundle["consumer_after"]
    assert bundle["baseline_before"] != bundle["baseline_after"]
    assert bundle["task"]["new_alias"] == "E2Gateway2.opcua.ConveyorSpeed"
    assert bundle["human_effort"] == "not_measured"


@pytest.mark.parametrize("change", ["consumer", "drop_old_alias", "wrong_unit", "baseline_missing"])
def test_bad_source_mapping_task_is_detected(tmp_path, change):
    bundle = prepare_addition(tmp_path, load_profile(PROFILE), "s7")
    if change == "consumer":
        bundle["consumer_after"] = "changed"
    elif change == "drop_old_alias":
        document = json.loads(bundle["after_ontology"])
        node = next(n for n in document["@graph"] if isinstance(n.get("eirvah:nodeId"), list))
        node["eirvah:nodeId"] = bundle["task"]["new_alias"]
        bundle["after_ontology"] = json.dumps(document)
    elif change == "wrong_unit":
        bundle["added_rule"]["unit"] = "rpm"
    else:
        bundle["baseline_after"].pop(bundle["task"]["new_alias"])

    assert not validate_bundle(bundle)
