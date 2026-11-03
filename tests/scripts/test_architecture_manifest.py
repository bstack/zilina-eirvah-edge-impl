from __future__ import annotations

import json
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
MANIFEST = ROOT / "config/architecture/local-control.json"


def load_manifest() -> dict:
    return json.loads(MANIFEST.read_text(encoding="utf-8"))


def test_architecture_references_and_typed_edges_are_complete() -> None:
    m = load_manifest()
    components = {c["id"]: c for c in m["components"]}
    vertices = {v["id"]: v for v in m["vertices"]}
    contracts = {k["id"]: k for k in m["contracts"]}
    assert len(components) == len(m["components"])
    assert len(vertices) == len(m["vertices"])
    assert len(contracts) == len(m["contracts"])
    assert len({e["id"] for e in m["edges"]}) == len(m["edges"])
    assert len({c["id"] for c in m["channels"]}) == len(m["channels"])
    channels = {c["id"] for c in m["channels"]}
    permitted = {tuple(x) for x in m["profile"]["permitted_interactions"]}
    for c in m["components"]:
        assert c["group"] in m["profile"]["groups"] or c["group"] == "infrastructure"
        for source in c["sources"]:
            assert (ROOT / source).is_file(), source
    for k in m["contracts"]:
        assert k["kind"] in m["profile"]["flow_kinds"]
        assert k["binding_status"] in {"partial", "proposed"}
        for source in k["sources"]:
            assert (ROOT / source).is_file(), source
    for v in vertices.values():
        assert v["components"] and set(v["components"]) <= components.keys()
        assert v["roles"] and set(v["roles"]) <= set(m["profile"]["roles"])
    for e in m["edges"]:
        assert e["source"] in vertices and e["target"] in vertices
        assert e["contract"] in contracts
        assert contracts[e["contract"]]["kind"] == e["flow"]
        assert e["channels"] and set(e["channels"]) <= channels
        assert any(
            (a, e["flow"], b) in permitted
            for a in vertices[e["source"]]["roles"]
            for b in vertices[e["target"]]["roles"]
        )
    for c in m["channels"]:
        assert set(c["components"]) <= components.keys()
        for source in c["sources"]:
            assert (ROOT / source).is_file(), source


def reachable(m: dict, start: str, end: str, flow: str, blocked: set[str]) -> bool:
    pending = [start]
    seen = set(blocked)
    while pending:
        current = pending.pop()
        if current in seen:
            continue
        if current == end:
            return True
        seen.add(current)
        pending.extend(
            e["target"] for e in m["edges"] if e["source"] == current and e["flow"] == flow
        )
    return False


def test_required_routes_exist_without_namespace_or_executor_bypass() -> None:
    m = load_manifest()
    for route in m["required_routes"]:
        assert reachable(m, route["source"], route["target"], route["flow"], set())
        for role in route["must_include_roles"]:
            blocked = {v["id"] for v in m["vertices"] if role in v["roles"]}
            assert not reachable(m, route["source"], route["target"], route["flow"], blocked)


def test_all_five_controls_match_deployed_policy_and_native_mapping() -> None:
    m = load_manifest()
    policy_path = ROOT / "deploy/k3s/base/actuation-event-validator/actuation-policy.yaml"
    policies = {p["uns_topic"] for p in yaml.safe_load(policy_path.read_text())["policies"]}
    native_path = ROOT / "deploy/k3s/base/actuation-signal-publisher/native-actuation-targets.yaml"
    targets = {t["uns_topic"]: t for t in yaml.safe_load(native_path.read_text())["targets"]}
    assert {c["id"] for c in m["controls"]} == {
        "temperature",
        "conveyor",
        "filler",
        "capper",
        "reject",
    }
    for control in m["controls"]:
        for topic in control["command_topics"]:
            assert topic in policies
            if control["protocol"] != "opcua":
                assert targets[topic]["protocol"] == control["protocol"]
                assert targets[topic]["host"] == control["physical_component"]


def test_pipeline_subjects_match_manifest_channels() -> None:
    m = load_manifest()
    channels = {c["id"]: c for c in m["channels"]}
    for pipeline in m["pipelines"]:
        config = yaml.safe_load((ROOT / pipeline["source"]).read_text())
        assert [(s["name"], s["subject"]) for s in config["stages"]] == [
            (s["stage"], channels[s["channel"]]["address"]) for s in pipeline["stages"]
        ]
        assert config["dlq_subject"] == pipeline["dlq_subject"]


def test_base_deployments_and_gaps_remain_explicit() -> None:
    m = load_manifest()
    base = ROOT / "deploy/k3s/base"
    configured = yaml.safe_load((base / "kustomization.yaml").read_text())["resources"]
    deployments = {
        p.parent.name for p in base.glob("*/deployment.yaml") if p.parent.name in configured
    }
    assert deployments == {c["deployment"] for c in m["components"] if "deployment" in c}
    assert m["placement"]["status"] == "documented_baseline_not_live_verified"
    assert m["conformance_verdict"] == "not_assessed"
    assert {g["id"] for g in m["gaps"]} >= {
        "approval_binding",
        "writer_deadline",
        "command_recovery",
        "distributed_placement",
        "event_version_binding",
    }


def test_recorded_controller_topics_and_versions_match_current_models() -> None:
    from decision_agent_stub.config import DecisionAgentSettings
    from eirvah_contracts.actuation import ActuationRequest
    from eirvah_contracts.inspection import BottleInspection

    m = load_manifest()
    edges = {e["id"]: e for e in m["edges"]}
    fields = DecisionAgentSettings.model_fields
    assert set(edges["consume-state"]["topic_filter"]) == {
        fields[name].default
        for name in ("subscribe_topic", "rejected_bottle_topic", "reject_count_topic")
    }
    assert edges["inspection-consume"]["topic_filter"] == [fields["inspection_topic"].default]
    contracts = {c["id"]: c for c in m["contracts"]}
    assert (
        contracts["inspection"]["wire_version"]
        == BottleInspection.model_fields["schema_version"].default
    )
    assert (
        contracts["actuation-request"]["wire_version"]
        == ActuationRequest.model_fields["schema_version"].default
    )
    assert contracts["canonical-state"]["wire_version"] is None
    for scenario in m["scenarios"]:
        assert scenario["route"] in {r["id"] for r in m["required_routes"]}
