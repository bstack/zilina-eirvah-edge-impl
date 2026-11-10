"""Native E2 evidence must reconcile real ingress, public payloads and independent reads."""

import copy

import pytest
from e2_native import SOURCES, evaluate, selected_cases
from eirvah_contracts.ulid import generate_correlation_id
from interoperability import PROFILE, digest, load_profile, run_case


def test_native_selection_covers_equivalent_states_and_range_refusal():
    cases = selected_cases(load_profile(PROFILE))

    assert len(cases) == 4
    assert [c["speed"]["expected"] for c in cases] == [0.4, 0.7, 0.8, 0.81]
    assert cases[-1]["speed"]["expected_error"] == "ValueError"


def test_absent_native_evidence_stays_unknown():
    report = evaluate({})

    assert report["native_case"] == "unknown"
    assert report["C04"] == "unknown"


@pytest.mark.parametrize(
    "change",
    [
        "missing",
        "wrong_value",
        "wrong_unit",
        "wrong_correlation",
        "changed_fixture",
        "not_cleaned",
        "wrong_producer",
    ],
)
def test_mutated_transport_evidence_cannot_pass(native_evidence, change):
    data = copy.deepcopy(native_evidence)
    if change == "missing":
        data["rows"].pop()
    elif change == "wrong_value":
        data["rows"][0]["packet"]["sosa:hasSimpleResult"] = 999
    elif change == "wrong_unit":
        data["rows"][0]["packet"]["eirvah:unit"] = "km/h"
    elif change == "wrong_correlation":
        data["rows"][0]["packet"]["eirvah:correlationId"] = "0" * 26
    elif change == "changed_fixture":
        data["source_hashes"] = {}
    elif change == "wrong_producer":
        data["rows"][0]["raw"]["payload"]["position"]["producer"] = "wrong"
    else:
        data["cleanup"]["confirmed"] = False

    assert evaluate(data)["native_case"] != "pass"


@pytest.fixture
def native_evidence(tmp_path):
    from eirvah_controls.recovery import ProducerPositions

    # Synthetic oracle fixture only; no test here claims a real native run occurred.
    profile = load_profile(PROFILE)
    cases = selected_cases(profile)
    replay_profile = copy.deepcopy(profile)
    replay_profile["samples"] = [sample for case in cases for sample in case.values()]
    replay = run_case(replay_profile)
    count = len(replay_profile["samples"])
    rows = []
    raw_records = []
    mqtt_records = []
    dlq_records = []
    for pindex, protocol in enumerate(profile["protocols"]):
        canonical = replay["rows"][pindex * count * 2 : pindex * count * 2 + count]
        baseline = replay["rows"][pindex * count * 2 + count : (pindex + 1) * count * 2]
        for index, (c, b) in enumerate(zip(canonical, baseline, strict=True)):
            case = index // 3
            signal = list(cases[case])[index % 3]
            sample = cases[case][signal]
            raw = c["raw"]
            store = ProducerPositions(
                tmp_path / (protocol + str(index) + ".sqlite3"), "e2-" + protocol
            )
            store.begin_epoch()
            position = store.next(raw["node_id"])
            store.close()
            raw["position"] = position
            raw["source_endpoint"] = {
                "opcua": "opc.tcp://127.0.0.1:14840/e2/",
                "modbus": "modbus-tcp://127.0.0.1:15020",
                "s7": "s7://127.0.0.1:11102",
            }[protocol]
            cid = c.get("public_payload", {}).get("eirvah:correlationId", generate_correlation_id())
            env = dict(correlation_id=cid, payload=raw)
            row = dict(
                protocol=protocol,
                case=case,
                signal=signal,
                sample=sample,
                independent_native=sample[protocol],
                raw=env,
                baseline=b.get("meaning"),
                baseline_error=b.get("error"),
                command=c.get("command"),
                baseline_command=b.get("command"),
            )
            raw_records.append(env)
            if sample.get("expected_error"):
                dlq = dict(
                    correlation_id=cid,
                    payload=raw,
                    error=dict(message="stage=convert reason=ValueError"),
                )
                row["dlq"] = dlq
                dlq_records.append(dlq)
            else:
                packet = c["public_payload"]
                packet["eirvah:position"] = position
                row.update(topic=c["meaning"]["topic"], packet=packet)
                mqtt_records.append(dict(topic=row["topic"], packet=packet))
            rows.append(row)
    return dict(
        schema_version="1.0",
        mode="isolated_native_loopback",
        endpoints=dict(native={"4840": 14840, "5020": 15020, "1102": 11102}),
        rows=rows,
        errors=[],
        cleanup=dict(confirmed=True),
        source_hashes={p.relative_to(PROFILE.parents[2]).as_posix(): digest(p) for p in SOURCES},
        transport_records=dict(raw=raw_records, mqtt=mqtt_records, dlq=dlq_records),
        containers=[
            dict(name=name, image_id="unit-fixture", ports={"1/tcp": [dict(HostIp="127.0.0.1")]})
            for name in ("fixture", "nats", "mqtt")
        ],
    )


def test_complete_scoped_oracle_fixture_passes_without_closing_c04(native_evidence):
    report = evaluate(native_evidence)

    assert report["native_case"] == "pass"
    assert report["C04"] == "unknown"
    assert len(report["checks"]) == 37


def test_worker_entrypoint_selects_a_windows_compatible_loop():
    from e2_native import ROLE_CODE

    assert "loop_factory=asyncio.SelectorEventLoop" in ROLE_CODE


def test_log_collection_failure_does_not_skip_owned_container_removal(tmp_path, monkeypatch):
    import json
    import subprocess
    from types import SimpleNamespace

    from e2_native import OwnedResources

    resources = OwnedResources(tmp_path)
    resources.containers = [dict(id="a" * 64, name="fixture")]
    calls = []

    def docker(*args):
        calls.append(args)
        if args[0] == "inspect":
            return json.dumps([dict(Config=dict(Labels={"eirvah.e2.run": resources.run_id}))])
        return ""

    def run(args, **kwargs):
        if args[1] == "logs":
            raise subprocess.TimeoutExpired(args, 15)
        return SimpleNamespace(returncode=1)

    monkeypatch.setattr(resources, "docker", docker)
    monkeypatch.setattr(subprocess, "run", run)

    cleanup = resources.cleanup()

    assert cleanup["confirmed"]
    assert ("rm", "-f", "a" * 64) in calls


def test_cleanup_refuses_container_with_a_different_owner(tmp_path, monkeypatch):
    import json

    from e2_native import OwnedResources

    resources = OwnedResources(tmp_path)
    resources.containers = [dict(id="b" * 64, name="fixture")]
    calls = []

    def docker(*args):
        calls.append(args)
        return json.dumps([dict(Config=dict(Labels={"eirvah.e2.run": "different-run"}))])

    monkeypatch.setattr(resources, "docker", docker)

    cleanup = resources.cleanup()

    assert not cleanup["confirmed"]
    assert not any(call[0] == "rm" for call in calls)


@pytest.mark.parametrize(
    "field", ["node", "topic", "unit", "value_type", "sensor", "feature", "property"]
)
def test_source_specific_semantic_binding_is_checked_independently(native_evidence, field):
    native_evidence["rows"][0]["baseline"][field] = "wrong"

    assert evaluate(native_evidence)["native_case"] != "pass"


@pytest.mark.parametrize(
    "field,value",
    [("source_endpoint", "modbus://unexpected:5020"), ("value", 999), ("quality", "bad")],
)
def test_raw_source_boundary_cannot_be_replaced_with_a_correct_summary(
    native_evidence, field, value
):
    native_evidence["rows"][0]["raw"]["payload"][field] = value

    assert evaluate(native_evidence)["native_case"] != "pass"


def test_unknown_gateway_extension_cannot_be_promoted_to_a_pass(native_evidence):
    native_evidence["source_addition_requested"] = True
    extra = copy.deepcopy(native_evidence["containers"][0])
    extra["name"] = "fixture-added"
    native_evidence["containers"].append(extra)
    native_evidence["source_addition"] = {}

    assert evaluate(native_evidence)["native_case"] != "pass"
