"""Paired gateway-alias integration tasks; consumer identity and control rule stay fixed."""

from __future__ import annotations

import copy
import hashlib
import json
import math
from datetime import timedelta
from pathlib import Path
from typing import Any

import yaml
from interoperability import ROOT, digest

RULES = ROOT / "deploy/k3s/base/data-converter/conversion-rules.yaml"
ONTOLOGY = ROOT / "config/eirvah-line-a.jsonld"
CONSUMER = ROOT / "services/decision-agent-stub/src/decision_agent_stub/service.py"


def consumer_hash() -> str:
    return hashlib.sha256((digest(CONSUMER) + digest(ONTOLOGY)).encode()).hexdigest()


def prepare_addition(output: Path, profile: dict[str, Any], protocol: str) -> dict[str, Any]:
    if protocol not in ("opcua", "modbus", "s7"):
        raise ValueError("unsupported protocol")
    output.mkdir(parents=True, exist_ok=True)
    old = profile["signals"]["speed"]["node"]
    alias = "E2Gateway2." + protocol + ".ConveyorSpeed"
    before_rules = RULES.read_text(encoding="utf-8")
    old_rule = next(r for r in yaml.safe_load(before_rules)["rules"] if r["node_id"] == old)
    added_rule = copy.deepcopy(old_rule)
    added_rule["node_id"] = alias
    after_rules = (
        before_rules
        + "\n"
        + "\n".join(
            "  " + line for line in yaml.safe_dump([added_rule], sort_keys=False).splitlines()
        )
        + "\n"
    )
    before = ONTOLOGY.read_text(encoding="utf-8")
    after = json.loads(before)
    node = next(n for n in after["@graph"] if n.get("eirvah:nodeId") == old)
    node["eirvah:nodeId"] = [old, alias]
    after_ontology = json.dumps(after, indent=2) + "\n"
    baseline_before = {old: profile["source_bindings"][protocol]["speed"]}
    baseline_after = copy.deepcopy(baseline_before)
    baseline_after[alias] = copy.deepcopy(baseline_before[old])
    values = dict(
        before_rules=before_rules,
        after_rules=after_rules,
        before_ontology=before,
        after_ontology=after_ontology,
        added_rule=added_rule,
        baseline_before=baseline_before,
        baseline_after=baseline_after,
        consumer_before=consumer_hash(),
        consumer_after=consumer_hash(),
        human_effort="not_measured",
        task=dict(
            protocol=protocol,
            old_alias=old,
            new_alias=alias,
            logical_topic=profile["signals"]["speed"]["topic"],
            kind="additional_gateway_for_same_signal",
        ),
        semantic_changes=dict(
            canonical_adapter_alias=1,
            canonical_conversion_entries=1,
            canonical_ontology_nodes=1,
            source_specific_consumer_entries=1,
            controller_code_changes=0,
        ),
    )
    for key, name in (
        ("before_rules", "rules-before.yaml"),
        ("after_rules", "rules-after.yaml"),
        ("before_ontology", "ontology-before.jsonld"),
        ("after_ontology", "ontology-after.jsonld"),
    ):
        (output / name).write_text(values[key], encoding="utf-8")
    for key in ("baseline_before", "baseline_after"):
        (output / (key + ".json")).write_text(
            json.dumps(values[key], indent=2) + "\n", encoding="utf-8"
        )
    return values


def validate_bundle(bundle: dict[str, Any]) -> bool:
    try:
        old = bundle["task"]["old_alias"]
        new = bundle["task"]["new_alias"]
        before_rules = yaml.safe_load(bundle["before_rules"])["rules"]
        after_rules = yaml.safe_load(bundle["after_rules"])["rules"]
        original = next(r for r in before_rules if r["node_id"] == old)
        expected = original | dict(node_id=new)
        if bundle["added_rule"] != expected or after_rules != [*before_rules, expected]:
            return False
        before = json.loads(bundle["before_ontology"])
        after = json.loads(bundle["after_ontology"])
        expected_graph = copy.deepcopy(before)
        node = next(n for n in expected_graph["@graph"] if n.get("eirvah:nodeId") == old)
        node["eirvah:nodeId"] = [old, new]
        return bool(
            after == expected_graph
            and bundle["baseline_after"]
            == bundle["baseline_before"] | {new: bundle["baseline_before"][old]}
            and bundle["consumer_before"] == bundle["consumer_after"] == consumer_hash()
            and bundle["human_effort"] == "not_measured"
        )
    except (KeyError, TypeError, ValueError, StopIteration):
        return False


async def live_addition(resources: Any, data: dict[str, Any], context: dict[str, Any]) -> None:
    from decision_agent_stub.service import ProductionModeRule
    from eirvah_contracts.observation import load_bindings, validate_observation
    from interoperability import AT, command_meaning, local_meaning, settings

    output = resources.output / "source-addition"
    output.mkdir()
    directory = output / "fixture"
    directory.mkdir()
    ports = resources.container(
        "fixture-added",
        "actuation-signal-publisher:local",
        [4840, 5020, 1102],
        [
            "--entrypoint",
            "python",
            "--mount",
            f"type=bind,source={(ROOT / 'scripts/e2_native_fixture.py').resolve()},target=/e2_native_fixture.py,readonly",
            "--mount",
            f"type=bind,source={directory.resolve()},target=/cases",
        ],
        ["/e2_native_fixture.py"],
    )
    wait = context["wait"]
    await wait(lambda: (directory / "ready.json").exists(), "added gateway ready")
    profile = context["profile"]
    nats = dict(nats_servers=[context["nats_url"]])
    result: dict[str, Any] = dict(
        mode="native_gateway_alias_addition",
        rows=[],
        bundles={},
        human_effort="not_measured",
        added_ports=ports,
    )
    data["source_addition"] = result
    sequence = 100

    async def workers(rule_path: Path, ontology_path: Path, label: str) -> None:
        for process, _, name in resources.processes:
            if name.startswith(("convert", "context")) and process.poll() is None:
                resources.stop(process)
        resources.role(
            "convert-" + label,
            "data_converter",
            "DataConverterWorker",
            "DataConverterSettings",
            nats | dict(rules_path=str(rule_path)),
        )
        resources.role(
            "context-" + label,
            "uns_auto_contextualizer",
            "AutoContextualizerWorker",
            "AutoContextualizerSettings",
            nats | dict(ontology_path=str(ontology_path)),
        )
        await wait(
            lambda: all(
                marker
                in (resources.output / (name + "-" + label + ".log")).read_text(encoding="utf-8")
                for name, marker in (
                    ("convert", "data_converter_ready"),
                    ("context", "contextualizer_ready"),
                )
            ),
            "replacement workers ready",
        )

    def adapter(protocol: str, alias: str, source_ports: dict[str, int], label: str) -> Any:
        mapping = context["adapter_config"](protocol, source_ports, profile)
        old = profile["signals"]["speed"]["node"]
        if protocol == "opcua":
            mapping["nodes"] = [
                n | dict(alias=alias) for n in mapping["nodes"] if n["alias"] == old
            ]
            key = "node_list_path"
        elif protocol == "modbus":
            mapping["registers"] = [
                n | dict(alias=alias) for n in mapping["registers"] if n["alias"] == old
            ]
            key = "register_map_path"
        else:
            mapping["data_blocks"][0]["signals"] = [
                n | dict(alias=alias)
                for n in mapping["data_blocks"][0]["signals"]
                if n["alias"] == old
            ]
            key = "unit_map_path"
        path = output / (label + "-adapter.yaml")
        path.write_text(yaml.safe_dump(mapping), encoding="utf-8")
        return resources.role(
            label,
            protocol + "_data_subscriber",
            "SubscriberRuntime",
            "SubscriberSettings",
            nats
            | {
                key: str(path),
                "position_path": str(output / (label + "-positions.sqlite3")),
                "source_id": "e2-" + label,
                "reconnect_delay_s": 0.2,
            },
        )

    async def collect(
        protocol: str,
        phase: str,
        alias: str,
        source_ports: dict[str, int],
        source_directory: Path,
        case: dict[str, Any],
        bundle: dict[str, Any],
    ) -> None:
        nonlocal sequence
        raw_start = len(context["raw"])
        mqtt_start = len(context["mqtt"])
        dlq_start = len(context["dlq"])
        sequence += 1
        source_directory.joinpath("control.json").write_text(
            json.dumps(dict(sequence=sequence, signals=case)), encoding="utf-8"
        )

        def applied() -> bool:
            try:
                return bool(
                    json.loads(source_directory.joinpath("applied.json").read_text())["sequence"]
                    == sequence
                )
            except (OSError, ValueError):
                return False

        await wait(applied, "gateway vector application")
        native = await context["read_native"](protocol, source_ports)
        expected = case["speed"]["expected"]
        found: dict[str, Any] = {}

        def matched() -> bool:
            for env in context["raw"][raw_start:]:
                raw = env["payload"]
                if raw["node_id"] != alias or not math.isclose(
                    raw["value"], expected, rel_tol=0, abs_tol=1e-6
                ):
                    continue
                cid = env["correlation_id"]
                if phase == "before_new":
                    reply = next(
                        (m for m in context["dlq"][dlq_start:] if m["correlation_id"] == cid), None
                    )
                    if reply is not None:
                        found.update(raw=env, dlq=reply)
                        return True
                else:
                    reply = next(
                        (
                            m
                            for m in context["mqtt"][mqtt_start:]
                            if m["packet"].get("eirvah:correlationId") == cid
                        ),
                        None,
                    )
                    if reply is not None:
                        found.update(raw=env, **reply)
                        return True
            return False

        await wait(matched, "gateway correlated " + protocol + "/" + phase)
        row = dict(
            protocol=protocol,
            phase=phase,
            alias=alias,
            sample=case["speed"],
            independent_native=native["speed"],
            **found,
        )
        if phase == "before_new":
            row["baseline_resolved"] = alias in bundle["baseline_before"]
        else:
            packet = found["packet"]
            observed = validate_observation(found["topic"], packet, load_bindings(ONTOLOGY))
            supplied = copy.deepcopy(case["speed"])
            supplied[protocol] = native["speed"]
            mapping = bundle["baseline_after"][alias]
            baseline = local_meaning(protocol, supplied, mapping, AT)
            rule = settings(profile)
            row.update(
                baseline=baseline,
                canonical_command=command_meaning(
                    ProductionModeRule(rule).update(
                        speed=float(observed.has_simple_result),
                        observed_at=AT,
                        now=AT + timedelta(milliseconds=100),
                    )
                ),
                baseline_command=command_meaning(
                    ProductionModeRule(rule).update(
                        speed=baseline["value"],
                        observed_at=AT,
                        now=AT + timedelta(milliseconds=100),
                    )
                ),
            )
        result["rows"].append(row)
        (output / "evidence-progress.json").write_text(
            json.dumps(result, indent=2) + "\n", encoding="utf-8"
        )

    for protocol in profile["protocols"]:
        task_dir = output / protocol
        bundle = prepare_addition(task_dir, profile, protocol)
        result["bundles"][protocol] = bundle
        await workers(RULES, ONTOLOGY, "before-" + protocol)
        new_adapter = adapter(protocol, bundle["task"]["new_alias"], ports, "added-" + protocol)
        try:
            await collect(
                protocol,
                "before_new",
                bundle["task"]["new_alias"],
                ports,
                directory,
                context["cases"][0],
                bundle,
            )
            await workers(
                task_dir / "rules-after.yaml",
                task_dir / "ontology-after.jsonld",
                "after-" + protocol,
            )
            await collect(
                protocol,
                "after_new",
                bundle["task"]["new_alias"],
                ports,
                directory,
                context["cases"][1],
                bundle,
            )
        finally:
            resources.stop(new_adapter)
        old_adapter = adapter(
            protocol, bundle["task"]["old_alias"], context["fixture_ports"], "retained-" + protocol
        )
        try:
            await collect(
                protocol,
                "after_old",
                bundle["task"]["old_alias"],
                context["fixture_ports"],
                context["directory"],
                context["cases"][0],
                bundle,
            )
        finally:
            resources.stop(old_adapter)
    result["consumer_after_all"] = consumer_hash()
    result["report"] = evaluate_addition(result)


def evaluate_addition(data: dict[str, Any]) -> dict[str, Any]:
    from eirvah_contracts.observation import load_bindings, validate_observation
    from interoperability import PROFILE, load_profile

    report: dict[str, Any] = dict(source_addition="unknown", checks=[], human_effort="not_measured")
    try:
        profile = load_profile(PROFILE)
        rows = data["rows"]
        expected = {
            (p, phase)
            for p in profile["protocols"]
            for phase in ("before_new", "after_new", "after_old")
        }
        keys = [(r["protocol"], r["phase"]) for r in rows]
        checks = [
            dict(
                check="complete_gateway_matrix",
                status="pass" if len(keys) == len(expected) and set(keys) == expected else "fail",
            )
        ]
        for protocol, bundle in data["bundles"].items():
            checks.append(
                dict(
                    check=protocol + "/mapping_task",
                    status="pass" if validate_bundle(bundle) else "fail",
                )
            )
        for row in rows:
            bundle = data["bundles"][row["protocol"]]
            cid = row["raw"]["correlation_id"]
            expected_alias = (
                bundle["task"]["old_alias"]
                if row["phase"] == "after_old"
                else bundle["task"]["new_alias"]
            )
            expected_id = "speed-high" if row["phase"] == "after_new" else "speed-normal"
            expected_sample = next(s for s in profile["samples"] if s["id"] == expected_id)
            good = (
                row["raw"]["payload"]["node_id"] == row["alias"] == expected_alias
                and row["sample"] == expected_sample
                and row["independent_native"] == expected_sample[row["protocol"]]
            )
            if row["phase"] == "before_new":
                good = (
                    good
                    and not row["baseline_resolved"]
                    and row["dlq"]["correlation_id"] == cid
                    and "stage=convert reason=UnknownNode" in row["dlq"]["error"]["message"]
                )
            else:
                observation = validate_observation(
                    row["topic"], row["packet"], load_bindings(ONTOLOGY)
                )
                value = 0.7 if row["phase"] == "after_new" else 0.4
                good = (
                    good
                    and row["topic"] == profile["signals"]["speed"]["topic"]
                    and math.isclose(
                        float(observation.has_simple_result), value, rel_tol=0, abs_tol=1e-6
                    )
                )
                good = (
                    good
                    and row["packet"]["eirvah:correlationId"] == cid
                    and row["canonical_command"] == row["baseline_command"]
                    and row["canonical_command"]["requested_value"]
                    == (26.0 if value == 0.7 else 22.0)
                )
            checks.append(
                dict(check=row["protocol"] + "/" + row["phase"], status="pass" if good else "fail")
            )
        checks.append(
            dict(
                check="consumer_unchanged",
                status="pass"
                if data["consumer_after_all"] == consumer_hash()
                and data["human_effort"] == "not_measured"
                else "fail",
            )
        )
        report.update(
            checks=checks,
            source_addition="pass" if all(c["status"] == "pass" for c in checks) else "fail",
        )
    except (KeyError, TypeError, ValueError, OSError):
        pass
    return report
