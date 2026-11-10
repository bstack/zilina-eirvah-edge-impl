"""Render/apply the local logical edge/cloud split with coordinated trust material.

No keys are printed or written to artifacts. The ordinary single-namespace setup
is untouched. One NATS backbone belongs to edge; cloud request/reply uses its
explicit configured address. This is not a separate-cluster/TLS implementation.
"""

from __future__ import annotations

import argparse
import base64
import copy
import hashlib
import json
import re
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import yaml
from configure_actuation_security import SECRET_NAME, build_secret
from interoperability import ROOT

DEFAULT_CONFIG = ROOT / "config/deployments/local-split.json"
CLOUD = [
    "rabbitmq",
    "amqp-actuation-event-subscriber",
    "actuation-control-orchestrator",
    "actuation-event-validator",
]
EDGE = [
    "nats",
    "mosquitto",
    "opcua-simulator",
    "opcua-data-subscriber",
    "modbus-simulator",
    "modbus-data-subscriber",
    "s7-simulator",
    "s7-data-subscriber",
    "data-converter",
    "uns-auto-contextualizer",
    "mqtt-uns-publisher",
    "uns-contextualizer-orchestrator",
    "actuation-signal-publisher",
    "decision-agent-stub",
]
PURPOSE = "split-local-rehearsal"
EDGE_KEYS = {"approval-key", "decision-amqp-url", "inspection-receipt-key"}
MASTER_KEYS = EDGE_KEYS | {
    "ingress-key",
    "definitions.json",
    "experiment-amqp-url",
    "ingress-amqp-url",
    "coordinator-amqp-url",
}


def load_config(path: Path) -> dict[str, Any]:
    data: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    if data.get("schema_version") != "1.0" or not re.fullmatch(r"[a-z0-9-]+", data.get("id", "")):
        raise ValueError("unsupported split configuration")
    for zone in ("edge", "cloud"):
        binding = data[zone]
        if not binding["context"] or not re.fullmatch(
            r"eirvah-split-[a-z0-9](?:[a-z0-9-]{0,45}[a-z0-9])?", binding["namespace"]
        ):
            raise ValueError("dedicated eirvah-split-* namespace required")
    if data["edge"] == data["cloud"]:
        raise ValueError("edge/cloud namespace bindings must differ")
    servers = data["connections"]["nats_servers"]
    if not servers or any(
        urlsplit(s).scheme not in ("nats", "tls") or not urlsplit(s).hostname for s in servers
    ):
        raise ValueError("explicit NATS server addresses required")
    host = data["connections"]["amqp_host"]
    if (
        not re.fullmatch(r"[a-zA-Z0-9.-]+", host)
        or not 1 <= data["connections"]["amqp_port"] <= 65535
    ):
        raise ValueError("invalid AMQP endpoint")
    return data


def target_mapping(config: dict[str, Any]) -> dict[str, Any]:
    return dict(
        schema_version="1.0", default=config["edge"], components={c: config["cloud"] for c in CLOUD}
    )


def credentials(
    config: dict[str, Any], master: dict[str, Any] | None = None
) -> tuple[dict[str, Any], dict[str, Any]]:
    cloud = (
        copy.deepcopy(master) if master is not None else build_secret(config["cloud"]["namespace"])
    )
    if not cloud.get("data", {}).keys() >= MASTER_KEYS:
        raise ValueError("existing cloud master credentials incomplete; refusing rotation")
    for field in (
        "decision-amqp-url",
        "experiment-amqp-url",
        "ingress-amqp-url",
        "coordinator-amqp-url",
    ):
        parts = urlsplit(base64.b64decode(cloud["data"][field]).decode())
        auth = parts.netloc.rsplit("@", 1)[0]
        value = urlunsplit(
            (
                parts.scheme,
                auth
                + "@"
                + config["connections"]["amqp_host"]
                + ":"
                + str(config["connections"]["amqp_port"]),
                parts.path,
                "",
                "",
            )
        )
        cloud["data"][field] = base64.b64encode(value.encode()).decode()
    cloud["metadata"] = dict(
        name=SECRET_NAME,
        namespace=config["cloud"]["namespace"],
        labels={"eirvah.uniza/purpose": PURPOSE, "eirvah.uniza/deployment": config["id"]},
    )
    edge = dict(
        apiVersion="v1",
        kind="Secret",
        type="Opaque",
        metadata=dict(
            name=SECRET_NAME,
            namespace=config["edge"]["namespace"],
            labels=cloud["metadata"]["labels"],
        ),
        data={k: cloud["data"][k] for k in EDGE_KEYS},
    )
    return cloud, edge


def validate_edge_projection(existing: dict[str, Any], expected: dict[str, Any]) -> None:
    before, after = existing.get("data", {}), expected["data"]
    if set(before) != EDGE_KEYS or any(
        before[k] != after[k] for k in EDGE_KEYS - {"decision-amqp-url"}
    ):
        raise ValueError("existing edge trust differs; refusing to overwrite or rotate it")
    a, b = (urlsplit(base64.b64decode(d["decision-amqp-url"]).decode()) for d in (before, after))
    if (a.scheme, a.username, a.password, a.path) != (b.scheme, b.username, b.password, b.path):
        raise ValueError("existing edge broker identity differs; refusing credential rotation")


def materialize(
    config: dict[str, Any], zone: str, documents: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    if zone not in ("edge", "cloud"):
        raise ValueError("invalid zone")
    result = copy.deepcopy(documents)
    fingerprint = hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()
    for doc in result:
        name = doc["metadata"]["name"]
        if doc["kind"] == "Namespace":
            doc["metadata"]["name"] = config[zone]["namespace"]
        else:
            doc["metadata"]["namespace"] = config[zone]["namespace"]
        if doc["kind"] != "Deployment":
            continue
        template = doc["spec"]["template"]
        template.setdefault("metadata", {}).setdefault("labels", {})["eirvah.uniza/placement"] = (
            zone
        )
        template["metadata"].setdefault("annotations", {})["eirvah.uniza/connections-sha256"] = (
            fingerprint
        )
        for container in template["spec"]["containers"]:
            for env in container.get("env", []):
                if env["name"].endswith("_NATS_SERVERS"):
                    env.pop("valueFrom", None)
                    env["value"] = json.dumps(config["connections"]["nats_servers"])
            if container["name"] == name and name in config.get("images", {}):
                container["image"] = config["images"][name]
            if name == "uns-auto-contextualizer":
                container["command"] = [
                    "/workspace/.venv/bin/python",
                    "-m",
                    "uns_auto_contextualizer",
                ]
            if name == "actuation-control-orchestrator":
                entries = container.setdefault("env", [])
                for key, value in dict(
                    ACTUATION_CONTROL_ORCHESTRATOR_NATS_SERVERS=json.dumps(
                        config["connections"]["nats_servers"]
                    ),
                    ACTUATION_CONTROL_ORCHESTRATOR_ALLOW_WRITES="true",
                ).items():
                    entries[:] = [e for e in entries if e["name"] != key]
                    entries.append(dict(name=key, value=value))
            if name == "modbus-simulator":
                entries = container.setdefault("env", [])
                entries[:] = [e for e in entries if e["name"] != "MODBUS_SIMULATOR_JAM_PROBABILITY"]
                entries.append(dict(name="MODBUS_SIMULATOR_JAM_PROBABILITY", value="0"))
    return result


def render(config: dict[str, Any], zone: str) -> list[dict[str, Any]]:
    output = subprocess.check_output(
        ["kubectl", "kustomize", str(ROOT / "deploy/k3s/overlays" / ("split-" + zone))],
        text=True,
        encoding="utf-8",
        timeout=30,
    )
    docs = [d for d in yaml.safe_load_all(output) if d]
    result = materialize(config, zone, docs)
    actual = {d["metadata"]["name"] for d in result if d["kind"] == "Deployment"}
    if actual != set(EDGE if zone == "edge" else CLOUD):
        raise ValueError("deployment selection does not match declared split")
    return result


def invoke(
    binding: dict[str, Any], *args: str, payload: dict[str, Any] | None = None, timeout: int = 60
) -> str:
    result = subprocess.run(
        ["kubectl", "--context", binding["context"], "-n", binding["namespace"], *args],
        input=json.dumps(payload) if payload is not None else None,
        text=True,
        encoding="utf-8",
        capture_output=True,
        timeout=timeout,
    )
    if result.returncode:
        # Never include stdout/stderr from a Secret operation in errors.
        raise RuntimeError("split kubectl operation failed: " + args[0])
    return result.stdout


def apply(config: dict[str, Any], docs: dict[str, list[dict[str, Any]]]) -> None:
    if any(config[z]["context"] != "kind-eirvah-edge" for z in ("edge", "cloud")):
        raise ValueError(
            "this first local apply is restricted to kind-eirvah-edge; rendering supports other explicit bindings"
        )
    for zone in ("cloud", "edge"):
        binding = config[zone]
        current = invoke(
            binding, "get", "namespace", binding["namespace"], "--ignore-not-found", "-o", "json"
        )
        if (
            current.strip()
            and json.loads(current)["metadata"].get("labels", {}).get("eirvah.uniza/purpose")
            != PURPOSE
        ):
            raise ValueError("existing namespace is not owned by the split rehearsal")
        namespace = next(d for d in docs[zone] if d["kind"] == "Namespace")
        invoke(binding, "apply", "-f", "-", payload=namespace)
    current = invoke(
        config["cloud"], "get", "secret", SECRET_NAME, "--ignore-not-found", "-o", "json"
    )
    master = json.loads(current) if current.strip() else None
    if (
        master is not None
        and master["metadata"].get("labels", {}).get("eirvah.uniza/deployment") != config["id"]
    ):
        raise ValueError("existing cloud secret has different ownership")
    cloud, edge = credentials(config, master)
    current_edge = invoke(
        config["edge"], "get", "secret", SECRET_NAME, "--ignore-not-found", "-o", "json"
    )
    if current_edge.strip():
        projected = json.loads(current_edge)
        if projected["metadata"].get("labels", {}).get("eirvah.uniza/deployment") != config["id"]:
            raise ValueError("existing edge secret has different ownership")
        validate_edge_projection(projected, edge)
    invoke(config["cloud"], "apply", "-f", "-", payload=cloud)
    invoke(config["edge"], "apply", "-f", "-", payload=edge)
    for zone in ("edge", "cloud"):
        invoke(
            config[zone],
            "apply",
            "-f",
            "-",
            payload=dict(
                apiVersion="v1",
                kind="List",
                items=[d for d in docs[zone] if d["kind"] != "Namespace"],
            ),
        )
    print(
        "Applied split resources; trust material retained without printing credentials.", flush=True
    )
    for zone, components in (("edge", EDGE), ("cloud", CLOUD)):
        for component in components:
            invoke(
                config[zone],
                "rollout",
                "status",
                "deployment/" + component,
                "--timeout=240s",
                timeout=250,
            )
            print(zone + ": " + component + " ready", flush=True)


def logical_manifest(config: dict[str, Any]) -> dict[str, Any]:
    manifest: dict[str, Any] = json.loads(
        (ROOT / "config/architecture/local-control.json").read_text()
    )
    manifest["id"] = "paper1-local-edge-cloud-split"
    manifest["scope"] = (
        "Selected logical edge/cloud split on one local Kubernetes cluster; not physical host or network isolation."
    )
    selected = set(EDGE + CLOUD)
    for component in manifest["components"]:
        if component["id"] not in selected and "deployment" in component:
            component.pop("deployment")
            component["availability"] = "not_deployed_in_split_profile"
    # Namespaces are logical placement groups, not distinct physical hosts.
    manifest["placement"] = dict(
        status="declared_local_namespace_split",
        host_set=sorted({config[z]["context"] for z in ("edge", "cloud")}),
        component_hosts={
            c["id"]: [config["cloud" if c["id"] in CLOUD else "edge"]["context"]]
            for c in manifest["components"]
            if "deployment" in c
        },
        namespace_groups={
            c: config["cloud" if c in CLOUD else "edge"]["namespace"] for c in selected
        },
        bindings=target_mapping(config),
        physical_host_separation="not_exercised",
    )

    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["render", "apply"])
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    config = load_config(args.config)
    docs = {zone: render(config, zone) for zone in ("edge", "cloud")}
    output = args.output or ROOT / "results/split-deployment" / datetime.now(UTC).strftime(
        "%Y-%m-%dT%H-%M-%S-%fZ"
    )
    output.mkdir(parents=True, exist_ok=False)
    for name, value in [
        ("config", config),
        ("targets", target_mapping(config)),
        ("architecture", logical_manifest(config)),
    ]:
        (output / (name + ".json")).write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    for zone in ("edge", "cloud"):
        (output / (zone + ".yaml")).write_text(
            yaml.safe_dump_all(docs[zone], sort_keys=False), encoding="utf-8"
        )
    if args.action == "apply":
        apply(config, docs)
    print(
        json.dumps(
            dict(action=args.action, output=str(output), secret_artifacts="not_written"), indent=2
        )
    )


if __name__ == "__main__":
    main()
