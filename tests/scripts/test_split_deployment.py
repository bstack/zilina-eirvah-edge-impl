"""Split deployment preserves approval identity without copying cloud-only keys to edge."""

import base64
import copy
import json
from pathlib import Path
from urllib.parse import urlsplit

import pytest
from split_deployment import credentials, load_config, materialize, target_mapping


@pytest.fixture
def config():
    return load_config(Path("config/deployments/local-split.json"))


def test_linked_credentials_share_approval_but_expose_only_edge_fields(config):
    cloud, edge = credentials(config)

    assert edge["data"]["approval-key"] == cloud["data"]["approval-key"]
    assert set(edge["data"]) == {"approval-key", "decision-amqp-url", "inspection-receipt-key"}
    assert "ingress-key" not in edge["data"]
    assert "definitions.json" not in edge["data"]
    url = base64.b64decode(edge["data"]["decision-amqp-url"]).decode()
    assert urlsplit(url).hostname == config["connections"]["amqp_host"]


def test_reconciliation_retains_existing_keys_and_passwords(config):
    first, edge = credentials(config)

    again, projected = credentials(config, first)

    assert again["data"] == first["data"]
    assert projected["data"] == edge["data"]


def test_incomplete_existing_master_is_not_silently_rotated(config):
    master, _ = credentials(config)
    del master["data"]["approval-key"]

    with pytest.raises(ValueError, match="incomplete"):
        credentials(config, master)


def test_only_cloud_services_move_and_writer_stays_on_edge(config):
    mapping = target_mapping(config)

    assert mapping["default"] == config["edge"]
    assert mapping["components"]["actuation-event-validator"] == config["cloud"]
    assert "actuation-signal-publisher" not in mapping["components"]


def test_cloud_connections_are_materialized_without_native_device_endpoints(config):
    raw = [
        dict(
            apiVersion="apps/v1",
            kind="Deployment",
            metadata=dict(name="actuation-control-orchestrator"),
            spec=dict(
                template=dict(
                    metadata={},
                    spec=dict(containers=[dict(name="actuation-control-orchestrator", env=[])]),
                )
            ),
        )
    ]

    deployment = materialize(config, "cloud", raw)[0]

    env = {
        e["name"]: e["value"]
        for e in deployment["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    assert (
        json.loads(env["ACTUATION_CONTROL_ORCHESTRATOR_NATS_SERVERS"])
        == config["connections"]["nats_servers"]
    )
    assert env["ACTUATION_CONTROL_ORCHESTRATOR_ALLOW_WRITES"] == "true"
    assert deployment["metadata"]["namespace"] == config["cloud"]["namespace"]
    assert not any("SIMULATOR" in name for name in env)


def test_rehearsal_cannot_overwrite_existing_baseline_namespace(tmp_path):
    data = json.loads(Path("config/deployments/local-split.json").read_text())
    data["edge"]["namespace"] = "eirvah-edge"
    path = tmp_path / "bad.json"
    path.write_text(json.dumps(data))

    with pytest.raises(ValueError, match="namespace"):
        load_config(path)


def test_endpoint_change_retains_trust_and_updates_only_connection_address(config):
    from split_deployment import validate_edge_projection

    original, edge = credentials(config)
    updated = copy.deepcopy(config)
    updated["connections"]["amqp_host"] = "rabbitmq.other.example"

    new_cloud, new_edge = credentials(updated, original)
    validate_edge_projection(edge, new_edge)

    assert new_cloud["data"]["approval-key"] == original["data"]["approval-key"]
    before = urlsplit(base64.b64decode(edge["data"]["decision-amqp-url"]).decode())
    after = urlsplit(base64.b64decode(new_edge["data"]["decision-amqp-url"]).decode())
    assert before.password == after.password
    assert before.hostname != after.hostname


def test_changed_edge_key_is_refused(config):
    from split_deployment import validate_edge_projection

    _, edge = credentials(config)
    changed = copy.deepcopy(edge)
    changed["data"]["approval-key"] = base64.b64encode(b"other").decode()

    with pytest.raises(ValueError, match="trust"):
        validate_edge_projection(changed, edge)
