"""Comparison uses equal budgets and the real selected domain operations."""

import pytest
from control import build_command
from e1_processing import allocation, compute, initialize
from eirvah_contracts.envelope import NATSEnvelope
from load_inject import NODE_IDS, _build_payload


def test_queue_allocation_keeps_worker_and_buffer_budgets_equal():
    shared = allocation("shared")
    separate = allocation("separate")

    assert (
        sum(p["workers"] for p in shared.values())
        == sum(p["workers"] for p in separate.values())
        == 2
    )
    assert (
        sum(p["capacity"] for p in shared.values())
        == sum(p["capacity"] for p in separate.values())
        == 128
    )
    assert set(shared) == {"shared"}
    assert set(separate) == {"data", "control"}
    with pytest.raises(ValueError):
        allocation("invalid")


def test_comparison_calls_production_context_and_policy_without_actuation():
    initialize(
        "config/eirvah-line-a.jsonld",
        "deploy/k3s/base/actuation-event-validator/actuation-policy.yaml",
    )
    request = NATSEnvelope.model_validate_json(_build_payload(NODE_IDS[0]))
    command = build_command("temperature", "set", 26.0)

    context = NATSEnvelope.model_validate_json(compute("data", request.model_dump_json().encode()))
    decision = NATSEnvelope.model_validate_json(
        compute(
            "command",
            NATSEnvelope(
                correlation_id=command.correlation_id, payload=command.model_dump(mode="json")
            )
            .model_dump_json()
            .encode(),
        )
    )

    assert context.status == "ok"
    assert context.payload["uns_topic"].endswith("/temperature")
    assert decision.payload["decision"] == "approve"
    assert decision.payload["approval"] is None
