"""E1 results must reconcile completions with per-pod metrics, not just replies."""

import copy

import pytest
from e1_rehearsal import check_hpa, check_sharing


def sharing():
    return dict(
        replicas=3,
        before={"a": 10, "b": 0, "c": 0},
        after={"a": 11, "b": 1, "c": 1},
        pods_before={"a": "uid-a", "b": "uid-b", "c": "uid-c"},
        pods_after={"a": "uid-a", "b": "uid-b", "c": "uid-c"},
        rows=[dict(worker=p, status="ok", correlation_id=p) for p in ("a", "b", "c")],
    )


def test_replica_delivery_reconciles_each_handler_with_unique_completed_work():
    assert check_sharing(sharing())


@pytest.mark.parametrize(
    "change", ["duplicate_handler", "missing_reply", "restart", "wrong_pod", "duplicate_id"]
)
def test_extra_effects_or_incomplete_evidence_cannot_pass(change):
    evidence = copy.deepcopy(sharing())
    if change == "duplicate_handler":
        evidence["after"]["a"] += 1
    elif change == "missing_reply":
        evidence["rows"].pop()
    elif change == "restart":
        evidence["pods_after"]["a"] = "new"
    elif change == "wrong_pod":
        evidence["rows"][0]["worker"] = "other"
    else:
        evidence["rows"][0]["correlation_id"] = "b"

    assert not check_sharing(evidence)


def test_hpa_needs_measured_cpu_and_actual_ready_scale_out_and_return():
    evidence = dict(
        samples=[
            dict(ready=1, cpu_present=True, active=True),
            dict(ready=2, cpu_present=True, active=True),
        ],
        cooled=dict(ready=1, desired=1, cpu_present=True, active=True),
    )
    assert check_hpa(evidence)
    evidence["samples"][1]["cpu_present"] = False
    assert not check_hpa(evidence)


def test_hpa_initial_null_metrics_are_missing_evidence_not_a_runner_crash():
    import json
    from types import SimpleNamespace

    from e1_rehearsal import hpa_sample

    def run(*args):
        return (
            json.dumps(dict(status=dict(currentMetrics=None, conditions=None)))
            if "hpa" in args
            else json.dumps(dict(spec=dict(replicas=1), status=dict(readyReplicas=1)))
        )

    sample = hpa_sample(SimpleNamespace(run=run))

    assert sample["cpu_present"] is False
    assert sample["active"] is False
    assert sample["ready"] == sample["desired"] == 1
