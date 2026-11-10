"""Only verified equivalent observations may select the paired operator commands."""

import pytest
from e2_command_response import select_commands


def test_unverified_or_incomplete_native_input_cannot_select_commands():
    with pytest.raises(ValueError):
        select_commands({})


def test_mismatched_canonical_and_source_specific_commands_are_refused():
    rows = [
        dict(
            protocol=p,
            case=i,
            signal="speed",
            command=dict(requested_value=v, target_uns_topic="wrong"),
            baseline_command=None,
        )
        for p in ("opcua", "modbus", "s7")
        for i, v in ((0, 22.0), (1, 26.0))
    ]
    with pytest.raises(ValueError):
        select_commands(dict(mode="isolated_native_loopback", rows=rows))


@pytest.fixture
def paired_evidence(tmp_path, monkeypatch):
    import json

    import e2_command_response as runner
    from control import build_command
    from eirvah_contracts.authorization import command_digest

    target = runner.load_profile(runner.PROFILE)["command_target"]
    native_rows = [
        dict(
            protocol=p,
            case=i,
            signal="speed",
            raw=dict(correlation_id=f"{p}-{i}"),
            command=dict(requested_value=v, target_uns_topic=target, value_type="double"),
            baseline_command=dict(requested_value=v, target_uns_topic=target, value_type="double"),
        )
        for p in ("opcua", "modbus", "s7")
        for i, v in ((0, 22.0), (1, 26.0))
    ]
    path = tmp_path / "native.json"
    path.write_text(json.dumps(dict(mode="isolated_native_loopback", rows=native_rows)))
    # This unit fixture tests downstream reconciliation; native evidence has its own real checker/tests.
    monkeypatch.setattr(runner, "evaluate_native", lambda data: dict(native_case="pass"))

    def entry(value):
        command = build_command("temperature", "set", value)
        payload = command.model_dump(mode="json")
        return dict(
            command=payload,
            result=dict(**payload, written_at="2026-11-10T08:00:00Z", decision="approve"),
            ledger=[
                dict(
                    command_id=command.correlation_id,
                    digest=command_digest(command),
                    state="completed",
                    result=json.dumps(dict(outcome="completed", written_at="2026-11-10T08:00:00Z")),
                )
            ],
        )

    rows = []
    for source in native_rows:
        row = entry(source["command"]["requested_value"])
        row["command"]["reason"] = "E2 paired replay of " + source["raw"]["correlation_id"]
        from eirvah_contracts.actuation import ActuationRequest

        row["result"]["reason"] = row["command"]["reason"]
        row["ledger"][0]["digest"] = command_digest(ActuationRequest.model_validate(row["command"]))
        value = row["command"]["requested_value"]
        row.update(
            protocol=source["protocol"],
            case=source["case"],
            source_observation=source["raw"]["correlation_id"],
            derived_command=source["command"],
            native_readback=value,
            initial_temperature=24.0,
            settled=True,
            sampling_started=100.0,
            process_samples=[dict(value=value, monotonic=t) for t in (101.0, 101.5, 102.0)],
        )
        rows.append(row)
    invalid = entry(99.0)
    invalid.update(
        result=dict(**invalid["command"], decision="reject"), before=26.0, after=26.0, ledger=[]
    )
    restore = entry(22.0)
    restore["readback"] = 22.0
    inventory = dict(pods={p: dict(component=p) for p in runner.ACTORS})
    return dict(
        mode="paired_replay_to_simulated_process",
        errors=[],
        controller_restored=True,
        source_hashes={
            p.relative_to(runner.ROOT).as_posix(): runner.digest(p) for p in runner.SOURCES
        },
        native_input=str(path),
        native_sha256=runner.digest(path),
        rows=rows,
        inventory_before=inventory,
        inventory_after=inventory,
        original_setpoint=22.0,
        invalid_command=invalid,
        setpoint_restoration=restore,
    )


def test_complete_paired_evidence_reconciles_input_request_ledger_and_response(paired_evidence):
    from e2_command_response import evaluate

    report = evaluate(paired_evidence)

    assert report["command_case"] == "pass"
    assert report["automatic_closed_loop"] == "not_exercised"


@pytest.mark.parametrize(
    "change",
    [
        "input",
        "lineage",
        "digest",
        "echo",
        "readback",
        "process",
        "time",
        "restore",
        "missing",
        "restart",
    ],
)
def test_partial_or_inconsistent_paired_evidence_cannot_pass(paired_evidence, change):
    import copy

    from e2_command_response import evaluate

    data = copy.deepcopy(paired_evidence)
    row = data["rows"][0]
    if change == "input":
        data["native_sha256"] = "wrong"
    elif change == "lineage":
        row["source_observation"] = "unrelated"
    elif change == "digest":
        row["ledger"][0]["digest"] = "wrong"
    elif change == "echo":
        row["result"]["target_uns_topic"] = (
            data["rows"][0]["command"]["target_uns_topic"] + "_other"
        )
    elif change == "readback":
        row["native_readback"] = 12
    elif change == "process":
        row["process_samples"][-1]["value"] = 40
    elif change == "time":
        row["process_samples"][-1]["monotonic"] = 200
    elif change == "restore":
        data["setpoint_restoration"]["readback"] = 40
    elif change == "missing":
        data["rows"].pop()
    else:
        data["inventory_after"] = {}

    assert evaluate(data)["command_case"] != "pass"
