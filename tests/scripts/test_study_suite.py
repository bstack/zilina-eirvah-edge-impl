"""Plans are validated before cluster mutation and report failures cannot be hidden."""

import copy

import pytest
from study_suite import Plan, commands_for, stage_status


def test_profile_rejects_zero_repetitions_and_unknown_stage():
    with pytest.raises(ValueError):
        Plan.model_validate(dict(id="bad", repetitions=0, stages=["probe"]))
    with pytest.raises(ValueError):
        Plan.model_validate(dict(id="bad", repetitions=1, stages=["shell"]))


def test_order_is_deterministic_and_counterbalanced():
    plan = Plan(id="pilot", repetitions=2, stages=["probe"])
    first = commands_for(plan, 1)
    second = commands_for(plan, 2)
    assert first[0]["order"] == ["shared", "separate"]
    assert second[0]["order"] == ["separate", "shared"]
    assert first == commands_for(copy.deepcopy(plan), 1)


def test_child_exit_code_and_scoped_verdict_are_both_required():
    assert stage_status(0, {"local_split": "pass"}, "split") == "pass"
    assert stage_status(1, {"local_split": "pass"}, "split") == "unknown"
    assert stage_status(0, {"local_split": "fail"}, "split") == "fail"
    assert stage_status(0, {}, "split") == "unknown"


def test_timed_out_child_runs_its_finally_block(tmp_path):
    from study_suite import execute

    marker = tmp_path / "cleanup.txt"
    script = tmp_path / "driver.py"
    script.write_text(
        "import time\nfrom pathlib import Path\ntry:\n while True: time.sleep(.05)\nfinally:\n Path("
        + repr(str(marker))
        + ").write_text('restored')\n"
    )
    plan = Plan.model_construct(
        id="cancel-test",
        stages=["probe"],
        stage_timeout_s=0.5,
        cleanup_grace_s=5,
        sample_interval_s=2,
    )

    result = execute(str(script), [], tmp_path / "capture", plan, [])

    assert result["timed_out"] is True
    assert result["exit_code"] != 0
    assert marker.read_text() == "restored"
    assert "cleanup" not in result


def test_unconfirmed_child_cleanup_is_retained_for_reconciliation(tmp_path, monkeypatch):
    import json
    import subprocess

    import study_suite

    class Process:
        pid = 12345

        def __init__(self, *args, **kwargs):
            pass

        def poll(self):
            return None

        def wait(self, timeout):
            raise subprocess.TimeoutExpired("owned-driver", timeout)

    class Sampler:
        def __init__(self, *args, **kwargs):
            self.errors = []

        def add_process(self, *args):
            pass

        def start(self):
            pass

        def stop(self):
            pass

    monkeypatch.setattr(study_suite.subprocess, "Popen", Process)
    monkeypatch.setattr(study_suite, "Sampler", Sampler)
    plan = study_suite.Plan.model_construct(
        stage_timeout_s=-1, cleanup_grace_s=0, sample_interval_s=2
    )
    directory = tmp_path / "capture"

    with pytest.raises(study_suite.CleanupUnconfirmed):
        study_suite.execute("driver.py", [], directory, plan, [])

    step = json.loads((directory / "step.json").read_text())
    assert step["child_pid"] == 12345
    assert step["cleanup"] == "unconfirmed_child_still_running"
    assert step["exit_code"] is None
    assert (directory / "cancel-requested").exists()


@pytest.mark.parametrize("status", ["pass", "fail"])
def test_native_stages_each_reset_and_park_their_fixture(tmp_path, monkeypatch, status):
    import json

    import study_suite

    calls = []
    config = dict(
        edge=dict(context="kind-eirvah-edge", namespace="eirvah-split-study-edge"),
        cloud=dict(context="kind-eirvah-edge", namespace="eirvah-split-study-cloud"),
    )
    monkeypatch.setattr(study_suite, "load_config", lambda path: config)
    monkeypatch.setattr(study_suite, "target_mapping", lambda config: {})
    monkeypatch.setattr(study_suite, "logical_manifest", lambda config: {})
    monkeypatch.setattr(study_suite, "render", lambda *args: [])
    monkeypatch.setattr(study_suite, "verify_boundary", lambda *args: None)

    def execute(script, args, directory, plan, bindings, owner=None):
        calls.append((script, "--park" in args))
        artifact = study_suite.Path(args[args.index("--output") + 1])
        artifact.mkdir(parents=True)
        name = "local-case-report.json" if script == "e0_capture.py" else "report.json"
        report = {
            "study_reset.py": {"reset": "pass", "restored": True},
            "e0_capture.py": {"status": status},
            "split_rehearsal.py": {"local_split": "pass"},
        }[script]
        (artifact / name).write_text(json.dumps(report))
        return dict(exit_code=0, timed_out=False, resource_errors=[])

    monkeypatch.setattr(study_suite, "execute", execute)
    output = tmp_path / "study"
    output.mkdir()

    study_suite.run_plan(
        study_suite.Plan(id="fixture-test", repetitions=1, stages=["e0", "split"]), output
    )

    assert json.loads((output / "suite.json").read_text())["status"] == status
    expected = [
        ("study_reset.py", False),
        ("e0_capture.py", False),
        ("study_reset.py", True),
        ("study_reset.py", False),
        ("split_rehearsal.py", False),
        ("study_reset.py", True),
    ]
    assert calls == (expected if status == "pass" else expected[:3])
