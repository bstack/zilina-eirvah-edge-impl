from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path

import pytest


def test_completed_result_is_replayed_after_reopening(tmp_path: Path) -> None:
    from actuation_signal_publisher.ledger import CommandLedger

    path = tmp_path / "commands.sqlite3"
    store = CommandLedger(path)
    claim = store.claim("id", "digest", deadline=2_000_000_000)
    assert claim.token is not None
    store.mark_started("id", claim.token)
    at = datetime.now(UTC)
    result = store.complete("id", claim.token, written_at=at)
    assert result.outcome == "completed" and not result.replayed
    replay = CommandLedger(path).claim("id", "digest", deadline=2_000_000_000)
    assert replay.token is None and replay.result is not None
    assert replay.result.outcome == "completed" and replay.result.replayed
    assert replay.result.written_at == at


def test_reused_identity_with_different_command_is_refused(tmp_path: Path) -> None:
    from actuation_signal_publisher.ledger import CommandLedger

    store = CommandLedger(tmp_path / "commands.sqlite3")
    store.claim("id", "original", deadline=2_000_000_000)
    conflict = store.claim("id", "changed", deadline=2_000_000_000)
    assert conflict.token is None and conflict.result.outcome == "conflict"


def test_concurrent_duplicate_claims_have_one_owner(tmp_path: Path) -> None:
    from actuation_signal_publisher.ledger import CommandLedger

    path = tmp_path / "commands.sqlite3"
    first, second = CommandLedger(path), CommandLedger(path)
    with ThreadPoolExecutor(max_workers=2) as executor:
        claims = list(
            executor.map(
                lambda store: store.claim("id", "digest", deadline=2_000_000_000), (first, second)
            )
        )
    assert sum(c.token is not None for c in claims) == 1
    assert [c.result.outcome for c in claims if c.result is not None] == ["pending"]


def test_restart_before_write_allows_same_command_to_retry(tmp_path: Path) -> None:
    from actuation_signal_publisher.ledger import CommandLedger

    path = tmp_path / "commands.sqlite3"
    old = CommandLedger(path).claim("id", "digest", deadline=2_000_000_000)
    store = CommandLedger(path)
    store.recover()
    retry = store.claim("id", "digest", deadline=2_000_000_000)
    assert retry.token is not None and retry.token != old.token
    with pytest.raises(ValueError):
        store.mark_started("id", old.token)


def test_restart_after_write_started_stays_unknown_without_new_claim(tmp_path: Path) -> None:
    from actuation_signal_publisher.ledger import CommandLedger

    path = tmp_path / "commands.sqlite3"
    first = CommandLedger(path)
    claim = first.claim("id", "digest", deadline=2_000_000_000)
    first.mark_started("id", claim.token)
    recovered = CommandLedger(path)
    recovered.recover()
    retry = recovered.claim("id", "digest", deadline=2_000_000_000)
    assert retry.token is None and retry.result.outcome == "unknown"
    with pytest.raises(ValueError):
        first.mark_started("id", claim.token)


def test_failures_before_and_after_initiation_have_different_outcomes(tmp_path: Path) -> None:
    from actuation_signal_publisher.ledger import CommandLedger

    store = CommandLedger(tmp_path / "commands.sqlite3")
    early = store.claim("early", "digest", deadline=2_000_000_000)
    result = store.fail("early", early.token, reason="ConnectionError")
    assert result.outcome == "not_executed" and result.retryable
    assert store.claim("early", "digest", deadline=2_000_000_000).token is not None
    late = store.claim("late", "digest", deadline=2_000_000_000)
    store.mark_started("late", late.token)
    result = store.fail("late", late.token, reason="TimeoutError")
    assert result.outcome == "unknown" and not result.retryable
    assert store.claim("late", "digest", deadline=2_000_000_000).token is None


def test_retention_holds_identity_until_after_deadline_and_retention_window(tmp_path: Path) -> None:
    from actuation_signal_publisher.ledger import CommandLedger

    now = [1_800_000_000.0]
    store = CommandLedger(tmp_path / "commands.sqlite3", retention_s=60, clock=lambda: now[0])
    claim = store.claim("id", "digest", deadline=now[0] + 120)
    store.mark_started("id", claim.token)
    store.complete("id", claim.token, written_at=datetime.fromtimestamp(now[0], UTC))
    now[0] += 179
    assert store.claim("id", "changed", deadline=now[0] + 10).result.outcome == "conflict"
    now[0] += 2
    assert store.claim("id", "changed", deadline=now[0] + 10).token is not None


async def invoke_writer(worker, command, key, *, approved=None):
    from unittest.mock import AsyncMock, MagicMock

    from eirvah_contracts.authorization import ApprovedWriteRequest, sign_approval
    from eirvah_contracts.envelope import NATSEnvelope

    approval = approved or sign_approval(command, key, "actuation-signal-publisher")
    envelope = NATSEnvelope(
        correlation_id=command.correlation_id,
        payload=ApprovedWriteRequest(command=command, approval=approval).model_dump(mode="json"),
    )
    message = MagicMock(data=envelope.model_dump_json().encode(), respond=AsyncMock())
    await worker._handle(message)
    reply = NATSEnvelope.model_validate_json(message.respond.call_args.args[0])
    from eirvah_contracts.actuation import WriteResult

    return WriteResult.model_validate(reply.payload)


def make_writer(path):
    from unittest.mock import MagicMock, patch

    from actuation_signal_publisher.config import SignalPublisherSettings
    from actuation_signal_publisher.ledger import CommandLedger
    from actuation_signal_publisher.service import SignalPublisherWorker

    key = "test-approval-key-with-at-least-32-bytes"
    ledger = CommandLedger(path)
    ledger.recover()
    with patch("actuation_signal_publisher.service.make_counter"):
        worker = SignalPublisherWorker(SignalPublisherSettings(approval_key=key), ledger=ledger)
    worker._native_targets["uniza/zilina/factory1/line_a/capper/motor_01/speed_setpoint"] = (
        MagicMock()
    )
    return worker, key


async def test_writer_replays_original_completion_after_restart_and_deadline(
    tmp_path: Path,
) -> None:
    from datetime import timedelta
    from unittest.mock import patch

    from control import build_command
    from eirvah_contracts.authorization import sign_approval

    worker, key = make_writer(tmp_path / "commands.sqlite3")
    command = build_command("capper", "speed", 1200)
    approval = sign_approval(command, key, "actuation-signal-publisher")
    effects = []

    async def native(target, value, *, before_write):
        before_write()
        effects.append(value)

    with patch("actuation_signal_publisher.service.write_native_value", side_effect=native):
        first = await invoke_writer(worker, command, key, approved=approval)
        reopened, _ = make_writer(tmp_path / "commands.sqlite3")
        with patch(
            "eirvah_contracts.authorization.current_time",
            return_value=command.deadline + timedelta(seconds=1),
        ):
            duplicate = await invoke_writer(reopened, command, key, approved=approval)
    assert effects == [1200]
    assert first.outcome == duplicate.outcome == "completed"
    assert first.written_at == duplicate.written_at and duplicate.replayed


async def test_writer_refuses_changed_command_with_reused_id(tmp_path: Path) -> None:
    from unittest.mock import patch

    from control import build_command

    worker, key = make_writer(tmp_path / "commands.sqlite3")
    command = build_command("capper", "speed", 1200)
    effects = []

    async def native(target, value, *, before_write):
        before_write()
        effects.append(value)

    with patch("actuation_signal_publisher.service.write_native_value", side_effect=native):
        await invoke_writer(worker, command, key)
        conflict = await invoke_writer(
            worker, command.model_copy(update={"requested_value": 1300}), key
        )
    assert effects == [1200] and conflict.outcome == "conflict"


async def test_cancel_after_initiation_recovers_as_unknown_without_reexecution(
    tmp_path: Path,
) -> None:
    import asyncio
    from unittest.mock import patch

    from control import build_command

    worker, key = make_writer(tmp_path / "commands.sqlite3")
    command = build_command("capper", "speed", 1200)
    effects = []

    async def native(target, value, *, before_write):
        before_write()
        effects.append(value)
        raise asyncio.CancelledError

    with patch("actuation_signal_publisher.service.write_native_value", side_effect=native):
        with pytest.raises(asyncio.CancelledError):
            await invoke_writer(worker, command, key)
        reopened, _ = make_writer(tmp_path / "commands.sqlite3")
        outcome = await invoke_writer(reopened, command, key)
    assert effects == [1200] and outcome.outcome == "unknown"


async def test_only_pre_write_failure_allows_a_new_attempt(tmp_path: Path) -> None:
    from unittest.mock import patch

    from control import build_command

    worker, key = make_writer(tmp_path / "commands.sqlite3")
    command = build_command("capper", "speed", 1200)
    with patch(
        "actuation_signal_publisher.service.write_native_value", side_effect=ConnectionError
    ):
        early = await invoke_writer(worker, command, key)
    assert early.outcome == "not_executed" and early.retryable

    async def late(target, value, *, before_write):
        before_write()
        raise TimeoutError

    with patch("actuation_signal_publisher.service.write_native_value", side_effect=late) as write:
        unknown = await invoke_writer(worker, command, key)
        replay = await invoke_writer(worker, command, key)
    assert write.call_count == 1
    assert unknown.outcome == replay.outcome == "unknown"


async def test_simultaneous_duplicate_is_pending_without_second_write(tmp_path: Path) -> None:
    import asyncio
    from unittest.mock import patch

    from control import build_command

    worker, key = make_writer(tmp_path / "commands.sqlite3")
    command = build_command("capper", "speed", 1200)
    started, release = asyncio.Event(), asyncio.Event()

    async def native(target, value, *, before_write):
        before_write()
        started.set()
        await release.wait()

    with patch(
        "actuation_signal_publisher.service.write_native_value", side_effect=native
    ) as write:
        running = asyncio.create_task(invoke_writer(worker, command, key))
        await started.wait()
        try:
            pending = await invoke_writer(worker, command, key)
        finally:
            release.set()
        completed = await running
    assert write.call_count == 1
    assert pending.outcome == "pending" and completed.outcome == "completed"


@pytest.mark.parametrize(
    "outcome,decision",
    [
        ("completed", "approve"),
        ("pending", "pending"),
        ("unknown", "unknown"),
        ("conflict", "reject"),
        ("not_executed", "reject"),
        ("timeout", "unknown"),
        ("malformed", "unknown"),
    ],
)
async def test_pipeline_reports_execution_uncertainty_and_replayed_results(
    outcome: str, decision: str
) -> None:
    import json
    from unittest.mock import AsyncMock, MagicMock

    from actuation_control_orchestrator.metrics import ActuationMetrics
    from actuation_control_orchestrator.models import load_pipeline_config
    from actuation_control_orchestrator.pipeline import run_actuation_pipeline
    from control import build_command
    from eirvah_bus.request_reply import RequestTimeout
    from eirvah_contracts.actuation import ValidationResult, WriteResult
    from eirvah_contracts.authorization import AuthenticatedRequest, sign_approval, sign_ingress
    from eirvah_contracts.envelope import NATSEnvelope
    from prometheus_client.registry import CollectorRegistry

    root = Path(__file__).resolve().parents[2]
    command = build_command("capper", "speed", 1200)
    key = "test-authentication-key-at-least-32-bytes"
    authenticated = AuthenticatedRequest(command=command, identity=sign_ingress(command, key))
    at = datetime.now(UTC)

    async def rpc(*, nc, subject, payload, correlation_id, timeout_s):
        if subject == "act.work.validate":
            result = ValidationResult(
                decision="approve",
                approval=sign_approval(command, key, "actuation-signal-publisher"),
            ).model_dump(mode="json")
        elif outcome == "timeout":
            raise RequestTimeout("reply lost")
        elif outcome == "malformed":
            return MagicMock(data=b"not-json")
        else:
            result = WriteResult(
                outcome=outcome,
                written_at=at if outcome == "completed" else None,
                reason="test",
                retryable=outcome == "not_executed",
                replayed=True,
            ).model_dump(mode="json")
        return MagicMock(
            data=NATSEnvelope(correlation_id=correlation_id, payload=result)
            .model_dump_json()
            .encode()
        )

    nc = MagicMock(publish=AsyncMock())
    exchange = MagicMock(publish=AsyncMock())
    reg = CollectorRegistry()
    await run_actuation_pipeline(
        envelope=NATSEnvelope(
            correlation_id=command.correlation_id, payload=authenticated.model_dump(mode="json")
        ),
        cfg=load_pipeline_config(root / "config/pipelines/actuation-control.yaml"),
        nc=nc,
        amqp_results_exchange=exchange,
        metrics=ActuationMetrics(registry=reg),
        allow_writes=True,
        request_reply_fn=rpc,
    )
    body = json.loads(exchange.publish.call_args.args[0].body)
    assert body["decision"] == decision
    if decision in {"unknown", "pending"}:
        nc.publish.assert_not_called()
    if outcome == "completed":
        assert datetime.fromisoformat(body["written_at"]) == at
        assert body["replayed"]
        assert reg.get_sample_value("eirvah_actuation_approved_total", {"path": "actuation"}) in (
            None,
            0,
        )


def test_control_retry_uses_saved_command_unchanged(tmp_path: Path, monkeypatch) -> None:
    import json
    import sys
    from unittest.mock import AsyncMock, patch

    import control
    from control import build_command

    command = build_command("capper", "speed", 1200)
    path = tmp_path / "command.json"
    path.write_text(command.model_dump_json(), encoding="utf-8")
    monkeypatch.setenv("EIRVAH_COMMAND_AMQP_URL", "amqp://test")
    monkeypatch.setattr(sys, "argv", ["control.py", "--retry-command", str(path)])
    with patch(
        "control.send", new_callable=AsyncMock, return_value={"decision": "approve"}
    ) as send:
        control.main()
    replay = send.call_args.args[0]
    assert replay.model_dump(mode="json") == json.loads(path.read_text())


def test_control_save_never_overwrites_existing_evidence(tmp_path: Path, monkeypatch) -> None:
    import sys
    from unittest.mock import AsyncMock, patch

    import control

    path = tmp_path / "command.json"
    path.write_text("preserve me", encoding="utf-8")
    monkeypatch.setenv("EIRVAH_COMMAND_AMQP_URL", "amqp://test")
    monkeypatch.setattr(
        sys,
        "argv",
        ["control.py", "--target", "capper", "--action", "start", "--save-command", str(path)],
    )
    with patch("control.send", new_callable=AsyncMock) as send, pytest.raises(SystemExit):
        control.main()
    send.assert_not_called()
    assert path.read_text() == "preserve me"


@pytest.mark.parametrize("started", [False, True])
def test_abrupt_process_exit_preserves_recovery_state(tmp_path: Path, started: bool) -> None:
    import subprocess
    import sys

    from actuation_signal_publisher.ledger import CommandLedger

    path = tmp_path / "commands.sqlite3"
    program = "import os,sys; from pathlib import Path; from actuation_signal_publisher.ledger import CommandLedger; s=CommandLedger(Path(sys.argv[1])); c=s.claim('id','digest',deadline=2000000000); "
    if started:
        program += "s.mark_started('id',c.token); "
    program += "os._exit(17)"
    process = subprocess.run([sys.executable, "-c", program, str(path)], capture_output=True)
    assert process.returncode == 17
    recovered = CommandLedger(path)
    recovered.recover()
    claim = recovered.claim("id", "digest", deadline=2_000_000_000)
    if started:
        assert claim.token is None and claim.result.outcome == "unknown"
    else:
        assert claim.token is not None


async def test_failed_completion_commit_is_unknown_and_cannot_reexecute(tmp_path: Path) -> None:
    import sqlite3
    from unittest.mock import patch

    from control import build_command

    worker, key = make_writer(tmp_path / "commands.sqlite3")
    command = build_command("capper", "speed", 1200)
    effects = []

    async def native(target, value, *, before_write):
        before_write()
        effects.append(value)

    with patch("actuation_signal_publisher.service.write_native_value", side_effect=native):
        with patch.object(
            worker._ledger, "complete", side_effect=sqlite3.OperationalError("disk unavailable")
        ):
            result = await invoke_writer(worker, command, key)
        same_process = await invoke_writer(worker, command, key)
        recovered, _ = make_writer(tmp_path / "commands.sqlite3")
        replay = await invoke_writer(recovered, command, key)
    assert (
        effects == [1200] and result.outcome == same_process.outcome == replay.outcome == "unknown"
    )


@pytest.mark.parametrize("loss", ["reply", "completion_commit"])
async def test_real_modbus_effect_is_not_repeated_after_lost_completion(
    tmp_path: Path, loss: str
) -> None:
    import asyncio
    import socket
    import sqlite3
    from contextlib import suppress
    from unittest.mock import AsyncMock, MagicMock, patch

    from actuation_signal_publisher.native import NativeTarget
    from control import build_command
    from eirvah_contracts.authorization import ApprovedWriteRequest, sign_approval
    from eirvah_contracts.envelope import NATSEnvelope
    from modbus_simulator.config import SimulatorSettings
    from modbus_simulator.server import SimulatorRuntime
    from pymodbus.client import AsyncModbusTcpClient
    from pymodbus.server import ServerAsyncStop

    with socket.socket() as socket_:
        socket_.bind(("127.0.0.1", 0))
        port = socket_.getsockname()[1]
    with patch("modbus_simulator.server.SimulatorMetrics"):
        plant = SimulatorRuntime(SimulatorSettings(host="127.0.0.1", port=port, tick_rate_ms=50))
    plant._block.conveyor.running = False
    plant._block.reject.observe(42)
    task = asyncio.create_task(plant.run())
    worker, key = make_writer(tmp_path / "commands.sqlite3")
    command = build_command("reject", "reject", 42)
    target = NativeTarget(
        protocol="modbus",
        host="127.0.0.1",
        port=port,
        command_address=28,
        ack_address=54,
        operation=1,
        value_type="int64",
        minimum=1,
        maximum=65535,
    )
    worker._native_targets[command.target_uns_topic] = target
    approval = sign_approval(command, key, "actuation-signal-publisher")
    envelope = NATSEnvelope(
        correlation_id=command.correlation_id,
        payload=ApprovedWriteRequest(command=command, approval=approval).model_dump(mode="json"),
    )
    message = MagicMock(
        data=envelope.model_dump_json().encode(),
        respond=AsyncMock(side_effect=ConnectionError("reply lost") if loss == "reply" else None),
    )
    try:
        await asyncio.sleep(0.2)
        if loss == "reply":
            with pytest.raises(ConnectionError):
                await worker._handle(message)
        else:
            with patch.object(
                worker._ledger,
                "complete",
                side_effect=sqlite3.OperationalError("commit unavailable"),
            ):
                await worker._handle(message)
        assert plant._block.reject.count == 1
        async with AsyncModbusTcpClient("127.0.0.1", port=port) as client:
            before = await client.read_holding_registers(54, count=2, slave=1)
            recovered, _ = make_writer(tmp_path / "commands.sqlite3")
            recovered._native_targets[command.target_uns_topic] = target
            result = await invoke_writer(recovered, command, key, approved=approval)
            after = await client.read_holding_registers(54, count=2, slave=1)
        assert plant._block.reject.count == 1 and before.registers == after.registers
        assert result.outcome == ("completed" if loss == "reply" else "unknown")
    finally:
        await ServerAsyncStop()
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


def test_persistent_volume_and_single_writer_configuration_match_ledger_path() -> None:
    import yaml
    from actuation_signal_publisher.config import SignalPublisherSettings

    root = Path(__file__).resolve().parents[2] / "deploy/k3s/base/actuation-signal-publisher"
    deployment = yaml.safe_load((root / "deployment.yaml").read_text())
    volume = yaml.safe_load((root / "pvc.yaml").read_text())
    spec = deployment["spec"]
    pod = spec["template"]["spec"]
    assert spec["replicas"] == 1 and spec["strategy"]["type"] == "Recreate"
    mount = next(m for m in pod["containers"][0]["volumeMounts"] if m["name"] == "command-ledger")
    claim = next(v for v in pod["volumes"] if v["name"] == "command-ledger")
    assert claim["persistentVolumeClaim"]["claimName"] == volume["metadata"]["name"]
    assert Path(mount["mountPath"]) == SignalPublisherSettings().ledger_path.parent
    assert pod["securityContext"]["fsGroup"] == 65534


async def test_unreadable_command_history_is_unknown_without_another_write(tmp_path: Path) -> None:
    import sqlite3
    from unittest.mock import patch

    from control import build_command

    worker, key = make_writer(tmp_path / "commands.sqlite3")
    command = build_command("capper", "speed", 1200)
    with (
        patch.object(
            worker._ledger, "lookup", side_effect=sqlite3.OperationalError("cannot read history")
        ),
        patch("actuation_signal_publisher.service.write_native_value") as native,
    ):
        result = await invoke_writer(worker, command, key)
    assert result.outcome == "unknown" and not result.retryable
    native.assert_not_called()
