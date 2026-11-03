from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from control import build_command
from eirvah_contracts.envelope import NATSEnvelope

KEY = "validator-writer-test-key-32-bytes-minimum"
INGRESS_KEY = "bridge-validator-test-key-32-bytes-minimum"
WRITER = "actuation-signal-publisher"


def test_approval_binds_entire_command_writer_and_signature() -> None:
    from eirvah_contracts.authorization import sign_approval, verify_evidence

    command = build_command("capper", "speed", 1200)
    approval = sign_approval(command, KEY, WRITER)
    verify_evidence(command, approval, KEY, "approval", WRITER)
    for change in (
        {"requested_value": 1500},
        {"target_uns_topic": command.target_uns_topic.replace("capper", "other")},
        {"correlation_id": build_command("capper", "stop").correlation_id},
        {"deadline": command.deadline + timedelta(seconds=1)},
        {"requester": "other"},
    ):
        with pytest.raises(ValueError):
            verify_evidence(command.model_copy(update=change), approval, KEY, "approval", WRITER)
    with pytest.raises(ValueError):
        verify_evidence(command, approval, KEY, "approval", "another-writer")
    with pytest.raises(ValueError):
        verify_evidence(
            command, approval, "wrong-key-with-at-least-32-characters", "approval", WRITER
        )
    with pytest.raises(ValueError):
        verify_evidence(
            command, approval.model_copy(update={"signature": "0" * 64}), KEY, "approval", WRITER
        )


def test_approval_rejects_missing_naive_expired_and_future_evidence() -> None:
    from eirvah_contracts.authorization import sign_approval, verify_evidence

    command = build_command("filler", "open")
    now = datetime.now(UTC)
    for deadline in (None, now.replace(tzinfo=None), now - timedelta(seconds=1)):
        with pytest.raises(ValueError):
            sign_approval(command.model_copy(update={"deadline": deadline}), KEY, WRITER)
    approval = sign_approval(command, KEY, WRITER)
    with pytest.raises(ValueError):
        verify_evidence(
            command,
            approval,
            KEY,
            "approval",
            WRITER,
            now=command.deadline + timedelta(microseconds=1),
        )
    with pytest.raises(ValueError):
        verify_evidence(command, approval, KEY, "approval", WRITER, now=now - timedelta(seconds=1))
    with pytest.raises(ValueError):
        sign_approval(command, "", WRITER)


def test_ingress_uses_broker_identity_and_authenticates_nats_handoff() -> None:
    from amqp_actuation_event_subscriber.service import build_nats_envelope
    from eirvah_contracts.authorization import AuthenticatedRequest, verify_evidence

    command = build_command("conveyor", "start").model_copy(update={"requester": "forged"})
    envelope = build_nats_envelope(
        command.model_dump_json().encode(), user_id="experiment-driver", ingress_key=INGRESS_KEY
    )
    authenticated = AuthenticatedRequest.model_validate(envelope.payload)
    assert authenticated.command.requester == "experiment-driver"
    verify_evidence(authenticated.command, authenticated.identity, INGRESS_KEY, "ingress", "")
    for user in (None, "eirvah", "unknown"):
        with pytest.raises(ValueError):
            build_nats_envelope(
                command.model_dump_json().encode(), user_id=user, ingress_key=INGRESS_KEY
            )
    with pytest.raises(ValueError):
        verify_evidence(authenticated.command, authenticated.identity, KEY, "approval", WRITER)


async def test_validator_signs_only_authenticated_permitted_commands() -> None:
    from actuation_event_validator.config import ValidatorSettings
    from actuation_event_validator.service import ValidatorWorker, load_policy
    from amqp_actuation_event_subscriber.service import build_nats_envelope
    from eirvah_contracts.actuation import ValidationResult
    from eirvah_contracts.authorization import verify_evidence

    root = __import__("pathlib").Path(__file__).resolve().parents[2]
    with patch("actuation_event_validator.service.make_counter"):
        worker = ValidatorWorker(ValidatorSettings(ingress_key=INGRESS_KEY, approval_key=KEY))
    worker._policies = load_policy(
        root / "deploy/k3s/base/actuation-event-validator/actuation-policy.yaml"
    )
    for speed, expected in ((1200, "approve"), (2000, "reject")):
        command = build_command("capper", "speed", speed)
        env = build_nats_envelope(
            command.model_dump_json().encode(), user_id="experiment-driver", ingress_key=INGRESS_KEY
        )
        msg = MagicMock(data=env.model_dump_json().encode(), respond=AsyncMock())
        await worker._handle(msg)
        reply = NATSEnvelope.model_validate_json(msg.respond.call_args.args[0])
        result = ValidationResult.model_validate(reply.payload)
        assert result.decision == expected
        if expected == "approve":
            assert result.approval is not None
            verify_evidence(command, result.approval, KEY, "approval", WRITER)
        else:
            assert result.approval is None
    raw = NATSEnvelope(
        correlation_id=command.correlation_id, payload=command.model_dump(mode="json")
    )
    msg = MagicMock(data=raw.model_dump_json().encode(), respond=AsyncMock())
    await worker._handle(msg)
    assert NATSEnvelope.model_validate_json(msg.respond.call_args.args[0]).status == "error"


@pytest.mark.parametrize(
    "mode", ["valid", "missing", "tampered", "expired", "wrong_writer", "wrong_envelope"]
)
async def test_writer_never_calls_native_write_without_matching_live_approval(mode: str) -> None:
    from actuation_signal_publisher.config import SignalPublisherSettings
    from actuation_signal_publisher.service import SignalPublisherWorker
    from eirvah_contracts.authorization import ApprovedWriteRequest, sign_approval

    command = build_command("capper", "speed", 1200)
    approval = sign_approval(command, KEY, WRITER if mode != "wrong_writer" else "another-writer")
    if mode == "tampered":
        command = command.model_copy(update={"requested_value": 1500})
    payload = ApprovedWriteRequest(command=command, approval=approval).model_dump(mode="json")
    if mode == "missing":
        payload = command.model_dump(mode="json")
    env = NATSEnvelope(correlation_id=command.correlation_id, payload=payload)
    if mode == "wrong_envelope":
        env.correlation_id = build_command("capper", "stop").correlation_id
    msg = MagicMock(data=env.model_dump_json().encode(), respond=AsyncMock())
    with (
        patch("actuation_signal_publisher.service.make_counter"),
        patch(
            "actuation_signal_publisher.service.write_native_value", new_callable=AsyncMock
        ) as write,
    ):
        worker = SignalPublisherWorker(SignalPublisherSettings(approval_key=KEY))
        worker._native_targets[command.target_uns_topic] = MagicMock()
        if mode == "expired":
            with patch(
                "eirvah_contracts.authorization.current_time",
                return_value=command.deadline + timedelta(seconds=1),
            ):
                await worker._handle(msg)
        else:
            await worker._handle(msg)
        reply = NATSEnvelope.model_validate_json(msg.respond.call_args.args[0])
        assert reply.status == ("ok" if mode == "valid" else "error")
        assert write.await_count == (1 if mode == "valid" else 0)
        if mode == "valid":
            assert callable(write.call_args.kwargs["before_write"])


async def test_opcua_guard_runs_after_connect_before_write() -> None:
    from actuation_signal_publisher.service import WriteTarget, write_opcua_value

    client = MagicMock()
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=False)
    client.get_namespace_index = AsyncMock(return_value=1)
    node = MagicMock(write_value=AsyncMock())
    client.nodes.objects.get_child = AsyncMock(return_value=node)
    with patch("actuation_signal_publisher.service.Client", return_value=client):

        def expired() -> None:
            raise ValueError("expired")

        with pytest.raises(ValueError, match="expired"):
            await write_opcua_value(
                target=WriteTarget(["setpoint"], "endpoint", "namespace"),
                value=22.0,
                before_write=expired,
            )
        node.write_value.assert_not_awaited()


def test_generated_credentials_separate_producers_and_trust_keys() -> None:
    import base64
    import json
    import re

    from configure_actuation_security import build_secret

    secret = build_secret("research-test")
    data = {key: base64.b64decode(value).decode() for key, value in secret["data"].items()}
    assert data["ingress-key"] != data["approval-key"]
    definitions = json.loads(data["definitions.json"])
    users = {user["name"]: user for user in definitions["users"]}
    permissions = {perm["user"]: perm for perm in definitions["permissions"]}
    for name in ("decision-agent-stub", "experiment-driver"):
        assert users[name]["tags"] == ""
        perm = permissions[name]
        assert re.fullmatch(perm["write"], "eirvah.actuation.commands")
        assert not re.fullmatch(perm["write"], "eirvah.actuation.results")
        assert not re.fullmatch(perm["write"], "amq.default")
        assert not re.fullmatch(perm["read"], "eirvah.actuation.requests")
        assert re.fullmatch(perm["configure"], "amq.gen-test")
        assert re.fullmatch(perm["configure"], "amq_" + "a" * 32)
        assert re.fullmatch(perm["write"], "amq_" + "a" * 32)
        assert re.fullmatch(perm["read"], "amq_" + "a" * 32)
        assert re.fullmatch(perm["read"], "eirvah.actuation.results")
    assert users["decision-agent-stub"]["password"] != users["experiment-driver"]["password"]


@pytest.mark.parametrize("protocol", ["modbus", "s7"])
async def test_native_guard_rechecks_after_connect_before_mailbox_write(protocol: str) -> None:
    from actuation_signal_publisher.native import NativeTarget, write_native_value

    target = NativeTarget(
        protocol=protocol,
        host="host",
        port=102,
        command_address=0,
        ack_address=6,
        operation=1,
        value_type="bool",
        minimum=0,
        maximum=1,
    )
    client = MagicMock()
    client.connect = AsyncMock(return_value=True) if protocol == "modbus" else MagicMock()
    client.read_holding_registers = AsyncMock(
        return_value=MagicMock(registers=[1, 1], isError=lambda: False)
    )
    client.write_registers = AsyncMock()
    client.db_read.return_value = bytes([0, 1, 0, 1])

    def expired() -> None:
        raise ValueError("expired after connection")

    factory = "AsyncModbusTcpClient" if protocol == "modbus" else "S7Client"
    with (
        patch("actuation_signal_publisher.native." + factory, return_value=client),
        pytest.raises(ValueError, match="expired after connection"),
    ):
        await write_native_value(target, True, before_write=expired)
    client.write_registers.assert_not_awaited()
    client.db_write.assert_not_called()


@pytest.mark.parametrize(
    "case", ["valid", "forged_ingress", "missing_approval", "altered_approval"]
)
async def test_authenticated_pipeline_hands_approval_to_writer(case: str) -> None:
    from pathlib import Path

    from actuation_control_orchestrator.metrics import ActuationMetrics
    from actuation_control_orchestrator.models import load_pipeline_config
    from actuation_control_orchestrator.pipeline import run_actuation_pipeline
    from actuation_event_validator.config import ValidatorSettings
    from actuation_event_validator.service import ValidatorWorker, load_policy
    from actuation_signal_publisher.config import SignalPublisherSettings
    from actuation_signal_publisher.service import SignalPublisherWorker
    from amqp_actuation_event_subscriber.service import build_nats_envelope
    from prometheus_client.registry import CollectorRegistry

    root = Path(__file__).resolve().parents[2]
    command = build_command("filler", "open")
    envelope = build_nats_envelope(
        command.model_dump_json().encode(),
        user_id="experiment-driver",
        ingress_key=KEY if case == "forged_ingress" else INGRESS_KEY,
    )
    with (
        patch("actuation_event_validator.service.make_counter"),
        patch("actuation_signal_publisher.service.make_counter"),
    ):
        validator = ValidatorWorker(ValidatorSettings(ingress_key=INGRESS_KEY, approval_key=KEY))
        writer = SignalPublisherWorker(SignalPublisherSettings(approval_key=KEY))
    validator._policies = load_policy(
        root / "deploy/k3s/base/actuation-event-validator/actuation-policy.yaml"
    )
    writer._native_targets[command.target_uns_topic] = MagicMock()
    effects = []

    async def native(target, value, *, before_write):
        before_write()
        effects.append(value)

    async def rpc(*, nc, subject, payload, correlation_id, timeout_s):
        message = MagicMock(data=payload, respond=AsyncMock())
        worker = validator if subject == "act.work.validate" else writer
        await worker._handle(message)
        reply = NATSEnvelope.model_validate_json(message.respond.call_args.args[0])
        if worker is validator and reply.status == "ok" and reply.payload:
            if case == "missing_approval":
                reply.payload["approval"] = None
            elif case == "altered_approval":
                reply.payload["approval"]["signature"] = "0" * 64
        return MagicMock(data=reply.model_dump_json().encode())

    nc = MagicMock(publish=AsyncMock())
    exchange = MagicMock(publish=AsyncMock())
    with patch("actuation_signal_publisher.service.write_native_value", side_effect=native):
        await run_actuation_pipeline(
            envelope=envelope,
            cfg=load_pipeline_config(
                root / "deploy/k3s/base/actuation-control-orchestrator/actuation-control.yaml"
            ),
            nc=nc,
            amqp_results_exchange=exchange,
            metrics=ActuationMetrics(registry=CollectorRegistry()),
            allow_writes=True,
            request_reply_fn=rpc,
        )
    import json

    outcome = json.loads(exchange.publish.call_args.args[0].body)
    assert outcome["decision"] == ("approve" if case == "valid" else "reject")
    assert effects == ([True] if case == "valid" else [])


def test_permission_refresh_preserves_keys_passwords_and_unrelated_fields() -> None:
    import base64
    import json

    from configure_actuation_security import build_secret, refresh_permissions

    secret = build_secret("test")
    original_data = dict(secret["data"])
    definitions = json.loads(base64.b64decode(secret["data"]["definitions.json"]))
    for permission in definitions["permissions"]:
        if permission["user"] in {"decision-agent-stub", "experiment-driver"}:
            permission["configure"] = r"^amq\.gen-.*$"
    secret["data"]["definitions.json"] = base64.b64encode(json.dumps(definitions).encode()).decode()
    assert refresh_permissions(secret)
    assert {k: v for k, v in secret["data"].items() if k != "definitions.json"} == {
        k: v for k, v in original_data.items() if k != "definitions.json"
    }
    refreshed = json.loads(base64.b64decode(secret["data"]["definitions.json"]))
    assert refreshed["users"] == definitions["users"]
    assert not refresh_permissions(secret)
