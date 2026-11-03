"""Opt-in AMQP 0-9-1 tests in a disposable loopback-only RabbitMQ container."""

from __future__ import annotations

import base64
import os
import secrets
import socket
import subprocess
import time
from unittest.mock import AsyncMock
from urllib.parse import urlsplit, urlunsplit

import aio_pika
import pytest
from aiormq.exceptions import AMQPError
from configure_actuation_security import build_secret
from control import build_command

pytestmark = pytest.mark.skipif(
    os.environ.get("EIRVAH_RUN_BROKER_TESTS") != "1",
    reason="Set EIRVAH_RUN_BROKER_TESTS=1 for disposable Docker broker checks",
)


@pytest.fixture(scope="module")
def broker(tmp_path_factory):
    directory = tmp_path_factory.mktemp("command-broker")
    data = {
        key: base64.b64decode(value).decode() for key, value in build_secret("test")["data"].items()
    }
    (directory / "definitions.json").write_text(data["definitions.json"], encoding="utf-8")
    (directory / "rabbitmq.conf").write_text(
        "load_definitions = /etc/rabbitmq/definitions.json\n", encoding="utf-8"
    )
    name = "eirvah-auth-test-" + secrets.token_hex(6)
    subprocess.run(
        [
            "docker",
            "run",
            "--detach",
            "--rm",
            "--cpus",
            "2",
            "-e",
            "RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS=+S 2:2 +SDcpu 1 +SDio 1",
            "--name",
            name,
            "-p",
            "127.0.0.1::5672",
            "--mount",
            f"type=bind,src={directory},dst=/etc/rabbitmq,readonly",
            "rabbitmq:3.13-management",
        ],
        capture_output=True,
        check=True,
    )
    try:
        mapped = subprocess.check_output(
            ["docker", "port", name, "5672/tcp"], text=True, timeout=10
        ).strip()
        port = int(mapped.rsplit(":", 1)[1])
        until = time.monotonic() + 90
        while True:
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=1) as probe:
                    probe.sendall(b"AMQP\x00\x00\x09\x01")
                    if probe.recv(1) != b"\x01":
                        raise OSError("AMQP not ready")
                    break
            except OSError:
                if time.monotonic() >= until:
                    logs = subprocess.check_output(
                        ["docker", "logs", name], stderr=subprocess.STDOUT, text=True, timeout=10
                    )
                    for value in data.values():
                        logs = logs.replace(value, "[redacted]")
                    raise RuntimeError(
                        "Disposable RabbitMQ did not become ready: " + logs[-2500:]
                    ) from None
                time.sleep(1)
        yield data, port
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True, check=False)
        (directory / "definitions.json").unlink(missing_ok=True)


def local_url(value: str, port: int) -> str:
    parts = urlsplit(value)
    return urlunsplit(
        (parts.scheme, parts.netloc.split("@", 1)[0] + f"@127.0.0.1:{port}", parts.path, "", "")
    )


@pytest.mark.parametrize(
    "key,user",
    [("decision-amqp-url", "decision-agent-stub"), ("experiment-amqp-url", "experiment-driver")],
)
@pytest.mark.parametrize("queue_style", ["server", "client"])
async def test_broker_authenticates_publisher_and_allows_result_subscription(
    broker, key: str, user: str, queue_style: str
) -> None:
    from amqp_actuation_event_subscriber.service import build_nats_envelope
    from eirvah_contracts.authorization import AuthenticatedRequest, verify_evidence

    data, port = broker
    producer = await aio_pika.connect(local_url(data[key], port))
    ingress = await aio_pika.connect(local_url(data["ingress-amqp-url"], port))
    async with producer, ingress:
        channel = await producer.channel()
        results = await channel.get_exchange("eirvah.actuation.results")
        temporary = await channel.declare_queue(
            "" if queue_style == "server" else "amq_" + secrets.token_hex(16),
            exclusive=True,
            auto_delete=True,
        )
        await temporary.bind(results, routing_key="actuation.result")
        commands = await channel.get_exchange("eirvah.actuation.commands")
        command = build_command("capper", "speed", 1200).model_copy(update={"requester": "forged"})
        await commands.publish(
            aio_pika.Message(body=command.model_dump_json().encode(), user_id=user),
            routing_key="eirvah.actuation.requests",
        )
        reader = await ingress.channel()
        queue = await reader.declare_queue("eirvah.actuation.requests", durable=True)
        message = await queue.get(timeout=5)
        assert message is not None
        assert message.user_id == user
        authenticated = AuthenticatedRequest.model_validate(
            build_nats_envelope(
                message.body, user_id=message.user_id, ingress_key=data["ingress-key"]
            ).payload
        )
        assert authenticated.command.requester == user
        verify_evidence(
            authenticated.command, authenticated.identity, data["ingress-key"], "ingress", ""
        )
        await message.ack()


@pytest.mark.parametrize(
    "violation", ["spoof_user", "publish_outcome", "default_exchange", "consume_commands"]
)
async def test_broker_denies_producer_impersonation_and_privileged_operations(
    broker, violation: str
) -> None:
    data, port = broker
    connection = await aio_pika.connect(local_url(data["experiment-amqp-url"], port))
    async with connection:
        channel = await connection.channel()
        with pytest.raises(AMQPError):
            if violation == "consume_commands":
                queue = await channel.get_queue("eirvah.actuation.requests", ensure=False)
                await queue.consume(AsyncMock(), no_ack=True)
            else:
                exchange = (
                    channel.default_exchange
                    if violation == "default_exchange"
                    else await channel.get_exchange(
                        "eirvah.actuation.results"
                        if violation == "publish_outcome"
                        else "eirvah.actuation.commands"
                    )
                )
                await exchange.publish(
                    aio_pika.Message(
                        body=b"test",
                        user_id="decision-agent-stub"
                        if violation == "spoof_user"
                        else "experiment-driver",
                    ),
                    routing_key="eirvah.actuation.requests",
                )
