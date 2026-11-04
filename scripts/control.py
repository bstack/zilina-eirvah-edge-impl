"""Send one command through the normal AMQP path and wait for its correlated result."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import aio_pika
from eirvah_contracts.actuation import ActuationRequest
from eirvah_contracts.signals import SignalValue, SignalValueType
from eirvah_contracts.ulid import generate_correlation_id

PREFIX = "uniza/zilina/factory1/line_a/"


def build_command(target: str, action: str, value: object = None) -> ActuationRequest:
    kind: SignalValueType
    if target == "temperature" and action == "set":
        path, kind = "bottler/setpoint_unit/setpoint_temperature", "double"
    elif target in {"conveyor", "capper"}:
        base = "conveyor/belt_01" if target == "conveyor" else "capper/motor_01"
        if action in {"start", "stop"}:
            path, kind, value = base + "/run", "bool", action == "start"
        elif action == "speed":
            path, kind = base + "/speed_setpoint", "double" if target == "conveyor" else "int64"
        elif action == "reset":
            path, kind, value = base + "/reset_fault", "bool", True
        else:
            raise ValueError("motor action must be start, stop, speed or reset")
    elif target == "filler" and action in {"open", "close"}:
        path, kind, value = "filler/valve_01/open", "bool", action == "open"
    elif target == "reject" and action == "reject":
        path, kind = "reject_station/rejector_01/reject_bottle", "int64"
    else:
        raise ValueError("unsupported target/action combination")
    if kind == "int64" and type(value) is not int:
        raise ValueError("this command requires an integer --value")
    if kind == "double" and type(value) not in (int, float):
        raise ValueError("this command requires a numeric --value")
    now = datetime.now(UTC)
    return ActuationRequest(
        correlation_id=generate_correlation_id(),
        requester="experiment-driver",
        target_uns_topic=PREFIX + path,
        requested_value=cast(SignalValue, value),
        value_type=kind,
        reason="local functional control check",
        requested_at=now,
        deadline=now + timedelta(seconds=10),
    )


async def send(command: ActuationRequest, amqp_url: str) -> dict[str, Any]:
    connection = await aio_pika.connect_robust(amqp_url)
    async with connection:
        channel = await connection.channel()
        exchange = await channel.get_exchange("eirvah.actuation.results")
        queue = await channel.declare_queue(exclusive=True, auto_delete=True)
        await queue.bind(exchange, routing_key="actuation.result")
        result: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()

        async def consume(message: aio_pika.abc.AbstractIncomingMessage) -> None:
            async with message.process():
                data = json.loads(message.body)
                if data.get("correlation_id") == command.correlation_id and not result.done():
                    result.set_result(data)

        await queue.consume(consume)
        commands = await channel.get_exchange("eirvah.actuation.commands")
        await commands.publish(
            aio_pika.Message(
                body=command.model_dump_json().encode(),
                user_id="experiment-driver",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key="eirvah.actuation.requests",
        )
        print("command_id=" + command.correlation_id, flush=True)
        return await asyncio.wait_for(result, timeout=12)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--target", choices=["temperature", "conveyor", "filler", "capper", "reject"]
    )
    parser.add_argument(
        "--action",
        choices=["set", "start", "stop", "speed", "reset", "open", "close", "reject"],
    )
    parser.add_argument(
        "--retry-command",
        type=Path,
        help="Replay the exact saved command without changing its ID, payload or deadline",
    )
    parser.add_argument(
        "--save-command",
        type=Path,
        help="Save the command before submitting it; never overwrite an existing file",
    )
    parser.add_argument("--value", type=json.loads)
    parser.add_argument("--amqp-url", default=os.environ.get("EIRVAH_COMMAND_AMQP_URL"))
    args = parser.parse_args()
    if not args.amqp_url:
        parser.error("set EIRVAH_COMMAND_AMQP_URL or --amqp-url with experiment-driver credentials")
    try:
        if args.retry_command:
            if args.target or args.action or args.value is not None or args.save_command:
                parser.error("--retry-command cannot be combined with new command arguments")
            command = ActuationRequest.model_validate_json(
                args.retry_command.read_text(encoding="utf-8")
            )
        else:
            if not args.target or not args.action:
                parser.error("--target and --action are required for a new command")
            command = build_command(args.target, args.action, args.value)
            if args.save_command:
                with args.save_command.open("x", encoding="utf-8") as saved:
                    saved.write(command.model_dump_json(indent=2) + "\n")
    except (ValueError, OSError) as exc:
        parser.error(str(exc))
    try:
        result = asyncio.run(send(command, args.amqp_url))
    except TimeoutError:
        print(
            "Outcome UNKNOWN: no correlated result. Replay the saved command to query retained status; do not create a replacement command blindly."
        )
        raise SystemExit(2) from None
    print(json.dumps(result, indent=2))
    if result.get("decision") in {"unknown", "pending"}:
        raise SystemExit(2)
    if result.get("decision") != "approve":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
