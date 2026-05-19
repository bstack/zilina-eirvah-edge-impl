"""Decision agent stub — closes the CPS loop (spec §3.3)."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta

import aio_pika
import aiomqtt
import structlog
from eirvah_contracts.actuation import ActuationRequest
from eirvah_contracts.ulid import generate_correlation_id
from eirvah_observability.logging import configure_logging
from eirvah_observability.metrics import make_counter

from decision_agent_stub.config import DecisionAgentSettings

_log = structlog.get_logger("decision-agent-stub")


class TriggerWindow:
    """Tracks sustained threshold breach and fires actuation request when triggered."""

    def __init__(
        self,
        *,
        threshold: float,
        duration_s: float,
        cooldown_s: float = 60.0,
        setpoint_target: float = 22.0,
        target_uns_topic: str = (
            "uniza/zilina/factory1/line_a/bottler/setpoint_unit/setpoint_temperature"
        ),
    ) -> None:
        self._threshold = threshold
        self._duration_s = duration_s
        self._cooldown_s = cooldown_s
        self._setpoint_target = setpoint_target
        self._target_uns_topic = target_uns_topic
        self._breach_start: datetime | None = None
        self._last_fired: datetime | None = None

    def update(
        self,
        *,
        value: float,
        ts: datetime,
        correlation_id: str,
    ) -> ActuationRequest | None:
        if value <= self._threshold:
            self._breach_start = None
            return None

        if self._breach_start is None:
            self._breach_start = ts
            return None

        if (ts - self._breach_start).total_seconds() < self._duration_s:
            return None

        # Breach sustained — check cooldown
        if self._last_fired is not None:
            if (ts - self._last_fired).total_seconds() < self._cooldown_s:
                return None

        self._last_fired = ts
        self._breach_start = None  # reset for next cycle
        now = datetime.now(UTC)
        return ActuationRequest(
            correlation_id=correlation_id,
            requester="decision-agent-stub",
            target_uns_topic=self._target_uns_topic,
            requested_value=self._setpoint_target,
            value_type="double",
            reason=f"telemetry threshold breach: temperature > {self._threshold} for {self._duration_s}s",
            requested_at=now,
            deadline=now + timedelta(seconds=10),
        )


class DecisionAgentRuntime:
    def __init__(self, settings: DecisionAgentSettings) -> None:
        self._settings = settings
        self._window = TriggerWindow(
            threshold=settings.threshold,
            duration_s=settings.trigger_duration_s,
            cooldown_s=settings.cooldown_s,
            setpoint_target=settings.setpoint_target,
            target_uns_topic=settings.target_uns_topic,
        )
        self._fired = make_counter(
            "decision_agent_actuation_fired_total",
            "Actuation requests emitted",
            labelnames=["reason"],
        )

    async def run(self) -> None:
        configure_logging(level=self._settings.log_level)
        amqp_conn = await aio_pika.connect_robust(self._settings.amqp_url)

        async with amqp_conn:
            amqp_channel = await amqp_conn.channel()

            async with aiomqtt.Client(
                hostname=self._settings.mqtt_host,
                port=self._settings.mqtt_port,
                username=self._settings.mqtt_username,
                password=self._settings.mqtt_password,
            ) as mqtt:
                await mqtt.subscribe(self._settings.subscribe_topic, qos=1)
                _log.info(
                    "decision_agent_ready",
                    topic=self._settings.subscribe_topic,
                    threshold=self._settings.threshold,
                )

                async for message in mqtt.messages:
                    try:
                        payload = json.loads(message.payload)
                        value = float(payload["value"])
                        correlation_id = payload.get("correlation_id") or generate_correlation_id()
                        ts = datetime.now(UTC)
                        req = self._window.update(value=value, ts=ts, correlation_id=correlation_id)
                        if req is not None:
                            await amqp_channel.default_exchange.publish(
                                aio_pika.Message(
                                    body=req.model_dump_json().encode(),
                                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                                ),
                                routing_key=self._settings.amqp_queue,
                            )
                            self._fired.labels(reason="threshold_breach").inc()
                            _log.info(
                                "actuation_request_emitted",
                                correlation_id=req.correlation_id,
                                value=value,
                            )
                    except Exception as exc:
                        _log.warning("message_processing_error", error=str(exc))


async def run(settings: DecisionAgentSettings) -> None:
    runtime = DecisionAgentRuntime(settings)
    await runtime.run()
