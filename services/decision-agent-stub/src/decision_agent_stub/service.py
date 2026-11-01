"""Select a warmer recipe from observed conveyor speed-setpoint changes."""

from __future__ import annotations

import json
import math
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


class ProductionModeRule:
    """One request per observed mode transition; no automatic retries of unknown work."""

    def __init__(self, settings: DecisionAgentSettings) -> None:
        self.settings = settings
        self._last_mode: str | None = None
        self._latest_at: datetime | None = None

    def update(
        self, *, speed: float, observed_at: datetime, now: datetime, quality: str = "good"
    ) -> ActuationRequest | None:
        if not self.settings.enabled or quality != "good":
            return None
        if type(speed) not in (int, float) or not math.isfinite(speed) or not 0 <= speed <= 0.8:
            return None
        if observed_at.tzinfo is None or now.tzinfo is None:
            return None
        age = (now - observed_at).total_seconds()
        if not 0 <= age <= self.settings.max_observation_age_s:
            return None
        if self._latest_at is not None and observed_at < self._latest_at:
            return None
        self._latest_at = observed_at
        if speed >= self.settings.high_speed_min:
            mode, target = "high", self.settings.high_setpoint
        elif speed <= self.settings.normal_speed_max:
            mode, target = "normal", self.settings.normal_setpoint
        else:
            return None  # Hysteresis: preserve the selected mode between the bands.
        if mode == self._last_mode:
            return None
        self._last_mode = mode
        return ActuationRequest(
            correlation_id=generate_correlation_id(),
            requester="decision-agent-stub",
            target_uns_topic=self.settings.target_uns_topic,
            requested_value=target,
            value_type="double",
            reason=f"warmer recipe: {mode} production mode, conveyor speed setpoint {speed} m/s",
            requested_at=now,
            deadline=now + timedelta(seconds=10),
        )


class DecisionAgentRuntime:
    def __init__(self, settings: DecisionAgentSettings) -> None:
        self._settings = settings
        self._rule = ProductionModeRule(settings)
        self._fired = make_counter(
            "decision_agent_actuation_fired_total",
            "Actuation requests emitted",
            labelnames=["reason"],
        )

    async def run(self) -> None:
        configure_logging(level=self._settings.log_level)
        amqp_conn = await aio_pika.connect_robust(self._settings.amqp_url)
        async with amqp_conn:
            channel = await amqp_conn.channel()
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
                    enabled=self._settings.enabled,
                )
                async for message in mqtt.messages:
                    try:
                        payload = json.loads(message.payload)
                        if payload.get("eirvah:unit") != "m/s":
                            continue
                        req = self._rule.update(
                            speed=payload["sosa:hasSimpleResult"],
                            observed_at=datetime.fromisoformat(payload["sosa:resultTime"]),
                            now=datetime.now(UTC),
                            quality=payload.get("eirvah:quality", "unknown"),
                        )
                        if req is None:
                            continue
                        await channel.default_exchange.publish(
                            aio_pika.Message(
                                body=req.model_dump_json().encode(),
                                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                            ),
                            routing_key=self._settings.amqp_queue,
                        )
                        self._fired.labels(reason="production_mode_change").inc()
                        _log.info(
                            "actuation_request_emitted",
                            correlation_id=req.correlation_id,
                            target=req.requested_value,
                            observation_id=payload.get("eirvah:correlationId"),
                        )
                    except Exception as exc:
                        _log.warning("message_processing_error", error=str(exc))


async def run(settings: DecisionAgentSettings) -> None:
    await DecisionAgentRuntime(settings).run()
