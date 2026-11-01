"""Select a warmer recipe from observed conveyor speed-setpoint changes."""

from __future__ import annotations

import json
import math
from datetime import UTC, datetime, timedelta
from typing import Any

import aio_pika
import aiomqtt
import structlog
from eirvah_contracts.actuation import ActuationRequest
from eirvah_contracts.inspection import BottleInspection
from eirvah_contracts.ulid import generate_correlation_id
from eirvah_observability.logging import configure_logging
from eirvah_observability.metrics import make_counter

from decision_agent_stub.config import DecisionAgentSettings
from decision_agent_stub.inspection import InspectionRule, Outcome, RejectionTracker

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
        self._inspection_rule = InspectionRule(settings)
        self._rejections = RejectionTracker()
        self._count: tuple[int, datetime] | None = None
        self._fired = make_counter(
            "decision_agent_actuation_fired_total",
            "Actuation requests emitted",
            labelnames=["reason"],
        )

    def _report(self, outcomes: list[Outcome]) -> None:
        for inspection_id, outcome in outcomes:
            _log.info("automatic_rejection_outcome", inspection_id=inspection_id, outcome=outcome)

    def observation(
        self, topic: str, payload: dict[str, Any], now: datetime
    ) -> ActuationRequest | None:
        self._report(self._rejections.expire(now))
        if payload.get("eirvah:quality") != "good":
            return None
        observed_at = datetime.fromisoformat(payload["sosa:resultTime"])
        if observed_at.tzinfo is None:
            return None
        age = (now - observed_at).total_seconds()
        if not 0 <= age <= self._settings.max_observation_age_s:
            return None
        value = payload["sosa:hasSimpleResult"]
        if topic == self._settings.subscribe_topic and payload.get("eirvah:unit") == "m/s":
            return self._rule.update(speed=value, observed_at=observed_at, now=now)
        if topic in (self._settings.reject_count_topic, self._settings.rejected_bottle_topic):
            if (
                type(value) is not int
                or not 0 <= value <= 65535
                or payload.get("eirvah:unit") != "dimensionless"
            ):
                return None
            kind = "count" if topic == self._settings.reject_count_topic else "bottle"
            if kind == "count" and (self._count is None or observed_at >= self._count[1]):
                self._count = (value, observed_at)
            self._report(self._rejections.feedback(kind, value, observed_at, now))
            return None
        if topic != self._settings.inspection_topic or payload.get("eirvah:unit") != "json":
            return None
        if type(value) is not str:
            return None
        event = BottleInspection.model_validate_json(value)
        if event.inspected_at != observed_at:
            return None
        # Wait for a fresh count baseline before submitting work. Saturated counters
        # cannot establish a further increment, so do not claim confirmation there.
        if self._count is None or self._count[0] == 65535:
            return None
        if (now - self._count[1]).total_seconds() > self._settings.max_observation_age_s:
            return None
        if len(self._rejections.pending) >= self._rejections.capacity:
            return None
        request = self._inspection_rule.update(event, now)
        if request is not None:
            self._rejections.add(request, event, self._count[0])
            _log.info(
                "automatic_rejection_requested",
                inspection_id=event.inspection_id,
                bottle_id=event.bottle_id,
                correlation_id=request.correlation_id,
            )
        return request

    async def _result(self, message: aio_pika.abc.AbstractIncomingMessage) -> None:
        async with message.process():
            try:
                result = json.loads(message.body)
                self._report(self._rejections.result(result, datetime.now(UTC)))
            except Exception as exc:
                _log.warning("invalid_rejection_result", error=str(exc))

    async def run(self) -> None:
        configure_logging(level=self._settings.log_level)
        amqp_conn = await aio_pika.connect_robust(self._settings.amqp_url)
        async with amqp_conn:
            channel = await amqp_conn.channel()
            results = await channel.declare_exchange(
                "eirvah.actuation.results",
                aio_pika.ExchangeType.TOPIC,
                durable=True,
            )
            result_queue = await channel.declare_queue(exclusive=True, auto_delete=True)
            await result_queue.bind(results, routing_key="actuation.result")
            await result_queue.consume(self._result)
            async with aiomqtt.Client(
                hostname=self._settings.mqtt_host,
                port=self._settings.mqtt_port,
                username=self._settings.mqtt_username,
                password=self._settings.mqtt_password,
            ) as mqtt:
                for topic in (
                    self._settings.subscribe_topic,
                    self._settings.inspection_topic,
                    self._settings.rejected_bottle_topic,
                    self._settings.reject_count_topic,
                ):
                    await mqtt.subscribe(topic, qos=1)
                _log.info(
                    "decision_agent_ready",
                    warmer_enabled=self._settings.enabled,
                    inspection_enabled=self._settings.inspection_enabled,
                )
                async for message in mqtt.messages:
                    try:
                        request = self.observation(
                            str(message.topic),
                            json.loads(message.payload),
                            datetime.now(UTC),
                        )
                        if request is None:
                            continue
                        await channel.default_exchange.publish(
                            aio_pika.Message(
                                body=request.model_dump_json().encode(),
                                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                            ),
                            routing_key=self._settings.amqp_queue,
                        )
                        reason = (
                            "failed_inspection"
                            if request.value_type == "int64"
                            else "production_mode_change"
                        )
                        self._fired.labels(reason=reason).inc()
                        _log.info(
                            "actuation_request_emitted",
                            correlation_id=request.correlation_id,
                            target=request.requested_value,
                            reason=reason,
                        )
                    except Exception as exc:
                        _log.warning("message_processing_error", error=str(exc))


async def run(settings: DecisionAgentSettings) -> None:
    await DecisionAgentRuntime(settings).run()
