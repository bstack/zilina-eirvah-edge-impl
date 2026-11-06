"""Select a warmer recipe from observed conveyor speed-setpoint changes."""

from __future__ import annotations

import asyncio
import json
import math
from datetime import UTC, datetime, timedelta
from typing import Any

import aio_pika
import aiomqtt
import structlog
from eirvah_contracts.actuation import ActuationRequest
from eirvah_contracts.authorization import require_key
from eirvah_contracts.inspection import BottleInspection, sign_receipt
from eirvah_contracts.ulid import generate_correlation_id
from eirvah_controls.recovery import ConsumerStore
from eirvah_observability.execution import record_command
from eirvah_observability.logging import configure_logging
from eirvah_observability.metrics import make_counter
from prometheus_client.registry import CollectorRegistry

from decision_agent_stub.config import DecisionAgentSettings
from decision_agent_stub.inspection import InspectionRule, Outcome, RejectionTracker
from decision_agent_stub.state import StateTracker

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
    def __init__(
        self, settings: DecisionAgentSettings, *, registry: CollectorRegistry | None = None
    ) -> None:
        self._settings = settings
        self._rule = ProductionModeRule(settings)
        self._store: ConsumerStore | None = None
        self._receipt_event: BottleInspection | None = None
        self._state = StateTracker(
            settings.max_observation_age_s, require_position=settings.require_positions
        )
        self._inspection_rule = InspectionRule(settings)
        self._rejections = RejectionTracker()
        self._count: tuple[int, datetime] | None = None
        self._fired = make_counter(
            "decision_agent_actuation_fired_total",
            "Actuation requests emitted",
            labelnames=["reason"],
            registry=registry,
        )

    @property
    def state_decision(self) -> dict[str, Any]:
        return dict(self._state.last_decision)

    def state_snapshot(self, now: datetime) -> dict[str, Any]:
        return {
            topic: self._state.snapshot(topic, now)
            for topic in (
                self._settings.subscribe_topic,
                self._settings.reject_count_topic,
                self._settings.rejected_bottle_topic,
            )
        }

    def _state_update(
        self, topic: str, value: Any, observed_at: datetime, now: datetime, payload: dict[str, Any]
    ) -> bool:
        accepted = self._state.update(
            topic,
            value,
            observed_at,
            now,
            payload["eirvah:quality"],
            payload.get("eirvah:position"),
        )
        if self._store is not None:
            self._store.audit_state(
                topic,
                payload,
                self.state_decision,
                self._state.snapshot(topic, now),
                now.timestamp(),
            )
        return accepted

    def _receive_durable(self, event: BottleInspection, now: datetime) -> None:
        assert self._store is not None
        fresh = (
            0 <= (now - event.inspected_at).total_seconds() < self._settings.inspection_max_age_s
        )
        baseline = self._count
        if (
            fresh
            and not event.passed
            and self._store.find(event.inspection_id) is None
            and (
                baseline is None
                or baseline[0] == 65535
                or (now - baseline[1]).total_seconds() > self._settings.max_observation_age_s
            )
        ):
            return

        def command() -> dict[str, Any]:
            return ActuationRequest(
                correlation_id=generate_correlation_id(),
                requester="decision-agent-stub",
                target_uns_topic=self._settings.reject_target_uns_topic,
                requested_value=event.bottle_id,
                value_type="int64",
                reason="failed bottle inspection " + event.inspection_id,
                requested_at=now,
                deadline=event.inspected_at
                + timedelta(seconds=self._settings.inspection_max_age_s),
            ).model_dump(mode="json")

        row = self._store.receive(
            event.model_dump(mode="json"),
            now.timestamp(),
            self._settings.inspection_max_age_s,
            command,
            baseline[0] if baseline else None,
        )
        self._receipt_event = event
        if (
            row["command"]
            and row["disposition"] == "pending"
            and row["command_id"] not in self._rejections.pending
            and fresh
        ):
            request = ActuationRequest.model_validate_json(row["command"])
            self._rejections.add(request, event, row["baseline_count"])

    async def _publish_request(self, commands: Any, request: ActuationRequest) -> None:
        record_command("decision-agent-stub", "request_submitting", request)
        await commands.publish(
            aio_pika.Message(
                body=request.model_dump_json().encode(),
                user_id="decision-agent-stub",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key=self._settings.amqp_queue,
        )
        record_command("decision-agent-stub", "request_published", request)
        self._fired.labels(
            reason="failed_inspection"
            if request.value_type == "int64"
            else "production_mode_change"
        ).inc()

    async def _send_pending(self, commands: Any) -> None:
        assert self._store is not None
        while True:
            now = datetime.now(UTC).timestamp()
            for row in self._store.ready(now):
                request = ActuationRequest.model_validate_json(row["command"])
                try:
                    self._store.sent(row["id"], now)
                    await self._publish_request(commands, request)
                    _log.info(
                        "inspection_command_sent",
                        inspection_id=row["id"],
                        correlation_id=request.correlation_id,
                    )
                except Exception as exc:
                    _log.warning("inspection_send_deferred", error_type=type(exc).__name__)
            await asyncio.sleep(0.2)

    def _report(self, outcomes: list[Outcome]) -> None:
        for inspection_id, outcome in outcomes:
            _log.info("automatic_rejection_outcome", inspection_id=inspection_id, outcome=outcome)

    def observation(
        self, topic: str, payload: dict[str, Any], now: datetime
    ) -> ActuationRequest | None:
        self._receipt_event = None
        self._report(self._rejections.expire(now))
        state_topic = topic in (
            self._settings.subscribe_topic,
            self._settings.reject_count_topic,
            self._settings.rejected_bottle_topic,
        )
        if state_topic:
            self._state.last_decision = {"accepted": False, "reason": "invalid_contract"}
        if not state_topic and payload.get("eirvah:quality") != "good":
            return None
        observed_at = datetime.fromisoformat(payload["sosa:resultTime"])
        if observed_at.tzinfo is None:
            return None
        age = (now - observed_at).total_seconds()
        if (
            not state_topic
            and self._store is None
            and not 0 <= age <= self._settings.max_observation_age_s
        ):
            return None
        value = payload["sosa:hasSimpleResult"]
        if topic == self._settings.subscribe_topic and payload.get("eirvah:unit") == "m/s":
            if type(value) not in (int, float) or not math.isfinite(value) or not 0 <= value <= 0.8:
                return None
            if not self._state_update(topic, value, observed_at, now, payload):
                return None
            return self._rule.update(speed=value, observed_at=observed_at, now=now)
        if topic in (self._settings.reject_count_topic, self._settings.rejected_bottle_topic):
            if (
                type(value) is not int
                or not 0 <= value <= 65535
                or payload.get("eirvah:unit") != "dimensionless"
            ):
                return None
            if not self._state_update(topic, value, observed_at, now, payload):
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
        if self._store is not None:
            self._receive_durable(event, now)
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
                if self._store is not None:
                    self._store.settle(result)
                self._report(self._rejections.result(result, datetime.now(UTC)))
            except Exception as exc:
                _log.warning("invalid_rejection_result", error=str(exc))

    async def run(self) -> None:
        configure_logging(level=self._settings.log_level)
        if self._settings.inbox_path is not None:
            require_key(self._settings.receipt_key.get_secret_value())
            self._store = ConsumerStore(self._settings.inbox_path)
            self._state.store = self._store
        try:
            amqp_conn = await aio_pika.connect_robust(self._settings.amqp_url)
            async with amqp_conn:
                channel = await amqp_conn.channel()
                commands = await channel.get_exchange("eirvah.actuation.commands")
                results = await channel.get_exchange("eirvah.actuation.results")
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
                        durable=self._store is not None,
                    )
                    async with asyncio.TaskGroup() as tasks:
                        if self._store is not None:
                            tasks.create_task(self._send_pending(commands))
                        async for message in mqtt.messages:
                            try:
                                request = self.observation(
                                    str(message.topic),
                                    json.loads(message.payload),
                                    datetime.now(UTC),
                                )
                                if self._receipt_event is not None:
                                    receipt = sign_receipt(
                                        self._receipt_event,
                                        self._settings.receipt_key.get_secret_value(),
                                    )
                                    await mqtt.publish(
                                        self._settings.receipt_topic,
                                        receipt.model_dump_json().encode(),
                                        qos=1,
                                        retain=False,
                                    )
                                if request is not None:
                                    await self._publish_request(commands, request)
                            except Exception as exc:
                                _log.warning(
                                    "message_processing_error", error_type=type(exc).__name__
                                )
        finally:
            if self._store is not None:
                self._store.close()


async def run(settings: DecisionAgentSettings) -> None:
    await DecisionAgentRuntime(settings).run()
