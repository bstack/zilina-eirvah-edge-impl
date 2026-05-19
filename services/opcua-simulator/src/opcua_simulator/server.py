"""OPC UA server + tick loop wiring (spec §9)."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import structlog
import uvicorn
from asyncua import Server, ua
from eirvah_observability.health import HealthApp
from eirvah_observability.logging import bind_correlation_id, configure_logging
from prometheus_client.registry import REGISTRY, CollectorRegistry

from opcua_simulator.address_space import AddressSpaceModel, NodeDefinition, load_address_space
from opcua_simulator.config import SimulatorSettings
from opcua_simulator.hot_spike import HotSpike
from opcua_simulator.metrics import SimulatorMetrics
from opcua_simulator.motor import Motor
from opcua_simulator.quality import QualityEmitter
from opcua_simulator.rng import SimulatorRNG
from opcua_simulator.setpoint import Setpoint
from opcua_simulator.temperature import TemperatureDynamics
from opcua_simulator.throughput import Throughput

if TYPE_CHECKING:
    from asyncua.common.node import Node

_log = structlog.get_logger("opcua-simulator")

_VALUE_TYPE_TO_VARIANT = {
    "double": ua.VariantType.Double,
    "int64": ua.VariantType.Int64,
    "bool": ua.VariantType.Boolean,
    "string": ua.VariantType.String,
}


class SimulatorRuntime:
    def __init__(
        self, settings: SimulatorSettings, registry: CollectorRegistry = REGISTRY
    ) -> None:
        self.settings = settings
        self.rng = SimulatorRNG(seed=settings.seed)
        self.metrics = SimulatorMetrics(registry=registry)
        self._address_space: AddressSpaceModel | None = None
        self._setpoint: Setpoint | None = None
        self._temperature: TemperatureDynamics | None = None
        self._motor: Motor | None = None
        self._throughput: Throughput | None = None
        self._hot_spike: HotSpike | None = None
        self._quality_per_node: dict[str, QualityEmitter] = {}
        self._nodes_by_def_id: dict[str, Node] = {}
        self._server: Server | None = None
        self._ready: bool = False

    def is_ready(self) -> bool:
        return self._ready

    async def start(self) -> None:
        self._address_space = load_address_space(self.settings.address_space_path)
        self._build_dynamics()

        self._server = Server()
        await self._server.init()
        self._server.set_endpoint(self.settings.endpoint)
        self._server.set_server_name("EirVah Bottling Line Simulator")

        idx = await self._server.register_namespace(self._address_space.namespace)
        await self._populate_address_space(idx)

        await self._server.start()
        self._ready = True
        _log.info(
            "opcua_server_started",
            endpoint=self.settings.endpoint,
            tick_rate_ms=self.settings.tick_rate_ms,
            seed=self.settings.seed,
        )

    async def stop(self) -> None:
        self._ready = False
        if self._server is not None:
            await self._server.stop()

    def _build_dynamics(self) -> None:
        assert self._address_space is not None
        setpoint_def = next(
            (n for n in self._address_space.iter_nodes() if n.kind == "setpoint"), None
        )
        if setpoint_def is None:
            raise ValueError("address space must contain a setpoint node")
        self._setpoint = Setpoint(initial=float(setpoint_def.initial))

        temp_def = next(
            (n for n in self._address_space.iter_nodes() if n.dynamics == "temperature"), None
        )
        if temp_def is None:
            raise ValueError("address space must contain a temperature node")
        self._temperature = TemperatureDynamics(
            initial=float(temp_def.initial),
            alpha=0.05,
            sigma=0.3,
            rng=self.rng,
        )
        self._motor = Motor(
            rng=self.rng,
            tick_ms=self.settings.tick_rate_ms,
            fault_probability=self.settings.motor_fault_probability,
        )
        self._throughput = Throughput(rng=self.rng)
        self._hot_spike = HotSpike(
            rng=self.rng,
            stochastic_probability=self.settings.hot_spike_probability,
        )
        for node_def in self._address_space.iter_nodes():
            self._quality_per_node[node_def.id] = QualityEmitter(
                rng=self.rng,
                bad_quality_pct=node_def.bad_quality_pct,
                uncertain_quality_pct=node_def.uncertain_quality_pct,
            )

    async def _populate_address_space(self, ns_idx: int) -> None:
        assert self._server is not None and self._address_space is not None
        objects = self._server.nodes.objects
        for eq in self._address_space.equipments:
            eq_folder = await objects.add_folder(ns_idx, eq.name)
            for node_def in eq.nodes:
                ua_node = await eq_folder.add_variable(
                    ns_idx,
                    node_def.id.split(".")[-1],
                    self._initial_ua_value(node_def),
                    varianttype=_VALUE_TYPE_TO_VARIANT[node_def.value_type],
                )
                if node_def.kind == "setpoint":
                    await ua_node.set_writable(True)
                self._nodes_by_def_id[node_def.id] = ua_node
            await eq_folder.add_method(
                ns_idx, "TriggerHotSpike", self._trigger_hot_spike_method, [], []
            )

    async def _trigger_hot_spike_method(self, _parent: Any) -> list[Any]:
        assert self._hot_spike is not None
        self._hot_spike.trigger_via_method()
        _log.info("hot_spike_method_invoked")
        return []

    async def run_tick_loop(self) -> None:
        period_s = self.settings.tick_rate_ms / 1000.0
        while self._ready:
            await self._tick()
            await asyncio.sleep(period_s)

    async def _tick(self) -> None:
        assert self._setpoint is not None
        assert self._temperature is not None
        assert self._motor is not None
        assert self._throughput is not None
        assert self._hot_spike is not None
        assert self._address_space is not None

        await self._reconcile_setpoint()

        prev_spike_counts = dict(self._hot_spike.trigger_counts())
        spike = self._hot_spike.tick()
        self._motor.tick()
        temp = self._temperature.tick(setpoint=self._setpoint.value, spike_contribution=spike)
        tput = self._throughput.compute(
            motor_state=self._motor.state, motor_rpm=self._motor.rpm
        )

        defaults = self._address_space.uns_defaults
        for node_def in self._address_space.iter_nodes():
            labels = {
                "enterprise": defaults.enterprise,
                "site": defaults.site,
                "area": defaults.area,
                "line": defaults.line,
                "cell": node_def.cell,
                "equipment": node_def.equipment,
            }
            value = self._value_for_node(node_def, temp=temp, tput=tput)
            await self._nodes_by_def_id[node_def.id].write_value(value)
            self._update_state_metric(node_def, labels=labels, value=value)
            quality = self._quality_per_node[node_def.id].next()
            self.metrics.inc_quality(labels=labels, quality=quality)

        for kind, count in self._hot_spike.trigger_counts().items():
            delta = count - prev_spike_counts.get(kind, 0)
            for _ in range(delta):
                self.metrics.inc_hot_spike(trigger=kind)

    async def _reconcile_setpoint(self) -> None:
        assert self._address_space is not None and self._setpoint is not None
        setpoint_def = next(n for n in self._address_space.iter_nodes() if n.kind == "setpoint")
        ua_value = await self._nodes_by_def_id[setpoint_def.id].read_value()
        if float(ua_value) != self._setpoint.value:
            self._setpoint.write(
                value=float(ua_value),
                writer_session="opcua-client",
                at=datetime.now(UTC),
            )
            self.metrics.inc_setpoint_write(writer="opcua-client")
            _log.info("setpoint_write_observed", new_value=float(ua_value))

    def _value_for_node(self, node_def: NodeDefinition, *, temp: float, tput: float) -> Any:
        assert self._motor is not None and self._setpoint is not None
        match node_def.dynamics:
            case "temperature":
                return float(temp)
            case "throughput":
                return float(tput)
            case "motor_state":
                return int(self._motor.state)
            case "motor_rpm":
                return float(self._motor.rpm)
            case None if node_def.kind == "setpoint":
                return float(self._setpoint.value)
            case _:
                return node_def.initial

    def _update_state_metric(
        self, node_def: NodeDefinition, *, labels: dict[str, str], value: Any
    ) -> None:
        match node_def.dynamics:
            case "temperature":
                self.metrics.set_temperature(labels, float(value))
            case "throughput":
                self.metrics.set_throughput(labels, float(value))
            case "motor_state":
                self.metrics.set_motor_state(labels, int(value))
            case "motor_rpm":
                self.metrics.set_motor_rpm(labels, float(value))
            case None if node_def.kind == "setpoint":
                self.metrics.set_setpoint(labels, float(value))

    def _initial_ua_value(self, node_def: NodeDefinition) -> Any:
        return node_def.initial


async def run(settings: SimulatorSettings) -> None:
    configure_logging(level=settings.log_level)
    bind_correlation_id("system")
    runtime = SimulatorRuntime(settings)
    health = HealthApp(is_ready=runtime.is_ready)

    config = uvicorn.Config(
        health.asgi,
        host="0.0.0.0",
        port=settings.http_port,
        log_level=settings.log_level.lower(),
    )
    http = uvicorn.Server(config)

    await runtime.start()
    try:
        await asyncio.gather(runtime.run_tick_loop(), http.serve())
    finally:
        await runtime.stop()
