"""Smoke tests for the OPC UA server (spec §9 acceptance)."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from asyncua import Client
from prometheus_client import CollectorRegistry

from opcua_simulator.config import SimulatorSettings
from opcua_simulator.server import SimulatorRuntime

REPO_ROOT = Path(__file__).resolve().parents[3]
SAMPLE_AS = REPO_ROOT / "config" / "opcua-address-space.yaml"


@pytest.mark.asyncio
async def test_runtime_start_then_stop_does_not_throw() -> None:
    settings = SimulatorSettings(
        endpoint="opc.tcp://127.0.0.1:54840/eirvah/simulator-test",
        address_space_path=SAMPLE_AS,
        tick_rate_ms=100,
        seed=1,
    )
    runtime = SimulatorRuntime(settings, registry=CollectorRegistry())
    await runtime.start()
    try:
        assert runtime.is_ready() is True
    finally:
        await runtime.stop()
    assert runtime.is_ready() is False


@pytest.mark.asyncio
async def test_setpoint_write_round_trips_through_opcua() -> None:
    settings = SimulatorSettings(
        endpoint="opc.tcp://127.0.0.1:54841/eirvah/simulator-test",
        address_space_path=SAMPLE_AS,
        tick_rate_ms=50,
        seed=2,
    )
    runtime = SimulatorRuntime(settings, registry=CollectorRegistry())
    await runtime.start()
    tick_task = asyncio.create_task(runtime.run_tick_loop())
    try:
        async with Client(url=settings.endpoint) as client:
            ns = await client.get_namespace_index(
                "https://eirvah.uniza/zilina/factory1"
            )
            bottler = await client.nodes.objects.get_child([f"{ns}:bottler"])
            sp = await bottler.get_child([f"{ns}:SetpointTemperature"])
            await sp.write_value(18.5)
            await asyncio.sleep(0.3)
            value = await sp.read_value()
            assert abs(float(value) - 18.5) < 0.001
    finally:
        tick_task.cancel()
        try:
            await tick_task
        except asyncio.CancelledError:
            pass
        await runtime.stop()
