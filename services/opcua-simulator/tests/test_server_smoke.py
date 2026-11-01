"""Smoke tests for the OPC UA server (spec §9 acceptance)."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from pathlib import Path

import pytest
from asyncua import Client
from opcua_simulator.config import SimulatorSettings
from opcua_simulator.server import SimulatorRuntime
from prometheus_client import CollectorRegistry

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
            ns = await client.get_namespace_index("https://eirvah.uniza/zilina/factory1")
            bottler = await client.nodes.objects.get_child([f"{ns}:bottler"])
            sp = await bottler.get_child([f"{ns}:SetpointTemperature"])
            await sp.write_value(18.5)
            await asyncio.sleep(0.3)
            value = await sp.read_value()
            assert abs(float(value) - 18.5) < 0.001
    finally:
        tick_task.cancel()
        with suppress(asyncio.CancelledError):
            await tick_task
        await runtime.stop()


@pytest.mark.asyncio
async def test_warmer_tracks_command_and_cooling_preserves_setpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = SimulatorSettings(
        endpoint="opc.tcp://127.0.0.1:54842/eirvah/warmer-test",
        address_space_path=SAMPLE_AS,
        temperature_noise=0.0,
    )
    runtime = SimulatorRuntime(settings, registry=CollectorRegistry())
    await runtime.start()
    try:
        async with Client(url=settings.endpoint) as client:
            ns = await client.get_namespace_index("https://eirvah.uniza/zilina/factory1")
            bottler = await client.nodes.objects.get_child([f"{ns}:bottler"])
            setpoint = await bottler.get_child([f"{ns}:SetpointTemperature"])
            temperature = await bottler.get_child([f"{ns}:Temperature"])
            clock = iter([1.0, 11.0, 21.0, 121.0])
            monkeypatch.setattr("opcua_simulator.server.monotonic", lambda: next(clock))
            await runtime._tick()
            await setpoint.write_value(26.0)
            await runtime._tick()
            before = float(await temperature.read_value())
            assert 22 < before < 26
            await bottler.call_method(f"{ns}:TriggerCoolingLoad")
            await runtime._tick()
            assert float(await temperature.read_value()) < before
            assert float(await setpoint.read_value()) == 26.0
            await runtime._tick()
            assert float(await temperature.read_value()) == pytest.approx(26.0, abs=0.001)
    finally:
        await runtime.stop()
