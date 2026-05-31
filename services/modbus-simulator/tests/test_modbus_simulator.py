from __future__ import annotations

import pytest
from modbus_simulator.server import RegisterBlock, scale_to_register, register_to_scale


def test_scale_to_register_temperature() -> None:
    assert scale_to_register(22.00, scale=100) == 2200


def test_scale_to_register_rounds() -> None:
    assert scale_to_register(22.005, scale=100) == 2201


def test_register_to_scale_temperature() -> None:
    assert register_to_scale(2200, scale=100) == pytest.approx(22.00)


def test_register_block_defaults() -> None:
    block = RegisterBlock()
    assert block.temperature_raw == 2200
    assert block.setpoint_raw == 2200
    assert block.motor_state == 1
    assert block.throughput_raw == 80


def test_register_block_as_list() -> None:
    block = RegisterBlock(temperature_raw=2350, setpoint_raw=2200, motor_state=1, throughput_raw=82)
    values = block.as_list()
    assert values == [2350, 2200, 1, 82]


def test_register_block_tick_changes_temperature() -> None:
    import random
    rng = random.Random(42)
    block = RegisterBlock()
    block.tick(rng=rng, delta_max=50)
    assert 1800 <= block.temperature_raw <= 5000


def test_register_block_tick_clamps_at_floor() -> None:
    import random
    rng = random.Random(0)
    block = RegisterBlock(temperature_raw=1800)
    for _ in range(20):
        block.tick(rng=rng, delta_max=500)
    assert block.temperature_raw >= 1800


def test_register_block_tick_clamps_at_ceiling() -> None:
    import random
    rng = random.Random(0)
    block = RegisterBlock(temperature_raw=5000)
    for _ in range(20):
        block.tick(rng=rng, delta_max=500)
    assert block.temperature_raw <= 5000
