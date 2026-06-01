from __future__ import annotations

import pytest
from modbus_simulator.server import RegisterBlock, scale_to_register, register_to_scale


def test_scale_to_register_temperature() -> None:
    assert scale_to_register(22.00, scale=100) == 2200


def test_scale_to_register_rounds() -> None:
    assert scale_to_register(22.005, scale=100) == 2201


def test_register_to_scale_fill_level() -> None:
    assert register_to_scale(750, scale=10) == pytest.approx(75.0)


def test_register_block_defaults() -> None:
    block = RegisterBlock()
    assert block.fill_level_raw == 750
    assert block.motor_state == 1
    assert block.throughput_raw == 80


def test_register_block_as_list() -> None:
    block = RegisterBlock(fill_level_raw=800, motor_state=1, throughput_raw=90)
    assert block.as_list() == [800, 1, 90]


def test_register_block_tick_changes_fill_level() -> None:
    import random
    rng = random.Random(42)
    block = RegisterBlock()
    block.tick(rng=rng, delta_max=20)
    assert 500 <= block.fill_level_raw <= 950


def test_register_block_tick_clamps_at_floor() -> None:
    import random
    rng = random.Random(0)
    block = RegisterBlock(fill_level_raw=500)
    for _ in range(20):
        block.tick(rng=rng, delta_max=200)
    assert block.fill_level_raw >= 500


def test_register_block_tick_clamps_at_ceiling() -> None:
    import random
    rng = random.Random(0)
    block = RegisterBlock(fill_level_raw=950)
    for _ in range(20):
        block.tick(rng=rng, delta_max=200)
    assert block.fill_level_raw <= 950
