from __future__ import annotations

import pytest
from modbus_simulator.server import RegisterBlock, register_to_scale, scale_to_register


def test_scale_to_register_basic() -> None:
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
    assert block.belt_speed_raw == 50
    assert block.jam_detected == 0
    assert block.bottle_count == 0
    assert block.reject_count == 0
    assert block.conveyor_active == 1


def test_register_block_as_list_length() -> None:
    block = RegisterBlock()
    assert len(block.as_list()) == 8


def test_register_block_as_list_order() -> None:
    block = RegisterBlock(
        fill_level_raw=700,
        motor_state=1,
        throughput_raw=75,
        belt_speed_raw=55,
        jam_detected=0,
        bottle_count=10,
        reject_count=1,
        conveyor_active=1,
    )
    assert block.as_list() == [700, 1, 75, 55, 0, 10, 1, 1]


def test_register_block_tick_changes_fill_level() -> None:
    import random

    rng = random.Random(42)
    block = RegisterBlock()
    block.tick(rng=rng)
    assert 500 <= block.fill_level_raw <= 950


def test_register_block_tick_changes_belt_speed() -> None:
    import random

    rng = random.Random(0)
    block = RegisterBlock()
    for _ in range(10):
        block.tick(rng=rng)
    assert 20 <= block.belt_speed_raw <= 80


def test_register_block_tick_bottle_count_increments() -> None:
    import random

    rng = random.Random(0)
    block = RegisterBlock()
    block.tick(rng=rng)
    assert block.bottle_count == 1


def test_register_block_tick_clamps_fill_level() -> None:
    import random

    rng = random.Random(0)
    block = RegisterBlock(fill_level_raw=500)
    for _ in range(30):
        block.tick(rng=rng, delta_max=200)
    assert block.fill_level_raw >= 500


def test_register_block_tick_clamps_belt_speed() -> None:
    import random

    rng = random.Random(0)
    block = RegisterBlock(belt_speed_raw=80)
    for _ in range(30):
        block.tick(rng=rng)
    assert block.belt_speed_raw <= 80


def test_fresh_study_source_starts_parked_until_control_starts_it(monkeypatch):
    import random

    import modbus_simulator.server as server
    from modbus_simulator.config import SimulatorSettings

    monkeypatch.setattr(server, "SimulatorMetrics", lambda: object())

    runtime = server.SimulatorRuntime(SimulatorSettings(initial_conveyor_running=False))
    for _ in range(20):
        runtime._block.tick(rng=random.Random(0), jam_probability=0)

    assert runtime._block.bottle_count == 0
    assert runtime._block.conveyor.running is False
    runtime._block.conveyor.apply(1, 1)
    runtime._block.tick(rng=random.Random(0), jam_probability=0)
    assert runtime._block.bottle_count == 1


def test_initial_park_setting_does_not_override_retained_source_state(tmp_path, monkeypatch):
    import modbus_simulator.server as server
    from modbus_simulator.config import SimulatorSettings

    monkeypatch.setattr(server, "SimulatorMetrics", lambda: object())
    path = tmp_path / "source.sqlite3"
    first = server.SimulatorRuntime(SimulatorSettings(journal_path=path))
    first._open_recovery()
    first._journal.db.close()

    recovered = server.SimulatorRuntime(
        SimulatorSettings(journal_path=path, initial_conveyor_running=False)
    )
    recovered._open_recovery()
    try:
        assert recovered._block.conveyor.running is True
    finally:
        recovered._journal.db.close()
