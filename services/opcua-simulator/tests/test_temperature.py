import pytest
from opcua_simulator.rng import SimulatorRNG
from opcua_simulator.temperature import TemperatureDynamics


def model() -> TemperatureDynamics:
    return TemperatureDynamics(initial=22.0, time_constant_s=10.0, sigma=0.0, rng=SimulatorRNG(1))


def test_response_depends_on_elapsed_time_not_number_of_ticks() -> None:
    fast, slow = model(), model()
    for _ in range(100):
        fast.tick(setpoint=26.0, elapsed_s=0.1)
    for _ in range(10):
        slow.tick(setpoint=26.0, elapsed_s=1.0)
    assert fast.value == pytest.approx(slow.value)
    assert 22.0 < fast.value < 26.0


def test_cooling_does_not_change_setpoint_and_recovers_without_new_command() -> None:
    plant = model()
    plant.inject_cooling(rate_celsius_per_s=0.5, duration_s=10.0)
    for _ in range(10):
        plant.tick(setpoint=22.0, elapsed_s=1.0)
    assert plant.value < 20.0
    for _ in range(100):
        plant.tick(setpoint=22.0, elapsed_s=1.0)
    assert plant.value == pytest.approx(22.0, abs=0.001)


def test_partial_cooling_interval_is_tick_independent() -> None:
    a, b = model(), model()
    for plant in (a, b):
        plant.inject_cooling(rate_celsius_per_s=0.5, duration_s=0.35)
    a.tick(setpoint=22.0, elapsed_s=1.0)
    for _ in range(10):
        b.tick(setpoint=22.0, elapsed_s=0.1)
    assert a.value == pytest.approx(b.value)


def test_supervisory_setpoint_change_differs_from_natural_recovery() -> None:
    unchanged, supervised = model(), model()
    for plant in (unchanged, supervised):
        plant.inject_cooling(rate_celsius_per_s=0.5, duration_s=10.0)
    for _ in range(100):
        unchanged.tick(setpoint=22.0, elapsed_s=1.0)
        supervised.tick(setpoint=26.0, elapsed_s=1.0)
    assert unchanged.value == pytest.approx(22.0, abs=0.001)
    assert supervised.value == pytest.approx(26.0, abs=0.001)


@pytest.mark.parametrize("elapsed", [0.0, -1.0, float("nan"), float("inf")])
def test_invalid_time_step_rejected(elapsed: float) -> None:
    with pytest.raises(ValueError):
        model().tick(setpoint=22.0, elapsed_s=elapsed)


def test_seed_repeats_same_workload() -> None:
    a = TemperatureDynamics(initial=22.0, time_constant_s=10.0, sigma=0.05, rng=SimulatorRNG(7))
    b = TemperatureDynamics(initial=22.0, time_constant_s=10.0, sigma=0.05, rng=SimulatorRNG(7))
    assert [a.tick(setpoint=26.0, elapsed_s=0.5) for _ in range(20)] == [
        b.tick(setpoint=26.0, elapsed_s=0.5) for _ in range(20)
    ]
