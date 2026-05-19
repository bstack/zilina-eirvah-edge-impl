from opcua_simulator.rng import SimulatorRNG
from opcua_simulator.temperature import TemperatureDynamics


def test_zero_noise_converges_toward_setpoint() -> None:
    dyn = TemperatureDynamics(initial=10.0, alpha=0.5, sigma=0.0, rng=SimulatorRNG(1))
    for _ in range(20):
        dyn.tick(setpoint=22.0, spike_contribution=0.0)
    assert abs(dyn.value - 22.0) < 1e-4  # 12 * 0.5^20 ≈ 1.14e-5


def test_spike_contribution_lifts_value() -> None:
    dyn = TemperatureDynamics(initial=22.0, alpha=0.0, sigma=0.0, rng=SimulatorRNG(1))
    dyn.tick(setpoint=22.0, spike_contribution=5.0)
    assert dyn.value == 27.0


def test_deterministic_under_same_seed() -> None:
    a = TemperatureDynamics(initial=22.0, alpha=0.05, sigma=0.3, rng=SimulatorRNG(42))
    b = TemperatureDynamics(initial=22.0, alpha=0.05, sigma=0.3, rng=SimulatorRNG(42))
    for _ in range(50):
        a.tick(setpoint=22.0, spike_contribution=0.0)
        b.tick(setpoint=22.0, spike_contribution=0.0)
    assert a.value == b.value


def test_setpoint_change_tracked_within_50_ticks() -> None:
    dyn = TemperatureDynamics(initial=22.0, alpha=0.1, sigma=0.0, rng=SimulatorRNG(7))
    for _ in range(50):
        dyn.tick(setpoint=18.0, spike_contribution=0.0)
    assert abs(dyn.value - 18.0) < 0.1
