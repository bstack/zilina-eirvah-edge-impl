from opcua_simulator.motor import (
    MOTOR_FAULT,
    MOTOR_RUNNING,
    MOTOR_STARTING,
    MOTOR_STOPPED,
    Motor,
)
from opcua_simulator.rng import SimulatorRNG


def _rng() -> SimulatorRNG:
    return SimulatorRNG(seed=0)


def test_starts_stopped() -> None:
    m = Motor(rng=_rng(), tick_ms=100, fault_probability=0.0)
    assert m.state == MOTOR_STOPPED
    assert m.rpm == 0.0


def test_transitions_stopped_to_starting_after_5s() -> None:
    m = Motor(rng=_rng(), tick_ms=100, fault_probability=0.0)
    for _ in range(49):
        m.tick()
        assert m.state == MOTOR_STOPPED
    m.tick()
    assert m.state == MOTOR_STARTING


def test_transitions_starting_to_running_after_3s() -> None:
    m = Motor(rng=_rng(), tick_ms=100, fault_probability=0.0)
    for _ in range(80):
        m.tick()
    assert m.state == MOTOR_RUNNING
    assert 1400 <= m.rpm <= 1600


def test_starting_ramp_increases_rpm() -> None:
    m = Motor(rng=_rng(), tick_ms=100, fault_probability=0.0)
    for _ in range(50):
        m.tick()
    samples = []
    for _ in range(30):
        m.tick()
        samples.append(m.rpm)
    assert samples[0] < samples[-1]


def test_fault_probability_one_forces_fault_when_running() -> None:
    m = Motor(rng=_rng(), tick_ms=100, fault_probability=1.0)
    for _ in range(81):
        m.tick()
    assert m.state == MOTOR_FAULT
    assert m.rpm == 0.0


def test_fault_recovers_to_stopped_after_10s() -> None:
    m = Motor(rng=_rng(), tick_ms=100, fault_probability=1.0)
    for _ in range(81):
        m.tick()
    assert m.state == MOTOR_FAULT
    for _ in range(100):
        m.tick()
    assert m.state == MOTOR_STOPPED
