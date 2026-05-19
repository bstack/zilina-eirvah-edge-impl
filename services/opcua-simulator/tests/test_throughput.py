from opcua_simulator.motor import MOTOR_FAULT, MOTOR_RUNNING, MOTOR_STARTING, MOTOR_STOPPED
from opcua_simulator.rng import SimulatorRNG
from opcua_simulator.throughput import Throughput


def test_stopped_throughput_is_zero() -> None:
    assert Throughput(rng=SimulatorRNG(0)).compute(motor_state=MOTOR_STOPPED, motor_rpm=0.0) == 0.0


def test_fault_throughput_is_zero() -> None:
    assert Throughput(rng=SimulatorRNG(0)).compute(motor_state=MOTOR_FAULT, motor_rpm=0.0) == 0.0


def test_running_throughput_scales_with_rpm() -> None:
    t = Throughput(rng=SimulatorRNG(0))
    low = t.compute(motor_state=MOTOR_RUNNING, motor_rpm=750.0)
    high = t.compute(motor_state=MOTOR_RUNNING, motor_rpm=1500.0)
    assert high > low


def test_running_throughput_at_1500rpm_near_target() -> None:
    t = Throughput(rng=SimulatorRNG(0))
    samples = [t.compute(motor_state=MOTOR_RUNNING, motor_rpm=1500.0) for _ in range(200)]
    assert 0.8 < sum(samples) / len(samples) < 1.0


def test_starting_ramps_with_rpm() -> None:
    t = Throughput(rng=SimulatorRNG(0))
    early = t.compute(motor_state=MOTOR_STARTING, motor_rpm=300.0)
    late = t.compute(motor_state=MOTOR_STARTING, motor_rpm=1200.0)
    assert late > early
