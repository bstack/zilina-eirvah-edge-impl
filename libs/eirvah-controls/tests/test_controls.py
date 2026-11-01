from __future__ import annotations

from eirvah_controls import MotorControl, RejectControl, ValveControl


def test_motor_stop_speed_start_and_fault_reset_are_separate() -> None:
    motor = MotorControl(max_speed=1800, setpoint=1500)
    assert motor.apply(1, 0) == 1 and motor.speed == 0
    assert motor.apply(2, 900) == 1 and motor.speed == 0
    assert motor.apply(1, 1) == 1 and motor.speed == 900
    assert motor.apply(4, 1) == 1 and motor.faulted and motor.speed == 0
    assert motor.apply(1, 1) == 3
    assert motor.apply(3, 1) == 1 and not motor.faulted and not motor.running
    assert motor.speed == 0
    assert motor.apply(3, 1) == 3


def test_targets_do_not_share_state() -> None:
    conveyor = MotorControl(max_speed=80, setpoint=50)
    capper = MotorControl(max_speed=1800, setpoint=1500)
    valve = ValveControl()
    conveyor.apply(2, 70)
    conveyor.apply(1, 0)
    assert capper.speed == 1500 and valve.opened
    valve.apply(1, 0)
    capper.apply(2, 1000)
    assert conveyor.setpoint == 70 and not conveyor.running
    assert not valve.opened and capper.speed == 1000


def test_invalid_commands_do_not_mutate_target() -> None:
    motor = MotorControl(max_speed=80, setpoint=50)
    assert motor.apply(2, 81) == 2
    assert motor.apply(1, 2) == 2
    assert motor.apply(999, 1) == 2
    assert motor.speed == 50
    valve = ValveControl()
    assert valve.apply(1, 2) == 2 and valve.opened


def test_reject_requires_an_observed_bottle_and_does_not_repeat_effect() -> None:
    reject = RejectControl(window=3)
    assert reject.apply(1, 1) == 3
    reject.observe(1)
    assert reject.apply(1, 1) == 1
    assert reject.apply(1, 1) == 4
    assert reject.count == 1 and reject.last_bottle == 1
    for bottle in [2, 3, 4]:
        reject.observe(bottle)
    assert reject.apply(1, 1) == 3
    assert reject.apply(1, 4) == 1 and reject.count == 2
