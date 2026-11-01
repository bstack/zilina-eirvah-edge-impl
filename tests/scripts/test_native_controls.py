from __future__ import annotations

from pathlib import Path
from random import Random

import pytest


def test_modbus_commands_survive_ticks_and_keep_targets_independent() -> None:
    from modbus_simulator.config import SimulatorSettings
    from modbus_simulator.server import SimulatorRuntime

    runtime = SimulatorRuntime(SimulatorSettings())
    runtime._context = runtime._build_context()
    store = runtime._context[1]
    # Commit sequence is the final write; payload is operation + value.
    store.setValues(3, 21, [2, 70])
    store.setValues(3, 20, [1])
    runtime._process_controls()
    assert store.getValues(3, 50, 2) == [1, 1]
    runtime._block.tick(rng=Random(0))
    assert runtime._block.belt_speed_raw == 70
    assert runtime._block.valve.opened
    store.setValues(3, 25, [1, 0])
    store.setValues(3, 24, [2])
    runtime._process_controls()
    assert store.getValues(3, 52, 2) == [2, 1]
    runtime._block.tick(rng=Random(0))
    assert runtime._block.throughput_raw == 0 and runtime._block.belt_speed_raw == 70


def test_s7_control_mailbox_does_not_overwrite_commands_or_palletizer() -> None:
    import struct

    from s7_simulator.config import SimulatorSettings
    from s7_simulator.server import SimulatorRuntime

    runtime = SimulatorRuntime(SimulatorSettings())
    runtime._db3[:6] = struct.pack(">HHH", 1, 2, 900)
    runtime._process_controls()
    runtime._sync_buffers()
    assert struct.unpack_from(">HH", runtime._db3, 6) == (1, 1)
    assert runtime._capper.motor.speed == 900
    assert runtime._palletizer.layer_count == 0
    assert struct.unpack_from(">HHH", runtime._db3) == (1, 2, 900)


def test_native_targets_and_policies_cover_all_new_operations() -> None:
    from actuation_event_validator.service import load_policy
    from actuation_signal_publisher.native import load_native_targets

    root = Path(__file__).resolve().parents[2]
    targets = load_native_targets(root / "config/native-actuation-targets.yaml")
    policies = load_policy(root / "config/actuation-policy.yaml")
    assert len(targets) == 8
    assert all(topic in policies for topic in targets)
    assert {target.protocol for target in targets.values()} == {"modbus", "s7"}


@pytest.mark.parametrize("value", [True, "0.5", float("nan"), float("inf"), -1, 1000])
def test_native_value_encoding_rejects_invalid_values(value: object) -> None:
    from actuation_signal_publisher.native import NativeTarget, encode_value

    target = NativeTarget(
        protocol="modbus",
        host="test",
        port=5020,
        command_address=20,
        ack_address=50,
        operation=2,
        value_type="double",
        scale=100,
        minimum=0,
        maximum=0.8,
    )
    with pytest.raises(ValueError):
        encode_value(target, value)
