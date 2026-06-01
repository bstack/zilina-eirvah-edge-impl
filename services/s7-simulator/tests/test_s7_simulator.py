from __future__ import annotations

import struct

import pytest
from s7_simulator.server import CapperBlock, PalletizerBlock, encode_real, encode_int


def test_encode_real_basic() -> None:
    buf = bytearray(4)
    encode_real(buf, 0, 2.5)
    assert struct.unpack(">f", buf[0:4])[0] == pytest.approx(2.5, abs=0.001)


def test_encode_int_basic() -> None:
    buf = bytearray(2)
    encode_int(buf, 0, 42)
    assert struct.unpack(">h", buf[0:2])[0] == 42


def test_capper_block_defaults() -> None:
    block = CapperBlock()
    assert block.torque_nm == pytest.approx(2.5)
    assert block.cap_presence == 1
    assert block.rejects_per_min == 0


def test_capper_block_to_bytes_length() -> None:
    block = CapperBlock()
    data = block.to_bytes()
    assert len(data) == 8


def test_capper_block_tick_torque_in_range() -> None:
    import random
    rng = random.Random(42)
    block = CapperBlock()
    for _ in range(20):
        block.tick(rng=rng)
    assert 1.5 <= block.torque_nm <= 4.0


def test_palletizer_block_defaults() -> None:
    block = PalletizerBlock()
    assert block.layer_count == 0
    assert block.pallet_complete == 0
    assert block.cycles_per_hr == 12


def test_palletizer_block_to_bytes_length() -> None:
    block = PalletizerBlock()
    assert len(block.to_bytes()) == 8


def test_palletizer_block_layer_increments() -> None:
    import random
    rng = random.Random(0)
    block = PalletizerBlock()
    for _ in range(12):
        block.tick(rng=rng)
    assert block.layer_count == 1


def test_palletizer_block_pallet_complete_resets_layer() -> None:
    import random
    rng = random.Random(0)
    block = PalletizerBlock()
    block.layer_count = 9
    block.tick(rng=rng)
    assert block.pallet_complete == 1
    assert block.layer_count == 0
