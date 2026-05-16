from opcua_simulator.rng import SimulatorRNG


def test_same_seed_yields_identical_sequence() -> None:
    a, b = SimulatorRNG(seed=42), SimulatorRNG(seed=42)
    assert [a.gauss(0, 1) for _ in range(10)] == [b.gauss(0, 1) for _ in range(10)]
    assert [a.random() for _ in range(10)] == [b.random() for _ in range(10)]


def test_different_seeds_diverge() -> None:
    a, b = SimulatorRNG(seed=42), SimulatorRNG(seed=43)
    assert [a.gauss(0, 1) for _ in range(5)] != [b.gauss(0, 1) for _ in range(5)]


def test_seed_zero_is_valid_and_deterministic() -> None:
    a, b = SimulatorRNG(seed=0), SimulatorRNG(seed=0)
    assert a.random() == b.random()
