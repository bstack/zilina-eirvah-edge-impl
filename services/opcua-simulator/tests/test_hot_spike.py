from opcua_simulator.hot_spike import SPIKE_AMPLITUDE, HotSpike
from opcua_simulator.rng import SimulatorRNG


def test_no_spike_when_probability_zero() -> None:
    hs = HotSpike(rng=SimulatorRNG(0), stochastic_probability=0.0)
    assert all(hs.tick() == 0.0 for _ in range(20))


def test_method_trigger_emits_amplitude_on_next_tick() -> None:
    hs = HotSpike(rng=SimulatorRNG(0), stochastic_probability=0.0)
    hs.trigger_via_method()
    assert hs.tick() == SPIKE_AMPLITUDE


def test_spike_decays_at_0_9_per_tick() -> None:
    hs = HotSpike(rng=SimulatorRNG(0), stochastic_probability=0.0)
    hs.trigger_via_method()
    first = hs.tick()
    second = hs.tick()
    third = hs.tick()
    assert first == SPIKE_AMPLITUDE
    assert abs(second - SPIKE_AMPLITUDE * 0.9) < 1e-9
    assert abs(third - SPIKE_AMPLITUDE * 0.9 * 0.9) < 1e-9


def test_stochastic_probability_one_always_triggers() -> None:
    hs = HotSpike(rng=SimulatorRNG(0), stochastic_probability=1.0)
    assert hs.tick() == SPIKE_AMPLITUDE


def test_trigger_counts_by_source() -> None:
    hs = HotSpike(rng=SimulatorRNG(0), stochastic_probability=1.0)
    hs.tick()
    hs.trigger_via_method()
    hs.tick()
    counts = hs.trigger_counts()
    assert counts["stochastic"] == 1
    assert counts["method"] == 1
