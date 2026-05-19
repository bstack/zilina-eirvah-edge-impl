from opcua_simulator.quality import QualityEmitter
from opcua_simulator.rng import SimulatorRNG


def test_default_is_always_good() -> None:
    q = QualityEmitter(rng=SimulatorRNG(0), bad_quality_pct=0.0, uncertain_quality_pct=0.0)
    assert all(q.next() == "good" for _ in range(100))


def test_full_bad_pct_always_returns_bad() -> None:
    q = QualityEmitter(rng=SimulatorRNG(0), bad_quality_pct=1.0, uncertain_quality_pct=0.0)
    assert all(q.next() == "bad" for _ in range(50))


def test_partial_bad_pct_is_approximately_correct() -> None:
    q = QualityEmitter(rng=SimulatorRNG(42), bad_quality_pct=0.1, uncertain_quality_pct=0.0)
    samples = [q.next() for _ in range(10_000)]
    assert 0.08 < samples.count("bad") / len(samples) < 0.12


def test_counters_track_emissions() -> None:
    q = QualityEmitter(rng=SimulatorRNG(0), bad_quality_pct=1.0, uncertain_quality_pct=0.0)
    for _ in range(5):
        q.next()
    counts = q.emission_counts()
    assert counts["bad"] == 5
    assert counts.get("good", 0) == 0
