from datetime import UTC, datetime

from opcua_simulator.setpoint import Setpoint, SetpointWrite


def _now() -> datetime:
    return datetime(2026, 5, 16, 13, 45, 22, tzinfo=UTC)


def test_initial_value_is_default() -> None:
    sp = Setpoint(initial=22.0)
    assert sp.value == 22.0
    assert sp.write_history() == []


def test_write_takes_effect_immediately() -> None:
    sp = Setpoint(initial=22.0)
    sp.write(value=18.0, writer_session="s1", at=_now())
    assert sp.value == 18.0


def test_write_history_records_each_write() -> None:
    sp = Setpoint(initial=22.0)
    sp.write(value=18.0, writer_session="s1", at=_now())
    sp.write(value=20.0, writer_session="s2", at=_now())
    history = sp.write_history()
    assert len(history) == 2
    assert history[0] == SetpointWrite(value=18.0, writer_session="s1", at=_now())
    assert history[1].value == 20.0


def test_write_count_increments() -> None:
    sp = Setpoint(initial=22.0)
    sp.write(value=18.0, writer_session="s1", at=_now())
    sp.write(value=20.0, writer_session="s1", at=_now())
    assert sp.write_count_by_writer() == {"s1": 2}
