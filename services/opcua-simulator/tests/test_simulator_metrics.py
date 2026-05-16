from opcua_simulator.metrics import SimulatorMetrics
from prometheus_client import CollectorRegistry


def _labels() -> dict[str, str]:
    return {
        "enterprise": "uniza",
        "site": "zilina",
        "area": "factory1",
        "line": "line_a",
        "cell": "bottler",
        "equipment": "temperature_sensor_01",
    }


def test_gauges_register_with_eirvah_prefix() -> None:
    reg = CollectorRegistry()
    m = SimulatorMetrics(registry=reg)
    m.set_temperature(_labels(), 23.4)
    m.set_setpoint({**_labels(), "equipment": "setpoint_unit"}, 22.0)
    m.set_motor_state({**_labels(), "equipment": "motor_01"}, 2)
    m.set_motor_rpm({**_labels(), "equipment": "motor_01"}, 1500.0)
    m.set_throughput({**_labels(), "equipment": "throughput_meter_01"}, 0.9)

    assert reg.get_sample_value("eirvah_simulator_temperature_celsius", _labels()) == 23.4
    assert (
        reg.get_sample_value(
            "eirvah_simulator_setpoint_celsius", {**_labels(), "equipment": "setpoint_unit"}
        )
        == 22.0
    )
    assert (
        reg.get_sample_value(
            "eirvah_simulator_motor_state", {**_labels(), "equipment": "motor_01"}
        )
        == 2
    )


def test_quality_counter_increments() -> None:
    reg = CollectorRegistry()
    m = SimulatorMetrics(registry=reg)
    m.inc_quality(labels=_labels(), quality="good")
    m.inc_quality(labels=_labels(), quality="good")
    m.inc_quality(labels=_labels(), quality="bad")
    assert (
        reg.get_sample_value("eirvah_simulator_quality_count_total", {**_labels(), "quality": "good"})
        == 2
    )
    assert (
        reg.get_sample_value("eirvah_simulator_quality_count_total", {**_labels(), "quality": "bad"})
        == 1
    )


def test_setpoint_writes_counter() -> None:
    reg = CollectorRegistry()
    m = SimulatorMetrics(registry=reg)
    m.inc_setpoint_write(writer="opcua-session-1")
    m.inc_setpoint_write(writer="opcua-session-1")
    assert (
        reg.get_sample_value("eirvah_simulator_setpoint_writes_total", {"writer": "opcua-session-1"})
        == 2
    )


def test_hot_spike_counter_by_trigger() -> None:
    reg = CollectorRegistry()
    m = SimulatorMetrics(registry=reg)
    m.inc_hot_spike(trigger="method")
    m.inc_hot_spike(trigger="stochastic")
    m.inc_hot_spike(trigger="stochastic")
    assert reg.get_sample_value("eirvah_simulator_hot_spikes_total", {"trigger": "method"}) == 1
    assert reg.get_sample_value("eirvah_simulator_hot_spikes_total", {"trigger": "stochastic"}) == 2
