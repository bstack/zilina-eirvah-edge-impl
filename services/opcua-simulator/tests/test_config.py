from opcua_simulator.config import SimulatorSettings


def test_defaults_are_safe(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    for var in (
        "OPCUA_SIMULATOR_ENDPOINT",
        "OPCUA_SIMULATOR_TICK_RATE_MS",
        "OPCUA_SIMULATOR_SEED",
        "OPCUA_SIMULATOR_ADDRESS_SPACE_PATH",
        "OPCUA_SIMULATOR_HTTP_PORT",
        "OPCUA_SIMULATOR_HOT_SPIKE_PROBABILITY",
    ):
        monkeypatch.delenv(var, raising=False)
    s = SimulatorSettings()
    assert s.endpoint == "opc.tcp://0.0.0.0:4840/eirvah/simulator"
    assert s.tick_rate_ms == 100
    assert s.seed == 0
    assert s.http_port == 8080
    assert s.hot_spike_probability == 0.0
    assert s.address_space_path.name == "opcua-address-space.yaml"


def test_env_vars_override(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("OPCUA_SIMULATOR_SEED", "1234")
    monkeypatch.setenv("OPCUA_SIMULATOR_TICK_RATE_MS", "50")
    s = SimulatorSettings()
    assert s.seed == 1234
    assert s.tick_rate_ms == 50
