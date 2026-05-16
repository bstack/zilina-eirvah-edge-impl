from pathlib import Path

import pytest
from opcua_simulator.address_space import AddressSpaceModel, load_address_space

REPO_ROOT = Path(__file__).resolve().parents[3]
SAMPLE = REPO_ROOT / "config" / "opcua-address-space.yaml"


def test_loads_sample_file() -> None:
    model = load_address_space(SAMPLE)
    assert isinstance(model, AddressSpaceModel)
    assert model.namespace.startswith("https://")
    assert model.uns_defaults.enterprise == "uniza"
    assert any(n.id.endswith("Temperature") for n in model.iter_nodes())


def test_each_node_carries_uns_path_fields() -> None:
    model = load_address_space(SAMPLE)
    for node in model.iter_nodes():
        assert node.cell
        assert node.equipment
        assert node.measurement


def test_setpoint_nodes_have_policy() -> None:
    model = load_address_space(SAMPLE)
    setpoints = [n for n in model.iter_nodes() if n.kind == "setpoint"]
    assert setpoints
    for n in setpoints:
        assert n.policy is not None
        assert n.policy.min < n.policy.max


def test_load_rejects_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_address_space(tmp_path / "missing.yaml")


def test_load_rejects_malformed_yaml(tmp_path: Path) -> None:
    bad = tmp_path / "bad.yaml"
    bad.write_text(": : :\n")
    with pytest.raises(ValueError):
        load_address_space(bad)
