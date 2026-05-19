"""Unit tests for actuation-signal-publisher."""

from __future__ import annotations

from pathlib import Path

import pytest


def _write_happy_path_files(tmp_path: Path) -> tuple[Path, Path]:
    mapping_file = tmp_path / "opcua-node-to-uns-mapping.yaml"
    mapping_file.write_text(
        "mappings:\n"
        "  - node_id: \"Bottler.SetpointUnit.SetpointTemperature\"\n"
        "    area: factory1\n"
        "    line: line_a\n"
        "    cell: bottler\n"
        "    equipment: setpoint_unit\n"
        "    measurement: setpoint_temperature\n"
        "    semantic_type: setpoint.target\n"
        "  - node_id: \"Bottler.Temperature01\"\n"
        "    area: factory1\n"
        "    line: line_a\n"
        "    cell: bottler\n"
        "    equipment: temperature_sensor_01\n"
        "    measurement: temperature\n"
        "    semantic_type: temperature.celsius\n"
    )
    node_list_file = tmp_path / "opcua-node-list.yaml"
    node_list_file.write_text(
        "endpoint: \"opc.tcp://opcua-simulator:4840/eirvah/simulator\"\n"
        "namespace_uri: \"https://eirvah.uniza/zilina/factory1\"\n"
        "publishing_interval_ms: 500\n"
        "nodes:\n"
        "  - browse_names: [\"bottler\", \"SetpointTemperature\"]\n"
        "    alias: \"Bottler.SetpointUnit.SetpointTemperature\"\n"
        "  - browse_names: [\"bottler\", \"Temperature\"]\n"
        "    alias: \"Bottler.Temperature01\"\n"
    )
    return mapping_file, node_list_file


def test_load_reverse_mapping_builds_uns_to_browse(tmp_path: Path) -> None:
    from actuation_signal_publisher.service import load_write_targets

    mapping_file, node_list_file = _write_happy_path_files(tmp_path)
    targets = load_write_targets(
        mapping_path=mapping_file,
        node_list_path=node_list_file,
        enterprise="uniza",
        site="zilina",
    )

    topic = "uniza/zilina/factory1/line_a/bottler/setpoint_unit/setpoint_temperature"
    assert topic in targets
    target = targets[topic]
    assert target.browse_names == ["bottler", "SetpointTemperature"]
    assert "opcua-simulator" in target.endpoint
    assert "factory1" in target.namespace_uri


def test_load_write_targets_fails_on_non_bijective_mapping(tmp_path: Path) -> None:
    from actuation_signal_publisher.service import load_write_targets

    mapping_file = tmp_path / "opcua-node-to-uns-mapping.yaml"
    mapping_file.write_text(
        "mappings:\n"
        "  - node_id: \"Bottler.SetpointUnit.SetpointTemperature\"\n"
        "    area: factory1\n"
        "    line: line_a\n"
        "    cell: bottler\n"
        "    equipment: setpoint_unit\n"
        "    measurement: setpoint_temperature\n"
        "    semantic_type: setpoint.target\n"
        "  - node_id: \"Bottler.SetpointUnit.SetpointTemperatureAlt\"\n"
        "    area: factory1\n"
        "    line: line_a\n"
        "    cell: bottler\n"
        "    equipment: setpoint_unit\n"
        "    measurement: setpoint_temperature\n"
        "    semantic_type: setpoint.target\n"
    )
    node_list_file = tmp_path / "opcua-node-list.yaml"
    node_list_file.write_text(
        "endpoint: \"opc.tcp://opcua-simulator:4840/eirvah/simulator\"\n"
        "namespace_uri: \"https://eirvah.uniza/zilina/factory1\"\n"
        "publishing_interval_ms: 500\n"
        "nodes:\n"
        "  - browse_names: [\"bottler\", \"SetpointTemperature\"]\n"
        "    alias: \"Bottler.SetpointUnit.SetpointTemperature\"\n"
        "  - browse_names: [\"bottler\", \"SetpointTemperatureAlt\"]\n"
        "    alias: \"Bottler.SetpointUnit.SetpointTemperatureAlt\"\n"
    )

    with pytest.raises(ValueError, match="bijective"):
        load_write_targets(
            mapping_path=mapping_file,
            node_list_path=node_list_file,
            enterprise="uniza",
            site="zilina",
        )
