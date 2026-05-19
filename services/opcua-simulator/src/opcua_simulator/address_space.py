"""Pydantic model + YAML loader for the simulator's address-space config."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError


class NodePolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")
    min: float
    max: float


class NodeDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: str
    kind: Literal["measurement", "setpoint"]
    cell: str
    equipment: str
    measurement: str
    value_type: Literal["double", "int64", "bool", "string"]
    unit: str
    initial: float | int | bool | str
    semantic_type: str
    dynamics: str | None = None
    policy: NodePolicy | None = None
    bad_quality_pct: float = 0.0
    uncertain_quality_pct: float = 0.0


class EquipmentDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    nodes: list[NodeDefinition]


class UNSDefaults(BaseModel):
    model_config = ConfigDict(extra="forbid")
    enterprise: str
    site: str
    area: str
    line: str


class AddressSpaceModel(BaseModel):
    model_config = ConfigDict(extra="forbid")
    namespace: str
    uns_defaults: UNSDefaults
    equipments: list[EquipmentDefinition] = Field(default_factory=list)

    def iter_nodes(self) -> Iterator[NodeDefinition]:
        for eq in self.equipments:
            yield from eq.nodes


def load_address_space(path: Path) -> AddressSpaceModel:
    if not path.is_file():
        raise FileNotFoundError(path)
    try:
        raw = yaml.safe_load(path.read_text())
    except yaml.YAMLError as exc:
        raise ValueError(f"malformed YAML at {path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise ValueError(f"address space at {path} must be a mapping at the top level")
    try:
        return AddressSpaceModel.model_validate(raw)
    except ValidationError as exc:
        raise ValueError(f"invalid address space at {path}: {exc}") from exc
