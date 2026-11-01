"""One immutable, bottle-specific inspection result carried through the UNS."""

from __future__ import annotations

from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field


class BottleInspection(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["1.0"] = "1.0"
    inspection_id: str = Field(pattern=r"^[0-9a-f]{16}:[0-9a-f]{4}$")
    bottle_id: int = Field(strict=True, ge=1, le=65535)
    passed: bool = Field(strict=True)
    inspected_at: AwareDatetime
