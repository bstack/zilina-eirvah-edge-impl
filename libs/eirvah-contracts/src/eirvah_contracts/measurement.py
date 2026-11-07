"""Engineering resolution and pre-rounding range checks for selected signal contracts."""

from __future__ import annotations

import math
from decimal import ROUND_HALF_EVEN, Decimal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class EngineeringPrecision(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    resolution: float = Field(gt=0, allow_inf_nan=False)
    minimum: float = Field(allow_inf_nan=False)
    maximum: float = Field(allow_inf_nan=False)
    encoding_error: float = Field(default=0, ge=0, allow_inf_nan=False)

    @model_validator(mode="after")
    def check_range(self) -> EngineeringPrecision:
        if self.minimum >= self.maximum:
            raise ValueError("minimum must be below maximum")
        if self.encoding_error >= self.resolution / 2:
            raise ValueError("encoding error must be below half a resolution step")
        step = Decimal(str(self.resolution))
        for limit in (self.minimum, self.maximum):
            ratio = Decimal(str(limit)) / step
            if ratio != ratio.to_integral_value():
                raise ValueError("range boundaries must lie on the resolution grid")
        return self


def normalize_measurement(value: object, policy: EngineeringPrecision) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("engineering value must be a finite number")
    try:
        number = float(value)
    except OverflowError as exc:
        raise ValueError("engineering value must be a finite number") from exc
    if not math.isfinite(number):
        raise ValueError("engineering value must be a finite number")
    # Guard the original engineering value before rounding; 0.804 must not become valid 0.80.
    if (
        number < policy.minimum - policy.encoding_error
        or number > policy.maximum + policy.encoding_error
    ):
        raise ValueError("engineering value outside declared range")
    bounded = min(policy.maximum, max(policy.minimum, number))
    step = Decimal(str(policy.resolution))
    rounded = (Decimal(str(bounded)) / step).to_integral_value(rounding=ROUND_HALF_EVEN) * step
    return float(rounded)
