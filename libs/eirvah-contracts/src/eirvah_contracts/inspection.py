"""One immutable, bottle-specific inspection result carried through the UNS."""

from __future__ import annotations

import hashlib
import hmac
import json
from datetime import UTC, datetime
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from eirvah_contracts.authorization import require_key


class BottleInspection(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["1.0"] = "1.0"
    inspection_id: str = Field(pattern=r"^[0-9a-f]{16}:[0-9a-f]{4}$")
    bottle_id: int = Field(strict=True, ge=1, le=65535)
    passed: bool = Field(strict=True)
    inspected_at: AwareDatetime


def decode_inspection(registers: list[int]) -> BottleInspection | None:
    if len(registers) != 10 or any(type(v) is not int or not 0 <= v <= 65535 for v in registers):
        raise ValueError("invalid inspection snapshot")
    session = sum(registers[i] << (48 - i * 16) for i in range(4))
    sequence, bottle, status = registers[4:7]
    if status == 0:
        return None
    if status not in (1, 2) or session == 0 or sequence == 0:
        raise ValueError("invalid inspection identity/status")
    millis = (registers[7] << 32) | (registers[8] << 16) | registers[9]
    return BottleInspection(
        inspection_id=f"{session:016x}:{sequence:04x}",
        bottle_id=bottle,
        passed=status == 1,
        inspected_at=datetime.fromtimestamp(millis / 1000, UTC),
    )


def encode_inspection(event: BottleInspection | None) -> list[int]:
    if event is None:
        return [0] * 10
    epoch, seq = event.inspection_id.split(":")
    session = int(epoch, 16)
    delta = event.inspected_at.astimezone(UTC) - datetime(1970, 1, 1, tzinfo=UTC)
    millis = delta.days * 86400000 + delta.seconds * 1000 + delta.microseconds // 1000
    return [
        *[(session >> shift) & 65535 for shift in (48, 32, 16, 0)],
        int(seq, 16),
        event.bottle_id,
        1 if event.passed else 2,
        *[(millis >> shift) & 65535 for shift in (32, 16, 0)],
    ]


def inspection_digest(event: BottleInspection) -> str:
    normalized = event.model_copy(update={"inspected_at": event.inspected_at.astimezone(UTC)})
    return hashlib.sha256(
        json.dumps(
            normalized.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        ).encode()
    ).hexdigest()


class InspectionReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal["1.0"] = "1.0"
    inspection_id: str
    event_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    consumer: str
    signature: str = Field(pattern=r"^[0-9a-f]{64}$")


def sign_receipt(
    event: BottleInspection, key: str, consumer: str = "decision-agent-stub"
) -> InspectionReceipt:
    require_key(key)
    body = dict(
        schema_version="1.0",
        inspection_id=event.inspection_id,
        event_digest=inspection_digest(event),
        consumer=consumer,
    )
    signature = hmac.new(
        key.encode(),
        json.dumps(body, sort_keys=True, separators=(",", ":")).encode(),
        hashlib.sha256,
    ).hexdigest()
    return InspectionReceipt.model_validate({**body, "signature": signature})


def verify_receipt(
    receipt: InspectionReceipt,
    event: BottleInspection,
    key: str,
    consumer: str = "decision-agent-stub",
) -> None:
    expected = sign_receipt(event, key, consumer)
    if receipt.model_copy(
        update={"signature": expected.signature}
    ) != expected or not hmac.compare_digest(receipt.signature, expected.signature):
        raise ValueError("invalid inspection receipt")
