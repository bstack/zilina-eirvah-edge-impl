"""Authenticated ingress and approval evidence independent of transport."""

from __future__ import annotations

import hashlib
import hmac
import json
from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict

from eirvah_contracts.actuation import ActuationRequest, SignedEvidence


class AuthenticatedRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1.0"] = "1.0"
    command: ActuationRequest
    identity: SignedEvidence


class ApprovedWriteRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1.0"] = "1.0"
    command: ActuationRequest
    approval: SignedEvidence


def current_time() -> datetime:
    return datetime.now(UTC)


def require_key(key: str) -> bytes:
    if len(key.encode()) < 32:
        raise ValueError("actuation authentication key must contain at least 32 bytes")
    return key.encode()


def command_digest(command: ActuationRequest) -> str:
    if command.deadline is None or command.deadline.utcoffset() is None:
        raise ValueError("command requires a timezone-aware deadline")
    if command.requested_at.utcoffset() is None:
        raise ValueError("command requires a timezone-aware requested_at")
    payload = command.model_dump(mode="json")
    payload["deadline"] = command.deadline.astimezone(UTC).isoformat()
    payload["requested_at"] = command.requested_at.astimezone(UTC).isoformat()
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def _signature(evidence: SignedEvidence, key: str) -> str:
    payload = evidence.model_dump(mode="json", exclude={"signature"})
    payload["issued_at"] = evidence.issued_at.astimezone(UTC).isoformat()
    return hmac.new(
        require_key(key),
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(),
        hashlib.sha256,
    ).hexdigest()


def _sign(
    command: ActuationRequest,
    key: str,
    purpose: Literal["ingress", "approval"],
    writer: str,
    *,
    allow_expired: bool = False,
) -> SignedEvidence:
    evidence = SignedEvidence(
        purpose=purpose,
        command_digest=command_digest(command),
        writer=writer,
        issued_at=current_time(),
        signature="0" * 64,
    )
    if command.deadline is None or (not allow_expired and current_time() > command.deadline):
        raise ValueError("command expired")
    return evidence.model_copy(update={"signature": _signature(evidence, key)})


def sign_ingress(
    command: ActuationRequest, key: str, *, allow_expired: bool = False
) -> SignedEvidence:
    return _sign(command, key, "ingress", "", allow_expired=allow_expired)


def sign_approval(
    command: ActuationRequest, key: str, writer: str, *, allow_expired: bool = False
) -> SignedEvidence:
    return _sign(command, key, "approval", writer, allow_expired=allow_expired)


def verify_evidence(
    command: ActuationRequest,
    evidence: SignedEvidence,
    key: str,
    purpose: Literal["ingress", "approval"],
    writer: str,
    *,
    now: datetime | None = None,
    allow_expired: bool = False,
) -> None:
    at = now if now is not None else current_time()
    if evidence.purpose != purpose or evidence.writer != writer:
        raise ValueError("approval purpose or writer mismatch")
    if not hmac.compare_digest(evidence.command_digest, command_digest(command)):
        raise ValueError("approved command mismatch")
    if not hmac.compare_digest(evidence.signature, _signature(evidence, key)):
        raise ValueError("invalid command authentication")
    if (
        command.deadline is None
        or (not allow_expired and at > command.deadline)
        or evidence.issued_at > at
    ):
        raise ValueError("command expired or evidence time invalid")
