"""Request-reply helper with per-call timeout and correlation-ID propagation."""

from __future__ import annotations

from nats.aio.client import Client as NATSClient
from nats.aio.msg import Msg

#: Header carrying the ULID correlation ID across every NATS hop (spec §4.4).
BUS_HEADER_CORRELATION_ID = "X-Correlation-Id"


class RequestTimeout(TimeoutError):
    """Raised when a NATS request-reply call exceeds its per-call timeout."""


async def request_reply(
    *,
    nc: NATSClient,
    subject: str,
    payload: bytes,
    correlation_id: str,
    timeout_s: float,
) -> Msg:
    """Send a NATS request and await a reply, with timeout + correlation header.

    Raises ``RequestTimeout`` on timeout so callers can distinguish bus-timeouts
    from other ``TimeoutError`` sources.
    """
    headers = {BUS_HEADER_CORRELATION_ID: correlation_id}
    try:
        return await nc.request(subject, payload, timeout=timeout_s, headers=headers)
    except TimeoutError as exc:
        raise RequestTimeout(
            f"NATS request to {subject!r} timed out after {timeout_s}s"
        ) from exc
