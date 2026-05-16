"""Queue-group consumer helper.

NATS queue groups load-balance messages across all subscribers in the group.
By convention we name the queue the same as the subject — so scaling a worker
to N replicas just works (spec §4.4).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from nats.aio.client import Client as NATSClient
from nats.aio.msg import Msg

Handler = Callable[[Msg], Awaitable[None]]


async def subscribe_queue_group(
    *,
    nc: NATSClient,
    subject: str,
    handler: Handler,
    queue: str | None = None,
) -> None:
    """Subscribe *handler* to *subject* in a NATS queue group.

    If *queue* is omitted, it defaults to *subject* — the EirVah convention.
    """
    await nc.subscribe(subject, queue=queue or subject, cb=handler)
