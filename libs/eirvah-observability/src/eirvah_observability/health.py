"""ASGI app exposing ``/healthz``, ``/readyz``, and ``/metrics``."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from prometheus_client.registry import REGISTRY, CollectorRegistry
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import PlainTextResponse, Response
from starlette.routing import Route

if TYPE_CHECKING:
    from starlette.types import ASGIApp


class HealthApp:
    """Bundle of the three operational endpoints every EirVah service exposes.

    ``is_ready`` is a callable so services can plug in their own readiness
    semantics (e.g. NATS connected, OPC UA session up).
    """

    def __init__(
        self,
        *,
        is_ready: Callable[[], bool],
        registry: CollectorRegistry = REGISTRY,
    ) -> None:
        self._is_ready = is_ready
        self._registry = registry
        self._app = Starlette(
            routes=[
                Route("/healthz", self._healthz, methods=["GET"]),
                Route("/readyz", self._readyz, methods=["GET"]),
                Route("/metrics", self._metrics, methods=["GET"]),
            ]
        )

    @property
    def asgi(self) -> ASGIApp:
        return self._app

    async def _healthz(self, _request: Request) -> Response:
        return PlainTextResponse("ok")

    async def _readyz(self, _request: Request) -> Response:
        if self._is_ready():
            return PlainTextResponse("ready")
        return PlainTextResponse("not ready", status_code=503)

    async def _metrics(self, _request: Request) -> Response:
        return Response(
            content=generate_latest(self._registry),
            media_type=CONTENT_TYPE_LATEST,
        )
