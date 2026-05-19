"""Entry point for the actuation control orchestrator pod."""

from __future__ import annotations

import asyncio

from actuation_control_orchestrator.config import ActuationOrchestratorSettings
from actuation_control_orchestrator.service import run


def main() -> None:
    asyncio.run(run(ActuationOrchestratorSettings()))


if __name__ == "__main__":  # pragma: no cover
    main()
