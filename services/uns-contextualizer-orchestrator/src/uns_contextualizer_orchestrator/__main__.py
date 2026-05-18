"""Entry point for the UNS contextualizer orchestrator pod."""

from __future__ import annotations

import asyncio

from uns_contextualizer_orchestrator.config import OrchestratorSettings
from uns_contextualizer_orchestrator.service import run


def main() -> None:
    asyncio.run(run(OrchestratorSettings()))


if __name__ == "__main__":  # pragma: no cover
    main()
