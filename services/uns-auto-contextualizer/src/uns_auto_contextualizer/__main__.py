"""Entry point for the UNS auto-contextualizer pod."""

from __future__ import annotations

import asyncio

from uns_auto_contextualizer.config import AutoContextualizerSettings
from uns_auto_contextualizer.service import run


def main() -> None:
    asyncio.run(run(AutoContextualizerSettings()))


if __name__ == "__main__":  # pragma: no cover
    main()
