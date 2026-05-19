"""Entry point for the actuation event validator pod."""

from __future__ import annotations

import asyncio

from actuation_event_validator.config import ValidatorSettings
from actuation_event_validator.service import run


def main() -> None:
    asyncio.run(run(ValidatorSettings()))


if __name__ == "__main__":  # pragma: no cover
    main()
