"""Entry point for the data-converter pod."""

from __future__ import annotations

import asyncio

from data_converter.config import DataConverterSettings
from data_converter.service import run


def main() -> None:
    asyncio.run(run(DataConverterSettings()))


if __name__ == "__main__":  # pragma: no cover
    main()
