"""Entry point for the decision agent stub pod."""

from __future__ import annotations

import asyncio

from decision_agent_stub.config import DecisionAgentSettings
from decision_agent_stub.service import run


def main() -> None:
    asyncio.run(run(DecisionAgentSettings()))


if __name__ == "__main__":  # pragma: no cover
    main()
