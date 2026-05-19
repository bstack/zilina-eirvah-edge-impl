"""ULID-based correlation IDs.

ULIDs are 26-char Crockford Base32, time-prefixed and lexicographically sortable.
We use them as the single end-to-end traceability mechanism — see spec §4.2.
"""

from __future__ import annotations

import re

from ulid import ULID

_ULID_RE = re.compile(r"^[0-9A-HJKMNP-TV-Z]{26}$")


def generate_correlation_id() -> str:
    """Return a new ULID in canonical 26-char uppercase Crockford Base32."""
    return str(ULID())


def is_valid_correlation_id(value: str) -> bool:
    """True iff ``value`` is a syntactically valid uppercase ULID."""
    return bool(_ULID_RE.fullmatch(value))
