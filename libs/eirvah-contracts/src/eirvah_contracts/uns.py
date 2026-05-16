"""UNS hierarchy model and MQTT topic helpers.

Strict 7-level ISA-95 hierarchy (spec §4.1):

    {enterprise}/{site}/{area}/{line}/{cell}/{equipment}/{measurement}

Segments are lowercase ASCII; allowed characters [a-z0-9_].
"""

from __future__ import annotations

import re
from typing import Annotated

from pydantic import BaseModel, ConfigDict, StringConstraints

# A single UNS segment: lowercase, alphanumerics + underscore, non-empty.
_SEGMENT_RE = r"^[a-z0-9_]+$"

UNSSegment = Annotated[
    str,
    StringConstraints(pattern=_SEGMENT_RE, min_length=1, max_length=128),
]


class UNSPath(BaseModel):
    """The 7-level ISA-95 hierarchy that uniquely names a UNS measurement."""

    model_config = ConfigDict(frozen=True, str_strip_whitespace=False)

    enterprise: UNSSegment
    site: UNSSegment
    area: UNSSegment
    line: UNSSegment
    cell: UNSSegment
    equipment: UNSSegment
    measurement: UNSSegment


_LEVELS = ("enterprise", "site", "area", "line", "cell", "equipment", "measurement")


def build_uns_topic(path: UNSPath) -> str:
    """Join the 7 segments of *path* into an MQTT topic string."""
    return "/".join(getattr(path, level) for level in _LEVELS)


def parse_uns_topic(topic: str) -> UNSPath:
    """Parse a 7-segment UNS topic into a :class:`UNSPath`.

    Raises ``ValueError`` if the topic has the wrong number of segments,
    or ``pydantic.ValidationError`` if any segment is malformed.
    """
    segments = topic.split("/")
    if len(segments) != len(_LEVELS):
        raise ValueError(
            f"UNS topic must have exactly {len(_LEVELS)} segments; got {len(segments)}: {topic!r}"
        )
    if any(not _is_segment(seg) for seg in segments):
        raise ValueError(f"UNS topic contains an invalid segment: {topic!r}")
    return UNSPath(**dict(zip(_LEVELS, segments, strict=True)))


def _is_segment(value: str) -> bool:
    return re.fullmatch(_SEGMENT_RE, value) is not None
