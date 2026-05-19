import re

import pytest
from eirvah_contracts.ulid import generate_correlation_id, is_valid_correlation_id


def test_generate_correlation_id_returns_26_char_crockford_base32() -> None:
    cid = generate_correlation_id()
    assert isinstance(cid, str)
    assert len(cid) == 26
    assert re.fullmatch(r"[0-9A-HJKMNP-TV-Z]{26}", cid) is not None


def test_generated_ids_are_unique_within_a_burst() -> None:
    ids = {generate_correlation_id() for _ in range(1000)}
    assert len(ids) == 1000


def test_generated_ids_are_lexicographically_sortable_by_time() -> None:
    earlier = generate_correlation_id()
    later = generate_correlation_id()
    assert earlier <= later  # ULID is monotonic given fast successive calls within ms


def test_is_valid_correlation_id_accepts_a_generated_id() -> None:
    assert is_valid_correlation_id(generate_correlation_id()) is True


@pytest.mark.parametrize(
    "bad",
    [
        "",
        "too-short",
        "01HZXC8P9G7Q3M6V0K2T8R5W4",       # 25 chars
        "01HZXC8P9G7Q3M6V0K2T8R5W4AX",     # 27 chars
        "01HZXC8P9G7Q3M6V0K2T8R5W4!",      # invalid char
        "01hzxc8p9g7q3m6v0k2t8r5w4a",      # lowercase not allowed
    ],
)
def test_is_valid_correlation_id_rejects_malformed(bad: str) -> None:
    assert is_valid_correlation_id(bad) is False
