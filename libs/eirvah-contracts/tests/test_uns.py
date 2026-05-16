import pytest
from pydantic import ValidationError

from eirvah_contracts.uns import UNSPath, parse_uns_topic, build_uns_topic


def _sample() -> UNSPath:
    return UNSPath(
        enterprise="uniza",
        site="zilina",
        area="factory1",
        line="line_a",
        cell="bottler",
        equipment="temperature_sensor_01",
        measurement="temperature",
    )


def test_build_topic_joins_seven_segments_with_slash() -> None:
    assert (
        build_uns_topic(_sample())
        == "uniza/zilina/factory1/line_a/bottler/temperature_sensor_01/temperature"
    )


def test_parse_round_trips_with_build() -> None:
    topic = "uniza/zilina/factory1/line_a/bottler/motor_01/rpm"
    path = parse_uns_topic(topic)
    assert build_uns_topic(path) == topic
    assert path.equipment == "motor_01"
    assert path.measurement == "rpm"


@pytest.mark.parametrize(
    "bad_topic",
    [
        "too/few/segments",
        "uniza/zilina/factory1/line_a/bottler/equipment",                 # 6 segments
        "uniza/zilina/factory1/line_a/bottler/equipment/x/y",             # 8 segments
        "uniza/zilina/factory1/line_a/bottler/equipment/UPPER",           # uppercase
        "uniza/zilina/factory1/line_a/bottler/equip-ment/measurement",    # hyphen
        "uniza//factory1/line_a/bottler/equipment/measurement",           # empty segment
        "uniza/zilina/factory1/line_a/bottler/equipment/m e a s",         # space
    ],
)
def test_parse_rejects_invalid_topics(bad_topic: str) -> None:
    with pytest.raises(ValueError):
        parse_uns_topic(bad_topic)


@pytest.mark.parametrize(
    "field, bad_value",
    [
        ("enterprise", "Uniza"),
        ("site", "zi-lina"),
        ("area", ""),
        ("equipment", "x y"),
        ("measurement", "TEMP"),
    ],
)
def test_segment_validation_rejects_disallowed_chars(field: str, bad_value: str) -> None:
    kwargs = dict(
        enterprise="uniza",
        site="zilina",
        area="factory1",
        line="line_a",
        cell="bottler",
        equipment="motor_01",
        measurement="rpm",
    )
    kwargs[field] = bad_value
    with pytest.raises(ValidationError):
        UNSPath(**kwargs)
