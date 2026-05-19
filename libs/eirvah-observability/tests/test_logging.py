import io
import json

import structlog
from eirvah_observability.logging import bind_correlation_id, configure_logging


def test_configure_logging_emits_json_to_stdout() -> None:
    buffer = io.StringIO()
    configure_logging(level="INFO", stream=buffer)
    log = structlog.get_logger("test")
    log.info("hello", k=1)

    line = buffer.getvalue().strip().splitlines()[-1]
    parsed = json.loads(line)
    assert parsed["event"] == "hello"
    assert parsed["k"] == 1
    assert parsed["level"] == "info"
    assert "timestamp" in parsed


def test_bind_correlation_id_attaches_field() -> None:
    buffer = io.StringIO()
    configure_logging(level="INFO", stream=buffer)
    bind_correlation_id("01HZXC8P9G7Q3M6V0K2T8R5W4A")
    structlog.get_logger("svc").info("event")

    parsed = json.loads(buffer.getvalue().strip().splitlines()[-1])
    assert parsed["correlation_id"] == "01HZXC8P9G7Q3M6V0K2T8R5W4A"


def test_level_filtering_drops_debug_when_info() -> None:
    buffer = io.StringIO()
    configure_logging(level="INFO", stream=buffer)
    structlog.get_logger("svc").debug("hidden")
    assert buffer.getvalue() == ""
