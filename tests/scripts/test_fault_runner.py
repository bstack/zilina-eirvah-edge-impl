"""Capture helpers close stdin and isolate each injected fault plan."""

import json
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from fault_case import FaultRun


def test_private_proxy_read_closes_stdin(tmp_path):
    reader = Mock()
    reader.run.return_value = '{"ready":true}'
    cluster = Mock()
    cluster.target.return_value = reader
    runner = FaultRun(SimpleNamespace(cluster=cluster, output=tmp_path))

    result = runner.proxy_exec("private read")

    assert result == {"ready": True}
    assert reader.run.call_args.kwargs["input"] == ""
    assert "-c" in reader.run.call_args.args
    assert "native-fault-proxy" in reader.run.call_args.args


async def test_fault_is_not_cleared_before_previous_attempt_settles(tmp_path):
    events = []
    source = {"clock": {"wall": datetime.now(UTC).timestamp()}, "last_serial": 0, "last_effect": 0}
    recovery = SimpleNamespace(
        cluster=Mock(),
        output=tmp_path,
        source=Mock(return_value=source),
        read=Mock(return_value=source),
    )
    runner = FaultRun(recovery)
    ledger = {"rows": [{"state": "retryable"}], "clock": {"domain": "local"}}
    runner.ledger = Mock(side_effect=lambda command: ledger)

    async def wait(read, predicate, seconds=90):
        packet = read()
        assert predicate(packet)
        events.append("settled")
        return packet

    recovery.wait = wait

    def plan(name, mode, command):
        if mode == "normal":
            assert "settled" in events
        events.append(mode)

    runner.plan = plan
    runner.proxy = Mock(return_value={"records": []})
    runner.journal = Mock(return_value={})
    runner.submit = AsyncMock(side_effect=[{"decision": "unknown"}, {"decision": "approve"}])

    await runner.case("before_write_disconnect", 10)

    assert events.index("settled") < events.index("normal")
    data = json.loads((tmp_path / "evidence.json").read_text())
    assert data["cases"][0]["command"] == data["cases"][0]["replay_command"]
