"""Load must reach the real contextualizer and account for every reply."""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import load_inject
import pytest
from eirvah_contracts.envelope import EnvelopeError, NATSEnvelope
from nats.errors import NoRespondersError, TimeoutError
from uns_auto_contextualizer.service import handle_contextualize_request, load_ontology


def test_generated_messages_are_valid_and_resolve_all_nodes() -> None:
    graph = load_ontology(Path(__file__).resolve().parents[2] / "config/eirvah-line-a.jsonld")
    identifiers = set()
    for node_id in load_inject.NODE_IDS:
        envelope = NATSEnvelope.model_validate_json(load_inject._build_payload(node_id))
        identifiers.add(envelope.correlation_id)
        reply = handle_contextualize_request(envelope, graph)
        assert reply.status == "ok", reply.error
        load_inject._validate_reply(reply.model_dump_json().encode(), envelope.correlation_id)
    assert len(identifiers) == len(load_inject.NODE_IDS)


async def test_requests_account_for_success_errors_and_unknown_timeouts() -> None:
    graph = load_ontology(Path(__file__).resolve().parents[2] / "config/eirvah-line-a.jsonld")

    async def respond(subject: str, data: bytes, timeout: float) -> SimpleNamespace:
        envelope = NATSEnvelope.model_validate_json(data)
        if envelope.payload["node_id"] == "timeout":
            raise TimeoutError
        if envelope.payload["node_id"] == "no_responder":
            raise NoRespondersError
        if envelope.payload["node_id"] == "invalid_reply":
            return SimpleNamespace(data=b"not json")
        reply = handle_contextualize_request(envelope, graph)
        return SimpleNamespace(data=reply.model_dump_json().encode())

    client = SimpleNamespace(request=AsyncMock(side_effect=respond))
    stats = load_inject.LoadStats()
    for node_id in [load_inject.NODE_IDS[0], "unknown", "timeout", "no_responder", "invalid_reply"]:
        await load_inject._request_one(client, node_id, stats, timeout=0.1)
    assert (stats.sent, stats.succeeded, stats.errors, stats.unresolved) == (5, 1, 3, 1)


def test_reply_validation_rejects_wrong_identity_and_wrong_topic() -> None:
    graph = load_ontology(Path(__file__).resolve().parents[2] / "config/eirvah-line-a.jsonld")
    envelope = NATSEnvelope.model_validate_json(load_inject._build_payload(load_inject.NODE_IDS[0]))
    reply = handle_contextualize_request(envelope, graph)
    with pytest.raises(ValueError, match="correlation"):
        load_inject._validate_reply(reply.model_dump_json().encode(), "0" * 26)
    reply.payload["uns_topic"] = "wrong/topic"
    with pytest.raises(ValueError, match="topic"):
        load_inject._validate_reply(reply.model_dump_json().encode(), envelope.correlation_id)
    rejected = NATSEnvelope(
        correlation_id=envelope.correlation_id,
        status="error",
        error=EnvelopeError(kind="UnknownNode", message="unknown"),
    )
    with pytest.raises(ValueError):
        load_inject._validate_reply(rejected.model_dump_json().encode(), envelope.correlation_id)


async def test_run_caps_pending_requests_and_accounts_for_all_work(
    capsys: pytest.CaptureFixture[str],
) -> None:
    active = 0
    maximum = 0
    graph = load_ontology(Path(__file__).resolve().parents[2] / "config/eirvah-line-a.jsonld")

    async def respond(subject: str, data: bytes, timeout: float) -> SimpleNamespace:
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        try:
            await asyncio.sleep(0.01)
            envelope = NATSEnvelope.model_validate_json(data)
            reply = handle_contextualize_request(envelope, graph)
            return SimpleNamespace(data=reply.model_dump_json().encode())
        finally:
            active -= 1

    client = SimpleNamespace(request=AsyncMock(side_effect=respond), close=AsyncMock())
    with patch("load_inject.nats.connect", AsyncMock(return_value=client)):
        stats = await load_inject.run(
            rate=1000, duration=0.05, nats_url="nats://test", max_pending=2, timeout=0.1
        )
    assert 0 < maximum <= 2
    assert stats.sent == stats.succeeded > 0
    assert stats.errors == stats.unresolved == 0
    client.close.assert_awaited_once()
    output = capsys.readouterr().out
    assert "send_rate=" in output and "completion_rate=" in output


@pytest.mark.parametrize(
    "rate,duration,timeout,max_pending",
    [
        (0, 1, 1, 1),
        (1, 0, 1, 1),
        (1, 1, 0, 1),
        (1, 1, 1, 0),
    ],
)
async def test_invalid_settings_fail_before_connect(
    rate: int, duration: int, timeout: float, max_pending: int
) -> None:
    with patch("load_inject.nats.connect", AsyncMock()) as connect:
        with pytest.raises(ValueError):
            await load_inject.run(
                rate=rate,
                duration=duration,
                timeout=timeout,
                max_pending=max_pending,
                nats_url="nats://test",
            )
        connect.assert_not_called()


def test_cli_reports_failure_when_requests_do_not_complete() -> None:
    failed = SimpleNamespace(sent=1, succeeded=0, errors=0, unresolved=1)
    with (
        patch("sys.argv", ["load_inject.py"]),
        patch("load_inject.run", AsyncMock(return_value=failed)),
        pytest.raises(SystemExit) as exc,
    ):
        load_inject.main()
    assert exc.value.code == 1
