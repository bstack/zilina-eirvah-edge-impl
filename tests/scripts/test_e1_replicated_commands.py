"""Replicated front-end handlers must retain one authoritative executor."""

from e1_replicated_commands import replicated_verdict


def evidence():
    actors = [
        "amqp-actuation-event-subscriber",
        "actuation-control-orchestrator",
        "actuation-event-validator",
    ]
    records = [
        dict(component=a, host=a + str(i), action="handled", correlation_id="id")
        for a in actors
        for i in (1, 2)
    ]
    records.append(
        dict(
            component="actuation-signal-publisher",
            host="writer",
            action="write_initiated",
            correlation_id="id",
        )
    )
    return dict(
        local=dict(status="pass"),
        records=records,
        replay_seen=True,
        restored=True,
        writer_replicas=1,
    )


def test_bounded_replay_passes_with_both_stateless_handlers_and_one_initiation():
    assert replicated_verdict(evidence()) == "pass"


def test_second_native_initiation_cannot_pass_even_if_public_responses_succeed():
    data = evidence()
    data["records"].append(
        dict(
            component="actuation-signal-publisher",
            host="writer",
            action="write_initiated",
            correlation_id="id",
        )
    )
    assert replicated_verdict(data) == "fail"


def test_missing_second_handler_evidence_remains_unknown():
    data = evidence()
    data["records"] = [r for r in data["records"] if r["host"] != "actuation-event-validator2"]
    assert replicated_verdict(data) == "unknown"
