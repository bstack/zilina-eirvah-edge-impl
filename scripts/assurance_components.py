"""Retained component checks using real contract parsers and isolated production ledger.

No native write or live ledger is used by this component runner. Retention uses
an explicitly injected clock and must not be labelled a 24-hour live experiment.
"""

from __future__ import annotations

import copy
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from actuation_signal_publisher.ledger import CommandLedger
from control import build_command
from e0_capture import save
from eirvah_contracts.actuation import ActuationRequest, ValidationResult, WriteResult
from eirvah_contracts.authorization import (
    ApprovedWriteRequest,
    AuthenticatedRequest,
    command_digest,
)
from eirvah_contracts.envelope import NATSEnvelope
from eirvah_contracts.inspection import BottleInspection, InspectionReceipt
from eirvah_contracts.observation import load_bindings, validate_observation
from eirvah_contracts.pipeline import ContextualizeResult, PublishRequest
from eirvah_contracts.signals import NormalizedSignalEnvelope, RawSignalEnvelope
from eirvah_contracts.sosa import SOSAObservation
from eirvah_contracts.uns import parse_uns_topic

ROOT = Path(__file__).resolve().parents[1]
CATALOGUE = ROOT / "config/architecture/contract-catalogue.json"
PROFILE = ROOT / "config/architecture/assurance-case.json"


def model_samples() -> dict[str, tuple[Any, dict[str, Any]]]:
    now = datetime(2026, 11, 6, 12, tzinfo=UTC)
    command = build_command("capper", "speed", 1200).model_dump(mode="json")
    evidence = dict(
        schema_version="1.0",
        purpose="approval",
        command_digest="0" * 64,
        writer="actuation-signal-publisher",
        issued_at=now.isoformat(),
        signature="0" * 64,
    )
    raw = RawSignalEnvelope(
        source_endpoint="opcua://fixture",
        node_id="Bottler.Temperature01",
        value=22.0,
        value_type="double",
        quality="good",
        source_timestamp=now,
        server_timestamp=now,
        received_at=now,
    )
    normal = NormalizedSignalEnvelope(
        node_id=raw.node_id,
        value=22.0,
        value_type="double",
        unit="degC",
        quality="good",
        source_timestamp=now,
        received_at=now,
    )
    topic = "uniza/zilina/factory1/line_a/bottler/temperature_sensor_01/temperature"
    binding = load_bindings(ROOT / "config/eirvah-line-a.jsonld")[topic]
    contextual = ContextualizeResult(
        uns_topic=topic,
        uns_path=parse_uns_topic(topic),
        semantic_type="temperature.celsius",
        sensor_uri=binding["sensor"],
        feature_uri=binding["feature"],
        property_uri=binding["property"],
    )
    publish = PublishRequest(
        uns_topic=topic,
        correlation_id=command["correlation_id"],
        value=22.0,
        value_type="double",
        unit="degC",
        quality="good",
        semantic_type="temperature.celsius",
        uns_path=parse_uns_topic(topic),
        source_endpoint=raw.source_endpoint,
        source_node_id=raw.node_id,
        source_timestamp=now,
        edge_ingress=now,
        sensor_uri=binding["sensor"],
        feature_uri=binding["feature"],
        property_uri=binding["property"],
    )
    return {
        "raw": (RawSignalEnvelope, raw.model_dump(mode="json")),
        "normalized": (NormalizedSignalEnvelope, normal.model_dump(mode="json")),
        "contextualized": (ContextualizeResult, contextual.model_dump(mode="json")),
        "publish": (PublishRequest, publish.model_dump(mode="json")),
        "envelope": (
            NATSEnvelope,
            NATSEnvelope(correlation_id=command["correlation_id"], payload={}).model_dump(
                mode="json"
            ),
        ),
        "command": (ActuationRequest, command),
        "authenticated": (
            AuthenticatedRequest,
            dict(
                schema_version="1.0",
                command=command,
                identity={**evidence, "purpose": "ingress", "writer": ""},
            ),
        ),
        "approved": (
            ApprovedWriteRequest,
            dict(schema_version="1.0", command=command, approval=evidence),
        ),
        "validation": (
            ValidationResult,
            dict(schema_version="1.0", decision="reject", reason="fixture"),
        ),
        "write_result": (WriteResult, dict(schema_version="1.0", outcome="not_executed")),
        "inspection": (
            BottleInspection,
            dict(
                schema_version="1.0",
                inspection_id="aaaaaaaaaaaaaaaa:0001",
                bottle_id=1,
                passed=True,
                inspected_at=now.isoformat(),
            ),
        ),
        "receipt": (
            InspectionReceipt,
            dict(
                schema_version="1.0",
                inspection_id="aaaaaaaaaaaaaaaa:0001",
                event_digest="0" * 64,
                consumer="decision-agent-stub",
                signature="0" * 64,
            ),
        ),
    }


def contract_capture() -> dict[str, Any]:
    cases = []
    for name, (model, payload) in model_samples().items():
        for mode in ("supported", "unsupported"):
            packet = {**payload, "schema_version": "1.0" if mode == "supported" else "2.0"}
            try:
                parsed = model.model_validate(packet)
                result = dict(accepted=True, output=parsed.model_dump(mode="json"))
            except ValueError as exc:
                result = dict(accepted=False, error_type=type(exc).__name__)
            cases.append(dict(model=name, mode=mode, input=packet, **result))
    bindings = load_bindings(ROOT / "config/eirvah-line-a.jsonld")
    observations: list[dict[str, Any]] = []
    for topic, b in bindings.items():
        value: Any = {"double": 1.0, "int64": 1, "bool": True, "string": "fixture"}[b["value_type"]]
        now = datetime(2026, 11, 6, 12, tzinfo=UTC)
        if b["flow"] == "event":
            value = BottleInspection(
                inspection_id="aaaaaaaaaaaaaaaa:0001", bottle_id=1, passed=True, inspected_at=now
            ).model_dump_json()
        packet = SOSAObservation(
            value_type=b["value_type"],
            made_by_sensor=b["sensor"],
            has_feature_of_interest=b["feature"],
            observed_property=b["property"],
            has_simple_result=value,
            result_time=now,
            unit=b["unit"],
            quality="good",
            correlation_id=build_command("capper", "stop").correlation_id,
        ).to_jsonld()
        validate_observation(topic, packet, bindings)
        observations.append(dict(topic=topic, packet=packet))
    public_cases = []
    topic = "uniza/zilina/factory1/line_a/conveyor/belt_01/speed_setpoint"
    base: dict[str, Any] = next(r["packet"] for r in observations if r["topic"] == topic)
    for mode in (
        "supported",
        "unsupported_version",
        "missing_version",
        "wrong_unit",
        "wrong_sensor",
        "wrong_feature",
        "wrong_property",
        "wrong_type",
        "wrong_topic",
        "naive_time",
    ):
        packet = copy.deepcopy(base)
        bound_topic = topic
        if mode == "unsupported_version":
            packet["eirvah:schemaVersion"] = "2.0"
        elif mode == "missing_version":
            del packet["eirvah:schemaVersion"]
        elif mode == "wrong_unit":
            packet["eirvah:unit"] = "rpm"
        elif mode in ("wrong_sensor", "wrong_feature", "wrong_property"):
            packet[
                {
                    "wrong_sensor": "sosa:madeBySensor",
                    "wrong_feature": "sosa:hasFeatureOfInterest",
                    "wrong_property": "sosa:observedProperty",
                }[mode]
            ]["@id"] = "wrong"
        elif mode == "wrong_type":
            packet["sosa:hasSimpleResult"] = True
        elif mode == "wrong_topic":
            bound_topic = topic.replace("conveyor", "other")
        elif mode == "naive_time":
            packet["sosa:resultTime"] = "2026-11-06T12:00:00"
        try:
            validate_observation(bound_topic, packet, bindings)
            accepted = True
        except (ValueError, KeyError):
            accepted = False
        public_cases.append(dict(mode=mode, topic=bound_topic, input=packet, accepted=accepted))
    return dict(
        public_cases=public_cases,
        mode="in_process_production_contract_parsers",
        cases=cases,
        bindings=bindings,
        observations=observations,
        catalogue=json.loads(CATALOGUE.read_text()),
    )


def retention_capture(directory: Path) -> dict[str, Any]:
    directory.mkdir(parents=True)
    clock = [1_800_000_000.0]
    path = directory / "ledger.sqlite3"
    store = CommandLedger(path, retention_s=86400, clock=lambda: clock[0])
    command = build_command("capper", "speed", 1200).model_copy(
        update={
            "requested_at": datetime.fromtimestamp(clock[0], UTC),
            "deadline": datetime.fromtimestamp(clock[0] + 120, UTC),
        }
    )
    digest = command_digest(command)
    claim = store.claim(command.correlation_id, digest, deadline=clock[0] + 120)
    if claim.token is None:
        raise RuntimeError("component claim unavailable")
    store.mark_started(command.correlation_id, claim.token)
    complete = store.complete(
        command.correlation_id, claim.token, written_at=datetime.fromtimestamp(clock[0], UTC)
    )
    import sqlite3
    from contextlib import closing

    def snapshot() -> list[dict[str, Any]]:
        with closing(sqlite3.connect(path)) as db:
            db.row_factory = sqlite3.Row
            return [dict(r) for r in db.execute("SELECT * FROM commands ORDER BY command_id")]

    initial = snapshot()
    keep = initial[0]["keep_until"]
    points = []
    for label, at in [("before", keep - 0.001), ("boundary", keep), ("after", keep + 0.001)]:
        clock[0] = at
        result = store.lookup(command.correlation_id, digest)
        points.append(
            dict(
                label=label,
                at=at,
                result=result.model_dump(mode="json") if result else None,
                rows=snapshot(),
            )
        )
    clock[0] = 1_800_000_000.0
    restarts = []
    for state in ("preparing", "started", "completed"):
        identity = "component-" + state
        claim = store.claim(identity, "fixture-digest", deadline=clock[0] + 120)
        if claim.token is None:
            raise RuntimeError("component recovery claim unavailable")
        if state != "preparing":
            store.mark_started(identity, claim.token)
        if state == "completed":
            store.complete(identity, claim.token, written_at=datetime.fromtimestamp(clock[0], UTC))
        old_token = claim.token
        restarted = CommandLedger(path, retention_s=86400, clock=lambda: clock[0])
        restarted.recover()
        retry = restarted.claim(identity, "fixture-digest", deadline=clock[0] + 120)
        old_token_refused = None
        if state == "preparing":
            try:
                restarted.mark_started(identity, old_token)
            except ValueError:
                old_token_refused = True
            else:
                old_token_refused = False
        restarts.append(
            dict(
                state=state,
                old_token=old_token,
                old_token_refused=old_token_refused,
                token=retry.token,
                result=retry.result.model_dump(mode="json") if retry.result else None,
                rows=snapshot(),
            )
        )
    return dict(
        mode="isolated_production_ledger_virtual_time",
        retention_seconds=86400,
        command=command.model_dump(mode="json"),
        initial=initial,
        completion=complete.model_dump(mode="json"),
        points=points,
        restarts=restarts,
        native_effects="not_exercised",
    )


def capture(output: Path) -> dict[str, Any]:
    paths = [
        ROOT / "libs/eirvah-contracts/src/eirvah_contracts" / name
        for name in (
            "signals.py",
            "pipeline.py",
            "envelope.py",
            "actuation.py",
            "sosa.py",
            "observation.py",
            "inspection.py",
            "authorization.py",
        )
    ]
    paths += [
        ROOT / "services/actuation-signal-publisher/src/actuation_signal_publisher/ledger.py",
        ROOT / "services/decision-agent-stub/src/decision_agent_stub/service.py",
        ROOT / "services/uns-auto-contextualizer/src/uns_auto_contextualizer/service.py",
        ROOT / "config/eirvah-line-a.jsonld",
        CATALOGUE,
        PROFILE,
        Path(__file__),
    ]
    data = dict(
        schema_version="1.0",
        profile_sha256=hashlib.sha256(PROFILE.read_bytes()).hexdigest(),
        contracts=contract_capture(),
        retention=retention_capture(output / "retention"),
        source_hashes={
            p.relative_to(ROOT).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in paths
        },
    )
    for path in paths:
        destination = output / "sources" / path.relative_to(ROOT)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(path.read_bytes())
    save(output / "component-evidence.json", data)
    return data
