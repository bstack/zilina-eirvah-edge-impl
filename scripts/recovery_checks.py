"""Evaluate retained live C05/C06 recovery evidence; never trust a summary pass."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from pathlib import Path
from typing import Any

from eirvah_contracts.actuation import ActuationRequest
from eirvah_contracts.authorization import command_digest
from eirvah_controls.recovery import canonical

ROOT = Path(__file__).resolve().parents[1]
PROFILE = ROOT / "config/architecture/recovery-case.json"
NAMES = [
    "C05.source_epochs",
    "C05.deployed_restart",
    "C06.business_effects",
    "C06.delivery_coverage",
    "C06.durable_recovery",
]


def check_recovery(package: dict[str, Any] | None) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    valid = (
        isinstance(package, dict)
        and package.get("schema_version") == "1.0"
        and package.get("mode") == "live_cluster_recovery"
        and package.get("profile_sha256") == hashlib.sha256(PROFILE.read_bytes()).hexdigest()
    )

    def add(name: str, predicate: Callable[[], bool], reason: str) -> None:
        if not valid:
            status = "unknown"
        else:
            try:
                status = "pass" if predicate() else "fail"
            except (KeyError, TypeError, ValueError, IndexError, AttributeError):
                status = "unknown"
        checks.append(
            dict(
                rule=name.split(".")[0],
                check=name,
                status=status,
                reason=reason,
                scope="bounded_live_recovery_profile",
                execution="executed" if valid else "not_run",
                evidence=[name],
            )
        )

    data = package or {}

    def epochs() -> bool:
        for role in ("opcua", "modbus", "s7"):
            pos = data["positions"][role]["eirvah:position"]
            if (
                pos["boundary"] != "adapter"
                or not pos["producer"]
                or pos["epoch"] < 1
                or pos["sequence"] < 1
            ):
                return False
        restart = data["producer_restart"]
        before = restart["before_packet"]["eirvah:position"]
        after = restart["after_packet"]["eirvah:position"]
        audit = data["old_state_audit"]
        pub = data["publisher_restart"]
        a = pub["before_packet"]["eirvah:position"]
        b = pub["after_packet"]["eirvah:position"]
        return (
            before["producer"] == after["producer"]
            and before["epoch"] < after["epoch"]
            and audit["decision"]["accepted"] is False
            and audit["decision"]["reason"] == "old_epoch"
            and audit["payload"]["eirvah:position"] == before
            and audit["snapshot"]["position"]["epoch"] >= after["epoch"]
            and a["producer"] == b["producer"]
            and a["epoch"] == b["epoch"]
            and a["sequence"] < b["sequence"]
        )

    def restarts() -> bool:
        p = data["producer_restart"]
        s = data["source_restart"]
        c = data["consumer_restart"]
        pub = data["publisher_restart"]
        return (
            p["before_uid"] != p["after_uid"]
            and p["before_volume"] == p["after_volume"]
            and s["before_uid"] != s["after_uid"]
            and s["before"]["meta"]["db_id"] == s["after"]["meta"]["db_id"]
            and int(s["before"]["meta"]["boots"]) < int(s["after"]["meta"]["boots"])
            and s["before"]["state"]["block"]["reject"]["count"]
            == s["after"]["state"]["block"]["reject"]["count"]
            and c["before_uid"] != c["after_uid"]
            and c["before_db"] == c["after_db"]
            and pub["before_uid"] != pub["after_uid"]
        )

    def coverage() -> bool:
        start = data["start"]["source"]
        end = data["end"]["source"]
        inbox = {r["id"]: r for r in data["end"]["consumer"]["inspections"]}
        events = end["events"]
        delta = int(end["meta"]["created"]) - int(start["meta"]["created"])
        ids = [r["id"] for r in events]
        if (
            delta <= 0
            or len(ids) != len(set(ids))
            or len(events) != delta
            or start["meta"]["db_id"] != end["meta"]["db_id"]
            or int(end["meta"]["created"]) != int(end["meta"]["received"])
            or end["pending"] != 0
        ):
            return False
        if [r["serial"] for r in events] != list(
            range(start["last_serial"] + 1, end["last_serial"] + 1)
        ):
            return False
        for event in events:
            row = inbox[event["id"]]
            if (
                not event["received"]
                or row["digest"] != hashlib.sha256(canonical(event["payload"]).encode()).hexdigest()
                or row["payload"] != event["payload"]
                or row["disposition"] == "pending"
            ):
                return False
        outage = data["outage"]
        forgery = data["forged_receipt"]
        return (
            outage["replicas_during"] == 0
            and outage["source_pending_during"] > 0
            and forgery["before"]["id"] == forgery["after"]["id"]
            and not forgery["before"]["received"]
            and not forgery["after"]["received"]
        )

    def recovery() -> bool:
        c = data["consumer_restart"]
        a = c["before_row"]
        b = c["after_row"]
        pending = data["pending_recovery"]
        before_pending = pending["before_row"]
        after_pending = pending["after_row"]
        unchanged_pending = all(
            before_pending[k] == after_pending[k] for k in ("id", "digest", "command_id", "command")
        )
        return bool(
            before_pending["disposition"] == "pending"
            and after_pending["disposition"] == "refused"
            and unchanged_pending
            and a["id"] == b["id"]
            and a["digest"] == b["digest"]
            and a["command_id"] == b["command_id"]
            and a["command"] == b["command"]
            and a["disposition"] == b["disposition"] == "completed"
            and a["result"]["written_at"] == b["result"]["written_at"]
            and c["before_db"] == c["after_db"]
        )

    def effects() -> bool:
        start = data["start"]["source"]
        end = data["end"]["source"]
        rows = data["end"]["consumer"]["inspections"]
        events = {r["id"]: r for r in end["events"]}
        ledger = {r["command_id"]: r for r in data["end"]["writer"]}
        native = end["effects"]
        if len(native) != end["last_effect"] - start["last_effect"]:
            return False
        controls = [r for r in native if r["payload"]["target"] == 28]
        completed = []
        for row in rows:
            if row["id"] not in events:
                continue
            if row["disposition"] in ("pending", "unknown"):
                return False
            if row["disposition"] == "completed":
                request = ActuationRequest.model_validate(row["command"])
                stored = ledger[request.correlation_id]
                if stored["state"] != "completed" or stored["digest"] != command_digest(request):
                    return False
                matches = [
                    e
                    for e in controls
                    if e["payload"]["value"] == row["payload"]["bottle_id"]
                    and e["payload"]["status"] == 1
                    and e["payload"]["after_count"] - e["payload"]["before_count"] == 1
                ]
                if len(matches) != 1 or request.requested_value != row["payload"]["bottle_id"]:
                    return False
                completed.append(row)
        successful = [e for e in controls if e["payload"]["status"] == 1]
        delta = (
            end["state"]["block"]["reject"]["count"] - start["state"]["block"]["reject"]["count"]
        )
        return (
            len(completed) > 0
            and len(successful) == len(completed) == delta
            and len({r["payload"]["bottle_id"] for r in completed}) == len(completed)
        )

    add(
        NAMES[0],
        epochs,
        "Observed adapter identity/epoch propagation, rejected prior epoch and publisher restart preservation",
    )
    add(
        NAMES[1],
        restarts,
        "Actual pod UID changes with retained producer/source/consumer volumes and counters",
    )
    add(
        NAMES[2],
        effects,
        "Owned commands reconcile with writer records and independently committed simulated native effects",
    )
    add(
        NAMES[3],
        coverage,
        "All source-created IDs in the bounded window reach durable consumer ownership; invalid receipt cannot discard the head",
    )
    add(
        NAMES[4],
        recovery,
        "Completed and pending ownership retain the original command identity, payload and deadline across consumer restart; expired pending work is refused",
    )
    try:
        domains = [
            data["end"][role]["clock"]["domain"]
            for role in ("source", "consumer", "writer_capture")
        ]
        clock_ok = all(isinstance(d, str) and bool(d) for d in domains) and len(set(domains)) == 1
    except (KeyError, TypeError):
        clock_ok = False
    if (
        not isinstance(data.get("capture_errors"), list)
        or data.get("capture_errors")
        or not clock_ok
    ):
        for item in checks:
            if item["status"] == "pass":
                item["status"] = "unknown"
                item["reason"] += "; capture/build/clock assumptions not established"
    return checks
