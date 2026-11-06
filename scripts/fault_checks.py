"""Independent bounded command-fault checks from raw source, proxy and ledger evidence."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from eirvah_contracts.actuation import ActuationRequest
from eirvah_contracts.authorization import command_digest

ROOT = Path(__file__).resolve().parents[1]
PROFILE = ROOT / "config/architecture/command-fault-case.json"
EXPECTED = {
    "caller_reply_lost": (1, "completed", "approve"),
    "before_write_disconnect": (1, "completed", "approve"),
    "commit_dropped": (0, "unknown", "unknown"),
    "native_ack_lost": (1, "unknown", "unknown"),
    "writer_restart_after_effect": (1, "unknown", "unknown"),
    "delayed_expired_command": (0, None, "reject"),
}


def writer_initiations(case: dict[str, Any], command: ActuationRequest) -> list[dict[str, Any]]:
    journals = case["writer_journals"]
    if not journals:
        raise KeyError("writer journals")
    retained: dict[str, dict[str, Any]] = {}
    for journal in journals:
        if journal["overflow"] or [r["record_seq"] for r in journal["records"]] != list(
            range(1, journal["record_seq"] + 1)
        ):
            raise KeyError("writer journal coverage")
        hashes = journal["source_hashes"]
        for path in (
            "libs/eirvah-observability/src/eirvah_observability/execution.py",
            "libs/eirvah-contracts/src/eirvah_contracts/actuation.py",
            "libs/eirvah-contracts/src/eirvah_contracts/authorization.py",
            "services/actuation-signal-publisher/src/actuation_signal_publisher/service.py",
        ):
            if hashes[path] != hashlib.sha256((ROOT / path).read_bytes()).hexdigest():
                raise KeyError("writer source attribution")
        for row in journal["records"]:
            if row["correlation_id"] == command.correlation_id:
                old = retained.get(row["record_id"])
                if old is not None and old != row:
                    raise AssertionError("conflicting writer record copies")
                retained[row["record_id"]] = row
    initiated = [r for r in retained.values() if r["action"] == "write_initiated"]
    for row in initiated:
        if (
            row["command_digest"] != command_digest(command)
            or row["target"] != command.target_uns_topic
            or row["requested_value"] != command.requested_value
            or row["approval_verified"] is not True
        ):
            raise AssertionError("initiation command or approval binding mismatch")
    return initiated


def check_fault_evidence(package: dict[str, Any] | None) -> list[dict[str, Any]]:
    data = package or {}
    valid = (
        data.get("schema_version") == "1.0"
        and data.get("mode") == "live_command_faults"
        and (data.get("profile_sha256") == hashlib.sha256(PROFILE.read_bytes()).hexdigest())
    )
    checks = []
    for name, (effect_count, state, reply) in EXPECTED.items():
        status = "unknown"
        reason = "Missing complete bounded command-fault evidence"
        try:
            rows = [c for c in data["cases"] if c["id"] == name]
            if not valid or len(rows) != 1:
                raise KeyError("case")
            case = rows[0]
            command = ActuationRequest.model_validate(case["command"])
            replay = ActuationRequest.model_validate(case["replay_command"])
            if command.deadline is None:
                raise ValueError("bounded deadline required")
            if not command.model_dump(mode="json") == replay.model_dump(mode="json"):
                raise AssertionError("retry changed original command")
            if not command.target_uns_topic.endswith("/reject_bottle"):
                raise AssertionError("wrong experimental target")
            if type(command.requested_value) is not int:
                raise AssertionError("wrong native value")
            before, after = (case["before"], case["after"])
            if not before["meta"]["db_id"] == after["meta"]["db_id"]:
                raise AssertionError("source storage replaced")
            native = after["effects"]
            if not len(native) == after["last_effect"] - before["last_effect"]:
                raise AssertionError("native effect records incomplete")
            effects = [r for r in native if r["payload"]["target"] == 28]
            successful = [r for r in effects if r["payload"]["status"] == 1]
            if not len(effects) == len(successful) == effect_count:
                raise AssertionError("unexpected native effect count")
            if not all(
                r["payload"]["value"] == command.requested_value
                and r["payload"]["operation"] == 1
                and (r["payload"]["after_count"] - r["payload"]["before_count"] == 1)
                for r in successful
            ):
                raise AssertionError("native effect binding mismatch")
            if (
                not after["state"]["block"]["reject"]["count"]
                - before["state"]["block"]["reject"]["count"]
                == effect_count
            ):
                raise AssertionError("counter reconciliation failed")
            ledger = case["ledger"]
            if state is None:
                if not ledger == []:
                    raise AssertionError("expired fresh command acquired writer history")
            elif not (
                len(ledger) == 1
                and ledger[0]["command_id"] == command.correlation_id
                and (ledger[0]["digest"] == command_digest(command))
                and (ledger[0]["state"] == state)
            ):
                raise AssertionError("writer ledger mismatch")
            result = case["replay_result"]
            if not (
                result["correlation_id"] == command.correlation_id and result["decision"] == reply
            ):
                raise AssertionError("retry outcome mismatch")
            if not all(
                result[k] == command.model_dump(mode="json")[k]
                for k in (
                    "target_uns_topic",
                    "requested_value",
                    "value_type",
                    "deadline",
                    "requester",
                )
            ):
                raise AssertionError("reply command binding changed")
            initiated = writer_initiations(case, command)
            if len(initiated) != (0 if name == "delayed_expired_command" else 1):
                raise AssertionError(
                    "unexpected native initiation count across original and replay"
                )
            records = case["proxy"]
            if not all(
                r["command_id"] == command.correlation_id and r["case_id"] == name for r in records
            ):
                raise AssertionError("proxy evidence binding mismatch")
            events = {r["event"] for r in records}
            if name == "caller_reply_lost":
                if not (case["reply_subscription_absent"] is True and case["first_result"] is None):
                    raise AssertionError("caller reply was not lost")
                if not (
                    case["first_ledger"][0]["state"] == "completed" and result["replayed"] is True
                ):
                    raise AssertionError("lost completion was not replayed")
                if not result["written_at"] == case["first_ledger"][0]["result"]["written_at"]:
                    raise AssertionError("completion time changed")
            elif name == "before_write_disconnect":
                if not (
                    "disconnected_before_write" in events
                    and case["first_ledger"][0]["state"] == "retryable"
                    and (case["first_source"]["last_effect"] == before["last_effect"])
                ):
                    raise AssertionError("pre-write failure not established")
                if not (
                    case["first_result"]["decision"] == "unknown"
                    or (
                        case["first_result"]["decision"] == "reject"
                        and case["first_result"]["retryable"] is True
                    )
                ):
                    raise AssertionError("pre-write retry policy mismatch")
            elif name == "commit_dropped":
                if not ("commit_dropped" in events and "commit_forwarded" not in events):
                    raise AssertionError("commit was not withheld")
                if not case["first_result"]["decision"] == "unknown":
                    raise AssertionError("uncertain initiation labelled refusal")
            elif name == "native_ack_lost":
                if not any(
                    r["event"] == "ack_dropped"
                    and r["status"] == 1
                    and (r["sequence"] == successful[0]["payload"]["sequence"])
                    for r in records
                ):
                    raise AssertionError("successful native acknowledgement not dropped")
                if not case["first_result"]["decision"] == "unknown":
                    raise AssertionError("lost native ack labelled refusal")
            elif name == "writer_restart_after_effect":
                if not any(
                    r["event"] == "ack_held"
                    and r["status"] == 1
                    and (r["sequence"] == successful[0]["payload"]["sequence"])
                    for r in records
                ):
                    raise AssertionError("restart boundary not held")
                if not case["first_ledger"][0]["state"] == "started":
                    raise AssertionError("writer not interrupted after initiation")
                restart = case["restart"]
                if not (
                    restart["before"]["uid"] != restart["after"]["uid"]
                    and bool(restart["before"]["volumes"])
                    and (restart["before"]["volumes"] == restart["after"]["volumes"])
                ):
                    raise AssertionError("real restart with retained ledger not established")
            elif name == "delayed_expired_command":
                if not (
                    case["published_clock"]["wall"] < command.deadline.timestamp()
                    and case["held_replicas"] == 0
                    and case["held_clock"]["wall"] > command.deadline.timestamp()
                ):
                    raise AssertionError("command was not held past expiry")
                if records:
                    raise AssertionError("expired command entered native transport")
            domains = [
                before["clock"]["domain"],
                after["clock"]["domain"],
                case["writer_clock"]["domain"],
            ]
            if (
                not all(domains)
                or len(set(domains)) != 1
                or data.get("capture_errors") != []
                or (data.get("restored") is not True)
            ):
                status, reason = (
                    "unknown",
                    "Clock, capture or restoration assumptions not established",
                )
            else:
                status, reason = (
                    "pass",
                    "Observed outcome, immutable replay, native effects and durable writer history reconcile",
                )
        except AssertionError as exc:
            status, reason = ("fail", str(exc))
        except (KeyError, TypeError, ValueError, IndexError, AttributeError):
            pass
        checks.append(
            dict(
                rule="C10",
                check="C10.fault." + name,
                status=status,
                reason=reason,
                scope="bounded_modbus_command_faults",
                execution="executed" if valid else "not_run",
                evidence=[name],
            )
        )
    return checks


def report(package: dict[str, Any]) -> dict[str, Any]:
    checks = check_fault_evidence(package)
    status = (
        "fail"
        if any(c["status"] == "fail" for c in checks)
        else "unknown"
        if any(c["status"] != "pass" for c in checks)
        else "pass"
    )
    return dict(fault_case=status, whole_study="unknown", checks=checks)
