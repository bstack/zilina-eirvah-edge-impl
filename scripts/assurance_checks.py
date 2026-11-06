"""Verify finite standalone E0 evidence without trusting a reported pass.

The scope is the supplied fenced command run, declared contracts and isolated
production-ledger boundary checks. This is not universal conformance or a soak.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections import Counter
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

from eirvah_contracts.actuation import ActuationRequest
from eirvah_contracts.authorization import command_digest

ROOT = Path(__file__).resolve().parents[1]
PROFILE = ROOT / "config/architecture/assurance-case.json"
CATALOGUE = ROOT / "config/architecture/contract-catalogue.json"
MODEL_NAMES = {
    "raw",
    "normalized",
    "contextualized",
    "publish",
    "envelope",
    "command",
    "authenticated",
    "approved",
    "validation",
    "write_result",
    "inspection",
    "receipt",
}
PUBLIC_CASES = {
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
}


def records_digest(rows: list[dict[str, Any]]) -> str:
    normalized = [{k: v for k, v in row.items() if k != "source"} for row in rows]
    return hashlib.sha256(
        json.dumps(
            sorted(normalized, key=lambda r: r["record_id"]), sort_keys=True, allow_nan=False
        ).encode()
    ).hexdigest()


def retention_valid(component: dict[str, Any]) -> bool:
    if (
        component["mode"] != "isolated_production_ledger_virtual_time"
        or component["retention_seconds"] != 86400
        or component["native_effects"] != "not_exercised"
    ):
        return False
    initial = component["initial"][0]
    request = ActuationRequest.model_validate(component["command"])
    if initial["digest"] != command_digest(request) or initial["state"] != "completed":
        return False
    if request.deadline is None or initial["keep_until"] < request.deadline.timestamp() + 86400:
        return False
    points = component["points"]
    if [p["label"] for p in points] != ["before", "boundary", "after"]:
        return False
    if not points[0]["at"] < initial["keep_until"] == points[1]["at"] < points[2]["at"]:
        return False
    for point in points[:2]:
        if (
            point["result"]["outcome"] != "completed"
            or not point["result"]["replayed"]
            or point["result"]["written_at"] != component["completion"]["written_at"]
        ):
            return False
    if points[2]["result"] is not None or points[2]["rows"]:
        return False
    states = {e["state"]: e for e in component["restarts"]}
    if set(states) != {"preparing", "started", "completed"}:
        return False
    if (
        not states["preparing"]["token"]
        or states["preparing"]["token"] == states["preparing"]["old_token"]
    ):
        return False
    if (
        states["started"]["token"] is not None
        or states["started"]["result"]["outcome"] != "unknown"
    ):
        return False
    if (
        states["completed"]["token"] is not None
        or states["completed"]["result"]["outcome"] != "completed"
        or not states["completed"]["result"]["replayed"]
    ):
        return False
    return states["preparing"]["old_token_refused"] is True


def check_assurance(
    package: dict[str, Any] | None,
    records: list[dict[str, Any]],
    commands: list[dict[str, Any]] | None,
) -> tuple[list[dict[str, Any]], set[str]]:
    data = package or {}
    results = []
    closed_no_write: set[str] = set()
    valid = (
        data.get("schema_version") == "1.0"
        and data.get("profile_sha256") == hashlib.sha256(PROFILE.read_bytes()).hexdigest()
    )
    profile = json.loads(PROFILE.read_text())

    def add(name: str, predicate: Callable[[], bool], reason: str) -> str:
        status = "unknown"
        if valid:
            try:
                status = "pass" if predicate() else "fail"
            except (
                KeyError,
                TypeError,
                ValueError,
                IndexError,
                AttributeError,
                OSError,
                StopIteration,
            ):
                status = "unknown"
        results.append(
            dict(
                rule=name.split(".")[0],
                check=name,
                status=status,
                reason=reason,
                scope="selected_standalone_e0_assurance",
                execution="executed" if valid else "not_run",
                evidence=[profile["id"]],
            )
        )
        return status

    def source_attribution() -> bool:
        component = data["components"]
        hashes = component["source_hashes"]
        required = [
            "libs/eirvah-contracts/src/eirvah_contracts/" + n
            for n in (
                "actuation.py",
                "authorization.py",
                "signals.py",
                "pipeline.py",
                "envelope.py",
                "sosa.py",
                "observation.py",
                "inspection.py",
            )
        ] + [
            "services/actuation-signal-publisher/src/actuation_signal_publisher/ledger.py",
            "services/decision-agent-stub/src/decision_agent_stub/service.py",
            "services/uns-auto-contextualizer/src/uns_auto_contextualizer/service.py",
            "scripts/assurance_components.py",
            "config/eirvah-line-a.jsonld",
            "config/architecture/contract-catalogue.json",
            "config/architecture/assurance-case.json",
        ]
        return all(
            hashes[p] == hashlib.sha256((ROOT / p).read_bytes()).hexdigest() for p in required
        )

    def closed_capture() -> bool:
        from e0_capture import calibrate_record, validate_fence

        if (
            data["capture_errors"]
            or not data["controller_state"]["restored"]
            or not data["source_case_state"]["restored"]
            or data["scenario"]["outcome"] != "passed"
        ):
            return False
        if data["inventory_start"] != data["inventory_end"]:
            return False
        actors = Counter(p["component"] for p in data["inventory_start"]["pods"].values())
        if actors != Counter(profile["capture_actors"]):
            return False
        for actor in data["inventory_start"]["pods"].values():
            if not all(
                isinstance(actor.get(k), str) and bool(actor[k])
                for k in ("uid", "pod", "node", "context", "namespace")
            ):
                return False
            if not actor.get("containers") or any(
                not c.get("imageID") for c in actor["containers"]
            ):
                return False
        if not data["inventory_start"]["ledger_volume"]["uid"]:
            return False
        raw = {}
        expected = set()
        for pod, start in data["starts"].items():
            end = data["ends"][pod]
            if validate_fence(start, end):
                return False
            hashes = start["remote"]["source_hashes"]
            if hashes != end["remote"]["source_hashes"]:
                return False
            for path, digest in hashes.items():
                target = (ROOT / path).resolve()
                if (
                    not target.is_relative_to(ROOT.resolve())
                    or hashlib.sha256(target.read_bytes()).hexdigest() != digest
                ):
                    return False
            for row in end["remote"]["records"]:
                if row["record_id"] in raw:
                    return False
                raw[row["record_id"]] = calibrate_record(row, start, end, data["host_samples"])
                expected.add(row["record_id"])
        supplied = data["all_records"]
        if len(supplied) != len(expected) or {r["record_id"] for r in supplied} != expected:
            return False
        for row in supplied:
            if records_digest([row]) != records_digest([raw[row["record_id"]]]):
                return False
            u = row["clock_uncertainty_s"]
            if (
                isinstance(u, bool)
                or not isinstance(u, (float, int))
                or not math.isfinite(u)
                or u < 0
            ):
                return False
        if records_digest(records) != records_digest(data["records"]):
            return False
        submissions = data["submissions"]
        ids = {e["command"]["correlation_id"] for e in submissions}
        if ids != {c["correlation_id"] for c in commands or []}:
            return False
        if ids != {r["correlation_id"] for r in data["records"]}:
            return False
        for entry in submissions:
            request = ActuationRequest.model_validate(entry["command"])
            result = entry["result"]
            if result["decision"] not in ("approve", "reject"):
                return False
            if type(result["requested_value"]) is not type(entry["command"]["requested_value"]):
                return False
            if any(result[k] != entry["command"][k] for k in ActuationRequest.model_fields):
                return False
            prior = entry["ledger_before"]
            if result.get("replayed"):
                if (
                    len(prior) != 1
                    or prior[0]["digest"] != command_digest(request)
                    or prior[0]["state"] != "completed"
                ):
                    return False
            elif prior:
                return False
        return True

    closed = add(
        "C12.collection_coverage",
        closed_capture,
        "Complete fenced actor segments, matching source/configuration/volume identity, recalculated clock bounds, submitted IDs and confirmed restoration; finite run only",
    )

    def contracts() -> bool:
        if not source_attribution():
            raise KeyError("component source attribution")
        component = data["components"]
        contract = component["contracts"]
        if contract["catalogue"] != json.loads(CATALOGUE.read_text()):
            return False
        manifest = json.loads((ROOT / "config/architecture/local-control.json").read_text())
        declared = {e["id"]: e for e in contract["catalogue"]["contracts"]}
        if set(declared) != {c["id"] for c in manifest["contracts"]}:
            return False
        for item in manifest["contracts"]:
            entry = declared[item["id"]]
            if (
                entry["version"] != "1.0"
                or entry["flow"] != item["kind"]
                or entry["sources"] != item["sources"]
            ):
                return False
            if entry["receivers"] != sorted(
                {e["target"] for e in manifest["edges"] if e["contract"] == item["id"]}
            ):
                return False
        cases = contract["cases"]
        if Counter((c["model"], c["mode"]) for c in cases) != Counter(
            (m, mode) for m in MODEL_NAMES for mode in ("supported", "unsupported")
        ):
            return False
        if any(
            c["accepted"] != (c["mode"] == "supported")
            or c["input"]["schema_version"] != ("1.0" if c["mode"] == "supported" else "2.0")
            for c in cases
        ):
            return False
        if Counter(c["mode"] for c in contract["public_cases"]) != Counter(PUBLIC_CASES):
            return False
        if any(c["accepted"] != (c["mode"] == "supported") for c in contract["public_cases"]):
            return False
        snapshot = data["receiver_snapshot"]
        if snapshot["guard_path"] != "/etc/decision-agent-stub/eirvah-line-a.jsonld":
            return False
        if snapshot["ontology"] != component["source_hashes"]["config/eirvah-line-a.jsonld"]:
            return False
        if any(
            digest != component["source_hashes"][path]
            for path, digest in snapshot["hashes"].items()
        ):
            return False
        if len(snapshot["hashes"]) != 3:
            return False
        bindings = contract["bindings"]
        samples = contract["observations"]
        if len(samples) != len(bindings) or {r["topic"] for r in samples} != set(bindings):
            return False
        live = data["live_samples"]
        expected_live = {
            "uniza/zilina/factory1/line_a/" + p
            for p in (
                "conveyor/belt_01/speed_setpoint",
                "reject_station/reject_counter_01/reject_count",
                "reject_station/rejector_01/last_rejected_bottle",
                "inspector/inspector_01/bottle_inspection",
            )
        }
        if set(live) != expected_live:
            return False
        for sample in samples + [{"topic": t, "packet": p} for t, p in live.items()]:
            topic = sample["topic"]
            packet = sample["packet"]
            binding = bindings[topic]
            if (
                packet["eirvah:schemaVersion"] != "1.0"
                or packet["eirvah:valueType"] != binding["value_type"]
                or packet["eirvah:unit"] != binding["unit"]
            ):
                return False
            for field, key in [
                ("sosa:madeBySensor", "sensor"),
                ("sosa:hasFeatureOfInterest", "feature"),
                ("sosa:observedProperty", "property"),
            ]:
                if packet[field]["@id"] != binding[key]:
                    return False
            if datetime.fromisoformat(packet["sosa:resultTime"]).tzinfo is None:
                return False
            value = packet["sosa:hasSimpleResult"]
            allowed = {"double": (int, float), "int64": (int,), "string": (str,), "bool": (bool,)}[
                binding["value_type"]
            ]
            if type(value) not in allowed:
                return False
        return True

    add(
        "C03.full_semantics",
        contracts,
        "Pinned versioned catalogue, production supported/unsupported parsers, ontology meaning checks and deployed receiver/public samples; native versions describe mappings, not invented device fields",
    )

    def bypass() -> bool:
        if closed != "pass":
            raise KeyError("closed capture")
        entries = data["bypass"]
        if Counter(e["mode"] for e in entries) != Counter(profile["bypass_modes"]):
            return False
        stored = {r["command_id"] for r in data["ledger"]}
        for entry in entries:
            ids = {
                entry["command"]["correlation_id"],
                entry.get("envelope_id", entry["command"]["correlation_id"]),
            }
            if ids & stored:
                return False
            if entry["native_before"] != entry["native_after"]:
                return False
            if (
                entry["reply"]["status"] != "error"
                or entry["reply"]["payload"]["outcome"] != "not_executed"
            ):
                return False
            if any(
                r["correlation_id"] in ids
                and r["action"] in ("executor_admitted", "write_initiated", "device_acknowledged")
                for r in data["all_records"]
            ):
                return False
            if (
                entry["mode"] == "wrong_writer"
                and entry["approval_claims"]["writer"] == "actuation-signal-publisher"
            ):
                return False
            if entry["mode"] == "tampered" and entry["approval_claims"][
                "command_digest"
            ] == command_digest(ActuationRequest.model_validate(entry["command"])):
                return False
        return True

    add(
        "C02.runtime_boundary",
        bypass,
        "Actual unsigned, changed-command, wrong-writer, expired and envelope-mismatch requests are refused at the writer with no native change or initiation; administrative/native/key-holder access remains trusted",
    )

    def retention() -> bool:
        if closed != "pass" or not source_attribution():
            raise KeyError("capture/source attribution")
        if not retention_valid(data["components"]["retention"]):
            return False
        actor = next(
            s
            for s in data["starts"].values()
            if "services/actuation-signal-publisher/src/actuation_signal_publisher/ledger.py"
            in s["remote"]["source_hashes"]
        )
        if actor["remote"]["writer_policy"] != {
            "retention_s": 86400,
            "ledger_path": "/var/lib/eirvah/commands.sqlite3",
        }:
            return False
        if (
            actor["remote"]["source_hashes"][
                "services/actuation-signal-publisher/src/actuation_signal_publisher/ledger.py"
            ]
            != data["components"]["source_hashes"][
                "services/actuation-signal-publisher/src/actuation_signal_publisher/ledger.py"
            ]
        ):
            return False
        for cid in {e["command"]["correlation_id"] for e in data["submissions"]}:
            count = sum(
                r["correlation_id"] == cid and r["action"] == "write_initiated" for r in records
            )
            if count > 1:
                return False
        return True

    add(
        "C09.complete_history",
        retention,
        "Closed new-command/replay history plus isolated production-ledger before/at/after retention and restart boundaries using declared injected time; no 24-hour live soak or universal-history claim",
    )
    if closed == "pass":
        initiated = {
            r["correlation_id"]
            for r in records
            if r["action"] in ("executor_admitted", "write_initiated")
        }
        stored = {r["command_id"] for r in data["ledger"]}
        for entry in data["submissions"]:
            cid = entry["command"]["correlation_id"]
            if (
                entry["result"]["decision"] == "reject"
                and cid not in initiated
                and cid not in stored
            ):
                closed_no_write.add(cid)
    return results, closed_no_write
