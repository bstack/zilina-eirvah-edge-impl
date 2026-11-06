"""SQLite recovery state at explicitly declared producer/consumer boundaries.

Uses persistent volumes, WAL and FULL commits. Loss of the volume is not covered.
No networking or global database is created at import time.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
import uuid
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any


def database(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=10)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=FULL")
    return db


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


class ProducerPositions:
    def __init__(self, path: Path, producer: str) -> None:
        if not producer:
            raise ValueError("producer identity required")
        self.db = database(path)
        self.producer = producer
        self.epoch = 0
        self.db.executescript(
            "CREATE TABLE IF NOT EXISTS producer_epoch(id TEXT PRIMARY KEY, epoch INTEGER NOT NULL); CREATE TABLE IF NOT EXISTS producer_sequence(id TEXT, node TEXT, sequence INTEGER NOT NULL, PRIMARY KEY(id,node));"
        )

    def begin_epoch(self) -> int:
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            row = self.db.execute(
                "SELECT epoch FROM producer_epoch WHERE id=?", (self.producer,)
            ).fetchone()
            self.epoch = int(row[0]) + 1 if row else 1
            self.db.execute(
                "INSERT OR REPLACE INTO producer_epoch VALUES(?,?)", (self.producer, self.epoch)
            )
            self.db.execute("DELETE FROM producer_sequence WHERE id=?", (self.producer,))
        return self.epoch

    def next(self, node: str, expected_epoch: int | None = None) -> dict[str, Any]:
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            row = self.db.execute(
                "SELECT epoch FROM producer_epoch WHERE id=?", (self.producer,)
            ).fetchone()
            if (
                not row
                or row[0] != self.epoch
                or (expected_epoch is not None and expected_epoch != self.epoch)
            ):
                raise RuntimeError("producer generation fenced")
            old = self.db.execute(
                "SELECT sequence FROM producer_sequence WHERE id=? AND node=?",
                (self.producer, node),
            ).fetchone()
            sequence = int(old[0]) + 1 if old else 1
            self.db.execute(
                "INSERT OR REPLACE INTO producer_sequence VALUES(?,?,?)",
                (self.producer, node, sequence),
            )
        return dict(
            producer=self.producer + "/" + node,
            epoch=self.epoch,
            sequence=sequence,
            boundary="adapter",
        )

    def close(self) -> None:
        self.db.close()


class SourceJournal:
    def __init__(self, path: Path, capacity: int = 4096) -> None:
        if capacity < 1:
            raise ValueError("positive capacity required")
        self.db = database(path)
        self.capacity = capacity
        self.db.executescript(
            "CREATE TABLE IF NOT EXISTS source_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL); CREATE TABLE IF NOT EXISTS source_events(serial INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT UNIQUE NOT NULL,payload TEXT NOT NULL,created REAL NOT NULL,received INTEGER NOT NULL DEFAULT 0); CREATE TABLE IF NOT EXISTS source_state(id INTEGER PRIMARY KEY CHECK(id=1),payload TEXT NOT NULL); CREATE TABLE IF NOT EXISTS native_effects(serial INTEGER PRIMARY KEY AUTOINCREMENT,at REAL NOT NULL,payload TEXT NOT NULL);"
        )
        with self.db:
            for key, value in (
                ("db_id", str(uuid.uuid4())),
                ("created", "0"),
                ("received", "0"),
                ("boots", "0"),
            ):
                self.db.execute("INSERT OR IGNORE INTO source_meta VALUES(?,?)", (key, value))

    def boot(self) -> int:
        with self.db:
            self.db.execute(
                "UPDATE source_meta SET value=CAST(value AS INTEGER)+1 WHERE key='boots'"
            )
        return int(self.db.execute("SELECT value FROM source_meta WHERE key='boots'").fetchone()[0])

    def commit(
        self,
        state: dict[str, Any],
        event: dict[str, Any] | None = None,
        effect: dict[str, Any] | None = None,
    ) -> None:
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            if event is not None:
                identity = event["inspection_id"]
                body = canonical(event)
                previous = self.db.execute(
                    "SELECT payload FROM source_events WHERE id=?", (identity,)
                ).fetchone()
                if previous and previous[0] != body:
                    raise ValueError("immutable source event identity conflict")
                if previous is None:
                    if self.pending() >= self.capacity:
                        raise RuntimeError("source outbox capacity reached")
                    self.db.execute(
                        "INSERT INTO source_events(id,payload,created) VALUES(?,?,?)",
                        (identity, body, time.time()),
                    )
                    self.db.execute(
                        "UPDATE source_meta SET value=CAST(value AS INTEGER)+1 WHERE key='created'"
                    )
            self.db.execute("INSERT OR REPLACE INTO source_state VALUES(1,?)", (canonical(state),))
            if effect is not None:
                self.db.execute(
                    "INSERT INTO native_effects(at,payload) VALUES(?,?)",
                    (time.time(), canonical(effect)),
                )
            self.db.execute(
                "DELETE FROM source_events WHERE received=1 AND created<?", (time.time() - 86400,)
            )

    def state(self) -> dict[str, Any] | None:
        row = self.db.execute("SELECT payload FROM source_state WHERE id=1").fetchone()
        return json.loads(row[0]) if row else None

    def pending(self) -> int:
        return int(
            self.db.execute("SELECT count(*) FROM source_events WHERE received=0").fetchone()[0]
        )

    def head(self) -> dict[str, Any] | None:
        row = self.db.execute(
            "SELECT payload FROM source_events WHERE received=0 ORDER BY serial LIMIT 1"
        ).fetchone()
        return json.loads(row[0]) if row else None

    def ack(self, identity: str) -> bool:
        with self.db:
            row = self.db.execute(
                "SELECT received FROM source_events WHERE id=?", (identity,)
            ).fetchone()
            if row is None:
                return False
            if not row[0]:
                self.db.execute("UPDATE source_events SET received=1 WHERE id=?", (identity,))
                self.db.execute(
                    "UPDATE source_meta SET value=CAST(value AS INTEGER)+1 WHERE key='received'"
                )
        return True

    def stats(self) -> dict[str, Any]:
        meta = {r[0]: r[1] for r in self.db.execute("SELECT key,value FROM source_meta")}
        return dict(
            db_id=meta["db_id"],
            created=int(meta["created"]),
            received=int(meta["received"]),
            boots=int(meta["boots"]),
            pending=self.pending(),
        )

    def close(self) -> None:
        self.db.close()


class ConsumerStore:
    def __init__(self, path: Path) -> None:
        self.db = database(path)
        self.db.executescript(
            "CREATE TABLE IF NOT EXISTS consumer_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL); CREATE TABLE IF NOT EXISTS positions(topic TEXT PRIMARY KEY,producer TEXT,epoch INTEGER,sequence INTEGER,binding TEXT); CREATE TABLE IF NOT EXISTS inspections(serial INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT UNIQUE NOT NULL,digest TEXT NOT NULL,payload TEXT NOT NULL,received REAL NOT NULL,disposition TEXT NOT NULL,command_id TEXT UNIQUE,command TEXT,result TEXT,last_sent REAL NOT NULL DEFAULT 0,baseline_count INTEGER); CREATE TABLE IF NOT EXISTS state_audit(serial INTEGER PRIMARY KEY AUTOINCREMENT,instance TEXT,topic TEXT,correlation TEXT,payload TEXT,decision TEXT,snapshot TEXT,at REAL);"
        )
        with self.db:
            self.db.execute(
                "INSERT OR IGNORE INTO consumer_meta VALUES('db_id',?)", (str(uuid.uuid4()),)
            )
        self.instance = str(uuid.uuid4())

    def advance(self, topic: str, position: dict[str, Any], binding: str) -> str:
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            old = self.db.execute("SELECT * FROM positions WHERE topic=?", (topic,)).fetchone()
            if old:
                if old["producer"] != position["producer"]:
                    return "producer_conflict"
                if position["epoch"] < old["epoch"]:
                    return "old_epoch"
                if position["epoch"] == old["epoch"]:
                    if position["sequence"] < old["sequence"]:
                        return "out_of_order"
                    if position["sequence"] == old["sequence"]:
                        return "duplicate" if old["binding"] == binding else "position_conflict"
            self.db.execute(
                "INSERT OR REPLACE INTO positions VALUES(?,?,?,?,?)",
                (topic, position["producer"], position["epoch"], position["sequence"], binding),
            )
        return "accepted"

    def audit_state(
        self,
        topic: str,
        payload: dict[str, Any],
        decision: dict[str, Any],
        snapshot: dict[str, Any],
        now: float,
    ) -> None:
        with self.db:
            self.db.execute(
                "INSERT INTO state_audit(instance,topic,correlation,payload,decision,snapshot,at) VALUES(?,?,?,?,?,?,?)",
                (
                    self.instance,
                    topic,
                    str(payload.get("eirvah:correlationId", "")),
                    canonical(payload),
                    canonical(decision),
                    canonical(snapshot),
                    now,
                ),
            )
            self.db.execute(
                "DELETE FROM state_audit WHERE serial < (SELECT coalesce(max(serial),0)-100000 FROM state_audit)"
            )

    def receive(
        self,
        event: dict[str, Any],
        now: float,
        max_age: float,
        factory: Callable[[], dict[str, Any]],
        baseline_count: int | None = None,
    ) -> dict[str, Any]:
        body = canonical(event)
        digest = hashlib.sha256(body.encode()).hexdigest()
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            old = self.db.execute(
                "SELECT * FROM inspections WHERE id=?", (event["inspection_id"],)
            ).fetchone()
            if old:
                if old["digest"] != digest:
                    raise ValueError("inspection identity conflict")
                return dict(old)
            at = datetime.fromisoformat(event["inspected_at"]).timestamp()
            age = now - at
            disposition = (
                "future"
                if age < 0
                else "expired"
                if age >= max_age
                else "passed"
                if event["passed"]
                else "pending"
            )
            command = None
            session = event["inspection_id"].split(":")[0]
            if disposition == "pending":
                for row in self.db.execute(
                    "SELECT payload FROM inspections WHERE received>=?", (now - max_age - 10,)
                ):
                    previous = json.loads(row[0])
                    if (
                        previous["inspection_id"].split(":")[0] == session
                        and previous["bottle_id"] == event["bottle_id"]
                    ):
                        disposition = "bottle_duplicate"
                        break
            if disposition == "pending":
                if (
                    self.db.execute(
                        "SELECT count(*) FROM inspections WHERE disposition='pending'"
                    ).fetchone()[0]
                    >= 128
                ):
                    raise RuntimeError("consumer command outbox capacity reached")
                command = factory()
            self.db.execute(
                "INSERT INTO inspections(id,digest,payload,received,disposition,command_id,command,baseline_count) VALUES(?,?,?,?,?,?,?,?)",
                (
                    event["inspection_id"],
                    digest,
                    body,
                    now,
                    disposition,
                    command["correlation_id"] if command else None,
                    canonical(command) if command else None,
                    baseline_count,
                ),
            )
            self.db.execute(
                "DELETE FROM inspections WHERE disposition!='pending' AND received<?",
                (now - 86400,),
            )
            result = self.db.execute(
                "SELECT * FROM inspections WHERE id=?", (event["inspection_id"],)
            ).fetchone()
        return dict(result)

    def find(self, identity: str) -> dict[str, Any] | None:
        row = self.db.execute("SELECT * FROM inspections WHERE id=?", (identity,)).fetchone()
        return dict(row) if row else None

    def ready(self, now: float) -> list[dict[str, Any]]:
        with self.db:
            self.db.execute(
                "UPDATE inspections SET disposition='unknown',result=? WHERE disposition='pending' AND received<?",
                (
                    canonical({"decision": "unknown", "reason": "recovery_window_elapsed"}),
                    now - 86400,
                ),
            )
        return [
            dict(r)
            for r in self.db.execute(
                "SELECT * FROM inspections WHERE disposition='pending' AND last_sent<? ORDER BY serial LIMIT 128",
                (now - 1.0,),
            )
        ]

    def sent(self, identity: str, now: float) -> None:
        with self.db:
            self.db.execute("UPDATE inspections SET last_sent=? WHERE id=?", (now, identity))

    def settle(self, result: dict[str, Any]) -> None:
        identity = result.get("correlation_id")
        row = self.db.execute(
            "SELECT * FROM inspections WHERE command_id=?", (identity,)
        ).fetchone()
        if row is None:
            return
        command = json.loads(row["command"])
        if any(
            result.get(k) != command.get(k)
            for k in ("requester", "target_uns_topic", "requested_value", "value_type")
        ):
            raise ValueError("result binding mismatch")
        decision = result.get("decision")
        if decision not in ("approve", "reject", "pending", "unknown"):
            raise ValueError("unknown result type")
        if decision == "pending":
            return
        if decision == "approve" and not result.get("written_at"):
            raise ValueError("completion acknowledgement missing")
        disposition = {"approve": "completed", "reject": "refused", "unknown": "unknown"}[decision]
        if row["disposition"] == "completed" and disposition != "completed":
            raise ValueError("completed outcome cannot be overwritten")
        with self.db:
            self.db.execute(
                "UPDATE inspections SET disposition=?,result=? WHERE command_id=?",
                (disposition, canonical(result), identity),
            )

    def close(self) -> None:
        self.db.close()
