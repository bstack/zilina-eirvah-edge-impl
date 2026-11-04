"""Single-writer durable execution ledger; commit initiation before issuing device writes."""

from __future__ import annotations

import secrets
import sqlite3
import time
from collections.abc import Callable, Iterator
from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from eirvah_contracts.actuation import WriteResult


@dataclass(frozen=True)
class Claim:
    token: str | None
    result: WriteResult | None


class CommandLedger:
    def __init__(
        self, path: Path, *, retention_s: float = 86400, clock: Callable[[], float] = time.time
    ) -> None:
        if retention_s <= 0:
            raise ValueError("retention must be positive")
        self.path = path
        self.retention_s = retention_s
        self.clock = clock
        path.parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(path)) as db:
            version = db.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, 1):
                raise ValueError("unsupported command ledger version")
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA synchronous=FULL")
            db.execute("""CREATE TABLE IF NOT EXISTS commands (
                command_id TEXT PRIMARY KEY, digest TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('preparing','started','completed','retryable','unknown')),
                token TEXT, result TEXT, started_at REAL, keep_until REAL NOT NULL
            )""")
            db.execute("PRAGMA user_version=1")
            db.commit()

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self.path, timeout=3)
        db.row_factory = sqlite3.Row
        try:
            db.execute("PRAGMA synchronous=FULL")
            db.execute("BEGIN IMMEDIATE")
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def _prune(self, db: sqlite3.Connection) -> None:
        db.execute(
            "DELETE FROM commands WHERE state IN ('completed','retryable','unknown') AND keep_until < ?",
            (self.clock(),),
        )

    @staticmethod
    def _result(row: sqlite3.Row, digest: str) -> WriteResult:
        if row["digest"] != digest:
            return WriteResult(outcome="conflict", reason="command_id_reused", replayed=True)
        if row["state"] in ("preparing", "started"):
            return WriteResult(outcome="pending", reason="execution_in_progress", replayed=True)
        return WriteResult.model_validate_json(row["result"]).model_copy(update={"replayed": True})

    def lookup(self, command_id: str, digest: str) -> WriteResult | None:
        with self._transaction() as db:
            self._prune(db)
            row = db.execute("SELECT * FROM commands WHERE command_id=?", (command_id,)).fetchone()
            return None if row is None else self._result(row, digest)

    def claim(self, command_id: str, digest: str, *, deadline: float) -> Claim:
        with self._transaction() as db:
            self._prune(db)
            row = db.execute("SELECT * FROM commands WHERE command_id=?", (command_id,)).fetchone()
            if row is not None and (row["digest"] != digest or row["state"] != "retryable"):
                return Claim(None, self._result(row, digest))
            token = secrets.token_hex(16)
            keep_until = max(self.clock(), deadline) + self.retention_s
            if row is None:
                db.execute(
                    "INSERT INTO commands(command_id,digest,state,token,keep_until) VALUES (?,?,'preparing',?,?)",
                    (command_id, digest, token, keep_until),
                )
            else:
                db.execute(
                    "UPDATE commands SET state='preparing',token=?,result=NULL,started_at=NULL,keep_until=MAX(keep_until,?) WHERE command_id=?",
                    (token, keep_until, command_id),
                )
            return Claim(token, None)

    def mark_started(self, command_id: str, token: str) -> None:
        with self._transaction() as db:
            updated = db.execute(
                "UPDATE commands SET state='started',started_at=COALESCE(started_at,?) WHERE command_id=? AND token=? AND state IN ('preparing','started')",
                (self.clock(), command_id, token),
            )
            if updated.rowcount != 1:
                raise ValueError("command claim is no longer active")

    def complete(self, command_id: str, token: str, *, written_at: datetime) -> WriteResult:
        result = WriteResult(outcome="completed", written_at=written_at)
        with self._transaction() as db:
            updated = db.execute(
                "UPDATE commands SET state='completed',token=NULL,result=?,keep_until=MAX(keep_until,?) WHERE command_id=? AND token=? AND state='started'",
                (result.model_dump_json(), self.clock() + self.retention_s, command_id, token),
            )
            if updated.rowcount != 1:
                raise ValueError("command completion has no active initiation record")
        return result

    def fail(self, command_id: str, token: str, *, reason: str) -> WriteResult:
        with self._transaction() as db:
            row = db.execute(
                "SELECT state FROM commands WHERE command_id=? AND token=?", (command_id, token)
            ).fetchone()
            if row is None or row["state"] not in ("preparing", "started"):
                raise ValueError("command claim is no longer active")
            started = row["state"] == "started"
            result = WriteResult(
                outcome="unknown" if started else "not_executed",
                reason=reason,
                retryable=not started,
            )
            db.execute(
                "UPDATE commands SET state=?,token=NULL,result=?,keep_until=MAX(keep_until,?) WHERE command_id=?",
                (
                    "unknown" if started else "retryable",
                    result.model_dump_json(),
                    self.clock() + self.retention_s,
                    command_id,
                ),
            )
        return result

    def recover(self) -> None:
        """Call only at startup, before consuming commands; deployment permits one writer."""
        with self._transaction() as db:
            rows = db.execute(
                "SELECT command_id,state FROM commands WHERE state IN ('preparing','started')"
            ).fetchall()
            for row in rows:
                started = row["state"] == "started"
                result = WriteResult(
                    outcome="unknown" if started else "not_executed",
                    reason="recovered_after_start" if started else "recovered_before_write",
                    retryable=not started,
                )
                db.execute(
                    "UPDATE commands SET state=?,token=NULL,result=?,keep_until=MAX(keep_until,?) WHERE command_id=?",
                    (
                        "unknown" if started else "retryable",
                        result.model_dump_json(),
                        self.clock() + self.retention_s,
                        row["command_id"],
                    ),
                )
            self._prune(db)
