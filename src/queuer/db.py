"""Storage layer for queuer: SQLite-backed job table.

All functions take an open sqlite3.Connection as their first argument so
they can be unit-tested against a throwaway DB file, and so the daemon can
manage the connection lifecycle itself (see Phase 2).
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_DB_PATH = Path.home() / ".local" / "state" / "queuer" / "queuer.db"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    status        TEXT NOT NULL CHECK (status IN
                    ('queued','running','done','failed','cancelled')),
    position      INTEGER,
    raw_cmd       TEXT NOT NULL,
    resolved_cmd  TEXT NOT NULL,
    cwd           TEXT NOT NULL,
    env_extra     TEXT,
    pid           INTEGER,
    pgid          INTEGER,
    enqueued_at   TEXT NOT NULL,
    started_at    TEXT,
    finished_at   TEXT,
    exit_code     INTEGER,
    log_path      TEXT NOT NULL,
    note          TEXT,
    timeout_secs  INTEGER
);

CREATE TABLE IF NOT EXISTS daemon_state (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


def connect(db_path: Path | str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(_SCHEMA)
    conn.commit()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return dict(row)


# ---------------------------------------------------------------------------
# Queue ordering helpers
# ---------------------------------------------------------------------------


def _queued_ids_ordered(conn: sqlite3.Connection) -> list[int]:
    rows = conn.execute(
        "SELECT id FROM jobs WHERE status = 'queued' ORDER BY position ASC"
    ).fetchall()
    return [r["id"] for r in rows]


def _reindex_queue(conn: sqlite3.Connection, ordered_ids: list[int]) -> None:
    conn.executemany(
        "UPDATE jobs SET position = ? WHERE id = ?",
        [(i, job_id) for i, job_id in enumerate(ordered_ids)],
    )


# ---------------------------------------------------------------------------
# Mutations
# ---------------------------------------------------------------------------


def enqueue(
    conn: sqlite3.Connection,
    *,
    raw_cmd: list[str],
    resolved_cmd: list[str],
    cwd: str,
    log_path: str,
    env_extra: dict[str, str] | None = None,
    timeout_secs: int | None = None,
    before: int | None = None,
    after: int | None = None,
) -> int:
    """Insert a new queued job. Returns the new job's id.

    With neither `before` nor `after`, the job is appended to the end of
    the queue. `before`/`after` are mutually exclusive and must reference
    an id that is currently queued.
    """
    if before is not None and after is not None:
        raise ValueError("before and after are mutually exclusive")

    cur = conn.execute(
        """
        INSERT INTO jobs
            (status, position, raw_cmd, resolved_cmd, cwd, env_extra,
             enqueued_at, log_path, timeout_secs)
        VALUES ('queued', NULL, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            json.dumps(raw_cmd),
            json.dumps(resolved_cmd),
            cwd,
            json.dumps(env_extra) if env_extra else None,
            _now(),
            log_path,
            timeout_secs,
        ),
    )
    new_id = cur.lastrowid

    existing = [i for i in _queued_ids_ordered(conn) if i != new_id]

    if before is not None:
        if before not in existing:
            raise ValueError(f"job {before} is not currently queued")
        idx = existing.index(before)
        existing.insert(idx, new_id)
    elif after is not None:
        if after not in existing:
            raise ValueError(f"job {after} is not currently queued")
        idx = existing.index(after)
        existing.insert(idx + 1, new_id)
    else:
        existing.append(new_id)

    _reindex_queue(conn, existing)
    conn.commit()
    return new_id


def get_queue(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM jobs WHERE status = 'queued' ORDER BY position ASC"
    ).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_running(conn: sqlite3.Connection) -> dict[str, Any] | None:
    row = conn.execute("SELECT * FROM jobs WHERE status = 'running' LIMIT 1").fetchone()
    return _row_to_dict(row) if row else None


def get_backlog(conn: sqlite3.Connection, limit: int = 10) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT * FROM jobs
        WHERE status IN ('done', 'failed', 'cancelled')
        ORDER BY finished_at DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_job(conn: sqlite3.Connection, job_id: int) -> dict[str, Any] | None:
    row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    return _row_to_dict(row) if row else None


def mark_running(conn: sqlite3.Connection, job_id: int, pid: int, pgid: int) -> None:
    job = get_job(conn, job_id)
    if job is None or job["status"] != "queued":
        raise ValueError(f"job {job_id} is not queued")
    conn.execute(
        """
        UPDATE jobs
        SET status = 'running', position = NULL, pid = ?, pgid = ?, started_at = ?
        WHERE id = ?
        """,
        (pid, pgid, _now(), job_id),
    )
    remaining = [i for i in _queued_ids_ordered(conn) if i != job_id]
    _reindex_queue(conn, remaining)
    conn.commit()


def mark_finished(
    conn: sqlite3.Connection,
    job_id: int,
    *,
    status: str,
    exit_code: int | None = None,
    note: str | None = None,
    keep_backlog: int = 10,
) -> None:
    if status not in ("done", "failed", "cancelled"):
        raise ValueError(f"invalid terminal status: {status}")
    conn.execute(
        """
        UPDATE jobs
        SET status = ?, exit_code = ?, note = ?, finished_at = ?
        WHERE id = ?
        """,
        (status, exit_code, note, _now(), job_id),
    )
    conn.commit()
    _prune_backlog(conn, keep=keep_backlog)


def _prune_backlog(conn: sqlite3.Connection, keep: int = 10) -> None:
    """Delete finished-job DB rows beyond the most recent `keep`. Log files
    on disk are left untouched — only the row is pruned."""
    rows = conn.execute(
        """
        SELECT id FROM jobs
        WHERE status IN ('done', 'failed', 'cancelled')
        ORDER BY finished_at DESC
        """
    ).fetchall()
    stale_ids = [r["id"] for r in rows[keep:]]
    if stale_ids:
        conn.executemany("DELETE FROM jobs WHERE id = ?", [(i,) for i in stale_ids])
        conn.commit()


def remove_from_queue(conn: sqlite3.Connection, job_id: int) -> None:
    job = get_job(conn, job_id)
    if job is None:
        raise ValueError(f"job {job_id} does not exist")
    if job["status"] != "queued":
        raise ValueError(
            f"job {job_id} is not queued (status={job['status']!r}); "
            "use cancel for a running job"
        )
    conn.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
    remaining = [i for i in _queued_ids_ordered(conn) if i != job_id]
    _reindex_queue(conn, remaining)
    conn.commit()


def reorder_before(conn: sqlite3.Connection, job_id: int, before_id: int) -> None:
    _reorder(conn, job_id, target_id=before_id, after=False)


def reorder_after(conn: sqlite3.Connection, job_id: int, after_id: int) -> None:
    _reorder(conn, job_id, target_id=after_id, after=True)


def _reorder(
    conn: sqlite3.Connection, job_id: int, target_id: int, after: bool
) -> None:
    if job_id == target_id:
        raise ValueError("cannot reorder a job relative to itself")
    ids = _queued_ids_ordered(conn)
    if job_id not in ids:
        raise ValueError(f"job {job_id} is not queued")
    if target_id not in ids:
        raise ValueError(f"job {target_id} is not queued")
    ids.remove(job_id)
    idx = ids.index(target_id)
    ids.insert(idx + 1 if after else idx, job_id)
    _reindex_queue(conn, ids)
    conn.commit()


def requeue(conn: sqlite3.Connection, job_id: int, *, log_path: str) -> int:
    """Re-enqueue a done/failed/cancelled job with its original argv/cwd/env/
    timeout, appended to the end of the queue. Returns the new job's id."""
    job = get_job(conn, job_id)
    if job is None:
        raise ValueError(f"job {job_id} does not exist")
    if job["status"] not in ("done", "failed", "cancelled"):
        raise ValueError(
            f"job {job_id} has status {job['status']!r}; "
            "only finished jobs can be requeued"
        )
    return enqueue(
        conn,
        raw_cmd=json.loads(job["raw_cmd"]),
        resolved_cmd=json.loads(job["resolved_cmd"]),
        cwd=job["cwd"],
        log_path=log_path,
        env_extra=json.loads(job["env_extra"]) if job["env_extra"] else None,
        timeout_secs=job["timeout_secs"],
    )


# ---------------------------------------------------------------------------
# Daemon pause state
# ---------------------------------------------------------------------------


def get_paused(conn: sqlite3.Connection) -> bool:
    row = conn.execute("SELECT value FROM daemon_state WHERE key = 'paused'").fetchone()
    return row is not None and row["value"] == "true"


def set_paused(conn: sqlite3.Connection, paused: bool) -> None:
    conn.execute(
        """
        INSERT INTO daemon_state (key, value) VALUES ('paused', ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        ("true" if paused else "false",),
    )
    conn.commit()
