"""Storage layer for queuer: SQLite-backed job table.

All functions take an open sqlite3.Connection as their first argument so
they can be unit-tested against a throwaway DB file, and so the daemon can
manage the connection lifecycle itself (see Phase 2).

Channels: each job belongs to a channel (int, default 1). Queue position,
the running slot, backlog, and paused state are all scoped per channel --
channel N's queue never mixes with channel M's. A `channels` registry
table gets a row the first time a job lands on that channel, so a channel
"exists" (shows up in list_channels()) only once it's actually been used.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_DB_PATH = Path.home() / ".local" / "state" / "queuer" / "queuer.db"
DEFAULT_CHANNEL = 1

_JOBS_TABLE = """
CREATE TABLE IF NOT EXISTS jobs (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    channel       INTEGER NOT NULL DEFAULT 1,
    status        TEXT NOT NULL CHECK (status IN
                    ('queued','running','stopped','done','failed','cancelled')),
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
    timeout_secs  INTEGER,
    is_priority   INTEGER NOT NULL DEFAULT 0
);
"""

_SCHEMA = _JOBS_TABLE + """
CREATE TABLE IF NOT EXISTS channels (
    id            INTEGER PRIMARY KEY,
    first_seen_at TEXT NOT NULL
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


def _migrate_add_priority_lane(conn: sqlite3.Connection) -> None:
    """DBs created before the `--now` priority lane lack `is_priority`
    and the `stopped` status. CHECK constraints can't be altered in
    place, so rebuild the table. No-op once migrated (or on a fresh DB,
    which is already created with the new schema)."""
    cols = [r["name"] for r in conn.execute("PRAGMA table_info(jobs)").fetchall()]
    if "is_priority" in cols:
        return
    conn.executescript("ALTER TABLE jobs RENAME TO jobs_old;")
    conn.executescript(_JOBS_TABLE)
    conn.execute(
        """
        INSERT INTO jobs (id, channel, status, position, raw_cmd, resolved_cmd, cwd,
                           env_extra, pid, pgid, enqueued_at, started_at, finished_at,
                           exit_code, log_path, note, timeout_secs, is_priority)
        SELECT id, channel, status, position, raw_cmd, resolved_cmd, cwd,
               env_extra, pid, pgid, enqueued_at, started_at, finished_at,
               exit_code, log_path, note, timeout_secs, 0
        FROM jobs_old
        """
    )
    conn.execute("DROP TABLE jobs_old")
    conn.commit()


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(_SCHEMA)
    conn.commit()
    _migrate_add_priority_lane(conn)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return dict(row)


# ---------------------------------------------------------------------------
# Channel registry
# ---------------------------------------------------------------------------


def register_channel(conn: sqlite3.Connection, channel: int) -> None:
    """Mark a channel as seen. No-op if already registered."""
    conn.execute(
        "INSERT OR IGNORE INTO channels (id, first_seen_at) VALUES (?, ?)",
        (channel, _now()),
    )
    conn.commit()


def list_channels(conn: sqlite3.Connection) -> list[int]:
    """Channels that have had at least one job enqueued, ascending."""
    rows = conn.execute("SELECT id FROM channels ORDER BY id ASC").fetchall()
    return [r["id"] for r in rows]


# ---------------------------------------------------------------------------
# Queue ordering helpers (scoped per channel)
# ---------------------------------------------------------------------------


def _queued_ids_ordered(conn: sqlite3.Connection, channel: int, is_priority: bool = False) -> list[int]:
    rows = conn.execute(
        "SELECT id FROM jobs WHERE status = 'queued' AND channel = ? AND is_priority = ? ORDER BY position ASC",
        (channel, int(is_priority)),
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
    channel: int = DEFAULT_CHANNEL,
    env_extra: dict[str, str] | None = None,
    timeout_secs: int | None = None,
    before: int | None = None,
    after: int | None = None,
    is_priority: bool = False,
) -> int:
    """Insert a new queued job on `channel`. Returns the new job's id.

    With neither `before` nor `after`, the job is appended to the end of
    that channel's queue. `before`/`after` are mutually exclusive and must
    reference an id currently queued on the *same* channel -- a stale id
    or one from a different channel is rejected the same way (ValueError:
    not currently queued).

    `is_priority` puts the job on that channel's priority lane instead of
    its normal queue -- a separate FIFO with its own position sequence,
    used by `--now`. `before`/`after` are always resolved against the same
    lane the new job is joining.
    """
    if before is not None and after is not None:
        raise ValueError("before and after are mutually exclusive")

    register_channel(conn, channel)

    cur = conn.execute(
        """
        INSERT INTO jobs
            (channel, status, position, raw_cmd, resolved_cmd, cwd, env_extra,
             enqueued_at, log_path, timeout_secs, is_priority)
        VALUES (?, 'queued', NULL, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            channel,
            json.dumps(raw_cmd),
            json.dumps(resolved_cmd),
            cwd,
            json.dumps(env_extra) if env_extra else None,
            _now(),
            log_path,
            timeout_secs,
            int(is_priority),
        ),
    )
    new_id = cur.lastrowid

    existing = [i for i in _queued_ids_ordered(conn, channel, is_priority) if i != new_id]

    if before is not None:
        if before not in existing:
            raise ValueError(f"job {before} is not currently queued on channel {channel}")
        idx = existing.index(before)
        existing.insert(idx, new_id)
    elif after is not None:
        if after not in existing:
            raise ValueError(f"job {after} is not currently queued on channel {channel}")
        idx = existing.index(after)
        existing.insert(idx + 1, new_id)
    else:
        existing.append(new_id)

    _reindex_queue(conn, existing)
    conn.commit()
    return new_id


def get_queue(
    conn: sqlite3.Connection, channel: int = DEFAULT_CHANNEL, is_priority: bool = False
) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM jobs WHERE status = 'queued' AND channel = ? AND is_priority = ? ORDER BY position ASC",
        (channel, int(is_priority)),
    ).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_running(conn: sqlite3.Connection, channel: int = DEFAULT_CHANNEL) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM jobs WHERE status = 'running' AND channel = ? LIMIT 1", (channel,)
    ).fetchone()
    return _row_to_dict(row) if row else None


def get_stopped(conn: sqlite3.Connection, channel: int = DEFAULT_CHANNEL) -> dict[str, Any] | None:
    """The job frozen (SIGSTOP'd) on this channel by a `--now` preemption,
    if any -- at most one per channel, since only a non-priority job can
    be frozen and a priority lane freezes it at most once."""
    row = conn.execute(
        "SELECT * FROM jobs WHERE status = 'stopped' AND channel = ? LIMIT 1", (channel,)
    ).fetchone()
    return _row_to_dict(row) if row else None


def get_backlog(
    conn: sqlite3.Connection, channel: int = DEFAULT_CHANNEL, limit: int = 10
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT * FROM jobs
        WHERE status IN ('done', 'failed', 'cancelled') AND channel = ?
        ORDER BY finished_at DESC
        LIMIT ?
        """,
        (channel, limit),
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
    remaining = [i for i in _queued_ids_ordered(conn, job["channel"], bool(job["is_priority"])) if i != job_id]
    _reindex_queue(conn, remaining)
    conn.commit()


def mark_stopped(conn: sqlite3.Connection, job_id: int) -> None:
    """Freeze a running job in place (paired with an external SIGSTOP) --
    used to preempt it for a `--now` priority job. Status only; pid/pgid/
    started_at are left untouched so resuming looks like nothing happened."""
    job = get_job(conn, job_id)
    if job is None or job["status"] != "running":
        raise ValueError(f"job {job_id} is not running")
    conn.execute("UPDATE jobs SET status = 'stopped' WHERE id = ?", (job_id,))
    conn.commit()


def mark_resumed(conn: sqlite3.Connection, job_id: int) -> None:
    """Reverse of mark_stopped (paired with an external SIGCONT), once the
    priority lane that preempted it has drained."""
    job = get_job(conn, job_id)
    if job is None or job["status"] != "stopped":
        raise ValueError(f"job {job_id} is not stopped")
    conn.execute("UPDATE jobs SET status = 'running' WHERE id = ?", (job_id,))
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
    job = get_job(conn, job_id)
    if job is None:
        raise ValueError(f"job {job_id} does not exist")
    conn.execute(
        """
        UPDATE jobs
        SET status = ?, exit_code = ?, note = ?, finished_at = ?
        WHERE id = ?
        """,
        (status, exit_code, note, _now(), job_id),
    )
    conn.commit()
    _prune_backlog(conn, job["channel"], keep=keep_backlog)


def _prune_backlog(conn: sqlite3.Connection, channel: int, keep: int = 10) -> None:
    """Delete finished-job DB rows beyond the most recent `keep`, scoped to
    one channel. Log files on disk are left untouched -- only the row is
    pruned."""
    rows = conn.execute(
        """
        SELECT id FROM jobs
        WHERE status IN ('done', 'failed', 'cancelled') AND channel = ?
        ORDER BY finished_at DESC
        """,
        (channel,),
    ).fetchall()
    stale_ids = [r["id"] for r in rows[keep:]]
    if stale_ids:
        conn.executemany("DELETE FROM jobs WHERE id = ?", [(i,) for i in stale_ids])
        conn.commit()


def clear_backlog(conn: sqlite3.Connection, channel: int | None = None) -> int:
    """Delete all finished (done/failed/cancelled) job rows -- the history
    shown by `list`/`status`, and nothing else. Queued, running, and
    stopped (frozen by `--now`) jobs are never touched by this. Log files
    on disk are left alone (see `clean-logs` for those). Returns the
    number of rows deleted.

    `channel=None` clears every channel's backlog; passing a channel
    number scopes it to just that one.
    """
    if channel is None:
        cur = conn.execute("DELETE FROM jobs WHERE status IN ('done', 'failed', 'cancelled')")
    else:
        cur = conn.execute(
            "DELETE FROM jobs WHERE status IN ('done', 'failed', 'cancelled') AND channel = ?",
            (channel,),
        )
    conn.commit()
    return cur.rowcount


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
    remaining = [i for i in _queued_ids_ordered(conn, job["channel"], bool(job["is_priority"])) if i != job_id]
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
    job = get_job(conn, job_id)
    if job is None:
        raise ValueError(f"job {job_id} does not exist")
    ids = _queued_ids_ordered(conn, job["channel"], bool(job["is_priority"]))
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
    timeout/channel, appended to the end of its channel's queue. Returns
    the new job's id."""
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
        channel=job["channel"],
        env_extra=json.loads(job["env_extra"]) if job["env_extra"] else None,
        timeout_secs=job["timeout_secs"],
    )


# ---------------------------------------------------------------------------
# Daemon pause state (per channel)
# ---------------------------------------------------------------------------


def get_paused(conn: sqlite3.Connection, channel: int = DEFAULT_CHANNEL) -> bool:
    row = conn.execute(
        "SELECT value FROM daemon_state WHERE key = ?", (f"paused:{channel}",)
    ).fetchone()
    return row is not None and row["value"] == "true"


def set_paused(conn: sqlite3.Connection, paused: bool, channel: int = DEFAULT_CHANNEL) -> None:
    conn.execute(
        """
        INSERT INTO daemon_state (key, value) VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (f"paused:{channel}", "true" if paused else "false"),
    )
    conn.commit()
