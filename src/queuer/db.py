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
import os
import signal
import sqlite3
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

DEFAULT_DB_PATH = Path.home() / ".local" / "state" / "queuer" / "queuer.db"
DEFAULT_CHANNEL = 1

# Every state a job can end in. `interrupted`: the daemon stopped while it ran.
# `skipped`: a dependency failed (or was removed), so it never ran.
TERMINAL_STATUSES = ("done", "failed", "cancelled", "skipped", "interrupted")
ACTIVE_STATUSES = ("queued", "running", "stopped")
DEP_MODES = ("success", "any")  # `--depends-on` (must end done) / `--after-any` (any end)
ON_FAIL_MODES = ("continue", "pause")
DEFAULT_KEEP = 200  # finished jobs kept per channel by the daemon (QUEUER_KEEP changes it)
LIST_BACKLOG = 10  # finished jobs `qur list` / the Telegram list show per channel
MAX_HISTORY = 10000  # most rows one history query returns
MAX_RETRY_DELAY_SECS = 30 * 86400  # longest wait before an automatic retry (also caps --retry-backoff)
MAX_HOOK_LEN = 4096  # longest --on-finish / --on-error command (characters)
STOP_SIGNALS = ("SIGTERM", "SIGINT", "SIGHUP", "SIGQUIT", "SIGUSR1", "SIGUSR2", "SIGKILL")  # F11: signals a job may be stopped with
MAX_STOP_GRACE_SECS = 86400  # F11: longest grace between the stop signal and SIGKILL (1 day)
BUSY_TIMEOUT_MS = 5000  # how long a connection waits for another one's write lock before "database is locked"
EDIT_LOCK_SECS = 30 * 60  # an edit lock older than this is stale and ignored (a crashed editor never blocks a channel)
MAX_SWEEP = 1000  # most jobs one `add` creates (--each/--array) and most ids one dependency list holds
MAX_CHANNEL = 9999  # L7: highest channel number (every channel gets a worker task and a registry row)
MAX_RETRIES = 1000  # L7: most automatic retries one job may ask for
MAX_TIMEOUT_SECS = 10 * 365 * 86400  # L7: longest --timeout (ten years)
MAX_INDEX = 10**9  # L7: highest QUR_INDEX of a sweep job

_JOBS_DDL = """
CREATE TABLE IF NOT EXISTS {name} (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    channel       INTEGER NOT NULL DEFAULT 1,
    status        TEXT NOT NULL CHECK (status IN
                    ('queued','running','stopped','done','failed','cancelled',
                     'interrupted','skipped')),
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
    is_priority   INTEGER NOT NULL DEFAULT 0,
    max_retries   INTEGER NOT NULL DEFAULT 0,
    attempt       INTEGER NOT NULL DEFAULT 0,
    parent_id     INTEGER,
    origin        TEXT,
    not_before    TEXT,
    on_fail       TEXT NOT NULL DEFAULT 'continue' CHECK (on_fail IN ('continue','pause')),
    retry_delay_secs INTEGER,
    retry_backoff INTEGER NOT NULL DEFAULT 0,
    max_rss_kb    INTEGER,
    cpu_user_secs REAL,
    cpu_sys_secs  REAL,
    on_finish_cmd TEXT,
    on_error_cmd  TEXT,
    stop_signal   TEXT,
    stop_grace_secs INTEGER
);
"""

_JOBS_TABLE = _JOBS_DDL.format(name="jobs")

_SCHEMA = _JOBS_TABLE + """
CREATE TABLE IF NOT EXISTS channels (
    id            INTEGER PRIMARY KEY,
    first_seen_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS daemon_state (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS job_deps (
    job_id  INTEGER NOT NULL,
    dep_id  INTEGER NOT NULL,
    mode    TEXT NOT NULL CHECK (mode IN ('success','any')),
    PRIMARY KEY (job_id, dep_id)
);

CREATE INDEX IF NOT EXISTS idx_job_deps_dep ON job_deps (dep_id);

-- idx_jobs_queue / idx_jobs_backlog are NOT here: on an old DB the jobs table can still lack
-- `is_priority` when this script runs, so they are created by init_db after the migrations.

CREATE TABLE IF NOT EXISTS edit_locks (
    job_id     INTEGER PRIMARY KEY,
    holder_pid INTEGER NOT NULL,
    taken_at   TEXT NOT NULL,
    expires_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS schema_version (
    id      INTEGER PRIMARY KEY CHECK (id = 1),
    version INTEGER NOT NULL
);
"""


def connect(db_path: Path | str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    db_path = Path(db_path)
    new_dir = not db_path.parent.exists()
    db_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    # 0700 for a folder we made or queuer's default one; never chmod a folder the
    # user pointed QUEUER_DB_PATH at (it may be $HOME) -- the daemon warns instead
    if new_dir or db_path.parent == DEFAULT_DB_PATH.parent:
        os.chmod(db_path.parent, 0o700)
    # create the file 0600 ourselves; sqlite would use the umask
    os.close(os.open(db_path, os.O_RDWR | os.O_CREAT, 0o600))
    os.chmod(db_path, 0o600)
    conn = sqlite3.connect(db_path, timeout=BUSY_TIMEOUT_MS / 1000)
    conn.row_factory = sqlite3.Row
    # busy_timeout first: the journal_mode switch itself needs a lock when the daemon and a
    # CLI command (clean-logs) connect at the same moment. WAL lets them read while another
    # writes. On a filesystem that cannot do WAL sqlite silently keeps the old mode.
    conn.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA journal_mode = WAL")
    _secure_db_files(db_path)
    return conn


def _secure_db_files(db_path: Path) -> None:
    """The WAL side files hold the same data as the database: keep them 0600 too (sqlite
    creates them with the main file's mode, this makes it explicit). They may not exist
    (no WAL, or already checkpointed and removed)."""
    for suffix in ("-wal", "-shm"):
        try:
            os.chmod(f"{db_path}{suffix}", 0o600)
        except FileNotFoundError:
            pass


def _migrate_add_priority_lane(conn: sqlite3.Connection) -> None:
    """DBs created before the `--now` priority lane lack `is_priority`
    and the `stopped` status. CHECK constraints can't be altered in
    place, so rebuild the table. No-op once migrated (or on a fresh DB,
    which is already created with the new schema)."""
    cols = [r["name"] for r in conn.execute("PRAGMA table_info(jobs)").fetchall()]
    if "is_priority" in cols:
        return
    conn.execute("ALTER TABLE jobs RENAME TO jobs_old")
    conn.execute(_JOBS_TABLE)
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


def _migrate_add_retries(conn: sqlite3.Connection) -> None:
    """DBs created before auto-retry lack `max_retries` / `attempt` /
    `parent_id` / `origin`. Plain
    ADD COLUMN works here (no CHECK change needed). No-op once migrated."""
    cols = [r["name"] for r in conn.execute("PRAGMA table_info(jobs)").fetchall()]
    if "max_retries" not in cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN max_retries INTEGER NOT NULL DEFAULT 0")
    if "attempt" not in cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN attempt INTEGER NOT NULL DEFAULT 0")
    # where a job came from: NULL origin = queued fresh by a human
    if "parent_id" not in cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN parent_id INTEGER")
    if "origin" not in cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN origin TEXT")


def _migrate_add_deps_and_delay(conn: sqlite3.Connection) -> None:
    """DBs created before dependencies / delayed start / `--on-fail` lack the
    `interrupted` and `skipped` statuses (a CHECK constraint, so the table must be
    rebuilt) and the `not_before` / `on_fail` columns. The whole rebuild runs in ONE
    transaction (the runner's): a crash half-way leaves the old table untouched. No-op once migrated
    (or on a fresh DB, which is created with the new schema). Runs after the two
    older migrations, so the old table already has every column up to `origin`."""
    row = conn.execute("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'jobs'").fetchone()
    if row is None or ("'interrupted'" in row["sql"] and "not_before" in row["sql"]):
        return
    seq = conn.execute("SELECT seq FROM sqlite_sequence WHERE name = 'jobs'").fetchone()
    conn.execute("DROP TABLE IF EXISTS jobs_new")
    conn.execute(_JOBS_DDL.format(name="jobs_new"))
    conn.execute(
        """
        INSERT INTO jobs_new (id, channel, status, position, raw_cmd, resolved_cmd, cwd,
                              env_extra, pid, pgid, enqueued_at, started_at, finished_at,
                              exit_code, log_path, note, timeout_secs, is_priority,
                              max_retries, attempt, parent_id, origin)
        SELECT id, channel, status, position, raw_cmd, resolved_cmd, cwd,
               env_extra, pid, pgid, enqueued_at, started_at, finished_at,
               exit_code, log_path, note, timeout_secs, is_priority,
               max_retries, attempt, parent_id, origin
        FROM jobs
        """
    )
    conn.execute("DROP TABLE jobs")
    conn.execute("ALTER TABLE jobs_new RENAME TO jobs")
    if seq is not None:
        # AUTOINCREMENT must never hand out an id that was used before (pruned
        # history rows included): dependencies refer to jobs by id
        cur = conn.execute("UPDATE sqlite_sequence SET seq = MAX(seq, ?) WHERE name = 'jobs'", (seq["seq"],))
        if cur.rowcount == 0:
            conn.execute("INSERT INTO sqlite_sequence (name, seq) VALUES ('jobs', ?)", (seq["seq"],))


def _migrate_add_retry_delay(conn: sqlite3.Connection) -> None:
    """DBs created before `--retry-delay` lack `retry_delay_secs` (NULL = retry at
    once) and `retry_backoff` (0/1). Plain ADD COLUMN, no CHECK change. No-op once
    migrated (a DB rebuilt by the migration above already has both)."""
    cols = [r["name"] for r in conn.execute("PRAGMA table_info(jobs)").fetchall()]
    if "retry_delay_secs" not in cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN retry_delay_secs INTEGER")
    if "retry_backoff" not in cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN retry_backoff INTEGER NOT NULL DEFAULT 0")


def _migrate_add_resource_usage(conn: sqlite3.Connection) -> None:
    """DBs created before F10 resource accounting lack `max_rss_kb` (peak RSS in
    KB, sampled from /proc while the job ran) and `cpu_user_secs` /
    `cpu_sys_secs`. Plain ADD COLUMN, no CHECK change. No-op once migrated."""
    cols = [r["name"] for r in conn.execute("PRAGMA table_info(jobs)").fetchall()]
    if "max_rss_kb" not in cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN max_rss_kb INTEGER")
    if "cpu_user_secs" not in cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN cpu_user_secs REAL")
    if "cpu_sys_secs" not in cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN cpu_sys_secs REAL")


def _migrate_add_hooks(conn: sqlite3.Connection) -> None:
    """DBs created before F12 job hooks lack `on_finish_cmd` / `on_error_cmd` (the
    shell commands of `--on-finish` / `--on-error`; NULL = none). Plain ADD COLUMN,
    no CHECK change. No-op once migrated."""
    cols = [r["name"] for r in conn.execute("PRAGMA table_info(jobs)").fetchall()]
    if "on_finish_cmd" not in cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN on_finish_cmd TEXT")
    if "on_error_cmd" not in cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN on_error_cmd TEXT")


def _migrate_add_stop_settings(conn: sqlite3.Connection) -> None:
    """DBs created before F11 lack `stop_signal` (the signal that stops the job on cancel
    or timeout; NULL = SIGTERM) and `stop_grace_secs` (seconds between that signal and
    SIGKILL; NULL = the daemon default). Plain ADD COLUMN, no CHECK change. No-op once migrated."""
    cols = [r["name"] for r in conn.execute("PRAGMA table_info(jobs)").fetchall()]
    if "stop_signal" not in cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN stop_signal TEXT")
    if "stop_grace_secs" not in cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN stop_grace_secs INTEGER")


def _migrate_add_jobs_indexes(conn: sqlite3.Connection) -> None:
    """DBs created before this migration lack indexes on `jobs` beyond the primary key, so
    get_queue/get_running/get_stopped/get_backlog/_prune_backlog scan the whole table.
    `idx_jobs_queue` (channel, status, is_priority, position) covers the queue-lane lookups
    (get_queue, get_queue_head, mark_running's reindex); `idx_jobs_backlog` (channel, status,
    finished_at) covers get_backlog/_prune_backlog. CREATE INDEX IF NOT EXISTS, no table
    rebuild, no data touched. No-op once migrated; init_db also calls it after the migrations so a
    fresh DB gets the indexes (they cannot be in _SCHEMA, see there)."""
    conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_queue ON jobs (channel, status, is_priority, position)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_backlog ON jobs (channel, status, finished_at)")


# Numbered migrations, run in order, each in its own transaction that also bumps the
# single-row `schema_version` table. To change the schema: append ONE new
# (number, function) entry (never edit or reorder old ones) and, if a fresh DB needs the
# change, put it in _SCHEMA / _JOBS_DDL too. A step must not commit (the runner does) and
# must not use executescript (it commits). Steps 1-4 probe the table first because a DB from
# before `schema_version` existed can be at any of those states.
_MIGRATIONS: tuple[tuple[int, Callable[[sqlite3.Connection], None]], ...] = (
    (1, _migrate_add_priority_lane),
    (2, _migrate_add_retries),
    (3, _migrate_add_deps_and_delay),
    (4, _migrate_add_retry_delay),
    (5, _migrate_add_resource_usage),
    (6, _migrate_add_hooks),
    (7, _migrate_add_stop_settings),
    (8, _migrate_add_jobs_indexes),
)
SCHEMA_VERSION = _MIGRATIONS[-1][0]


def get_schema_version(conn: sqlite3.Connection) -> int | None:
    """The DB's schema version, or None when the table or its row does not exist yet."""
    has = conn.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'schema_version'").fetchone()
    if has is None:
        return None
    row = conn.execute("SELECT version FROM schema_version WHERE id = 1").fetchone()
    return None if row is None else int(row["version"])


def _run_migration(conn: sqlite3.Connection, version: int, step: Callable[[sqlite3.Connection], None]) -> None:
    conn.commit()  # never BEGIN on top of a half-open implicit transaction
    conn.execute("BEGIN IMMEDIATE")  # the write lock: two processes cannot run the same step
    try:
        row = conn.execute("SELECT version FROM schema_version WHERE id = 1").fetchone()
        if row is not None and row["version"] >= version:
            conn.rollback()  # another process just did it
            return
        step(conn)
        conn.execute("UPDATE schema_version SET version = ? WHERE id = 1", (version,))
        conn.commit()
    except BaseException:
        conn.rollback()
        raise


def init_db(conn: sqlite3.Connection) -> None:
    """Create what is missing and bring an older DB up to SCHEMA_VERSION, without losing
    data. A fresh DB is created at the latest version; a DB without a `schema_version`
    row (made before it existed) counts as version 0 and runs every step. Safe to call
    again and from several processes."""
    conn.commit()
    fresh = conn.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'jobs'").fetchone() is None
    conn.executescript(_SCHEMA)
    conn.execute("INSERT OR IGNORE INTO schema_version (id, version) VALUES (1, ?)", (SCHEMA_VERSION if fresh else 0,))
    conn.commit()
    current = get_schema_version(conn)
    if current is None:  # cannot happen (inserted above); keeps mypy and a hand-edited DB honest
        raise RuntimeError("schema_version row is missing")
    if current > SCHEMA_VERSION:
        raise RuntimeError(
            f"the database is at schema version {current}, newer than this queuer supports "
            f"({SCHEMA_VERSION}); upgrade queuer instead of using an old copy"
        )
    for version, step in _MIGRATIONS:
        if version > current:
            _run_migration(conn, version, step)
    # the `jobs` table has every column now (a fresh DB was created complete): make sure the
    # indexes exist. Not in _SCHEMA, which runs before the migrations and would fail on an old
    # table without `is_priority`. Idempotent (CREATE INDEX IF NOT EXISTS).
    _migrate_add_jobs_indexes(conn)
    conn.commit()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return dict(row)


# ---------------------------------------------------------------------------
# Channel registry
# ---------------------------------------------------------------------------


def register_channel(conn: sqlite3.Connection, channel: int, commit: bool = True) -> None:
    """Mark a channel as seen. No-op if already registered."""
    conn.execute(
        "INSERT OR IGNORE INTO channels (id, first_seen_at) VALUES (?, ?)",
        (channel, _now()),
    )
    if commit:
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


# `position` only has to ORDER a lane (channel + is_priority), it is not 0..n-1. Positions are
# spaced POSITION_STEP apart so a job can be put between two others without touching the rest;
# a lane is renumbered only when an insert finds no free number between its two neighbours.
# Appending, starting, removing and cancelling never renumber (5.todo item 10).
POSITION_STEP = 1024


def _reindex_queue(conn: sqlite3.Connection, ordered_ids: list[int]) -> None:
    """Give `ordered_ids` (one lane, in order) fresh positions POSITION_STEP apart."""
    conn.executemany(
        "UPDATE jobs SET position = ? WHERE id = ?",
        [(i * POSITION_STEP, job_id) for i, job_id in enumerate(ordered_ids)],
    )


def _next_position(conn: sqlite3.Connection, channel: int, is_priority: bool) -> int:
    """The position that appends a job to the end of a lane: one indexed MAX() query."""
    row = conn.execute(
        "SELECT MAX(position) AS last FROM jobs WHERE status = 'queued' AND channel = ? AND is_priority = ?",
        (channel, int(is_priority)),
    ).fetchone()
    return 0 if row["last"] is None else int(row["last"]) + POSITION_STEP


def _position_beside(
    conn: sqlite3.Connection,
    target_id: int,
    channel: int,
    is_priority: bool,
    *,
    after: bool,
    exclude_id: int | None = None,
) -> int:
    """A position that puts a job right before (`after=False`) or after queued job `target_id`
    in its lane. `exclude_id` is a job of that lane being moved, ignored when looking for the
    neighbour. If there is no free number between the two neighbours the lane is renumbered once
    (after which there is a gap of POSITION_STEP)."""
    skip = -1 if exclude_id is None else exclude_id
    for _ in range(2):
        target = conn.execute("SELECT position FROM jobs WHERE id = ?", (target_id,)).fetchone()["position"]
        if after:
            row = conn.execute(
                "SELECT MIN(position) AS near FROM jobs WHERE status = 'queued' AND channel = ? "
                "AND is_priority = ? AND position > ? AND id != ?",
                (channel, int(is_priority), target, skip),
            ).fetchone()
            if row["near"] is None:
                return int(target) + POSITION_STEP
            low, high = int(target), int(row["near"])
        else:
            row = conn.execute(
                "SELECT MAX(position) AS near FROM jobs WHERE status = 'queued' AND channel = ? "
                "AND is_priority = ? AND position < ? AND id != ?",
                (channel, int(is_priority), target, skip),
            ).fetchone()
            if row["near"] is None:
                return int(target) - POSITION_STEP
            low, high = int(row["near"]), int(target)
        if high - low >= 2:
            return (low + high) // 2
        _reindex_queue(conn, _queued_ids_ordered(conn, channel, is_priority))
    raise RuntimeError("no free queue position after renumbering")  # pragma: no cover


def _is_queued_behind(
    conn: sqlite3.Connection, other_id: int, channel: int, is_priority: bool, position: int
) -> bool:
    """True if `other_id` is queued in this lane after `position`."""
    row = conn.execute(
        "SELECT 1 FROM jobs WHERE id = ? AND status = 'queued' AND channel = ? AND is_priority = ? AND position > ?",
        (other_id, channel, int(is_priority), position),
    ).fetchone()
    return row is not None


# ---------------------------------------------------------------------------
# Mutations
# ---------------------------------------------------------------------------


def check_hook(value: Any, what: str) -> str | None:
    """Validate a hook command (F12): None or an empty/blank string = no hook (None);
    otherwise a string without NUL bytes, at most MAX_HOOK_LEN characters. Returns the
    command as stored."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{what} must be a string")
    if not value.strip():
        return None
    if "\0" in value:
        raise ValueError(f"{what} must not contain NUL bytes")
    if len(value) > MAX_HOOK_LEN:
        raise ValueError(f"{what} is too long (at most {MAX_HOOK_LEN} characters)")
    return value


def check_stop_signal(value: Any, what: str = "stop signal") -> str | None:
    """Validate a stop signal (F11): None or a blank string = the default (None);
    otherwise a name from STOP_SIGNALS (`INT`, `int` and `SIGINT` all work) or its number
    (`2`). Returns the canonical name (`SIGINT`). SIGSTOP/SIGCONT are refused: the daemon
    uses them to freeze jobs."""
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ValueError(f"{what} must be a signal name or number")
    text = str(value).strip().upper()
    if not text:
        return None
    if text.isdigit():
        try:
            text = signal.Signals(int(text)).name
        except ValueError:
            raise ValueError(f"{what}: {value!r} is not a signal number") from None
    elif not text.startswith("SIG"):
        text = "SIG" + text
    if text not in STOP_SIGNALS:
        raise ValueError(f"{what} must be one of {', '.join(n[3:] for n in STOP_SIGNALS)} (got {value!r})")
    return text


def check_stop_grace(value: Any, what: str = "stop grace") -> int | None:
    """Validate a grace period in seconds (F11): None = the daemon default; otherwise an
    integer from 0 (SIGKILL right after the signal) to MAX_STOP_GRACE_SECS."""
    if value is None:
        return None
    if type(value) is not int or not 0 <= value <= MAX_STOP_GRACE_SECS:
        raise ValueError(f"{what} must be a whole number of seconds between 0 and {MAX_STOP_GRACE_SECS}")
    return value


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
    max_retries: int = 0,
    attempt: int = 0,
    parent_id: int | None = None,
    origin: str | None = None,
    deps: dict[int, str] | None = None,
    not_before: str | None = None,
    on_fail: str = "continue",
    retry_delay_secs: int | None = None,
    retry_backoff: bool = False,
    on_finish_cmd: str | None = None,
    on_error_cmd: str | None = None,
    stop_signal: str | None = None,
    stop_grace_secs: int | None = None,
    commit: bool = True,
    check_deadlock: bool = True,
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

    `max_retries` is how many times the daemon may automatically requeue
    this job if it fails; `attempt` is how many automatic retries have
    already happened (0 for a job a human queued). `parent_id`/`origin`
    record where the job came from: origin is None for a fresh job,
    'retry' (daemon auto-retry) or 'requeue' (manual), with parent_id the
    job it was copied from.

    `deps` maps a job id to "success" (run only if it ended `done`) or "any"
    (run once it ended in any terminal state). Every id must exist. A set of
    dependencies that could never be satisfied -- a job queued behind this one
    on the same channel, or any longer cycle through queue order and
    dependencies -- is rejected with a ValueError. `not_before` is an absolute
    ISO timestamp the job must not start before; `on_fail` ("continue" or
    "pause") says what a failure of this job does to its channel. Priority
    (`--now`) jobs can have neither dependencies nor a start time.

    `retry_delay_secs` (None = none) is how long an automatic retry of this job
    waits before it may start; `retry_backoff` doubles that wait for every
    attempt already made. Both are copied to retries and requeues.

    `on_finish_cmd` / `on_error_cmd` (F12; None = none) are shell commands the daemon
    runs after the job's final outcome (see `check_hook`); copied to retries and requeues.

    `stop_signal` / `stop_grace_secs` (F11; None = SIGTERM / the daemon default of 5 s) are the
    signal that stops this job on `cancel` or timeout and the seconds it gets before SIGKILL
    (see `check_stop_signal`, `check_stop_grace`); copied to retries and requeues.

    Nothing is written if anything is rejected. `commit=False` leaves the
    insert in the open transaction so several jobs can be added all-or-nothing
    (the caller commits, or rolls back).

    `check_deadlock=False` skips the deadlock search (the dependency-behind check always runs).
    It is for a sweep that adds one chain of jobs with the same placement and dependencies: a
    cycle through any job of the chain also passes through the LAST one, so the caller checks only
    that one and the search runs once per sweep instead of once per job.
    """
    if before is not None and after is not None:
        raise ValueError("before and after are mutually exclusive")
    if on_fail not in ON_FAIL_MODES:
        raise ValueError(f"on_fail must be one of {', '.join(ON_FAIL_MODES)}")
    if retry_delay_secs is not None and not 0 < retry_delay_secs <= MAX_RETRY_DELAY_SECS:
        raise ValueError(f"retry delay must be between 1 second and {MAX_RETRY_DELAY_SECS // 86400} days")
    if retry_backoff and retry_delay_secs is None:
        raise ValueError("retry backoff needs a retry delay")
    on_finish_cmd = check_hook(on_finish_cmd, "on-finish command")
    on_error_cmd = check_hook(on_error_cmd, "on-error command")
    stop_signal = check_stop_signal(stop_signal)
    stop_grace_secs = check_stop_grace(stop_grace_secs)
    deps = deps or {}
    if is_priority and (deps or not_before):
        raise ValueError("--now cannot be combined with dependencies or a start time")
    for dep_id, mode in deps.items():
        if mode not in DEP_MODES:
            raise ValueError(f"dependency mode must be one of {', '.join(DEP_MODES)}")
        if get_job(conn, dep_id) is None:
            raise ValueError(f"dependency job {dep_id} does not exist")

    try:
        register_channel(conn, channel, commit=False)

        if before is not None or after is not None:
            target = before if before is not None else after
            assert target is not None
            if conn.execute(
                "SELECT 1 FROM jobs WHERE id = ? AND status = 'queued' AND channel = ? AND is_priority = ?",
                (target, channel, int(is_priority)),
            ).fetchone() is None:
                raise ValueError(f"job {target} is not currently queued on channel {channel}")
            position = _position_beside(conn, target, channel, is_priority, after=before is None)
        else:
            position = _next_position(conn, channel, is_priority)

        cur = conn.execute(
            """
            INSERT INTO jobs
                (channel, status, position, raw_cmd, resolved_cmd, cwd, env_extra,
                 enqueued_at, log_path, timeout_secs, is_priority, max_retries, attempt,
                 parent_id, origin, not_before, on_fail, retry_delay_secs, retry_backoff,
                 on_finish_cmd, on_error_cmd, stop_signal, stop_grace_secs)
            VALUES (?, 'queued', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                channel,
                position,
                json.dumps(raw_cmd),
                json.dumps(resolved_cmd),
                cwd,
                json.dumps(env_extra) if env_extra else None,
                _now(),
                log_path,
                timeout_secs,
                int(is_priority),
                max_retries,
                attempt,
                parent_id,
                origin,
                not_before,
                on_fail,
                retry_delay_secs,
                int(retry_backoff),
                on_finish_cmd,
                on_error_cmd,
                stop_signal,
                stop_grace_secs,
            ),
        )
        new_id = cur.lastrowid
        if new_id is None:  # an INSERT always sets it; this narrows the type and fails loudly
            raise RuntimeError("INSERT into jobs returned no rowid")

        if deps:
            for dep_id in deps:
                if _is_queued_behind(conn, dep_id, channel, is_priority, position):
                    raise ValueError(
                        f"job {dep_id} is queued behind this job on channel {channel}, so this job could "
                        f"never start (strict FIFO: a waiting job blocks the ones behind it). Queue it "
                        f"after job {dep_id} or on another channel"
                    )
            conn.executemany(
                "INSERT INTO job_deps (job_id, dep_id, mode) VALUES (?, ?, ?)",
                [(new_id, dep_id, mode) for dep_id, mode in deps.items()],
            )
        if check_deadlock and (deps or before is not None or after is not None):
            # appended without dependencies, nothing can wait on the new job, so no
            # cycle is possible; every other case is checked
            loop = _find_deadlock(conn, new_id)
            if loop:
                raise ValueError("this would deadlock: " + _describe_loop(loop))
    except BaseException:
        conn.rollback()  # an uncommitted half-insert must never be committed by a later call
        raise
    if commit:
        conn.commit()
    return new_id


def _find_deadlock(conn: sqlite3.Connection, job_id: int) -> list[tuple[int, str]] | None:
    """A cycle through `job_id` in the "waits for" graph, else None. A queued
    job waits for (a) each unfinished dependency and (b) the job ahead of it in its
    channel's normal lane (strict FIFO). Every edge a new job adds touches that
    job, so any new cycle passes through it. Returns [(job, why), ...]: each job
    reached on the way round, with the reason for the step into it; the last
    entry is `job_id` itself."""
    pred: dict[int, int] = {}
    last: dict[int, int] = {}
    for r in conn.execute(
        "SELECT id, channel FROM jobs WHERE status = 'queued' AND is_priority = 0 ORDER BY channel, position"
    ):
        if r["channel"] in last:
            pred[r["id"]] = last[r["channel"]]
        last[r["channel"]] = r["id"]
    deps: dict[int, list[int]] = {}
    for r in conn.execute(
        """
        SELECT d.job_id, d.dep_id FROM job_deps d JOIN jobs dj ON dj.id = d.dep_id
        WHERE dj.status IN ('queued', 'running', 'stopped')
        """
    ):
        deps.setdefault(r["job_id"], []).append(r["dep_id"])

    parent: dict[int, tuple[int, str]] = {}
    seen = {job_id}
    todo = [job_id]
    while todo:
        node = todo.pop()
        steps = [(d, "dependency") for d in deps.get(node, [])]
        if node in pred:
            steps.append((pred[node], "queue order"))
        for nxt, why in steps:
            if nxt == job_id:
                loop = [(job_id, why)]
                cur = node
                while cur != job_id:
                    prev, prev_why = parent[cur]
                    loop.append((cur, prev_why))
                    cur = prev
                loop.reverse()
                return loop
            if nxt not in seen:
                seen.add(nxt)
                parent[nxt] = (node, why)
                todo.append(nxt)
    return None


def _describe_loop(loop: list[tuple[int, str]]) -> str:
    parts = ["this job"]
    for node, why in loop:
        name = "this job" if node == loop[-1][0] else f"job {node}"
        parts.append(f"-> {name} ({why})")
    return " ".join(parts)


def get_queue(
    conn: sqlite3.Connection, channel: int = DEFAULT_CHANNEL, is_priority: bool = False
) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM jobs WHERE status = 'queued' AND channel = ? AND is_priority = ? ORDER BY position ASC",
        (channel, int(is_priority)),
    ).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_queue_ids(
    conn: sqlite3.Connection, channel: int = DEFAULT_CHANNEL, is_priority: bool = False
) -> list[int]:
    """Like `get_queue`, but only ids, in queue order (5.todo item 7) -- for callers
    that only need positions/order (e.g. `add --dry-run`'s position report), not
    raw_cmd/resolved_cmd/env_extra/on_finish_cmd/on_error_cmd or any other column."""
    rows = conn.execute(
        "SELECT id FROM jobs WHERE status = 'queued' AND channel = ? AND is_priority = ? ORDER BY position ASC",
        (channel, int(is_priority)),
    ).fetchall()
    return [r["id"] for r in rows]


def get_queue_head(
    conn: sqlite3.Connection, channel: int = DEFAULT_CHANNEL, is_priority: bool = False
) -> dict[str, Any] | None:
    """The first queued job (lowest `position`) on `channel`/`is_priority`, or None if
    the lane is empty. Equivalent to `get_queue(...)[0]` but reads one row instead of
    the whole lane -- use this wherever only the head is needed (e.g. the worker loops,
    which only ever act on `queue[0]`)."""
    row = conn.execute(
        "SELECT * FROM jobs WHERE status = 'queued' AND channel = ? AND is_priority = ? "
        "ORDER BY position ASC LIMIT 1",
        (channel, int(is_priority)),
    ).fetchone()
    return _row_to_dict(row) if row else None


def get_channel_counts(conn: sqlite3.Connection, channel: int = DEFAULT_CHANNEL) -> dict[str, int]:
    """Cheap one-query summary of `channel`'s activity: how many jobs are running,
    frozen (stopped by `--now`), queued and priority-queued. Used by `wait --channel`,
    which only needs to know whether the channel is idle, not the full per-job rows
    that `get_queue` / the `list` command return."""
    row = conn.execute(
        """
        SELECT
            SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running,
            SUM(CASE WHEN status = 'stopped' THEN 1 ELSE 0 END) AS frozen,
            SUM(CASE WHEN status = 'queued' AND is_priority = 0 THEN 1 ELSE 0 END) AS queued,
            SUM(CASE WHEN status = 'queued' AND is_priority = 1 THEN 1 ELSE 0 END) AS priority_queued
        FROM jobs WHERE channel = ?
        """,
        (channel,),
    ).fetchone()
    return {
        "running": row["running"] or 0,
        "frozen": row["frozen"] or 0,
        "queued": row["queued"] or 0,
        "priority_queued": row["priority_queued"] or 0,
    }


def get_running(conn: sqlite3.Connection, channel: int = DEFAULT_CHANNEL) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM jobs WHERE status = 'running' AND channel = ? LIMIT 1", (channel,)
    ).fetchone()
    return _row_to_dict(row) if row else None


def get_running_id(conn: sqlite3.Connection, channel: int = DEFAULT_CHANNEL) -> int | None:
    """Like `get_running`, but only the id (5.todo item 7) -- for callers that only
    need to know whether something is running and which job it is, not raw_cmd/
    resolved_cmd/env_extra/on_finish_cmd/on_error_cmd or any other column of the row."""
    row = conn.execute(
        "SELECT id FROM jobs WHERE status = 'running' AND channel = ? LIMIT 1", (channel,)
    ).fetchone()
    return row["id"] if row else None


def get_stopped(conn: sqlite3.Connection, channel: int = DEFAULT_CHANNEL) -> dict[str, Any] | None:
    """The job frozen (SIGSTOP'd) on this channel by a `--now` preemption,
    if any -- at most one per channel, since only a non-priority job can
    be frozen and a priority lane freezes it at most once."""
    row = conn.execute(
        "SELECT * FROM jobs WHERE status = 'stopped' AND channel = ? LIMIT 1", (channel,)
    ).fetchone()
    return _row_to_dict(row) if row else None


def get_stopped_id(conn: sqlite3.Connection, channel: int = DEFAULT_CHANNEL) -> int | None:
    """Like `get_stopped`, but only the id (5.todo item 7)."""
    row = conn.execute(
        "SELECT id FROM jobs WHERE status = 'stopped' AND channel = ? LIMIT 1", (channel,)
    ).fetchone()
    return row["id"] if row else None


def get_backlog(
    conn: sqlite3.Connection, channel: int = DEFAULT_CHANNEL, limit: int = LIST_BACKLOG
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT * FROM jobs
        WHERE status IN ('done', 'failed', 'cancelled', 'skipped', 'interrupted') AND channel = ?
        ORDER BY finished_at DESC, id DESC
        LIMIT ?
        """,
        (channel, limit),
    ).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_history(
    conn: sqlite3.Connection,
    *,
    statuses: list[str] | None = None,
    channel: int | None = None,
    since: str | None = None,
    grep: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Finished jobs, newest first, filtered. `statuses` (terminal ones only; None = all),
    `channel`, `since` (an ISO timestamp: finished at or after it), `grep` (case-insensitive
    substring of the raw command, its words joined by spaces), at most `limit` rows."""
    for status in statuses or []:
        if status not in TERMINAL_STATUSES:
            raise ValueError(f"unknown status {status!r}; use one of {', '.join(TERMINAL_STATUSES)}")
    if limit < 1:
        raise ValueError("limit must be at least 1")
    wanted = list(statuses) if statuses else list(TERMINAL_STATUSES)
    # only "?" placeholders are interpolated; every value below is bound as a parameter
    sql = f"SELECT * FROM jobs WHERE status IN ({','.join('?' * len(wanted))})"  # nosec B608
    params: list[Any] = list(wanted)
    if channel is not None:
        sql += " AND channel = ?"
        params.append(channel)
    if since is not None:
        sql += " AND finished_at >= ?"
        params.append(since)
    sql += " ORDER BY finished_at DESC, id DESC"
    needle = grep.lower() if grep else None
    out: list[dict[str, Any]] = []
    for row in conn.execute(sql, params):
        job = _row_to_dict(row)
        if needle is not None:
            try:
                words = json.loads(job["raw_cmd"])
            except ValueError:
                words = [job["raw_cmd"]]
            if needle not in " ".join(str(w) for w in words).lower():
                continue
        out.append(job)
        if len(out) >= limit:
            break
    return out


def get_export(
    conn: sqlite3.Connection, *, statuses: list[str] | None = None, channel: int | None = None
) -> list[dict[str, Any]]:
    """F24: every job row (queued, running, frozen and finished), oldest id first, for
    `qur export`. `statuses` (any job status; None = all) and `channel` filter it."""
    valid = ACTIVE_STATUSES + TERMINAL_STATUSES
    for status in statuses or []:
        if status not in valid:
            raise ValueError(f"unknown status {status!r}; use one of {', '.join(valid)}")
    # only "?" placeholders are interpolated; every value below is bound as a parameter
    sql = "SELECT * FROM jobs WHERE 1 = 1"
    params: list[Any] = []
    if statuses:
        sql += f" AND status IN ({','.join('?' * len(statuses))})"  # nosec B608
        params.extend(statuses)
    if channel is not None:
        sql += " AND channel = ?"
        params.append(channel)
    sql += " ORDER BY id ASC"
    return [_row_to_dict(r) for r in conn.execute(sql, params)]


def get_job(conn: sqlite3.Connection, job_id: int) -> dict[str, Any] | None:
    row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    return _row_to_dict(row) if row else None


STATE_COLUMNS = ("id", "status", "exit_code", "note", "attempt", "max_retries", "channel")
_STATE_CHUNK = 500  # ids per query: well under SQLite's bound-variable limit (999 on old builds)


def get_states(conn: sqlite3.Connection, job_ids: list[int]) -> dict[int, dict[str, Any]]:
    """Just the STATE_COLUMNS of every existing job in `job_ids`, keyed by id (an id with no
    row is absent). One query per 500 ids, no long text columns: this is what `qur wait JOBS`
    polls instead of a full `show` per job."""
    found: dict[int, dict[str, Any]] = {}
    columns = ", ".join(STATE_COLUMNS)
    for start in range(0, len(job_ids), _STATE_CHUNK):
        chunk = job_ids[start:start + _STATE_CHUNK]
        placeholders = ",".join("?" * len(chunk))
        # the columns are the fixed STATE_COLUMNS tuple and the ids are bound parameters
        rows = conn.execute(f"SELECT {columns} FROM jobs WHERE id IN ({placeholders})", chunk).fetchall()  # nosec B608
        for r in rows:
            found[r["id"]] = {k: r[k] for k in STATE_COLUMNS}
    return found


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
    conn.execute("DELETE FROM edit_locks WHERE job_id = ?", (job_id,))
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
    max_rss_kb: int | None = None,
    cpu_user_secs: float | None = None,
    cpu_sys_secs: float | None = None,
) -> None:
    if status not in TERMINAL_STATUSES:
        raise ValueError(f"invalid terminal status: {status}")
    # existence check + channel lookup + update in one round trip (was get_job + UPDATE)
    row = conn.execute(
        """
        UPDATE jobs
        SET status = ?, exit_code = ?, note = ?, finished_at = ?,
            max_rss_kb = ?, cpu_user_secs = ?, cpu_sys_secs = ?
        WHERE id = ?
        RETURNING channel
        """,
        (status, exit_code, note, _now(), max_rss_kb, cpu_user_secs, cpu_sys_secs, job_id),
    ).fetchone()
    if row is None:
        raise ValueError(f"job {job_id} does not exist")
    conn.commit()
    _prune_backlog(conn, row["channel"], keep=keep_backlog)


def _drop_orphan_deps(conn: sqlite3.Connection) -> None:
    """Forget dependency rows whose job is gone. A row whose DEPENDENCY is gone is
    kept while its job is still queued, so that job is skipped ("dependency
    removed") instead of silently losing the condition and running."""
    conn.execute(
        """
        DELETE FROM job_deps
        WHERE job_id NOT IN (SELECT id FROM jobs)
           OR (dep_id NOT IN (SELECT id FROM jobs)
               AND job_id NOT IN (SELECT id FROM jobs WHERE status = 'queued'))
        """
    )


def _prune_backlog(conn: sqlite3.Connection, channel: int, keep: int = 10) -> None:
    """Delete finished-job DB rows beyond the most recent `keep` (the daemon passes
    DEFAULT_KEEP or QUEUER_KEEP; the bare default is 10), scoped to
    one channel. Log files on disk are left untouched -- only the row is
    pruned. A finished job a still-queued job depends on is kept (it may push
    the history past `keep` until that job has run or been skipped)."""
    # OFFSET keep skips the rows we're keeping, so only the stale ids come back
    # (was: fetch every finished id, then slice rows[keep:] in Python).
    rows = conn.execute(
        """
        SELECT id FROM jobs
        WHERE status IN ('done', 'failed', 'cancelled', 'skipped', 'interrupted') AND channel = ?
          AND id NOT IN (SELECT d.dep_id FROM job_deps d JOIN jobs w ON w.id = d.job_id
                         WHERE w.status = 'queued')
        ORDER BY finished_at DESC, id DESC
        LIMIT -1 OFFSET ?
        """,
        (channel, keep),
    ).fetchall()
    stale_ids = [r["id"] for r in rows]
    if stale_ids:
        conn.executemany("DELETE FROM jobs WHERE id = ?", [(i,) for i in stale_ids])
        _drop_orphan_deps(conn)
        conn.commit()


def clear_backlog(conn: sqlite3.Connection, channel: int | None = None) -> int:
    """Delete all finished (done/failed/cancelled/skipped/interrupted) job rows
    -- the history shown by `list`/`status`, and nothing else. Queued, running,
    and stopped (frozen by `--now`) jobs are never touched by this, nor is a
    finished job that a still-queued job depends on. Log files on disk are left
    alone (see `clean-logs` for those). Returns the number of rows deleted.

    `channel=None` clears every channel's backlog; passing a channel
    number scopes it to just that one.
    """
    if channel is None:
        cur = conn.execute(
            """
            DELETE FROM jobs
            WHERE status IN ('done', 'failed', 'cancelled', 'skipped', 'interrupted')
              AND id NOT IN (SELECT d.dep_id FROM job_deps d JOIN jobs w ON w.id = d.job_id
                             WHERE w.status = 'queued')
            """
        )
    else:
        cur = conn.execute(
            """
            DELETE FROM jobs
            WHERE status IN ('done', 'failed', 'cancelled', 'skipped', 'interrupted') AND channel = ?
              AND id NOT IN (SELECT d.dep_id FROM job_deps d JOIN jobs w ON w.id = d.job_id
                             WHERE w.status = 'queued')
            """,
            (channel,),
        )
    _drop_orphan_deps(conn)
    # Drop registry rows for channels (other than the default) that now have
    # no jobs at all, so `list`/`status` stop showing empty channels. L2: and their pause
    # flag, or `qur add -c N` months later would create the channel again, silently paused.
    gone = [
        r["id"]
        for r in conn.execute(
            "SELECT id FROM channels WHERE id != ? AND id NOT IN (SELECT DISTINCT channel FROM jobs)",
            (DEFAULT_CHANNEL,),
        ).fetchall()
    ]
    for channel_id in gone:
        conn.execute("DELETE FROM channels WHERE id = ?", (channel_id,))
        conn.execute("DELETE FROM daemon_state WHERE key = ?", (f"paused:{channel_id}",))
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
    dependants = queued_dependants(conn, job_id)
    if dependants:
        raise ValueError(
            f"job {job_id} is a dependency of queued job(s) {', '.join(str(i) for i in dependants)}; "
            "remove or cancel those first (or `qur cancel` this one, which skips them)"
        )
    conn.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
    conn.execute("DELETE FROM job_deps WHERE job_id = ? OR dep_id = ?", (job_id, job_id))
    conn.execute("DELETE FROM edit_locks WHERE job_id = ?", (job_id,))  # rm wins over an open editor
    conn.commit()


def _end_queued(conn: sqlite3.Connection, job_id: int, status: str, note: str, keep_backlog: int = 10) -> None:
    """Move a job that has not started straight to a terminal state (kept in
    history). The gap it leaves in the queue needs no closing (positions only order). Works on both lanes."""
    job = get_job(conn, job_id)
    if job is None:
        raise ValueError(f"job {job_id} does not exist")
    if job["status"] != "queued":
        raise ValueError(f"job {job_id} is not queued (status={job['status']!r})")
    conn.execute("UPDATE jobs SET position = NULL WHERE id = ?", (job_id,))
    conn.execute("DELETE FROM edit_locks WHERE job_id = ?", (job_id,))  # cancel wins over an open editor
    conn.commit()
    mark_finished(conn, job_id, status=status, note=note, keep_backlog=keep_backlog)


def cancel_queued(conn: sqlite3.Connection, job_id: int, keep_backlog: int = 10) -> None:
    """Cancel a job that has not started: it stays in history as
    `cancelled` (unlike remove_from_queue, which deletes it). Works on both
    the normal and the priority lane."""
    _end_queued(conn, job_id, "cancelled", "cancelled while queued", keep_backlog)


def skip_queued(conn: sqlite3.Connection, job_id: int, note: str, keep_backlog: int = 10) -> None:
    """A queued job that can no longer run (a dependency failed): it stays in
    history as `skipped` with `note` saying why."""
    _end_queued(conn, job_id, "skipped", note, keep_backlog)


# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------


def queued_dependants(conn: sqlite3.Connection, job_id: int) -> list[int]:
    """Ids of still-queued jobs that depend on `job_id` (either mode), ascending."""
    rows = conn.execute(
        """
        SELECT d.job_id FROM job_deps d JOIN jobs w ON w.id = d.job_id
        WHERE d.dep_id = ? AND w.status = 'queued' ORDER BY d.job_id
        """,
        (job_id,),
    ).fetchall()
    return [r["job_id"] for r in rows]


def dep_info(conn: sqlite3.Connection, job_id: int) -> dict[str, list[int]]:
    """A job's dependencies for display: `depends_on` (--depends-on ids),
    `after_any` (--after-any ids) and `waiting_on` (those of either kind that
    have not ended yet)."""
    rows = conn.execute(
        """
        SELECT d.dep_id, d.mode, j.status AS dep_status
        FROM job_deps d LEFT JOIN jobs j ON j.id = d.dep_id
        WHERE d.job_id = ? ORDER BY d.dep_id
        """,
        (job_id,),
    ).fetchall()
    return {
        "depends_on": [r["dep_id"] for r in rows if r["mode"] == "success"],
        "after_any": [r["dep_id"] for r in rows if r["mode"] == "any"],
        "waiting_on": [r["dep_id"] for r in rows if r["dep_status"] in ACTIVE_STATUSES],
    }


def dep_info_bulk(conn: sqlite3.Connection, job_ids: list[int]) -> dict[int, dict[str, list[int]]]:
    """`dep_info` for every id in `job_ids` in one query instead of one per job (used by
    `qur list`, which otherwise re-queries per queued job -- N+1 queries on a big queue).
    Every id in `job_ids` gets an entry (empty lists if it has no dependencies), same as
    calling `dep_info` on it alone."""
    result: dict[int, dict[str, list[int]]] = {
        job_id: {"depends_on": [], "after_any": [], "waiting_on": []} for job_id in job_ids
    }
    if not job_ids:
        return result
    placeholders = ",".join("?" * len(job_ids))
    rows = conn.execute(
        f"""
        SELECT d.job_id, d.dep_id, d.mode, j.status AS dep_status
        FROM job_deps d LEFT JOIN jobs j ON j.id = d.dep_id
        WHERE d.job_id IN ({placeholders}) ORDER BY d.job_id, d.dep_id
        """,  # nosec B608 - `placeholders` is only "?,?,..." built from len(job_ids); the ids are bound parameters
        job_ids,
    ).fetchall()
    for r in rows:
        info = result[r["job_id"]]
        if r["mode"] == "success":
            info["depends_on"].append(r["dep_id"])
        elif r["mode"] == "any":
            info["after_any"].append(r["dep_id"])
        if r["dep_status"] in ACTIVE_STATUSES:
            info["waiting_on"].append(r["dep_id"])
    return result


def dep_verdict(conn: sqlite3.Connection, job_id: int) -> tuple[str, int | None]:
    """Whether a job's dependencies let it run: ("ready", None) if it has none
    or they all ended as required; ("waiting", id) if one has not ended yet;
    ("failed", id) if a `--depends-on` job ended in anything but `done`;
    ("removed", id) if a dependency's row no longer exists. "failed" and
    "removed" win over "waiting": the job can never run."""
    rows = conn.execute(
        """
        SELECT d.dep_id, d.mode, j.status AS dep_status
        FROM job_deps d LEFT JOIN jobs j ON j.id = d.dep_id
        WHERE d.job_id = ? ORDER BY d.dep_id
        """,
        (job_id,),
    ).fetchall()
    waiting: int | None = None
    for r in rows:
        st = r["dep_status"]
        if st is None:
            return "removed", r["dep_id"]
        if st in TERMINAL_STATUSES:
            if r["mode"] == "success" and st != "done":
                return "failed", r["dep_id"]
        elif waiting is None:
            waiting = r["dep_id"]
    return ("waiting", waiting) if waiting is not None else ("ready", None)


def skip_dependants(
    conn: sqlite3.Connection, job_id: int, keep_backlog: int = 10
) -> tuple[list[int], set[int]]:
    """`job_id` just reached a terminal state. Skip every still-queued job that
    needs it to have succeeded (`--depends-on`, and it did not end `done`), and
    cascade: a skipped job is itself a failed dependency of whatever needed it.
    Returns (ids skipped, channels holding any queued job that depends on a job
    touched here -- their queue head may be free to start now)."""
    skipped: list[int] = []
    channels: set[int] = set()
    todo = [job_id]
    while todo:
        dep_id = todo.pop()
        dep = get_job(conn, dep_id)
        if dep is None:
            continue
        rows = conn.execute("SELECT job_id, mode FROM job_deps WHERE dep_id = ? ORDER BY job_id", (dep_id,)).fetchall()
        for r in rows:
            waiter = get_job(conn, r["job_id"])
            if waiter is None or waiter["status"] != "queued":
                continue
            channels.add(waiter["channel"])
            if r["mode"] == "success" and dep["status"] != "done":
                skip_queued(conn, waiter["id"], f"dependency {dep_id} failed", keep_backlog)
                skipped.append(waiter["id"])
                todo.append(waiter["id"])
    return skipped, channels


# ---------------------------------------------------------------------------
# Edit lock + `qur edit`
# ---------------------------------------------------------------------------


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)  # signal 0: only asks whether the process exists
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # exists, belongs to someone else
    except (OSError, OverflowError, ValueError):  # H2: a pid past the C int range raises OverflowError, not OSError
        return False
    return True


def live_edit_lock(conn: sqlite3.Connection, job_id: int) -> dict[str, Any] | None:
    """The edit lock on `job_id` if there is one that still counts, else None. A lock
    whose holder process is gone, or that is past its expiry, is stale: it is deleted
    here, so a crashed editor can never hold a channel for ever."""
    row = conn.execute("SELECT * FROM edit_locks WHERE job_id = ?", (job_id,)).fetchone()
    if row is None:
        return None
    lock = _row_to_dict(row)
    try:
        expired = datetime.fromisoformat(lock["expires_at"]) <= datetime.now(timezone.utc)
    except ValueError:
        expired = True
    if expired or not _pid_alive(lock["holder_pid"]):
        conn.execute("DELETE FROM edit_locks WHERE job_id = ?", (job_id,))
        conn.commit()
        return None
    return lock


def clear_edit_locks(conn: sqlite3.Connection) -> int:
    """Forget every edit lock (daemon start: the editors talked to the previous daemon)."""
    cur = conn.execute("DELETE FROM edit_locks")
    conn.commit()
    return cur.rowcount


def _editable_job(conn: sqlite3.Connection, job_id: int) -> dict[str, Any]:
    job = get_job(conn, job_id)
    if job is None:
        raise ValueError(f"job {job_id} does not exist")
    if job["status"] != "queued":
        raise ValueError(f"job {job_id} is {job['status']}, not queued: only queued jobs can be edited")
    if job["is_priority"]:
        raise ValueError(f"job {job_id} is a --now job: it starts at once and cannot be edited")
    return job


def take_edit_lock(conn: sqlite3.Connection, job_id: int, holder_pid: int, secs: int = EDIT_LOCK_SECS) -> dict[str, Any]:
    """Lock a queued job for editing: while the lock is live the daemon does not start
    it (strict FIFO: its channel waits). Re-taking your own lock renews it; a live lock
    of another process is refused. Returns the lock row."""
    _editable_job(conn, job_id)
    held = live_edit_lock(conn, job_id)
    if held is not None and held["holder_pid"] != holder_pid:
        raise ValueError(f"job {job_id} is already being edited (process {held['holder_pid']})")
    now = datetime.now(timezone.utc)
    conn.execute(
        "INSERT OR REPLACE INTO edit_locks (job_id, holder_pid, taken_at, expires_at) VALUES (?, ?, ?, ?)",
        (job_id, holder_pid, now.isoformat(), (now + timedelta(seconds=secs)).isoformat()),
    )
    conn.commit()
    lock = live_edit_lock(conn, job_id)
    if lock is None:
        raise ValueError(f"could not lock job {job_id} (is process {holder_pid} running?)")
    return lock


def release_edit_lock(conn: sqlite3.Connection, job_id: int, holder_pid: int | None = None) -> bool:
    """Drop the lock on `job_id` (only the holder's, when `holder_pid` is given).
    True if a lock was removed."""
    if holder_pid is None:
        cur = conn.execute("DELETE FROM edit_locks WHERE job_id = ?", (job_id,))
    else:
        cur = conn.execute("DELETE FROM edit_locks WHERE job_id = ? AND holder_pid = ?", (job_id, holder_pid))
    conn.commit()
    return cur.rowcount > 0


# columns `edit_job` may set: a fixed whitelist, so the SET clause below is never built from input
_EDITABLE_COLUMNS = (
    "raw_cmd", "resolved_cmd", "cwd", "env_extra", "timeout_secs", "max_retries", "on_fail",
    "not_before", "retry_delay_secs", "retry_backoff", "on_finish_cmd", "on_error_cmd",
    "stop_signal", "stop_grace_secs",
)


def edit_job(
    conn: sqlite3.Connection,
    job_id: int,
    *,
    updates: dict[str, Any],
    deps: dict[int, str] | None = None,
    channel: int | None = None,
    holder_pid: int | None = None,
) -> dict[str, Any]:
    """Change a queued job, all or nothing. `updates` maps column names (a subset of
    _EDITABLE_COLUMNS; values already in their stored form) to new values; `deps` (None =
    unchanged) replaces the job's whole dependency set; `channel` moves it to the END of
    another channel's queue. The dependency/deadlock rules are those of `enqueue`.

    `holder_pid` is the editor holding the job's edit lock: the lock must still be live
    and theirs, and it is released with the edit. Without it the job must not be locked
    by someone else. Returns the new row. Nothing is written if anything is rejected."""
    unknown = sorted(set(updates) - set(_EDITABLE_COLUMNS))
    if unknown:
        raise ValueError(f"cannot edit: {', '.join(unknown)}")
    job = _editable_job(conn, job_id)
    held = live_edit_lock(conn, job_id)
    if holder_pid is None:
        if held is not None:
            raise ValueError(f"job {job_id} is being edited by process {held['holder_pid']}")
    elif held is None or held["holder_pid"] != holder_pid:
        raise ValueError(f"the edit lock on job {job_id} was lost (expired, or the daemon restarted): edit again")
    if "on_fail" in updates and updates["on_fail"] not in ON_FAIL_MODES:
        raise ValueError(f"on_fail must be one of {', '.join(ON_FAIL_MODES)}")
    updates = dict(updates)
    for hook_column, hook_label in (("on_finish_cmd", "on-finish command"), ("on_error_cmd", "on-error command")):
        if hook_column in updates:
            updates[hook_column] = check_hook(updates[hook_column], hook_label)
    if "stop_signal" in updates:
        updates["stop_signal"] = check_stop_signal(updates["stop_signal"])
    if "stop_grace_secs" in updates:
        updates["stop_grace_secs"] = check_stop_grace(updates["stop_grace_secs"])
    deps = dict(deps) if deps is not None else None
    if deps is not None:
        for dep_id, mode in deps.items():
            if mode not in DEP_MODES:
                raise ValueError(f"dependency mode must be one of {', '.join(DEP_MODES)}")
            if dep_id == job_id:
                raise ValueError("a job cannot depend on itself")
            if get_job(conn, dep_id) is None:
                raise ValueError(f"dependency job {dep_id} does not exist")
    try:
        for column, value in updates.items():
            # column comes from the fixed _EDITABLE_COLUMNS whitelist checked above; the value is bound
            conn.execute(f"UPDATE jobs SET {column} = ? WHERE id = ?", (value, job_id))  # nosec B608
        old_channel = job["channel"]
        if channel is not None and channel != old_channel:
            register_channel(conn, channel, commit=False)
            conn.execute(
                "UPDATE jobs SET channel = ?, position = ? WHERE id = ?",
                (channel, _next_position(conn, channel, False), job_id),
            )
        if deps is not None:
            conn.execute("DELETE FROM job_deps WHERE job_id = ?", (job_id,))
            conn.executemany(
                "INSERT INTO job_deps (job_id, dep_id, mode) VALUES (?, ?, ?)",
                [(job_id, dep_id, mode) for dep_id, mode in deps.items()],
            )
        if deps or channel is not None:
            lane_channel = channel if channel is not None else old_channel
            here = get_job(conn, job_id)
            assert here is not None and here["position"] is not None
            for dep_id in deps or {}:
                if _is_queued_behind(conn, dep_id, lane_channel, False, here["position"]):
                    raise ValueError(
                        f"job {dep_id} is queued behind this job on channel {job['channel'] if channel is None else channel}, "
                        f"so this job could never start (strict FIFO: a waiting job blocks the ones behind it)"
                    )
            loop = _find_deadlock(conn, job_id)
            if loop:
                raise ValueError("this would deadlock: " + _describe_loop(loop))
        conn.execute("DELETE FROM edit_locks WHERE job_id = ?", (job_id,))
    except BaseException:
        conn.rollback()
        raise
    conn.commit()
    new = get_job(conn, job_id)
    assert new is not None
    return new


def list_active(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Every job marked running or frozen (stopped), on any channel."""
    rows = conn.execute("SELECT * FROM jobs WHERE status IN ('running', 'stopped') ORDER BY id").fetchall()
    return [_row_to_dict(r) for r in rows]


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
    channel, lane = job["channel"], bool(job["is_priority"])
    ids = set(_queued_ids_ordered(conn, channel, lane))
    if job_id not in ids:
        raise ValueError(f"job {job_id} is not queued")
    if target_id not in ids:
        raise ValueError(f"job {target_id} is not queued")
    new_position = _position_beside(conn, target_id, channel, lane, after=after, exclude_id=job_id)
    conn.execute("UPDATE jobs SET position = ? WHERE id = ?", (new_position, job_id))
    if not job["is_priority"]:
        # moving a job can put it ahead of something it waits for (or behind something
        # that waits for it): that would hold its channel forever. Every new edge
        # touches the moved job, so a cycle through it is the only way this can happen.
        loop = _find_deadlock(conn, job_id)
        if loop:
            conn.rollback()
            raise ValueError("this would deadlock: " + _describe_loop(loop))
    conn.commit()


def requeue(conn: sqlite3.Connection, job_id: int, *, log_path: str, auto: bool = False) -> int:
    """Re-enqueue a finished job (any terminal state) with its original argv/
    cwd/env/timeout/channel/max_retries/on_fail/retry delay/hooks, appended to the end
    of its channel's queue. Dependencies and a start time are NOT copied: the copy
    runs when it reaches the front. Returns the new job's id.

    `auto=False` is a manual requeue: origin 'requeue', fresh retry count
    (attempt 0). `auto=True` is the daemon retrying a failure: origin
    'retry', attempt = old attempt + 1, and the copy may not start before
    now + the job's retry delay (doubled per earlier attempt with backoff,
    capped at MAX_RETRY_DELAY_SECS). A manual requeue is never delayed.
    Either way parent_id is `job_id`."""
    job = get_job(conn, job_id)
    if job is None:
        raise ValueError(f"job {job_id} does not exist")
    if job["status"] not in TERMINAL_STATUSES:
        raise ValueError(
            f"job {job_id} has status {job['status']!r}; "
            "only finished jobs can be requeued"
        )
    not_before: str | None = None
    delay = job["retry_delay_secs"]
    if auto and delay:
        if job["retry_backoff"]:
            delay = delay * 2 ** min(job["attempt"], 40)
        delay = min(delay, MAX_RETRY_DELAY_SECS)
        not_before = (datetime.now(timezone.utc) + timedelta(seconds=delay)).isoformat()
    return enqueue(
        conn,
        raw_cmd=json.loads(job["raw_cmd"]),
        resolved_cmd=json.loads(job["resolved_cmd"]),
        cwd=job["cwd"],
        log_path=log_path,
        channel=job["channel"],
        env_extra=json.loads(job["env_extra"]) if job["env_extra"] else None,
        timeout_secs=job["timeout_secs"],
        max_retries=job["max_retries"],
        on_fail=job["on_fail"],
        retry_delay_secs=job["retry_delay_secs"],
        retry_backoff=bool(job["retry_backoff"]),
        on_finish_cmd=job["on_finish_cmd"],
        on_error_cmd=job["on_error_cmd"],
        stop_signal=job["stop_signal"],
        stop_grace_secs=job["stop_grace_secs"],
        not_before=not_before,
        attempt=job["attempt"] + 1 if auto else 0,
        parent_id=job_id,
        origin="retry" if auto else "requeue",
    )


def recent_requeue(conn: sqlite3.Connection, job_id: int, within_secs: float) -> int | None:
    """Id of the newest manual requeue of `job_id` made within the last
    `within_secs` seconds, else None. Lets the Telegram button treat a double
    tap as one request. Auto-retries do not count."""
    row = conn.execute(
        "SELECT id, enqueued_at FROM jobs WHERE parent_id = ? AND origin = 'requeue' ORDER BY id DESC LIMIT 1",
        (job_id,),
    ).fetchone()
    if row is None:
        return None
    age = (datetime.now(timezone.utc) - datetime.fromisoformat(row["enqueued_at"])).total_seconds()
    return row["id"] if age <= within_secs else None


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


def get_state(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute("SELECT value FROM daemon_state WHERE key = ?", (key,)).fetchone()
    return row["value"] if row is not None else None


def set_state(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        """
        INSERT INTO daemon_state (key, value) VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (key, value),
    )
    conn.commit()
