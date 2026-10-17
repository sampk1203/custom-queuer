"""Log cleaning (F14): which job logs may go, by age and by total size.

Shared by `qur clean-logs` (by hand) and the daemon's timer (`QUEUER_LOG_MAX_AGE_DAYS`,
`QUEUER_LOG_MAX_MB`, `QUEUER_LOG_CLEAN_HOURS`). Only files named `<job id>.log` are ever
considered. The logs of queued, running and frozen jobs are never deleted (they still count
towards the total size). A log whose job row is gone (pruned from the history) is an orphan;
its age is the file's modification time.
"""

from __future__ import annotations

import sqlite3
import time
from datetime import datetime
from pathlib import Path

from queuer import db


def _finished_ts(finished_at: str) -> float:
    return datetime.fromisoformat(finished_at).timestamp()


def plan(
    conn: sqlite3.Connection,
    log_dir: Path,
    *,
    max_age_days: float | None = None,
    max_bytes: int | None = None,
    now: float | None = None,
) -> list[tuple[Path, int]]:
    """The logs to delete as (path, size), oldest first. Nothing is deleted here.

    Age: a log is selected when its job finished (or, for an orphan, the file was last
    written) more than `max_age_days` ago. Size: if the logs left after that still add up to
    more than `max_bytes`, the oldest deletable ones are selected too until they fit. Logs of
    unfinished jobs are never selected, so the total can stay above the limit.
    """
    if now is None:
        now = time.time()
    cutoff = now - max_age_days * 86400 if max_age_days is not None else None
    sizes: dict[Path, int] = {}
    deletable: list[tuple[float, Path]] = []  # (age timestamp, file)
    for log_file in log_dir.glob("*.log"):
        try:
            job_id = int(log_file.stem)
            st = log_file.stat()
        except (ValueError, OSError):
            continue  # not a job log (or gone meanwhile): leave it alone
        sizes[log_file] = st.st_size
        job = db.get_job(conn, job_id)
        if job is None:
            deletable.append((st.st_mtime, log_file))
        elif job["status"] in db.TERMINAL_STATUSES and job["finished_at"]:
            deletable.append((_finished_ts(job["finished_at"]), log_file))
    deletable.sort(key=lambda item: (item[0], str(item[1])))

    selected: list[Path] = []
    if cutoff is not None:
        selected = [f for ts, f in deletable if ts < cutoff]
    if max_bytes is not None:
        chosen = set(selected)
        total = sum(size for f, size in sizes.items() if f not in chosen)
        for _, f in deletable:
            if total <= max_bytes:
                break
            if f not in chosen:
                chosen.add(f)
                total -= sizes[f]
        selected = [f for _, f in deletable if f in chosen]
    return [(f, sizes[f]) for f in selected]


def delete(files: list[tuple[Path, int]]) -> tuple[int, int]:
    """Unlink the planned files; returns (files deleted, bytes freed). A file that is already
    gone is not an error; any other OSError propagates after the rest were tried."""
    count = freed = 0
    first_error: OSError | None = None
    for f, size in files:
        try:
            f.unlink()
        except FileNotFoundError:
            continue
        except OSError as e:
            first_error = first_error or e
            continue
        count += 1
        freed += size
    if first_error is not None:
        raise first_error
    return count, freed
