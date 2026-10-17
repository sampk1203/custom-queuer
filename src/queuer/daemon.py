"""queuerd: the background daemon that owns the job queue and runs jobs.

Listens on a Unix domain socket for newline-delimited JSON requests. All
state mutation happens here -- the CLI is a thin client that only ever
talks to this process.

Channels: each channel is an independent serial queue with its own worker
loop (own current-job slot, own pause flag, own wake event) -- channels
run in parallel with each other, jobs within one channel still run one
at a time. A channel's worker loop is spawned lazily: on daemon startup
for any channel that already has rows in the DB, and on first `add` to a
brand-new channel.

Run standalone for manual testing with: `python -m queuer.daemon`
Normally launched by the systemd --user unit (Phase 4).
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import shlex
import signal
import socket
import sqlite3
import stat
import struct
import sys
import time
import traceback
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from queuer import db, jobenv, logclean, notify
from queuer.pathresolve import resolve_command

MAX_LOG_BYTES = 50 * 1024 * 1024  # 50MB per-job log cap
DEFAULT_GRACE_SECS = 5
LISTEN_IDLE_SECS = 5  # recheck config when Telegram listening is off
LISTEN_GAP_SECS = 0.1  # pause between polls
CLIENT_READ_TIMEOUT_SECS = 10  # a client must send its request line within this
LISTEN_RETRY_SECS = 30  # back off after a failed poll (offline, 409, ...)
IDLE_POLL_SECS = 30  # idle worker safety-net recheck; every real change wakes the worker at once
MAX_CLIENTS = 32  # socket connections served at the same time; more get "too many connections"
TG_STALE_SECS = 300  # offline longer than this -> button presses that piled up meanwhile are dropped
TG_ALIVE_SAVE_SECS = 60  # how often "the bot was polled fine just now" is written to the DB
REQUEUE_GUARD_SECS = 30  # a second Telegram Requeue of the same job within this is a double tap
# Shutdown budget (install.sh sets TimeoutStopSec=30): job kill grace (DEFAULT_GRACE_SECS, 5 s,
# all jobs in parallel) + workers recording the result (<= SHUTDOWN_WAIT_SECS) + the
# "interrupted" Telegram messages going out (<= SHUTDOWN_NOTIFY_SECS) stays well under it.
SHUTDOWN_WAIT_SECS = 8
SHUTDOWN_NOTIFY_SECS = 8
EDIT_LOCK_RECHECK_SECS = 5  # a worker whose head job is locked for editing rechecks this often (a dead editor frees it)
STOP_RACE_SECS = 1.0  # systemd signals jobs and daemon together: how long a signal-death waits for our own SIGTERM
LOG_CLEAN_FIRST_SECS = 60  # first automatic log clean this long after the daemon starts
LOG_CLEAN_DEFAULT_HOURS = 6.0  # then every this often (QUEUER_LOG_CLEAN_HOURS)
LOG_CLEAN_MIN_SECS = 60.0  # an interval from the environment is never shorter than this
HOOK_TIMEOUT_SECS = 300  # F12: a hook still running after this long is killed (it never holds a channel)
HOOK_OUTPUT_BYTES = 65536  # F12: most hook output appended to the job's log; the rest is dropped
POST_JOB_HOOK_ENV = "QUEUER_POST_JOB_HOOK"  # F12: global hook command, set in the daemon's environment
HOOK_STATUSES = ("done", "failed", "cancelled")  # outcomes that run hooks (not skipped, not interrupted)
STOP_NOTE = "daemon stopped"  # note of a job the daemon's stop (or a crash/reboot) cut short
RESOURCE_SAMPLE_SECS = 0.2  # F10: how often /proc is polled for a running job's peak RSS / CPU time
_CLK_TCK = os.sysconf("SC_CLK_TCK") if hasattr(os, "sysconf") else 100  # ticks/sec for /proc/<pid>/stat
# exit codes of a job that died to SIGHUP/SIGINT/SIGKILL/SIGTERM: negative = killed by the signal
# itself, 128+n = a shell/wrapper that reported it
_SIGNAL_EXITS = frozenset({-1, -2, -9, -15, 129, 130, 137, 143})


def _default_db_path() -> Path:
    override = os.environ.get("QUEUER_DB_PATH")
    return Path(override) if override else db.DEFAULT_DB_PATH


def _default_keep() -> int:
    """Finished jobs kept per channel: `QUEUER_KEEP` (a positive integer) or db.DEFAULT_KEEP.
    A bad value is ignored with a warning rather than stopping the daemon."""
    raw = os.environ.get("QUEUER_KEEP")
    if raw is None or not raw.strip():
        return db.DEFAULT_KEEP
    try:
        keep = int(raw)
    except ValueError:
        keep = 0
    if keep < 1:
        print(f"[queuerd] ignoring QUEUER_KEEP={raw!r} (need a positive integer); keeping {db.DEFAULT_KEEP}", file=sys.stderr)
        return db.DEFAULT_KEEP
    return keep


def _env_positive_int(name: str) -> int | None:
    """A positive integer from the environment, or None if unset/empty. A bad value is
    ignored with a warning rather than stopping the daemon."""
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return None
    try:
        value = int(raw)
    except ValueError:
        value = 0
    if value < 1:
        print(f"[queuerd] ignoring {name}={raw!r} (need a positive integer)", file=sys.stderr)
        return None
    return value


def _read_proc_usage(pid: int) -> tuple[int | None, float, float] | None:
    """F10: one best-effort snapshot of a running process's own (not its
    children's) resource use, read straight from /proc -- no os.wait4 /
    RUSAGE_CHILDREN involved, so it works per-job even with several channels
    reaping children at the same time. Returns (peak_rss_kb, user_secs,
    sys_secs), or None if `pid` is gone (already exited, or not Linux).
    peak_rss_kb is None if /proc/<pid>/status has no VmHWM line. user_secs /
    sys_secs are cumulative since the process started, so the latest sample is
    the final value; VmHWM is already a kernel-maintained high-water mark, so
    the latest sample is already the peak -- no running-max needed here."""
    try:
        with open(f"/proc/{pid}/stat", "rb") as f:
            stat_line = f.read().decode("utf-8", errors="replace")
    except OSError:
        return None
    # comm (field 2) is in parens and may itself contain spaces/parens/newlines;
    # split after the LAST ')' so the numeric fields can't be confused with it
    rparen = stat_line.rfind(")")
    if rparen == -1:
        return None
    fields = stat_line[rparen + 2:].split()
    try:
        utime_ticks = int(fields[11])  # field 14 overall: utime
        stime_ticks = int(fields[12])  # field 15 overall: stime
    except (IndexError, ValueError):
        return None
    user_secs = utime_ticks / _CLK_TCK
    sys_secs = stime_ticks / _CLK_TCK
    rss_kb: int | None = None
    try:
        with open(f"/proc/{pid}/status", "rb") as f:
            for line in f:
                if line.startswith(b"VmHWM:"):
                    parts = line.split()
                    if len(parts) >= 2:
                        try:
                            rss_kb = int(parts[1])
                        except ValueError:
                            rss_kb = None
                    break
    except OSError:
        pass
    return rss_kb, user_secs, sys_secs


def _default_log_clean_secs() -> float:
    """Seconds between automatic log cleans: `QUEUER_LOG_CLEAN_HOURS` (a positive number,
    at least one minute) or LOG_CLEAN_DEFAULT_HOURS."""
    raw = os.environ.get("QUEUER_LOG_CLEAN_HOURS")
    default = LOG_CLEAN_DEFAULT_HOURS * 3600
    if raw is None or not raw.strip():
        return default
    try:
        hours = float(raw)
    except ValueError:
        hours = 0.0
    if not math.isfinite(hours) or hours <= 0:
        print(f"[queuerd] ignoring QUEUER_LOG_CLEAN_HOURS={raw!r} (need a positive number); using {LOG_CLEAN_DEFAULT_HOURS:g}", file=sys.stderr)
        return default
    return max(LOG_CLEAN_MIN_SECS, hours * 3600)


def _default_socket_path() -> Path:
    override = os.environ.get("QUEUER_SOCKET_PATH")
    if override:
        return Path(override)
    runtime_dir = os.environ.get("XDG_RUNTIME_DIR")  # unset or empty -> private fallback
    return (Path(runtime_dir) if runtime_dir else _fallback_socket_dir()) / "queuer.sock"


def _fallback_socket_dir() -> Path:
    """Per-user dir used when XDG_RUNTIME_DIR is unset (cron, sudo, ...). Never
    a bare /tmp: another user could pre-create the socket path there."""
    return Path("/tmp") / f"queuer-{os.getuid()}"  # nosec B108 - uid-scoped, ownership/mode/symlink checked by the caller


def _ensure_private_dir(path: Path) -> None:
    """Create `path` (0700), or accept an existing one only if it is a real
    directory (not a symlink) owned by us; loose modes are tightened. Anything
    else could be a trap planted by another user, so refuse."""
    try:
        path.mkdir(mode=0o700)
    except FileExistsError:
        pass
    st = os.lstat(path)
    if not stat.S_ISDIR(st.st_mode) or st.st_uid != os.getuid():
        raise RuntimeError(f"refusing to use {path}: not a directory owned by you")
    if st.st_mode & 0o077:
        os.chmod(path, 0o700)


def _default_log_dir() -> Path:
    override = os.environ.get("QUEUER_LOG_DIR")
    if override:
        return Path(override)
    return Path.home() / ".local" / "state" / "queuer" / "logs"


class AlreadyRunning(RuntimeError):
    """Another queuerd already answers on the socket."""


def _socket_answers(path: Path) -> bool:
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(1)
    try:
        s.connect(str(path))
        return True
    except OSError:  # missing, refused (stale file) or not ours
        return False
    finally:
        s.close()


def _tighten_or_warn(path: Path, created: bool, ours_by_default: bool) -> None:
    """State folders: 0700 when we made them or they are queuer's own default
    location. A folder the user pointed QUEUER_* at is never chmod'ed (it may be
    $HOME); its files are still 0600, so we only warn if it is open to others."""
    if created or ours_by_default:
        os.chmod(path, 0o700)
    elif path.stat().st_mode & 0o077:
        print(f"[queuerd] warning: {path} is accessible to other users (files inside are 0600; "
              f"chmod 700 it if it is dedicated to queuer)", file=sys.stderr)


class Daemon:
    def __init__(
        self,
        db_path: Path | None = None,
        socket_path: Path | None = None,
        log_dir: Path | None = None,
        notify_path: Path | None = None,
        keep: int | None = None,
        log_max_age_days: int | None = None,
        log_max_mb: int | None = None,
        log_clean_secs: float | None = None,
        post_job_hook: str | None = None,
    ) -> None:
        self.keep = keep if keep is not None else _default_keep()  # finished jobs kept per channel
        # F14 automatic log cleaning: off unless an age and/or a size limit is set
        self.log_max_age_days = log_max_age_days if log_max_age_days is not None else _env_positive_int("QUEUER_LOG_MAX_AGE_DAYS")
        self.log_max_mb = log_max_mb if log_max_mb is not None else _env_positive_int("QUEUER_LOG_MAX_MB")
        self.log_clean_secs = log_clean_secs if log_clean_secs is not None else _default_log_clean_secs()
        # F12 global post-job hook: a shell command run after every job's final outcome
        self.post_job_hook: str | None = None
        try:
            self.post_job_hook = db.check_hook(post_job_hook if post_job_hook is not None else os.environ.get(POST_JOB_HOOK_ENV), POST_JOB_HOOK_ENV)
        except ValueError as e:
            print(f"[queuerd] ignoring {POST_JOB_HOOK_ENV}: {e}", file=sys.stderr)
        self.db_path = db_path or _default_db_path()
        self.socket_path = socket_path or _default_socket_path()
        self.log_dir = log_dir or _default_log_dir()
        self.notify_path = notify_path or notify.default_path()
        self.conn: sqlite3.Connection | None = None
        self._uid = os.getuid()  # the only user whose socket requests are served

        # per-channel: each channel gets its own wake event, its own
        # current-job slot, its own worker task.
        self._wake: dict[int, asyncio.Event] = {}
        self._current_proc: dict[int, asyncio.subprocess.Process] = {}
        self._current_job_id: dict[int, int] = {}
        self._worker_tasks: dict[int, asyncio.Task] = {}

        # ids of jobs a user asked to cancel while running. Per job, not per
        # channel: a frozen (--now) job and the priority job that froze it
        # share a channel, and cancelling one must not label the other.
        self._cancelled: set[int] = set()
        # ids of jobs being spawned right now (the process is being created): too late to edit them
        self._starting: set[int] = set()

        # in-flight notification sends (kept so they aren't garbage-collected)
        self._notify_tasks: set[asyncio.Task] = set()
        self._hook_tasks: set[asyncio.Task] = set()  # F12: hook runs in flight
        self._tg_offsets: dict[str, int] = {}  # bot token -> next getUpdates offset
        self._tg_alive: dict[str, float] = {}  # bot token -> time of its last good poll
        self._tg_alive_saved: dict[str, float] = {}  # bot token -> when that was last written to the DB
        self._clients = 0  # socket connections being served right now
        # set by SIGTERM/SIGINT (systemctl stop/restart, reboot): no new job starts, running
        # jobs are killed and recorded as `interrupted`, then the daemon exits
        self._stopping = asyncio.Event()
        self._seen_callbacks: list[str] = []

        # `--now` priority lane: at most one job can be frozen per channel
        # at a time (a frozen normal job, held here with its own proc
        # reference since self._current_proc gets reassigned to whatever
        # priority job is running in the foreground while it's frozen).
        # The priority worker task runs the priority queue FIFO, then
        # SIGCONTs the frozen job once that queue drains.
        self._frozen: dict[int, tuple[int, asyncio.subprocess.Process]] = {}
        self._priority_worker_tasks: dict[int, asyncio.Task] = {}

    # -- setup -----------------------------------------------------------

    def _connect(self) -> None:
        self.conn = db.connect(self.db_path)
        db.init_db(self.conn)
        _tighten_or_warn(self.db_path.parent, created=False, ours_by_default=False)  # warn only

    def _recover_on_startup(self) -> None:
        """A row still marked 'running' (or 'stopped', frozen by `--now`) from a
        previous daemon instance cannot actually still be running -- we just
        started. That daemon died without recording it (crash, SIGKILL, power
        loss, reboot): mark it `interrupted`, not `failed`, rather than silently
        losing it or re-running it. It is not retried and does not use up
        --retries. Checked across every channel that has ever been used."""
        assert self.conn is not None
        db.clear_edit_locks(self.conn)  # editors were talking to the previous daemon; a lost lock makes their edit fail cleanly
        for channel in db.list_channels(self.conn):
            for stale in (db.get_running(self.conn, channel), db.get_stopped(self.conn, channel)):
                if stale is not None:
                    db.mark_finished(self.conn, stale["id"], status="interrupted", note=STOP_NOTE, keep_backlog=self.keep)
                    self._settle_dependants(stale["id"])
                    self._notify_finished(stale["id"])

    def _ensure_channel_worker(self, channel: int) -> None:
        """Lazily spawn a channel's worker loop if it isn't running yet (never
        once the daemon is stopping: a new worker would start the next job)."""
        if self._stopping.is_set():
            return
        task = self._worker_tasks.get(channel)
        if task is not None and not task.done():
            return
        self._wake[channel] = asyncio.Event()
        self._worker_tasks[channel] = asyncio.create_task(self.worker_loop(channel))

    # -- worker loop -------------------------------------------------------

    async def worker_loop(self, channel: int) -> None:
        assert self.conn is not None
        wake = self._wake[channel]
        while True:
            wake.clear()
            if self._stopping.is_set():
                return
            sleep_for: float = IDLE_POLL_SECS
            if not db.get_paused(self.conn, channel):
                queue = db.get_queue(self.conn, channel)
                if queue:
                    # strict FIFO: only the head is ever considered. A head that waits on
                    # an unfinished dependency holds the whole channel (nothing jumps
                    # ahead); it is woken when that dependency ends (`_settle_dependants`)
                    verdict, dep_id = db.dep_verdict(self.conn, queue[0]["id"])
                    if verdict == "ready":
                        # a delayed head (`--delay` / `--at`) holds the channel until its
                        # time: sleep exactly that long (a wake event still ends the sleep
                        # early, e.g. the head was removed or cancelled)
                        remaining = self._secs_until(queue[0].get("not_before"))
                        if remaining <= 0 and db.live_edit_lock(self.conn, queue[0]["id"]) is not None:
                            # `qur edit` has the head open: strict FIFO, the channel waits until the
                            # edit is applied or abandoned (a stale lock is cleared by the check above)
                            sleep_for = EDIT_LOCK_RECHECK_SECS
                        elif remaining <= 0:
                            await self._run_job_safely(channel, queue[0])
                            continue
                        else:
                            sleep_for = min(remaining, IDLE_POLL_SECS)
                    elif verdict != "waiting" and dep_id is not None:
                        self._skip_blocked(queue[0]["id"], verdict, dep_id)
                        continue
            # idle: wait for something to wake us (new job, resume, retry, dependency
            # finished, ...), with a slow periodic recheck as a safety net. Sleeps are
            # capped at the recheck interval so a clock jump or a suspend cannot make a
            # delayed job late by more than that; the last sleep before not_before is
            # exactly the time that is left.
            try:
                await asyncio.wait_for(wake.wait(), timeout=sleep_for)
            except asyncio.TimeoutError:
                pass

    @staticmethod
    def _secs_until(not_before: str | None) -> float:
        """Seconds until the absolute time `not_before` (<= 0: already passed, or none
        set). An unreadable value counts as passed: a job must never be stuck behind
        a bad timestamp."""
        if not not_before:
            return 0.0
        try:
            when = datetime.fromisoformat(not_before)
        except ValueError:
            return 0.0
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone.utc)
        return (when - datetime.now(timezone.utc)).total_seconds()

    @staticmethod
    def _parse_not_before(value: Any) -> str | None:
        """The `not_before` of an `add` request as a normalised UTC ISO string (None if
        absent). It must be an ISO timestamp WITH a UTC offset: the client works out
        local times, the daemon only ever stores absolute ones."""
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError("not_before must be an ISO timestamp string")
        try:
            when = datetime.fromisoformat(value)
        except ValueError:
            raise ValueError(f"not_before is not an ISO timestamp: {value!r}") from None
        if when.tzinfo is None:
            raise ValueError("not_before must include a UTC offset")
        return when.astimezone(timezone.utc).isoformat()

    def _wake_worker(self, channel: int) -> None:
        self._ensure_channel_worker(channel)
        if channel in self._wake:
            self._wake[channel].set()

    # -- priority lane (`--now`) ------------------------------------------

    def _ensure_priority_worker(self, channel: int) -> None:
        """Spawn the priority-lane runner for a channel if it isn't
        already draining one. Idempotent -- additional `--now` jobs while
        one is already running just land in the DB queue and get picked
        up by the existing task's next loop iteration."""
        if self._stopping.is_set():
            return
        task = self._priority_worker_tasks.get(channel)
        if task is not None and not task.done():
            return
        self._priority_worker_tasks[channel] = asyncio.create_task(self._priority_worker_loop(channel))

    async def _priority_worker_loop(self, channel: int) -> None:
        assert self.conn is not None
        while not self._stopping.is_set():
            queue = db.get_queue(self.conn, channel, is_priority=True)
            if not queue:
                break
            await self._run_job_safely(channel, queue[0])
        if channel in self._frozen and not self._stopping.is_set():
            await self._resume_frozen(channel)

    def _freeze_current_if_normal(self, channel: int) -> None:
        """Called synchronously from `add --now`: if a normal (non-
        priority) job is currently running on this channel and nothing is
        already frozen there, SIGSTOP it and hand the channel over to the
        priority lane. No-op if the channel is idle (nothing to freeze --
        the priority job just runs like any other) or if a priority job
        is already in the foreground (the new one queues in FIFO behind
        it, no additional freeze needed)."""
        assert self.conn is not None
        if channel in self._frozen or self._stopping.is_set():
            return
        job_id = self._current_job_id.get(channel)
        proc = self._current_proc.get(channel)
        if job_id is None or proc is None:
            return
        job = db.get_job(self.conn, job_id)
        if job is None or bool(job["is_priority"]):
            return
        try:
            os.killpg(proc.pid, signal.SIGSTOP)
        except ProcessLookupError:
            return
        db.mark_stopped(self.conn, job_id)
        self._frozen[channel] = (job_id, proc)

    async def _resume_frozen(self, channel: int) -> None:
        assert self.conn is not None
        job_id, proc = self._frozen.pop(channel)
        try:
            os.killpg(proc.pid, signal.SIGCONT)
        except ProcessLookupError:
            # process died while frozen (shouldn't normally happen) -- its
            # own worker task's proc.wait() will still return and finalize
            # it; nothing more to do here.
            return
        db.mark_resumed(self.conn, job_id)
        self._current_proc[channel] = proc
        self._current_job_id[channel] = job_id

    async def _run_job_safely(self, channel: int, job: dict[str, Any]) -> None:
        """Run one job; whatever goes wrong, the job is marked failed and the
        channel's worker keeps going (one bad job must never stall a channel or
        take the daemon down)."""
        assert self.conn is not None
        try:
            await self._run_job(channel, job)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"[queuerd] job {job['id']} crashed the runner: {type(e).__name__}: {e}", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            try:
                current = db.get_job(self.conn, job["id"])
                if current is not None and current["status"] in ("queued", "running", "stopped"):
                    db.mark_finished(
                        self.conn, job["id"], status="failed", note=f"internal error: {type(e).__name__}: {e}",
                        keep_backlog=self.keep,
                    )
                self._release(channel, job["id"])
                self._after_finish(job["id"])
            except Exception as e2:  # last resort: never let the worker die
                print(f"[queuerd] could not record failure of job {job['id']}: {e2}", file=sys.stderr)

    async def _run_job(self, channel: int, job: dict[str, Any]) -> None:
        assert self.conn is not None
        job_id = job["id"]
        argv = json.loads(job["resolved_cmd"])
        cwd = job["cwd"]
        env_extra = json.loads(job["env_extra"]) if job["env_extra"] else {}
        env = {"PATH": os.environ.get("PATH", ""), "HOME": os.environ.get("HOME", "")}
        env.update(jobenv.job_environment(env_extra))
        log_path = Path(job["log_path"])
        log_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if self._stopping.is_set():
            return  # the job stays queued; the next daemon runs it

        self._starting.add(job_id)  # `qur edit` is refused from here until the row says running
        try:
            try:
                proc = await asyncio.create_subprocess_exec(
                    *argv,
                    cwd=cwd,
                    env=env,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.STDOUT,
                    start_new_session=True,  # own process group -> group-kill later
                )
            except (OSError, ValueError, TypeError) as e:
                db.mark_finished(self.conn, job_id, status="failed", note=f"failed to start: {e}", keep_backlog=self.keep)
                self._after_finish(job_id)
                return

            db.mark_running(self.conn, job_id, pid=proc.pid, pgid=proc.pid)
        finally:
            self._starting.discard(job_id)
        self._current_proc[channel] = proc
        self._current_job_id[channel] = job_id

        log_task = asyncio.create_task(self._pump_log(proc, log_path))
        # F10: peak RSS / user+system CPU time, sampled from /proc while the job runs
        # (see _read_proc_usage -- not os.wait4/RUSAGE_CHILDREN, which asyncio's own
        # child-reaping already forecloses and which can't be diffed cleanly across
        # channels running in parallel anyway).
        usage: dict[str, Any] = {"rss_kb": None, "cpu_user": None, "cpu_sys": None}
        usage_task = asyncio.create_task(self._sample_usage(proc.pid, usage))
        timeout = job["timeout_secs"]
        if self._stopping.is_set():
            # the stop began while the process was being spawned, so _shutdown did not see it
            await self._kill_group_for_shutdown(proc)

        try:
            if timeout:
                exit_code = await self._wait_running_time(proc, channel, job_id, timeout)
            else:
                exit_code = await proc.wait()
        except asyncio.TimeoutError:
            await self._kill_process_group(proc)
            exit_code = await proc.wait()
            await log_task
            await self._stop_usage_sampling(usage_task)
            db.mark_finished(
                self.conn, job_id, status="failed", exit_code=exit_code, note="timed out", keep_backlog=self.keep,
                max_rss_kb=usage["rss_kb"], cpu_user_secs=usage["cpu_user"], cpu_sys_secs=usage["cpu_sys"],
            )
            self._cancelled.discard(job_id)
            self._release(channel, job_id)
            self._after_finish(job_id)
            return

        await log_task
        await self._stop_usage_sampling(usage_task)

        note: str | None = None
        if job_id in self._cancelled:
            status = "cancelled"
            self._cancelled.discard(job_id)
        elif await self._cut_short_by_stop(exit_code):
            status = "interrupted"  # not "failed": the job did nothing wrong; never auto-retried
            note = STOP_NOTE
        else:
            status = "done" if exit_code == 0 else "failed"
        db.mark_finished(
            self.conn, job_id, status=status, exit_code=exit_code, note=note, keep_backlog=self.keep,
            max_rss_kb=usage["rss_kb"], cpu_user_secs=usage["cpu_user"], cpu_sys_secs=usage["cpu_sys"],
        )
        self._release(channel, job_id)
        self._after_finish(job_id)

    @staticmethod
    async def _stop_usage_sampling(usage_task: asyncio.Task[None]) -> None:
        usage_task.cancel()
        try:
            await usage_task
        except asyncio.CancelledError:
            pass

    async def _cut_short_by_stop(self, exit_code: int) -> bool:
        """True if the daemon's stop ended this job. Under systemd's default
        KillMode the job gets SIGTERM in the same instant as the daemon, so the job's
        exit can be seen a moment BEFORE our own signal handler has run: a job that
        died to a signal waits briefly for the stop flag before it is called failed."""
        if exit_code == 0:
            return False  # it finished by itself (or handled the TERM and finished cleanly): done
        if self._stopping.is_set():
            return True
        if exit_code not in _SIGNAL_EXITS:
            return False
        try:
            await asyncio.wait_for(self._stopping.wait(), timeout=STOP_RACE_SECS)
        except asyncio.TimeoutError:
            return False
        return True

    async def _wait_running_time(self, proc: asyncio.subprocess.Process, channel: int, job_id: int, timeout: float) -> int:
        """Wait for `proc`, enforcing `timeout` seconds of RUN time: time spent
        frozen (SIGSTOP by a `--now` job) does not count. Raises
        asyncio.TimeoutError like wait_for would."""
        waiter = asyncio.ensure_future(proc.wait())
        ran = 0.0
        tick = 0.2
        try:
            while True:
                done, _ = await asyncio.wait({waiter}, timeout=tick)
                if done:
                    return waiter.result()
                frozen = self._frozen.get(channel)
                if frozen is not None and frozen[0] == job_id:
                    continue
                ran += tick
                if ran >= timeout:
                    raise asyncio.TimeoutError
        finally:
            if not waiter.done():
                waiter.cancel()

    def _release(self, channel: int, job_id: int) -> None:
        """Clear a channel's current-job slot, but only if it still points at
        `job_id`. A frozen job that is cancelled finishes while the priority
        job that froze it owns the slot -- that slot must be left alone."""
        if self._current_job_id.get(channel) == job_id:
            self._current_proc.pop(channel, None)
            self._current_job_id.pop(channel, None)

    def _after_finish(self, job_id: int) -> None:
        """A job just reached a terminal state: retry it if it has retries
        left, otherwise tell the user. A failure that was requeued stays
        silent -- you hear about the final outcome only."""
        self._settle_dependants(job_id)
        if not self._maybe_auto_retry(job_id):
            self._notify_finished(job_id)
            self._pause_if_asked(job_id)
            self._start_hooks(job_id)

    def _hooks_for(self, job: dict[str, Any]) -> list[tuple[str, str]]:
        """F12: the (kind, command) hooks to run for a job that just reached its FINAL
        outcome, in order: the global post-job hook, `--on-finish`, then `--on-error`
        (failed only). Only a job that ran and ended done, failed or cancelled has hooks:
        skipped jobs never ran, interrupted ones were cut short by a daemon stop, and a
        job cancelled while still queued never started."""
        if job["status"] not in HOOK_STATUSES or (job["status"] == "cancelled" and not job["started_at"]):
            return []
        hooks: list[tuple[str, str]] = []
        if self.post_job_hook:
            hooks.append(("post-job", self.post_job_hook))
        if job.get("on_finish_cmd"):
            hooks.append(("on-finish", job["on_finish_cmd"]))
        if job["status"] == "failed" and job.get("on_error_cmd"):
            hooks.append(("on-error", job["on_error_cmd"]))
        return hooks

    def _start_hooks(self, job_id: int) -> None:
        """Run the job's hooks in the background (one task, hooks one after another): a
        slow or hung hook never holds the channel; each is killed after HOOK_TIMEOUT_SECS."""
        assert self.conn is not None
        job = db.get_job(self.conn, job_id)
        if job is None:
            return
        hooks = self._hooks_for(job)
        if not hooks:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        task = loop.create_task(self._run_hooks(job, hooks))
        self._hook_tasks.add(task)
        task.add_done_callback(self._hook_tasks.discard)

    @staticmethod
    def _hook_env(kind: str, job: dict[str, Any]) -> dict[str, str]:
        """The job's own environment plus QUR_* facts about how it ended."""
        env = {"PATH": os.environ.get("PATH", ""), "HOME": os.environ.get("HOME", "")}
        env.update(jobenv.job_environment(json.loads(job["env_extra"]) if job["env_extra"] else {}))
        duration = ""
        if job["started_at"] and job["finished_at"]:
            secs = (datetime.fromisoformat(job["finished_at"]) - datetime.fromisoformat(job["started_at"])).total_seconds()
            duration = str(int(max(secs, 0)))
        env.update({
            "QUR_HOOK": kind,
            "QUR_JOB_ID": str(job["id"]),
            "QUR_STATUS": job["status"],
            "QUR_EXIT_CODE": "" if job["exit_code"] is None else str(job["exit_code"]),
            "QUR_CHANNEL": str(job["channel"]),
            "QUR_CWD": job["cwd"],
            "QUR_LOG": job["log_path"],
            "QUR_COMMAND": shlex.join(json.loads(job["raw_cmd"])),
            "QUR_DURATION_SECS": duration,
        })
        return env

    @staticmethod
    def _append_to_log(log_path: str, text: str) -> None:
        """Append to the job's log (0600, never through a symlink); a failure only warns."""
        try:
            fd = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, "O_NOFOLLOW", 0), 0o600)
            with os.fdopen(fd, "ab") as f:
                f.write(text.encode("utf-8", "replace"))
        except OSError as e:
            print(f"[queuerd] cannot write to log {log_path}: {e}", file=sys.stderr)

    async def _run_hooks(self, job: dict[str, Any], hooks: list[tuple[str, str]]) -> None:
        for kind, command in hooks:
            try:
                await self._run_hook(kind, command, job)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # a broken hook must never reach the daemon
                print(f"[queuerd] job {job['id']} hook {kind} crashed: {type(e).__name__}: {e}", file=sys.stderr)

    async def _run_hook(self, kind: str, command: str, job: dict[str, Any]) -> None:
        """Run one hook through `/bin/sh -c` in the job's cwd, with the job's environment
        and QUR_* variables. Its output (capped) and exit code are appended to the job's
        log, so `qur logs` shows what the hook did; a non-zero exit also goes to the journal."""
        log_path = job["log_path"]
        self._append_to_log(log_path, f"\n[queuer] hook {kind}: {command}\n")
        try:
            proc = await asyncio.create_subprocess_exec(
                "/bin/sh", "-c", command,
                cwd=job["cwd"],
                env=self._hook_env(kind, job),
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                start_new_session=True,
            )
        except (OSError, ValueError) as e:
            self._append_to_log(log_path, f"[queuer] hook {kind} could not start: {e}\n")
            print(f"[queuerd] job {job['id']} hook {kind} could not start: {e}", file=sys.stderr)
            return
        output = b""
        outcome = ""
        try:
            output = await asyncio.wait_for(self._drain_hook(proc), timeout=HOOK_TIMEOUT_SECS)
            code = await proc.wait()
            outcome = f"exited {code}"
            if code != 0:
                print(f"[queuerd] job {job['id']} hook {kind} exited {code}", file=sys.stderr)
        except asyncio.TimeoutError:
            await self._kill_process_group(proc)
            await proc.wait()
            outcome = f"killed after {HOOK_TIMEOUT_SECS}s (timed out)"
            print(f"[queuerd] job {job['id']} hook {kind} timed out", file=sys.stderr)
        except asyncio.CancelledError:  # the daemon is stopping: do not leave the hook behind
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            raise
        if output:
            self._append_to_log(log_path, output.decode("utf-8", "replace"))
            if not output.endswith(b"\n"):
                self._append_to_log(log_path, "\n")
        self._append_to_log(log_path, f"[queuer] hook {kind} {outcome}\n")

    @staticmethod
    async def _drain_hook(proc: asyncio.subprocess.Process) -> bytes:
        """Read a hook's combined output to the end, keeping at most HOOK_OUTPUT_BYTES."""
        assert proc.stdout is not None
        kept = b""
        while True:
            chunk = await proc.stdout.read(65536)
            if not chunk:
                return kept
            if len(kept) < HOOK_OUTPUT_BYTES:
                kept += chunk[: HOOK_OUTPUT_BYTES - len(kept)]

    def _pause_if_asked(self, job_id: int) -> None:
        """`--on-fail pause`: a job that ended `failed` (not cancelled, skipped or
        interrupted) pauses its channel -- the queued jobs behind it stay queued until
        `qur resume`. Only called for a FINAL failure: a failure that `--retries`
        requeued does not pause, the retry would just sit there."""
        assert self.conn is not None
        job = db.get_job(self.conn, job_id)
        if job is None or job["status"] != "failed" or job["on_fail"] != "pause":
            return
        db.set_paused(self.conn, True, job["channel"])
        failed_job: dict[str, Any] = job
        self._notify_text(lambda bcfg: notify.format_paused(failed_job, bcfg))

    def _settle_dependants(self, job_id: int) -> None:
        """`job_id` just reached a terminal state: skip every queued job that needed
        it to succeed (cascading to their own dependants), tell the user about each,
        and wake every channel holding a job that waited on it -- its head may be free
        to start now. A failed job that is auto-retried still skips its dependants:
        the retry is a new job at the end of the queue, so waiting for it could
        deadlock a dependant that sits ahead of it."""
        assert self.conn is not None
        skipped, channels = db.skip_dependants(self.conn, job_id, self.keep)
        for skipped_id in skipped:
            self._notify_finished(skipped_id)
        for channel in channels:
            self._wake_worker(channel)

    def _skip_blocked(self, job_id: int, verdict: str, dep_id: int) -> None:
        """A queued job whose dependency can never be satisfied (it failed, or its
        row is gone): record it as `skipped`, then skip whatever needed it."""
        assert self.conn is not None
        what = "removed" if verdict == "removed" else "failed"
        db.skip_queued(self.conn, job_id, f"dependency {dep_id} {what}", self.keep)
        self._notify_finished(job_id)
        self._settle_dependants(job_id)

    def _notify_finished(self, job_id: int) -> None:
        """Fire-and-forget Telegram message for a finished job, sent through
        every connected bot. No-op if notifications aren't set up (`qur
        connect`). Config is re-read each time, so connecting needs no daemon
        restart."""
        assert self.conn is not None
        cfg = notify.load(self.notify_path)
        if cfg is None:
            return
        job = db.get_job(self.conn, job_id)
        if job is None:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        for bcfg in notify.bot_cfgs(cfg):
            if not bcfg["chat_ids"]:
                continue
            # the buttons only go out through bots this machine listens to
            markup = notify.job_markup(bcfg, job_id) if notify.listening(bcfg) else None
            task = loop.create_task(self._send_notification(bcfg, notify.format_job(job, bcfg), markup))
            self._notify_tasks.add(task)
            task.add_done_callback(self._notify_tasks.discard)

    def _notify_text(self, make_text: Callable[[dict[str, Any]], str]) -> None:
        """Fire-and-forget plain Telegram message through every connected bot
        (`make_text` gets each bot's flat config). No-op if notifications aren't set up."""
        cfg = notify.load(self.notify_path)
        if cfg is None:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        for bcfg in notify.bot_cfgs(cfg):
            if not bcfg["chat_ids"]:
                continue
            task = loop.create_task(self._send_notification(bcfg, make_text(bcfg)))
            self._notify_tasks.add(task)
            task.add_done_callback(self._notify_tasks.discard)

    async def _send_notification(self, cfg: dict[str, Any], text: str, markup: dict[str, Any] | None = None) -> None:
        """`cfg` is one bot's flat view. One task per bot, so a slow or
        failing bot never delays the others."""
        # urllib blocks, so run it off the event loop; and nothing that goes
        # wrong here may ever touch the queue.
        try:
            errors = await asyncio.to_thread(notify.send, cfg, text, markup)
        except Exception as e:
            errors = [f"{type(e).__name__}: {e}"]
        for err in errors:
            print(f"[queuerd] notification failed (bot {notify.bot_id(cfg['token'])}): {err}", file=sys.stderr)

    def _listening_bot(self, token: str) -> dict[str, Any] | None:
        """The current flat config of the bot with this token, if it should
        be polled right now (has chats, `listen` on, no pairing running)."""
        cfg = notify.load(self.notify_path)
        if cfg is None or notify.pairing_active(self.notify_path):
            return None
        for bcfg in notify.bot_cfgs(cfg):
            if bcfg["token"] == token:
                return bcfg if bcfg["chat_ids"] and notify.listening(bcfg) else None
        return None

    def _clean_logs_once(self) -> tuple[int, int]:
        """One automatic log clean (F14), with its own DB connection so it can run in a
        thread. Returns (logs deleted, bytes freed)."""
        conn = db.connect(self.db_path)
        try:
            files = logclean.plan(
                conn,
                self.log_dir,
                max_age_days=self.log_max_age_days,
                max_bytes=self.log_max_mb * 1024 * 1024 if self.log_max_mb is not None else None,
            )
        finally:
            conn.close()
        return logclean.delete(files)

    async def _log_cleaner(self) -> None:
        """Timer for automatic log cleaning. Does nothing at all unless an age or size limit
        is set. No error may escape: a failure is reported once and the timer carries on."""
        if self.log_max_age_days is None and self.log_max_mb is None:
            return
        delay: float = LOG_CLEAN_FIRST_SECS
        while True:
            await asyncio.sleep(delay)
            delay = self.log_clean_secs
            try:
                count, freed = await asyncio.to_thread(self._clean_logs_once)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # the timer must survive anything
                notify.warn_once(f"automatic log clean failed: {type(e).__name__}: {e}")
                continue
            if count:
                print(f"[queuerd] cleaned {count} log(s), {freed / 1024:.0f} KB", file=sys.stderr)

    async def _telegram_listener(self) -> None:
        """Supervisor: one poller task per connected bot that has `listen`
        on, started and stopped as the config changes (re-read every few
        seconds). Steps aside while `qur connect` pairs. No error may
        escape."""
        pollers: dict[str, asyncio.Task] = {}
        warned: str | None = None
        try:
            while True:
                try:
                    problem = notify.check_permissions(self.notify_path, fix=True)
                except OSError as e:
                    problem = f"cannot check {self.notify_path}: {e}"
                if problem and problem != warned:
                    print(f"[queuerd] warning: {problem}", file=sys.stderr)
                warned = problem
                cfg = notify.load(self.notify_path)
                wanted: set[str] = set()
                if cfg is not None and not notify.pairing_active(self.notify_path):
                    wanted = {b["token"] for b in notify.bot_cfgs(cfg) if b["chat_ids"] and notify.listening(b)}
                for token in list(pollers):
                    if token not in wanted or pollers[token].done():
                        pollers.pop(token).cancel()
                for token in wanted - set(pollers):
                    pollers[token] = asyncio.create_task(self._poll_bot(token))
                await asyncio.sleep(LISTEN_IDLE_SECS)
        finally:
            for task in pollers.values():
                task.cancel()

    async def _poll_bot(self, token: str) -> None:
        """Poll one bot for Requeue/Details presses until it is removed,
        stops listening, or pairing starts (the supervisor restarts it).
        Nothing here may touch the queue except an explicit requeue, and no
        error may escape."""
        # kept across restarts of this poller (e.g. a pairing pause) so a
        # press that was already handled is never handed to us again
        offset: int | None = self._tg_offsets.get(token)
        if offset is None and self.conn is not None:
            # survives daemon restarts, so a press that was already handled is never replayed
            saved = db.get_state(self.conn, f"tg_offset:{notify.bot_id(token)}")
            if saved is not None and saved.isdecimal():
                offset = self._tg_offsets[token] = int(saved)
        # Telegram stamps no time on a button press, so a press that piled up while
        # this daemon was off cannot be told from a fresh one. After a long outage
        # (or a long pairing pause) everything waiting is therefore dropped, not run.
        last_alive = self._tg_last_alive(token)
        draining = last_alive is not None and time.time() - last_alive > TG_STALE_SECS
        last_err: str | None = None
        while True:
            cfg = self._listening_bot(token)
            if cfg is None:
                return
            try:
                if draining:
                    updates = await asyncio.to_thread(notify.get_updates, cfg, offset, 0)  # no waiting
                else:
                    updates = await asyncio.to_thread(notify.get_updates, cfg, offset)
                last_err = None
            except Exception as e:
                err = f"{type(e).__name__}: {e}"
                if err != last_err:  # don't spam the journal while offline
                    print(f"[queuerd] telegram listener (bot {notify.bot_id(token)}): {err}", file=sys.stderr)
                    last_err = err
                await asyncio.sleep(LISTEN_RETRY_SECS)
                continue
            self._tg_mark_alive(token)
            for update in updates:
                offset = self._tg_offsets[token] = update["update_id"] + 1
                if self.conn is not None:
                    db.set_state(self.conn, f"tg_offset:{notify.bot_id(token)}", str(offset))
                try:
                    if draining:
                        await self._drop_stale_update(cfg, update)
                    else:
                        await self._handle_update(cfg, update)
                except Exception as e:
                    print(f"[queuerd] telegram update failed: {type(e).__name__}: {e}", file=sys.stderr)
            if draining and not updates:
                draining = False  # backlog gone: from now on presses are fresh
            await asyncio.sleep(LISTEN_GAP_SECS)  # never hot-loop on instant empty replies

    def _tg_last_alive(self, token: str) -> float | None:
        """When this bot was last polled fine (memory first, then the DB), or None
        if never -- e.g. the first start after an upgrade."""
        seen = self._tg_alive.get(token)
        if seen is not None:
            return seen
        if self.conn is not None:
            saved = db.get_state(self.conn, f"tg_alive:{notify.bot_id(token)}")
            if saved is not None and saved.isdecimal():
                return float(saved)
        return None

    def _tg_mark_alive(self, token: str) -> None:
        now = time.time()
        self._tg_alive[token] = now
        if self.conn is not None and now - self._tg_alive_saved.get(token, 0.0) >= TG_ALIVE_SAVE_SECS:
            db.set_state(self.conn, f"tg_alive:{notify.bot_id(token)}", str(int(now)))
            self._tg_alive_saved[token] = now

    async def _drop_stale_update(self, cfg: dict[str, Any], update: dict[str, Any]) -> None:
        """A press or message that waited while the daemon was off: never acted on.
        A paired chat's button press gets a toast (best effort; Telegram may
        already have expired it) so the user knows to press again."""
        cq = update.get("callback_query")
        if cq and (cq.get("from") or {}).get("id") in cfg["chat_ids"]:
            await asyncio.to_thread(notify.answer_callback, cfg, cq["id"], "queuer was offline - press again")

    async def _handle_update(self, cfg: dict[str, Any], update: dict[str, Any]) -> None:
        """A button press that arrived on the bot `cfg` (its flat view): an
        inline Requeue/Details/Logs button, or a Status/List menu key. Only
        presses from a chat paired with that bot count, and replies go out
        through that same bot."""
        if update.get("message"):
            await self._handle_menu(cfg, update["message"])
            return
        cq = update.get("callback_query")
        if not cq:
            return
        msg = cq.get("message") or {}
        chat_id = (msg.get("chat") or {}).get("id")
        parsed = notify.parse_callback(cq.get("data"))
        if parsed is None:
            return
        if chat_id is None:  # Telegram hides messages that are too old: still stop the button spinner
            if (cq.get("from") or {}).get("id") in cfg["chat_ids"]:
                await asyncio.to_thread(notify.answer_callback, cfg, cq["id"], "message too old - use the Status/List keys")
            return
        if chat_id not in cfg["chat_ids"]:
            return
        if self._seen_callback(cfg, cq.get("id")):  # a double tap or a replayed update
            return
        action, qur_id, job_id = parsed
        reply = lambda text: asyncio.to_thread(notify.answer_callback, cfg, cq["id"], text)  # noqa: E731
        if qur_id != cfg["qur_id"]:
            await reply("this message came from another machine or an old id")
            return
        to_chat = {**cfg, "chat_ids": [chat_id]}

        if action == "lc":  # job_id slot carries the channel here
            data = await self._cmd_list({})
            if job_id not in data["channels"]:
                await reply(f"channel {job_id} has no jobs")
                return
            await reply("")
            markup = notify.list_markup(cfg, data["channels"][job_id])
            await asyncio.to_thread(notify.send, to_chat, notify.format_list(data, cfg, job_id), markup, "HTML")
            return

        if action == "cn":  # always by id: same as `qur cancel <id>`, never "whatever runs on the channel"
            try:
                await self._cmd_cancel({"id": job_id})
            except ValueError as e:
                await reply(str(e))
                return
            await reply(f"cancelled job {job_id}")  # the job's own "cancelled" message follows
            spent = notify.without_button(msg.get("reply_markup"), cq["data"])
            await asyncio.to_thread(notify.edit_markup, cfg, chat_id, msg["message_id"], spent)
            return

        if action in ("sh", "lg"):
            assert self.conn is not None
            job = db.get_job(self.conn, job_id)
            if job is None:
                await reply(f"job {job_id} does not exist")
                return
            await reply("")
            if action == "sh":
                await asyncio.to_thread(notify.send, to_chat, notify.format_details(self._with_deps(job), cfg), None, "HTML")
            else:
                # path from our own log dir + the integer id, not the DB's log_path column
                tail = await asyncio.to_thread(notify.read_log_tail, self.log_dir / f"{job_id}.log")
                await asyncio.to_thread(notify.send, to_chat, notify.format_log(job, tail, cfg), None, "HTML")
            return

        assert self.conn is not None
        dup = db.recent_requeue(self.conn, job_id, REQUEUE_GUARD_SECS)
        if dup is not None:  # two quick taps reach us as two presses of the same old keyboard
            await reply(f"already requeued as job {dup}")
            return
        try:
            new_id = await self._requeue_job(job_id)
        except ValueError as e:
            await reply(str(e))
            return
        await reply(f"requeued as job {new_id}")
        if action == "rl":  # from a list: drop only this job's button
            spent = notify.without_button(msg.get("reply_markup"), cq["data"])
        else:  # from a finished-job message: Details and Logs stay
            spent = notify.details_markup(cfg, job_id)
        await asyncio.to_thread(notify.edit_markup, cfg, chat_id, msg["message_id"], spent)
        text = f"🔁 job {job_id} requeued as job {new_id}\n{notify._who(cfg)}"
        await asyncio.to_thread(notify.send, to_chat, text)

    def _seen_callback(self, cfg: dict[str, Any], cb_id: Any) -> bool:
        """True if this bot already handled this callback_query id (bounded memory;
        ids are only unique per bot)."""
        if cb_id is None:
            return False
        key = f"{notify.bot_id(cfg['token'])}:{cb_id}"
        if key in self._seen_callbacks:
            return True
        self._seen_callbacks.append(key)
        if len(self._seen_callbacks) > 200:
            del self._seen_callbacks[:100]
        return False

    async def _handle_menu(self, cfg: dict[str, Any], msg: dict[str, Any]) -> None:
        """Persistent Status / List keys (or /status, /list; /menu and /start
        bring the keys back). Read-only: never touches the queue."""
        chat = msg.get("chat") or {}
        if chat.get("id") not in cfg["chat_ids"]:
            return
        text = (msg.get("text") or "").strip()
        action = "menu" if text.startswith("/start") else notify.parse_menu(text)
        if action is None:
            return
        to_chat = {**cfg, "chat_ids": [chat["id"]]}
        if action == "menu":
            await asyncio.to_thread(notify.send, to_chat, notify.hello_text(cfg), notify.menu_markup())
            return
        if action == "status":
            data = await self._cmd_status({})
            await asyncio.to_thread(notify.send, to_chat, notify.format_status(data, cfg), None, "HTML")
            return
        data = await self._cmd_list({})
        if not data["channels"]:
            await asyncio.to_thread(notify.send, to_chat, "nothing has ever been queued")
            return
        await asyncio.to_thread(notify.send, to_chat, "📋 List: pick a channel", notify.list_picker_markup(cfg, sorted(data["channels"])))

    def _maybe_auto_retry(self, job_id: int) -> bool:
        """If a just-finished job failed and still has retries left,
        requeue it at the end of its channel's normal queue as a new job
        (same command/cwd/env/timeout/max_retries, attempt + 1). Cancelled
        and done jobs are never retried. The failed job's note records the
        retry so `show` explains where it went. Returns True if a retry was
        queued."""
        assert self.conn is not None
        job = db.get_job(self.conn, job_id)
        if job is None or job["status"] != "failed" or job["attempt"] >= job["max_retries"]:
            return False
        attempt = job["attempt"] + 1
        new_id = db.requeue(self.conn, job_id, log_path="pending", auto=True)
        log_path = str(self.log_dir / f"{new_id}.log")
        retry_note = f"auto-retry {attempt}/{job['max_retries']} queued as job {new_id}"
        note = f"{job['note']}; {retry_note}" if job["note"] else retry_note
        self.conn.execute("UPDATE jobs SET log_path = ? WHERE id = ?", (log_path, new_id))
        self.conn.execute("UPDATE jobs SET note = ? WHERE id = ?", (note, job_id))
        self.conn.commit()
        self._wake_worker(job["channel"])
        return True

    async def _pump_log(self, proc: asyncio.subprocess.Process, log_path: Path) -> None:
        """Stream the job's combined stdout/stderr to its log file, capped
        at MAX_LOG_BYTES with a single truncation marker line. If the log
        cannot be opened or written (disk full, ...) the output is dropped
        but still drained, so the job keeps running and the daemon stays up."""
        written = 0
        truncated = False
        f = None
        try:
            f = os.fdopen(os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_NOFOLLOW", 0), 0o600), "wb")
        except OSError as e:
            print(f"[queuerd] cannot open log {log_path}: {e}", file=sys.stderr)
        try:
            assert proc.stdout is not None
            while True:
                chunk = await proc.stdout.read(65536)
                if not chunk:
                    break
                if truncated or f is None:
                    continue
                try:
                    if written + len(chunk) > MAX_LOG_BYTES:
                        remaining = MAX_LOG_BYTES - written
                        if remaining > 0:
                            f.write(chunk[:remaining])
                        f.write(f"\n[queuer] log truncated at {MAX_LOG_BYTES} bytes\n".encode())
                        truncated = True
                        written = MAX_LOG_BYTES
                    else:
                        f.write(chunk)
                        written += len(chunk)
                    f.flush()  # so a running job's log can be read (qur logs --tail, Telegram Logs)
                except OSError as e:
                    print(f"[queuerd] cannot write log {log_path}: {e}; dropping the rest of its output", file=sys.stderr)
                    truncated = True
        finally:
            if f is not None:
                try:
                    f.close()
                except OSError as e:
                    print(f"[queuerd] cannot close log {log_path}: {e}", file=sys.stderr)

    async def _sample_usage(self, pid: int, usage: dict[str, Any]) -> None:
        """F10: poll `pid` (the job's direct process, not its whole tree --
        see _read_proc_usage) every RESOURCE_SAMPLE_SECS and keep the latest
        reading in `usage` (shared with the caller). Runs for as long as the
        job does; the caller cancels it once the job exits. A job that exits
        before the first poll leaves `usage` at its initial all-None state --
        there is nothing to report for it."""
        try:
            while True:
                sample = _read_proc_usage(pid)
                if sample is not None:
                    rss_kb, user_secs, sys_secs = sample
                    usage["rss_kb"] = rss_kb
                    usage["cpu_user"] = user_secs
                    usage["cpu_sys"] = sys_secs
                await asyncio.sleep(RESOURCE_SAMPLE_SECS)
        except asyncio.CancelledError:
            pass

    async def _kill_process_group(self, proc: asyncio.subprocess.Process, grace: float = DEFAULT_GRACE_SECS) -> None:
        """SIGTERM the job's whole process group, wait up to `grace`
        seconds for it to exit on its own, then SIGKILL if it's still
        alive. Used by both `cancel` and timeout enforcement."""
        pgid = proc.pid  # start_new_session=True makes pid == pgid (session leader)
        try:
            os.killpg(pgid, signal.SIGTERM)
        except ProcessLookupError:
            return
        try:
            await asyncio.wait_for(proc.wait(), timeout=grace)
            return
        except asyncio.TimeoutError:
            pass
        try:
            os.killpg(pgid, signal.SIGKILL)
        except ProcessLookupError:
            pass

    async def _kill_group_for_shutdown(self, proc: asyncio.subprocess.Process, grace: float | None = None) -> None:
        """SIGTERM the job's whole process group, give it `grace` seconds, then
        SIGKILL whatever is left. Unlike `_kill_process_group` this watches the
        GROUP, not just the main process: a backgrounded child that ignores SIGTERM
        would otherwise keep the log pipe (and so the job) open past our exit."""
        pgid = proc.pid
        try:
            os.killpg(pgid, signal.SIGTERM)
        except ProcessLookupError:
            return
        deadline = time.monotonic() + (DEFAULT_GRACE_SECS if grace is None else grace)
        while time.monotonic() < deadline:
            await asyncio.sleep(0.05)
            try:
                os.killpg(pgid, 0)  # signal 0: only asks whether the group still has members
            except ProcessLookupError:
                return
        try:
            os.killpg(pgid, signal.SIGKILL)
        except ProcessLookupError:
            pass

    async def _shutdown(self, server: asyncio.AbstractServer) -> None:
        """SIGTERM/SIGINT path (systemctl stop/restart, reboot). Order matters:
        1. stop taking requests and starting jobs (`_stopping`, wake every worker so
           idle ones return);
        2. kill every running/frozen job's process group (in parallel);
        3. wait for the workers: each records its job as `interrupted` (see
           `_cut_short_by_stop`) BEFORE we return, so the status is on disk before the
           process exits;
        4. let the "interrupted" Telegram messages go out.
        Every wait is bounded, so the whole thing fits in TimeoutStopSec=30."""
        self._stopping.set()
        server.close()
        for event in self._wake.values():
            event.set()
        procs: dict[int, asyncio.subprocess.Process] = {p.pid: p for p in self._current_proc.values()}
        for _job_id, proc in self._frozen.values():
            procs[proc.pid] = proc
            try:
                os.killpg(proc.pid, signal.SIGCONT)  # a stopped group does not act on SIGTERM
            except ProcessLookupError:
                pass
        if procs:
            await asyncio.gather(*(self._kill_group_for_shutdown(p) for p in procs.values()))
        tasks: set[asyncio.Task[Any]] = {
            t for t in (*self._worker_tasks.values(), *self._priority_worker_tasks.values()) if not t.done()
        }
        if tasks:
            await asyncio.wait(tasks, timeout=SHUTDOWN_WAIT_SECS)
            for t in tasks:
                if not t.done():
                    t.cancel()
        pending: set[asyncio.Task[Any]] = set(self._notify_tasks) | set(self._hook_tasks)
        if pending:
            await asyncio.wait(pending, timeout=SHUTDOWN_NOTIFY_SECS)
            for t in self._hook_tasks:  # a hook still running is killed (see _run_hook)
                if not t.done():
                    t.cancel()
            if self._hook_tasks:
                await asyncio.wait(set(self._hook_tasks), timeout=1)

    # -- request handlers --------------------------------------------------

    async def handle_request(self, req: dict[str, Any]) -> dict[str, Any]:
        assert self.conn is not None
        if not isinstance(req, dict):
            return {"ok": False, "error": "invalid request"}
        cmd = req.get("cmd")
        args = req.get("args", {})
        if not isinstance(args, dict):
            return {"ok": False, "error": "arguments must be an object"}
        if not isinstance(cmd, str) or cmd.startswith("_"):
            return {"ok": False, "error": f"unknown command: {cmd}"}
        handler = getattr(self, f"_cmd_{cmd}", None)
        if handler is None:
            return {"ok": False, "error": f"unknown command: {cmd}"}
        try:
            data = await handler(args)
            return {"ok": True, "data": data}
        except (ValueError, KeyError, TypeError) as e:
            return {"ok": False, "error": str(e)}

    async def _cmd_add(self, args: dict[str, Any]) -> dict[str, Any]:
        """Queue one job, or -- with `jobs` (a list of {argv, raw_argv, index}, from
        `--each` / `--array`) -- one independent job per entry, all-or-nothing, with
        the same channel/cwd/timeout/flags/dependencies. `index` becomes the job's
        QUR_INDEX env var. With `dry_run` (F16) everything is validated and placed
        exactly as for a real add, then rolled back: nothing is queued and no worker
        is woken; the reply describes what would happen."""
        # `raw_argv` is exactly what the user typed after `--`, with no
        # interpreter-pinning or bash-wrapping applied -- kept separate from
        # `argv` (which is what's actually resolved/exec'd) so `raw command`
        # in show/list reflects what was typed. Older clients that don't
        # send it fall back to `argv`, same as before this field existed.
        jobs_arg = args.get("jobs")
        specs: list[dict[str, Any]]
        if jobs_arg is None:
            specs = [{"argv": args["argv"], "raw_argv": args.get("raw_argv"), "index": None}]
        else:
            if not isinstance(jobs_arg, list) or not jobs_arg or not all(isinstance(j, dict) for j in jobs_arg):
                raise ValueError("jobs must be a non-empty list of objects")
            if len(jobs_arg) > db.MAX_SWEEP:
                raise ValueError(f"too many jobs: at most {db.MAX_SWEEP} per add")
            specs = [{"argv": j.get("argv"), "raw_argv": j.get("raw_argv"), "index": j.get("index")} for j in jobs_arg]
            for spec in specs:
                idx = spec["index"]
                if idx is not None and (type(idx) is not int or idx < 0):
                    raise ValueError("index must be a non-negative integer")
        cwd = args["cwd"]
        channel = args.get("channel") or db.DEFAULT_CHANNEL
        now = bool(args.get("now"))
        max_retries = args.get("max_retries") or 0
        timeout_secs = args.get("timeout_secs")
        # validate everything before anything is written
        for spec in specs:
            argv = spec["argv"]
            if not argv:
                raise ValueError("empty command")
            if not (isinstance(argv, list) and all(isinstance(a, str) for a in argv)) or not isinstance(cwd, str):
                raise ValueError("argv must be a list of strings and cwd a string")
        if type(channel) is not int or channel < 1:
            raise ValueError("channel must be a positive integer")
        if timeout_secs is not None and (type(timeout_secs) is not int or timeout_secs < 0):
            raise ValueError("timeout must be a non-negative integer")
        for key in ("before", "after"):
            if args.get(key) is not None and type(args[key]) is not int:
                raise ValueError(f"{key} must be a job id")
        if not isinstance(max_retries, int) or max_retries < 0:
            raise ValueError("retries must be a non-negative integer")
        if now and (args.get("before") is not None or args.get("after") is not None):
            raise ValueError("--now cannot be combined with --before/--after")
        on_fail = args.get("on_fail") or "continue"
        if on_fail not in db.ON_FAIL_MODES:  # also catches non-strings: they are never in the tuple
            raise ValueError(f"on_fail must be one of {', '.join(db.ON_FAIL_MODES)}")
        on_finish_cmd = db.check_hook(args.get("on_finish"), "on-finish command")
        on_error_cmd = db.check_hook(args.get("on_error"), "on-error command")
        deps = self._dep_args(args)
        if now and deps:
            raise ValueError("--now cannot be combined with --depends-on/--after-any")
        not_before = self._parse_not_before(args.get("not_before"))
        if now and not_before:
            raise ValueError("--now cannot be combined with --delay/--at")
        retry_delay_secs = args.get("retry_delay_secs")
        if retry_delay_secs is not None and (
            type(retry_delay_secs) is not int or not 0 < retry_delay_secs <= db.MAX_RETRY_DELAY_SECS
        ):
            raise ValueError(f"retry delay must be between 1 second and {db.MAX_RETRY_DELAY_SECS // 86400} days")
        retry_backoff = args.get("retry_backoff") or False
        if type(retry_backoff) is not bool:
            raise ValueError("retry_backoff must be true or false")
        if retry_backoff and retry_delay_secs is None:
            raise ValueError("--retry-backoff needs --retry-delay")
        if retry_delay_secs is not None and not max_retries:
            raise ValueError("--retry-delay needs --retries")
        dry_run = args.get("dry_run")
        if dry_run is None:
            dry_run = False
        if type(dry_run) is not bool:  # strict: a falsy non-bool must never turn into a real add
            raise ValueError("dry_run must be true or false")

        for key in ("path", "virtual_env", "python_path"):
            if args.get(key) is not None and not isinstance(args[key], str):
                raise ValueError(f"{key} must be a string")
        client_path = args.get("path") or None
        prepared: list[tuple[list[str], list[str], int | None]] = []  # (raw argv, resolved argv, index)
        for spec in specs:
            argv = spec["argv"]
            raw_argv = spec["raw_argv"] or argv
            if raw_argv is not argv and not (isinstance(raw_argv, list) and all(isinstance(a, str) for a in raw_argv)):
                raise ValueError("raw_argv must be a list of strings")
            if any("\0" in a for a in argv) or any("\0" in a for a in raw_argv) or "\0" in cwd:
                raise ValueError("arguments must not contain NUL bytes")
            if any("\0" in (args.get(k) or "") for k in ("path", "virtual_env", "python_path")):
                raise ValueError("environment values must not contain NUL bytes")
            if not os.path.isdir(cwd):
                raise ValueError(f"working directory does not exist: {cwd}")
            resolved = resolve_command(argv, cwd, path_env=client_path)
            exe = resolved[0]
            if not (os.path.isfile(exe) and os.access(exe, os.X_OK)):
                raise ValueError(f"executable not found or not executable: {exe}")
            prepared.append((raw_argv, resolved, spec["index"]))
        # layers, later wins: passthrough copied by the client, PATH/VIRTUAL_ENV/PYTHONPATH,
        # then the explicit --env (a sweep's QUR_INDEX is set per job below, over all of them)
        env_pass = jobenv.check_env(args.get("env_pass") or {}, "env_pass")
        env_explicit = jobenv.check_env(args.get("env") or {}, "env")
        env_extra: dict[str, str] = dict(env_pass)
        if client_path:
            env_extra["PATH"] = client_path
        virtual_env = args.get("virtual_env")
        if virtual_env:
            env_extra["VIRTUAL_ENV"] = virtual_env
        python_path = args.get("python_path")
        if python_path:
            env_extra["PYTHONPATH"] = python_path
        env_extra.update(env_explicit)
        passed = sorted(k for k in env_pass if k not in env_explicit)
        if passed:
            env_extra[jobenv.PASSTHROUGH_KEY] = ",".join(passed)

        # log_path needs the row's own id, which we don't have until after
        # insert -- write a placeholder, then patch it in immediately
        # (no await happens in between, so this is effectively atomic). A sweep is
        # inserted in ONE transaction: if any job is rejected, none is created.
        assert self.conn is not None
        job_ids: list[int] = []
        for raw_argv, resolved, index in prepared:
            job_env = dict(env_extra)
            if index is not None:
                job_env["QUR_INDEX"] = str(index)
            job_ids.append(db.enqueue(
                self.conn,
                raw_cmd=raw_argv,
                resolved_cmd=resolved,
                cwd=cwd,
                log_path="pending",
                channel=channel,
                timeout_secs=timeout_secs,
                before=args.get("before"),
                # after=X would put every new job right behind X, i.e. in reverse order
                after=job_ids[-1] if job_ids and args.get("after") is not None else args.get("after"),
                is_priority=now,
                env_extra=job_env or None,
                max_retries=max_retries,
                deps=deps,
                not_before=not_before,
                on_fail=on_fail,
                retry_delay_secs=retry_delay_secs,
                retry_backoff=retry_backoff,
                on_finish_cmd=on_finish_cmd,
                on_error_cmd=on_error_cmd,
                commit=False,
            ))
        if dry_run:
            # no await between the inserts above and this rollback, so nothing else can
            # see (or commit) the half-written rows
            try:
                return self._dry_run_report(job_ids, prepared, env_extra, channel, cwd, now, deps, on_finish_cmd, on_error_cmd)
            finally:
                self.conn.rollback()
        self.conn.executemany(
            "UPDATE jobs SET log_path = ? WHERE id = ?",
            [(str(self.log_dir / f"{i}.log"), i) for i in job_ids],
        )
        self.conn.commit()
        job_id = job_ids[0]
        result: dict[str, Any] = {"id": job_id, "channel": channel}
        if jobs_arg is not None:
            result["ids"] = job_ids

        if deps:
            was_skipped = False
            skipped_note: str | None = None
            for new_id in job_ids:
                verdict, dep_id = db.dep_verdict(self.conn, new_id)
                if verdict in ("failed", "removed") and dep_id is not None:
                    # a dependency has already ended badly: this job can never run
                    self._skip_blocked(new_id, verdict, dep_id)
                    was_skipped = True
                    skipped_job = db.get_job(self.conn, new_id)
                    skipped_note = skipped_job["note"] if skipped_job else None
            if was_skipped:
                result["skipped"] = skipped_note

        if now:
            self._freeze_current_if_normal(channel)
            self._ensure_priority_worker(channel)
        else:
            self._wake_worker(channel)
        return result

    def _dry_run_report(
        self,
        job_ids: list[int],
        prepared: list[tuple[list[str], list[str], int | None]],
        env_extra: dict[str, str],
        channel: int,
        cwd: str,
        now: bool,
        deps: dict[int, str],
        on_finish_cmd: str | None = None,
        on_error_cmd: str | None = None,
    ) -> dict[str, Any]:
        """What `add --dry-run` returns. Called while the not-yet-committed rows of the
        would-be jobs are in the open transaction (the caller rolls it back), so the
        positions and dependency verdicts are the real ones."""
        assert self.conn is not None
        lane = [j["id"] for j in db.get_queue(self.conn, channel, now)]
        running = db.get_running(self.conn, channel)
        stopped = db.get_stopped(self.conn, channel)
        jobs: list[dict[str, Any]] = []
        for new_id, (raw_argv, resolved, index) in zip(job_ids, prepared, strict=True):
            verdict, dep_id = db.dep_verdict(self.conn, new_id) if deps else ("ready", None)
            jobs.append({
                "index": index,
                "raw_argv": raw_argv,
                "argv": resolved,
                "position": lane.index(new_id) + 1,
                "dep_verdict": verdict,
                "dep_id": dep_id,
            })
        return {
            "dry_run": True,
            "channel": channel,
            "cwd": cwd,
            "priority": now,
            "queued_before": len(lane) - len(job_ids),
            "running_id": running["id"] if running else None,
            "frozen_id": stopped["id"] if stopped else None,
            "paused": db.get_paused(self.conn, channel),
            "env": env_extra,
            "on_finish": on_finish_cmd,
            "on_error": on_error_cmd,
            "jobs": jobs,
        }

    @staticmethod
    def _dep_args(args: dict[str, Any]) -> dict[int, str]:
        """`depends_on` (must end done) and `after_any` (any end) job ids from an
        `add` request, as the {id: mode} map db.enqueue takes. An id in both lists
        gets the stricter mode, `success`."""
        deps: dict[int, str] = {}
        for key, mode in (("after_any", "any"), ("depends_on", "success")):
            ids = args.get(key) or []
            if not isinstance(ids, list) or any(type(i) is not int or i < 1 for i in ids):
                raise ValueError(f"{key} must be a list of job ids")
            if len(ids) > db.MAX_SWEEP:
                raise ValueError(f"too many jobs in {key} (at most {db.MAX_SWEEP})")
            for i in ids:
                deps[i] = mode
        return deps

    def _with_deps(self, job: dict[str, Any]) -> dict[str, Any]:
        """`job` plus its dependencies for display: `depends_on`, `after_any` and
        `waiting_on` (job ids; the last is those that have not ended yet)."""
        assert self.conn is not None
        info: dict[str, Any] = {**job, **db.dep_info(self.conn, job["id"])}
        info["editing"] = False
        if job["status"] == "queued":
            lock = db.live_edit_lock(self.conn, job["id"])
            if lock is not None:
                info["editing"] = True
                info["edit_pid"] = lock["holder_pid"]
                info["edit_expires_at"] = lock["expires_at"]
        return info

    async def _cmd_list(self, args: dict[str, Any]) -> dict[str, Any]:
        assert self.conn is not None
        channels: dict[int, dict[str, Any]] = {}
        for channel in db.list_channels(self.conn):
            channels[channel] = {
                "running": db.get_running(self.conn, channel),
                "queue": [self._with_deps(j) for j in db.get_queue(self.conn, channel)],
                "priority_queue": db.get_queue(self.conn, channel, is_priority=True),
                "frozen": db.get_stopped(self.conn, channel),
                "backlog": db.get_backlog(self.conn, channel),
                "paused": db.get_paused(self.conn, channel),
            }
        return {"channels": channels}

    async def _cmd_history(self, args: dict[str, Any]) -> dict[str, Any]:
        """Finished jobs, newest first. Optional args: `statuses` (list of terminal
        statuses), `channel`, `since` (ISO timestamp), `grep` (substring of the raw
        command), `limit` (default 50, at most db.MAX_HISTORY)."""
        assert self.conn is not None
        statuses = args.get("statuses")
        if statuses is not None and (
            not isinstance(statuses, list) or not all(isinstance(s, str) for s in statuses)
        ):
            raise ValueError("statuses must be a list of strings")
        channel = self._int_arg(args, "channel", required=False, positive=True)
        limit = self._int_arg(args, "limit", required=False, positive=True)
        if limit is None:
            limit = 50
        if limit > db.MAX_HISTORY:
            raise ValueError(f"limit must be at most {db.MAX_HISTORY}")
        since = args.get("since")
        if since is not None:
            if not isinstance(since, str):
                raise ValueError("since must be an ISO timestamp")
            try:
                parsed = datetime.fromisoformat(since)
            except ValueError:
                raise ValueError(f"since is not an ISO timestamp: {since!r}") from None
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            since = parsed.astimezone(timezone.utc).isoformat()
        grep = args.get("grep")
        if grep is not None and not isinstance(grep, str):
            raise ValueError("grep must be a string")
        jobs = db.get_history(
            self.conn, statuses=statuses, channel=channel, since=since, grep=grep or None, limit=limit
        )
        return {"jobs": jobs}

    async def _cmd_export(self, args: dict[str, Any]) -> dict[str, Any]:
        """Every job row with its dependencies, oldest first (F24, `qur export`). Optional
        args: `statuses` (list of job statuses, queued/running/stopped or terminal) and
        `channel`. No logs, just the rows."""
        assert self.conn is not None
        statuses = args.get("statuses")
        if statuses is not None and (
            not isinstance(statuses, list) or not all(isinstance(s, str) for s in statuses)
        ):
            raise ValueError("statuses must be a list of strings")
        channel = self._int_arg(args, "channel", required=False, positive=True)
        jobs = db.get_export(self.conn, statuses=statuses, channel=channel)
        return {"jobs": [self._with_deps(j) for j in jobs]}

    async def _cmd_status(self, args: dict[str, Any]) -> dict[str, Any]:
        assert self.conn is not None
        channels: dict[int, dict[str, Any]] = {}
        for channel in db.list_channels(self.conn):
            channels[channel] = {
                "running": db.get_running(self.conn, channel),
                "frozen": db.get_stopped(self.conn, channel),
                "paused": db.get_paused(self.conn, channel),
            }
        return {"channels": channels}

    @staticmethod
    def _int_arg(args: Any, key: str, *, required: bool = True, positive: bool = False) -> int | None:
        """A JSON int (not bool) from a request, or ValueError with a clear message."""
        if not isinstance(args, dict):
            raise ValueError("arguments must be an object")
        value = args.get(key)
        if value is None:
            if required:
                raise ValueError(f"{key} is required")
            return None
        if type(value) is not int or (positive and value < 1):
            raise ValueError(f"{key} must be a{' positive' if positive else 'n'} integer")
        return value

    async def _cmd_show(self, args: dict[str, Any]) -> dict[str, Any]:
        assert self.conn is not None
        self._int_arg(args, "id")
        job = db.get_job(self.conn, args["id"])
        if job is None:
            raise ValueError(f"job {args['id']} does not exist")
        return self._with_deps(job)

    async def _cmd_clear(self, args: dict[str, Any]) -> dict[str, Any]:
        assert self.conn is not None
        channel = self._int_arg(args, "channel", required=False, positive=True)
        cleared = db.clear_backlog(self.conn, channel=channel)
        return {"cleared": cleared, "channel": channel}

    async def _cmd_rm(self, args: dict[str, Any]) -> dict[str, Any]:
        assert self.conn is not None
        self._int_arg(args, "id")
        job = db.get_job(self.conn, args["id"])
        db.remove_from_queue(self.conn, args["id"])
        if job is not None:
            self._wake_worker(job["channel"])  # it may have been the waiting head that held the channel
        return {"removed": args["id"]}

    async def _cmd_cancel(self, args: dict[str, Any]) -> dict[str, Any]:
        """Without `id`: kill the running job on `channel` (default 1), as
        before. With `id`: cancel that job wherever it is -- running, frozen
        by `--now`, or still queued (kept in history as `cancelled`). If
        `channel` is also given it must match the job's channel."""
        assert self.conn is not None
        job_id = self._int_arg(args, "id", required=False)
        channel = self._int_arg(args, "channel", required=False, positive=True)

        if job_id is None:
            channel = channel or db.DEFAULT_CHANNEL
            proc = self._current_proc.get(channel)
            job_id = self._current_job_id.get(channel)
            if proc is None or job_id is None:
                raise ValueError(f"no job is currently running on channel {channel}")
            await self._cancel_running(job_id, proc)
            return {"cancelled": job_id, "channel": channel}

        job = db.get_job(self.conn, job_id)
        if job is None:
            raise ValueError(f"job {job_id} does not exist")
        channel = job["channel"] if channel is None else channel
        if channel != job["channel"]:
            raise ValueError(f"job {job_id} is on channel {job['channel']}, not channel {channel}")

        status = job["status"]
        if status == "queued":
            db.cancel_queued(self.conn, job_id, self.keep)
            self._settle_dependants(job_id)
            self._wake_worker(channel)  # it may have been the waiting head that held the channel
            self._notify_finished(job_id)
        elif status == "running":
            proc = self._current_proc.get(channel)
            if proc is None or self._current_job_id.get(channel) != job_id:
                raise ValueError(f"job {job_id} is not running")
            await self._cancel_running(job_id, proc)
        elif status == "stopped":
            frozen = self._frozen.get(channel)
            if frozen is None or frozen[0] != job_id:
                raise ValueError(f"job {job_id} is not frozen")
            # drop it from the frozen slot first so the priority lane does
            # not try to resume a job that is about to die
            del self._frozen[channel]
            await self._cancel_running(job_id, frozen[1], frozen=True)
        else:
            raise ValueError(f"job {job_id} is already {status}")
        return {"cancelled": job_id, "channel": channel}

    async def _cancel_running(self, job_id: int, proc: asyncio.subprocess.Process, frozen: bool = False) -> None:
        self._cancelled.add(job_id)
        if frozen:
            # a SIGSTOPped group never acts on SIGTERM until continued
            try:
                os.killpg(proc.pid, signal.SIGCONT)
            except ProcessLookupError:
                pass
        await self._kill_process_group(proc)

    async def _cmd_requeue(self, args: dict[str, Any]) -> dict[str, Any]:
        self._int_arg(args, "id")
        return {"id": await self._requeue_job(args["id"])}

    async def _requeue_job(self, job_id: int) -> int:
        """Manual requeue, shared by `qur requeue` and the Telegram button."""
        assert self.conn is not None
        new_id = db.requeue(self.conn, job_id, log_path="pending")
        log_path = str(self.log_dir / f"{new_id}.log")
        self.conn.execute("UPDATE jobs SET log_path = ? WHERE id = ?", (log_path, new_id))
        self.conn.commit()
        new_job = db.get_job(self.conn, new_id)
        assert new_job is not None
        self._wake_worker(new_job["channel"])
        return new_id

    # -- edit (qur edit) ------------------------------------------------------

    def _editable(self, job_id: int) -> dict[str, Any]:
        assert self.conn is not None
        if job_id in self._starting:
            raise ValueError(f"job {job_id} is starting right now and can no longer be edited")
        return db._editable_job(self.conn, job_id)

    async def _cmd_edit_lock(self, args: dict[str, Any]) -> dict[str, Any]:
        """Lock a queued job for editing (`qur edit`): until `edit` or `edit_unlock` it is not
        started, and its channel waits behind it. `pid` is the editor's process id; the lock
        is dropped by the daemon if that process dies or after db.EDIT_LOCK_SECS. Returns the job."""
        assert self.conn is not None
        job_id = self._int_arg(args, "id")
        pid = self._int_arg(args, "pid", positive=True)
        assert job_id is not None and pid is not None
        job = self._editable(job_id)
        db.take_edit_lock(self.conn, job_id, pid)
        self._wake_worker(job["channel"])  # so a worker about to start it re-reads the lock
        return self._with_deps(job)

    async def _cmd_edit_unlock(self, args: dict[str, Any]) -> dict[str, Any]:
        """Abandon an edit: drop the caller's lock and let the channel go on."""
        assert self.conn is not None
        job_id = self._int_arg(args, "id")
        pid = self._int_arg(args, "pid", positive=True)
        assert job_id is not None and pid is not None
        released = db.release_edit_lock(self.conn, job_id, pid)
        job = db.get_job(self.conn, job_id)
        if job is not None:
            self._wake_worker(job["channel"])
        return {"released": released}

    def _checked_argv(self, args: dict[str, Any], cwd: str) -> tuple[list[str], list[str]]:
        """(raw argv, resolved argv) of an edited command, checked like an `add`."""
        argv = args.get("argv")
        raw_argv = args.get("raw_argv") or argv
        for what, words in (("argv", argv), ("raw_argv", raw_argv)):
            if not words or not isinstance(words, list) or not all(isinstance(a, str) for a in words):
                raise ValueError(f"{what} must be a non-empty list of strings")
        assert isinstance(argv, list) and isinstance(raw_argv, list)
        if any("\0" in a for a in argv) or any("\0" in a for a in raw_argv):
            raise ValueError("arguments must not contain NUL bytes")
        path = args.get("path")
        for key in ("path", "virtual_env", "python_path"):
            if args.get(key) is not None and (not isinstance(args[key], str) or "\0" in args[key]):
                raise ValueError(f"{key} must be a string without NUL bytes")
        resolved = resolve_command(argv, cwd, path_env=path or None)
        exe = resolved[0]
        if not (os.path.isfile(exe) and os.access(exe, os.X_OK)):
            raise ValueError(f"executable not found or not executable: {exe}")
        return raw_argv, resolved

    async def _cmd_edit(self, args: dict[str, Any]) -> dict[str, Any]:
        """Change a queued job, all or nothing. Only the keys present are changed:
        `argv`/`raw_argv` (+ `path`, `virtual_env`, `python_path` of the editing shell: they
        replace the job's own), `cwd`, `timeout_secs` (null = none), `channel` (moves the job to
        the end of that channel), `max_retries`, `on_fail`, `on_finish` / `on_error` (hook commands; null or blank = remove),
        `not_before` (null = clear),
        `depends_on` / `after_any` (replace that kind of dependency), `clear_deps`, `env`
        (merged over the job's). `pid` is the editor's lock holder (see `edit_lock`); without it
        the job must not be locked by someone else. Returns {"id", "changed": [...]}."""
        assert self.conn is not None
        job_id = self._int_arg(args, "id")
        pid = self._int_arg(args, "pid", required=False, positive=True)
        assert job_id is not None
        job = self._editable(job_id)
        updates: dict[str, Any] = {}
        changed: list[str] = []

        cwd = job["cwd"]
        if "cwd" in args:
            cwd = args["cwd"]
            if not isinstance(cwd, str) or not cwd or "\0" in cwd:
                raise ValueError("cwd must be a non-empty string without NUL bytes")
            if cwd != job["cwd"]:
                updates["cwd"] = cwd
                changed.append("cwd")
        if not os.path.isdir(cwd):
            raise ValueError(f"working directory does not exist: {cwd}")

        env_extra: dict[str, str] = json.loads(job["env_extra"]) if job["env_extra"] else {}
        new_env = dict(env_extra)
        if "argv" in args:
            raw_argv, resolved = self._checked_argv(args, cwd)
            updates["raw_cmd"] = json.dumps(raw_argv)
            updates["resolved_cmd"] = json.dumps(resolved)
            changed.append("command")
            # the new command was resolved against the editing shell: its PATH etc. replace the job's
            for key, name in (("path", "PATH"), ("virtual_env", "VIRTUAL_ENV"), ("python_path", "PYTHONPATH")):
                if args.get(key):
                    new_env[name] = args[key]
                else:
                    new_env.pop(name, None)
        elif "cwd" in args and cwd != job["cwd"]:
            raise ValueError("a new cwd needs the command re-resolved: send argv/raw_argv with it")
        if "env" in args:
            explicit = jobenv.check_env(args["env"] or {}, "env")
            for name, value in explicit.items():
                if name != "QUR_INDEX" or "QUR_INDEX" not in new_env:  # a sweep's index is not editable
                    new_env[name] = value
            if jobenv.PASSTHROUGH_KEY in new_env:
                passed = [n for n in new_env[jobenv.PASSTHROUGH_KEY].split(",") if n and n not in explicit]
                if passed:
                    new_env[jobenv.PASSTHROUGH_KEY] = ",".join(passed)
                else:
                    del new_env[jobenv.PASSTHROUGH_KEY]
        if new_env != env_extra:
            updates["env_extra"] = json.dumps(new_env) if new_env else None
            if "env" in args:
                changed.append("env")

        if "timeout_secs" in args:
            timeout_secs = args["timeout_secs"]
            if timeout_secs is not None and (type(timeout_secs) is not int or timeout_secs < 0):
                raise ValueError("timeout must be a non-negative integer")
            timeout_secs = timeout_secs or None  # 0 = no timeout
            if timeout_secs != job["timeout_secs"]:
                updates["timeout_secs"] = timeout_secs
                changed.append("timeout")
        if "max_retries" in args:
            max_retries = args["max_retries"]
            if type(max_retries) is not int or max_retries < 0:
                raise ValueError("retries must be a non-negative integer")
            if max_retries != job["max_retries"]:
                updates["max_retries"] = max_retries
                changed.append("retries")
                if max_retries == 0 and job["retry_delay_secs"] is not None:
                    updates["retry_delay_secs"] = None  # a retry delay without retries makes no sense
                    updates["retry_backoff"] = 0
        if "on_fail" in args:
            on_fail = args["on_fail"]
            if on_fail not in db.ON_FAIL_MODES:
                raise ValueError(f"on_fail must be one of {', '.join(db.ON_FAIL_MODES)}")
            if on_fail != job["on_fail"]:
                updates["on_fail"] = on_fail
                changed.append("on-fail")
        for key, column, label in (("on_finish", "on_finish_cmd", "on-finish"), ("on_error", "on_error_cmd", "on-error")):
            if key in args:
                hook = db.check_hook(args[key], f"{label} command")  # null or blank removes the hook
                if hook != job[column]:
                    updates[column] = hook
                    changed.append(label)
        if "not_before" in args:
            not_before = self._parse_not_before(args["not_before"])
            if not_before != job["not_before"]:
                updates["not_before"] = not_before
                changed.append("start time")

        deps: dict[int, str] | None = None
        if args.get("clear_deps") is not None and type(args["clear_deps"]) is not bool:
            raise ValueError("clear_deps must be true or false")
        if args.get("clear_deps") and ("depends_on" in args or "after_any" in args):
            raise ValueError("clear_deps cannot be combined with depends_on/after_any")
        if args.get("clear_deps") or "depends_on" in args or "after_any" in args:
            info = db.dep_info(self.conn, job_id)
            current = {**{i: "any" for i in info["after_any"]}, **{i: "success" for i in info["depends_on"]}}
            if args.get("clear_deps"):
                deps = {}
            else:
                keep = {i: m for i, m in current.items() if ("depends_on" not in args or m != "success")
                        and ("after_any" not in args or m != "any")}
                given = self._dep_args({k: args[k] for k in ("depends_on", "after_any") if k in args})
                deps = {**keep, **given}
                for dep_id in given:  # an id in both kinds gets the stricter mode
                    if keep.get(dep_id) == "success":
                        deps[dep_id] = "success"
            if deps != current:
                changed.append("dependencies")
        channel: int | None = None
        if "channel" in args:
            channel = args["channel"]
            if type(channel) is not int or channel < 1:
                raise ValueError("channel must be a positive integer")
            if channel == job["channel"]:
                channel = None
            else:
                changed.append("channel")

        if not changed:
            raise ValueError("nothing to change")
        db.edit_job(
            self.conn, job_id, updates=updates, deps=deps if "dependencies" in changed else None,
            channel=channel, holder_pid=pid,
        )
        if "dependencies" in changed:
            verdict, blocker = db.dep_verdict(self.conn, job_id)
            if verdict in ("failed", "removed") and blocker is not None:
                self._skip_blocked(job_id, verdict, blocker)  # a dependency already ended badly: it can never run
        self._wake_worker(job["channel"])
        if channel is not None:
            self._wake_worker(channel)
        return {"id": job_id, "changed": changed}

    async def _cmd_pause(self, args: dict[str, Any]) -> dict[str, Any]:
        assert self.conn is not None
        channel = self._int_arg(args, "channel", required=False, positive=True) or db.DEFAULT_CHANNEL
        db.set_paused(self.conn, True, channel)
        return {"paused": True, "channel": channel}

    async def _cmd_resume(self, args: dict[str, Any]) -> dict[str, Any]:
        assert self.conn is not None
        channel = self._int_arg(args, "channel", required=False, positive=True) or db.DEFAULT_CHANNEL
        db.set_paused(self.conn, False, channel)
        self._wake_worker(channel)
        return {"paused": False, "channel": channel}

    # -- socket server ------------------------------------------------------

    def _peer_is_me(self, writer: asyncio.StreamWriter) -> bool:
        """Defense in depth behind the 0600 socket: the connecting process
        must run as our uid (SO_PEERCRED, Linux). Elsewhere the mode is the guard."""
        sock = writer.get_extra_info("socket")
        so_peercred = getattr(socket, "SO_PEERCRED", None)
        if sock is None or so_peercred is None:
            return True
        try:
            _pid, uid, _gid = struct.unpack("iII", sock.getsockopt(socket.SOL_SOCKET, so_peercred, struct.calcsize("iII")))  # pid, uid, gid
        except (OSError, struct.error):
            return False
        return uid == self._uid

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if self._clients >= MAX_CLIENTS:
            # a flood of same-user connections must not eat every file descriptor
            try:
                await asyncio.wait_for(reader.readline(), timeout=1)  # read first, or the close resets the reply away
                writer.write(b'{"ok": false, "error": "too many connections, try again"}\n')
                await writer.drain()
            except (asyncio.TimeoutError, OSError, ValueError):
                pass
            writer.close()
            return
        self._clients += 1
        try:
            await self._serve_client(reader, writer)
        finally:
            self._clients -= 1

    async def _serve_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if not self._peer_is_me(writer):
            print("[queuerd] refused a connection from another user", file=sys.stderr)
            writer.close()
            return
        req: Any = None
        try:
            try:
                line = await asyncio.wait_for(reader.readline(), timeout=CLIENT_READ_TIMEOUT_SECS)
            except asyncio.TimeoutError:
                writer.close()
                return
            if not line:
                writer.close()
                await writer.wait_closed()
                return
            req = json.loads(line.decode())
            resp = await self.handle_request(req)
        except ValueError:  # bad JSON, bad UTF-8, or line over the stream limit
            resp = {"ok": False, "error": "invalid request"}
        except Exception as e:  # last-resort guard: one bad request must not kill the daemon
            print(f"[queuerd] unexpected error handling request {req!r}:", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            resp = {"ok": False, "error": f"internal error: {e}"}
        writer.write((json.dumps(resp) + "\n").encode())
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    async def serve(self) -> None:
        if self.socket_path.parent == _fallback_socket_dir():
            _ensure_private_dir(self.socket_path.parent)
        else:
            self.socket_path.parent.mkdir(parents=True, exist_ok=True)
        # before touching the db: a second daemon must not mark the first's
        # running job failed, nor steal its socket
        if _socket_answers(self.socket_path):
            raise AlreadyRunning(f"queuerd is already running (socket {self.socket_path} answers)")
        if self.socket_path.exists():
            self.socket_path.unlink()

        self._connect()
        self._recover_on_startup()

        new_log_dir = not self.log_dir.exists()
        self.log_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        _tighten_or_warn(self.log_dir, new_log_dir, self.log_dir == _default_log_dir())

        # channel 1 always gets a worker, plus one for every other channel
        # that already has rows from a previous run.
        self._ensure_channel_worker(db.DEFAULT_CHANNEL)
        assert self.conn is not None
        for channel in db.list_channels(self.conn):
            self._ensure_channel_worker(channel)

        # umask 077 while binding: socket is born 0600, no window where it is looser
        old_umask = os.umask(0o077)
        try:
            server = await asyncio.start_unix_server(self._handle_client, path=str(self.socket_path))
        finally:
            os.umask(old_umask)
        os.chmod(self.socket_path, 0o600)

        async with server:
            listener = asyncio.create_task(self._telegram_listener())
            cleaner = asyncio.create_task(self._log_cleaner())
            serving = asyncio.create_task(server.serve_forever())
            stop_wait = asyncio.ensure_future(self._stopping.wait())
            loop = asyncio.get_running_loop()
            handled: list[signal.Signals] = []
            for sig in (signal.SIGTERM, signal.SIGINT):
                try:
                    loop.add_signal_handler(sig, self._stopping.set)
                    handled.append(sig)
                except (NotImplementedError, RuntimeError, ValueError):  # not the main thread
                    pass
            try:
                waitables: set[asyncio.Future[Any]] = {serving, stop_wait, *self._worker_tasks.values()}
                done, _ = await asyncio.wait(waitables, return_when=asyncio.FIRST_COMPLETED)
            except asyncio.CancelledError:
                # cancelled from outside (not a stop request): hard stop, as before
                for task in self._worker_tasks.values():
                    task.cancel()
                raise
            finally:
                for sig in handled:
                    loop.remove_signal_handler(sig)
                stop_wait.cancel()
                serving.cancel()
                listener.cancel()
                cleaner.cancel()
            # a stop request, or a worker/server that ended: either way the daemon is going
            # down, so running jobs are recorded as interrupted before the process exits
            await self._shutdown(server)
            for finished in done:
                if finished is not stop_wait and not finished.cancelled():
                    finished.result()  # a crashed worker still surfaces its error


def main() -> None:
    daemon = Daemon()
    try:
        asyncio.run(daemon.serve())
    except KeyboardInterrupt:
        pass
    except AlreadyRunning as e:
        print(f"[queuerd] {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
