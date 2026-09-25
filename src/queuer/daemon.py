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
import os
import signal
import sqlite3
import sys
import traceback
from pathlib import Path
from typing import Any

from queuer import db
from queuer.pathresolve import resolve_command

MAX_LOG_BYTES = 50 * 1024 * 1024  # 50MB per-job log cap
DEFAULT_GRACE_SECS = 5


def _default_db_path() -> Path:
    override = os.environ.get("QUEUER_DB_PATH")
    return Path(override) if override else db.DEFAULT_DB_PATH


def _default_socket_path() -> Path:
    override = os.environ.get("QUEUER_SOCKET_PATH")
    if override:
        return Path(override)
    runtime_dir = os.environ.get("XDG_RUNTIME_DIR", "/tmp")
    return Path(runtime_dir) / "queuer.sock"


def _default_log_dir() -> Path:
    override = os.environ.get("QUEUER_LOG_DIR")
    if override:
        return Path(override)
    return Path.home() / ".local" / "state" / "queuer" / "logs"


class Daemon:
    def __init__(
        self,
        db_path: Path | None = None,
        socket_path: Path | None = None,
        log_dir: Path | None = None,
    ) -> None:
        self.db_path = db_path or _default_db_path()
        self.socket_path = socket_path or _default_socket_path()
        self.log_dir = log_dir or _default_log_dir()
        self.conn: sqlite3.Connection | None = None

        # all per-channel: each channel gets its own wake event, its own
        # current-job slot, its own cancel flag, its own worker task.
        self._wake: dict[int, asyncio.Event] = {}
        self._current_proc: dict[int, asyncio.subprocess.Process] = {}
        self._current_job_id: dict[int, int] = {}
        self._cancel_requested: dict[int, bool] = {}
        self._worker_tasks: dict[int, asyncio.Task] = {}

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

    def _recover_on_startup(self) -> None:
        """A row still marked 'running' from a previous daemon instance
        cannot actually still be running -- we just started. Mark it
        failed rather than silently losing it or re-running it. Checked
        across every channel that has ever been used."""
        assert self.conn is not None
        for channel in db.list_channels(self.conn):
            running = db.get_running(self.conn, channel)
            if running is not None:
                db.mark_finished(
                    self.conn,
                    running["id"],
                    status="failed",
                    note="interrupted by daemon restart",
                )

    def _ensure_channel_worker(self, channel: int) -> None:
        """Lazily spawn a channel's worker loop if it isn't running yet."""
        if channel in self._worker_tasks:
            return
        self._wake[channel] = asyncio.Event()
        self._cancel_requested[channel] = False
        self._worker_tasks[channel] = asyncio.create_task(self.worker_loop(channel))

    # -- worker loop -------------------------------------------------------

    async def worker_loop(self, channel: int) -> None:
        assert self.conn is not None
        wake = self._wake[channel]
        while True:
            wake.clear()
            if not db.get_paused(self.conn, channel):
                queue = db.get_queue(self.conn, channel)
                if queue:
                    await self._run_job(channel, queue[0])
                    continue
            # idle: wait for something to wake us (new job, resume, etc.),
            # but also poll periodically as a safety net
            try:
                await asyncio.wait_for(wake.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pass

    def _wake_worker(self, channel: int) -> None:
        self._ensure_channel_worker(channel)
        self._wake[channel].set()

    # -- priority lane (`--now`) ------------------------------------------

    def _ensure_priority_worker(self, channel: int) -> None:
        """Spawn the priority-lane runner for a channel if it isn't
        already draining one. Idempotent -- additional `--now` jobs while
        one is already running just land in the DB queue and get picked
        up by the existing task's next loop iteration."""
        task = self._priority_worker_tasks.get(channel)
        if task is not None and not task.done():
            return
        self._priority_worker_tasks[channel] = asyncio.create_task(self._priority_worker_loop(channel))

    async def _priority_worker_loop(self, channel: int) -> None:
        assert self.conn is not None
        while True:
            queue = db.get_queue(self.conn, channel, is_priority=True)
            if not queue:
                break
            await self._run_job(channel, queue[0])
        if channel in self._frozen:
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
        if channel in self._frozen:
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

    async def _run_job(self, channel: int, job: dict[str, Any]) -> None:
        assert self.conn is not None
        job_id = job["id"]
        argv = json.loads(job["resolved_cmd"])
        cwd = job["cwd"]
        env_extra = json.loads(job["env_extra"]) if job["env_extra"] else {}
        env = {"PATH": os.environ.get("PATH", ""), "HOME": os.environ.get("HOME", "")}
        env.update(env_extra)
        log_path = Path(job["log_path"])
        log_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            proc = await asyncio.create_subprocess_exec(
                *argv,
                cwd=cwd,
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                start_new_session=True,  # own process group -> group-kill later
            )
        except (OSError, FileNotFoundError) as e:
            db.mark_finished(self.conn, job_id, status="failed", note=f"failed to start: {e}")
            return

        db.mark_running(self.conn, job_id, pid=proc.pid, pgid=proc.pid)
        self._current_proc[channel] = proc
        self._current_job_id[channel] = job_id
        self._cancel_requested[channel] = False

        log_task = asyncio.create_task(self._pump_log(proc, log_path))
        timeout = job["timeout_secs"]

        try:
            if timeout:
                exit_code = await asyncio.wait_for(proc.wait(), timeout=timeout)
            else:
                exit_code = await proc.wait()
        except asyncio.TimeoutError:
            await self._kill_process_group(proc)
            exit_code = await proc.wait()
            await log_task
            db.mark_finished(self.conn, job_id, status="failed", exit_code=exit_code, note="timed out")
            self._current_proc.pop(channel, None)
            self._current_job_id.pop(channel, None)
            return

        await log_task

        if self._cancel_requested[channel]:
            status = "cancelled"
            self._cancel_requested[channel] = False
        else:
            status = "done" if exit_code == 0 else "failed"
        db.mark_finished(self.conn, job_id, status=status, exit_code=exit_code)
        self._current_proc.pop(channel, None)
        self._current_job_id.pop(channel, None)

    async def _pump_log(self, proc: asyncio.subprocess.Process, log_path: Path) -> None:
        """Stream the job's combined stdout/stderr to its log file, capped
        at MAX_LOG_BYTES with a single truncation marker line."""
        written = 0
        truncated = False
        with open(log_path, "wb") as f:
            assert proc.stdout is not None
            while True:
                chunk = await proc.stdout.read(65536)
                if not chunk:
                    break
                if truncated:
                    continue
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

    # -- request handlers --------------------------------------------------

    async def handle_request(self, req: dict[str, Any]) -> dict[str, Any]:
        assert self.conn is not None
        cmd = req.get("cmd")
        args = req.get("args", {})
        handler = getattr(self, f"_cmd_{cmd}", None)
        if handler is None:
            return {"ok": False, "error": f"unknown command: {cmd}"}
        try:
            data = await handler(args)
            return {"ok": True, "data": data}
        except (ValueError, KeyError, TypeError) as e:
            return {"ok": False, "error": str(e)}

    async def _cmd_add(self, args: dict[str, Any]) -> dict[str, Any]:
        argv = args["argv"]
        # `raw_argv` is exactly what the user typed after `--`, with no
        # interpreter-pinning or bash-wrapping applied -- kept separate from
        # `argv` (which is what's actually resolved/exec'd) so `raw command`
        # in show/list reflects what was typed. Older clients that don't
        # send it fall back to `argv`, same as before this field existed.
        raw_argv = args.get("raw_argv") or argv
        cwd = args["cwd"]
        channel = args.get("channel") or db.DEFAULT_CHANNEL
        now = bool(args.get("now"))
        if not argv:
            raise ValueError("empty command")
        if now and (args.get("before") is not None or args.get("after") is not None):
            raise ValueError("--now cannot be combined with --before/--after")

        client_path = args.get("path") or None
        resolved = resolve_command(argv, cwd, path_env=client_path)
        exe = resolved[0]
        if not (os.path.isfile(exe) and os.access(exe, os.X_OK)):
            raise ValueError(f"executable not found or not executable: {exe}")
        env_extra: dict[str, str] = {}
        if client_path:
            env_extra["PATH"] = client_path
        virtual_env = args.get("virtual_env")
        if virtual_env:
            env_extra["VIRTUAL_ENV"] = virtual_env
        env_extra = env_extra or None

        # log_path needs the row's own id, which we don't have until after
        # insert -- write a placeholder, then patch it in immediately
        # (no await happens in between, so this is effectively atomic).
        assert self.conn is not None
        job_id = db.enqueue(
            self.conn,
            raw_cmd=raw_argv,
            resolved_cmd=resolved,
            cwd=cwd,
            log_path="pending",
            channel=channel,
            timeout_secs=args.get("timeout_secs"),
            before=args.get("before"),
            after=args.get("after"),
            is_priority=now,
            env_extra=env_extra,
        )
        log_path = str(self.log_dir / f"{job_id}.log")
        self.conn.execute("UPDATE jobs SET log_path = ? WHERE id = ?", (log_path, job_id))
        self.conn.commit()

        if now:
            self._freeze_current_if_normal(channel)
            self._ensure_priority_worker(channel)
        else:
            self._wake_worker(channel)
        return {"id": job_id, "channel": channel}

    async def _cmd_list(self, args: dict[str, Any]) -> dict[str, Any]:
        assert self.conn is not None
        channels: dict[int, dict[str, Any]] = {}
        for channel in db.list_channels(self.conn):
            channels[channel] = {
                "running": db.get_running(self.conn, channel),
                "queue": db.get_queue(self.conn, channel),
                "priority_queue": db.get_queue(self.conn, channel, is_priority=True),
                "frozen": db.get_stopped(self.conn, channel),
                "backlog": db.get_backlog(self.conn, channel),
                "paused": db.get_paused(self.conn, channel),
            }
        return {"channels": channels}

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

    async def _cmd_show(self, args: dict[str, Any]) -> dict[str, Any]:
        assert self.conn is not None
        job = db.get_job(self.conn, args["id"])
        if job is None:
            raise ValueError(f"job {args['id']} does not exist")
        return job

    async def _cmd_clear(self, args: dict[str, Any]) -> dict[str, Any]:
        assert self.conn is not None
        channel = args.get("channel")
        cleared = db.clear_backlog(self.conn, channel=channel)
        return {"cleared": cleared, "channel": channel}

    async def _cmd_rm(self, args: dict[str, Any]) -> dict[str, Any]:
        assert self.conn is not None
        db.remove_from_queue(self.conn, args["id"])
        return {"removed": args["id"]}

    async def _cmd_cancel(self, args: dict[str, Any]) -> dict[str, Any]:
        channel = args.get("channel") or db.DEFAULT_CHANNEL
        proc = self._current_proc.get(channel)
        job_id = self._current_job_id.get(channel)
        if proc is None or job_id is None:
            raise ValueError(f"no job is currently running on channel {channel}")
        self._cancel_requested[channel] = True
        await self._kill_process_group(proc)
        return {"cancelled": job_id, "channel": channel}

    async def _cmd_requeue(self, args: dict[str, Any]) -> dict[str, Any]:
        assert self.conn is not None
        new_id = db.requeue(self.conn, args["id"], log_path="pending")
        log_path = str(self.log_dir / f"{new_id}.log")
        self.conn.execute("UPDATE jobs SET log_path = ? WHERE id = ?", (log_path, new_id))
        self.conn.commit()
        new_job = db.get_job(self.conn, new_id)
        assert new_job is not None
        self._wake_worker(new_job["channel"])
        return {"id": new_id}

    async def _cmd_pause(self, args: dict[str, Any]) -> dict[str, Any]:
        assert self.conn is not None
        channel = args.get("channel") or db.DEFAULT_CHANNEL
        db.set_paused(self.conn, True, channel)
        return {"paused": True, "channel": channel}

    async def _cmd_resume(self, args: dict[str, Any]) -> dict[str, Any]:
        assert self.conn is not None
        channel = args.get("channel") or db.DEFAULT_CHANNEL
        db.set_paused(self.conn, False, channel)
        self._wake_worker(channel)
        return {"paused": False, "channel": channel}

    # -- socket server ------------------------------------------------------

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            line = await reader.readline()
            if not line:
                writer.close()
                await writer.wait_closed()
                return
            req = json.loads(line.decode())
            resp = await self.handle_request(req)
        except json.JSONDecodeError:
            resp = {"ok": False, "error": "invalid JSON request"}
        except Exception as e:  # last-resort guard: one bad request must not kill the daemon
            print(f"[queuerd] unexpected error handling request {req!r}:", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            resp = {"ok": False, "error": f"internal error: {e}"}
        writer.write((json.dumps(resp) + "\n").encode())
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    async def serve(self) -> None:
        self._connect()
        self._recover_on_startup()

        if self.socket_path.exists():
            self.socket_path.unlink()
        self.socket_path.parent.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)

        # channel 1 always gets a worker, plus one for every other channel
        # that already has rows from a previous run.
        self._ensure_channel_worker(db.DEFAULT_CHANNEL)
        assert self.conn is not None
        for channel in db.list_channels(self.conn):
            self._ensure_channel_worker(channel)

        server = await asyncio.start_unix_server(self._handle_client, path=str(self.socket_path))

        async with server:
            await asyncio.gather(server.serve_forever(), *self._worker_tasks.values())


def main() -> None:
    daemon = Daemon()
    try:
        asyncio.run(daemon.serve())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
