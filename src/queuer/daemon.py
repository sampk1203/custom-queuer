"""queuerd: the background daemon that owns the job queue and runs jobs
one at a time.

Listens on a Unix domain socket for newline-delimited JSON requests (see
the plan, section 5, for the protocol). All state mutation happens here --
the CLI is a thin client that only ever talks to this process.

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

        self._wake = asyncio.Event()
        self._current_proc: asyncio.subprocess.Process | None = None
        self._current_job_id: int | None = None
        self._cancel_requested = False

    # -- setup -----------------------------------------------------------

    def _connect(self) -> None:
        self.conn = db.connect(self.db_path)
        db.init_db(self.conn)

    def _recover_on_startup(self) -> None:
        """A row still marked 'running' from a previous daemon instance
        cannot actually still be running -- we just started. Mark it
        failed rather than silently losing it or re-running it."""
        assert self.conn is not None
        running = db.get_running(self.conn)
        if running is not None:
            db.mark_finished(
                self.conn,
                running["id"],
                status="failed",
                note="interrupted by daemon restart",
            )

    # -- worker loop -------------------------------------------------------

    async def worker_loop(self) -> None:
        assert self.conn is not None
        while True:
            self._wake.clear()
            if not db.get_paused(self.conn):
                queue = db.get_queue(self.conn)
                if queue:
                    await self._run_job(queue[0])
                    continue
            # idle: wait for something to wake us (new job, resume, etc.),
            # but also poll periodically as a safety net
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pass

    def _wake_worker(self) -> None:
        self._wake.set()

    async def _run_job(self, job: dict[str, Any]) -> None:
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
        self._current_proc = proc
        self._current_job_id = job_id
        self._cancel_requested = False

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
            self._current_proc = None
            self._current_job_id = None
            return

        await log_task

        if self._cancel_requested:
            status = "cancelled"
        else:
            status = "done" if exit_code == 0 else "failed"
        db.mark_finished(self.conn, job_id, status=status, exit_code=exit_code)
        self._current_proc = None
        self._current_job_id = None

    async def _pump_log(self, proc: asyncio.subprocess.Process, log_path: Path) -> None:
        """Stream the job's combined stdout/stderr to its log file, capped
        at MAX_LOG_BYTES with a single truncation marker line."""
        written = 0
        truncated = False
        with open(log_path, "ab") as f:
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
                    f.write(b"\n[queuer] log truncated at 50MB\n")
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
        cwd = args["cwd"]
        if not argv:
            raise ValueError("empty command")

        resolved = resolve_command(argv, cwd)
        exe = resolved[0]
        if not (os.path.isfile(exe) and os.access(exe, os.X_OK)):
            raise ValueError(f"executable not found or not executable: {exe}")

        # log_path needs the row's own id, which we don't have until after
        # insert -- write a placeholder, then patch it in immediately
        # (no await happens in between, so this is effectively atomic).
        assert self.conn is not None
        job_id = db.enqueue(
            self.conn,
            raw_cmd=argv,
            resolved_cmd=resolved,
            cwd=cwd,
            log_path="pending",
            timeout_secs=args.get("timeout_secs"),
            before=args.get("before"),
            after=args.get("after"),
        )
        log_path = str(self.log_dir / f"{job_id}.log")
        self.conn.execute("UPDATE jobs SET log_path = ? WHERE id = ?", (log_path, job_id))
        self.conn.commit()

        self._wake_worker()
        return {"id": job_id}

    async def _cmd_list(self, args: dict[str, Any]) -> dict[str, Any]:
        assert self.conn is not None
        return {
            "running": db.get_running(self.conn),
            "queue": db.get_queue(self.conn),
            "backlog": db.get_backlog(self.conn),
            "paused": db.get_paused(self.conn),
        }

    async def _cmd_status(self, args: dict[str, Any]) -> dict[str, Any]:
        assert self.conn is not None
        return {"running": db.get_running(self.conn), "paused": db.get_paused(self.conn)}

    async def _cmd_show(self, args: dict[str, Any]) -> dict[str, Any]:
        assert self.conn is not None
        job = db.get_job(self.conn, args["id"])
        if job is None:
            raise ValueError(f"job {args['id']} does not exist")
        return job

    async def _cmd_rm(self, args: dict[str, Any]) -> dict[str, Any]:
        assert self.conn is not None
        db.remove_from_queue(self.conn, args["id"])
        return {"removed": args["id"]}

    async def _cmd_cancel(self, args: dict[str, Any]) -> dict[str, Any]:
        if self._current_proc is None or self._current_job_id is None:
            raise ValueError("no job is currently running")
        self._cancel_requested = True
        job_id = self._current_job_id
        await self._kill_process_group(self._current_proc)
        return {"cancelled": job_id}

    async def _cmd_requeue(self, args: dict[str, Any]) -> dict[str, Any]:
        assert self.conn is not None
        new_id = db.requeue(self.conn, args["id"], log_path="pending")
        log_path = str(self.log_dir / f"{new_id}.log")
        self.conn.execute("UPDATE jobs SET log_path = ? WHERE id = ?", (log_path, new_id))
        self.conn.commit()
        self._wake_worker()
        return {"id": new_id}

    async def _cmd_pause(self, args: dict[str, Any]) -> dict[str, Any]:
        assert self.conn is not None
        db.set_paused(self.conn, True)
        return {"paused": True}

    async def _cmd_resume(self, args: dict[str, Any]) -> dict[str, Any]:
        assert self.conn is not None
        db.set_paused(self.conn, False)
        self._wake_worker()
        return {"paused": False}

    # -- socket server ------------------------------------------------------

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            line = await reader.readline()
            if not line:
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

        server = await asyncio.start_unix_server(self._handle_client, path=str(self.socket_path))
        worker_task = asyncio.create_task(self.worker_loop())

        async with server:
            await asyncio.gather(server.serve_forever(), worker_task)


def main() -> None:
    daemon = Daemon()
    try:
        asyncio.run(daemon.serve())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
