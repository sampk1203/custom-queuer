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

from queuer import db, notify
from queuer.pathresolve import resolve_command

MAX_LOG_BYTES = 50 * 1024 * 1024  # 50MB per-job log cap
DEFAULT_GRACE_SECS = 5
LISTEN_IDLE_SECS = 5  # recheck config when Telegram listening is off
LISTEN_GAP_SECS = 0.1  # pause between polls
LISTEN_RETRY_SECS = 30  # back off after a failed poll (offline, 409, ...)


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
        notify_path: Path | None = None,
    ) -> None:
        self.db_path = db_path or _default_db_path()
        self.socket_path = socket_path or _default_socket_path()
        self.log_dir = log_dir or _default_log_dir()
        self.notify_path = notify_path or notify.default_path()
        self.conn: sqlite3.Connection | None = None

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

        # in-flight notification sends (kept so they aren't garbage-collected)
        self._notify_tasks: set[asyncio.Task] = set()
        self._tg_offsets: dict[str, int] = {}  # bot token -> next getUpdates offset

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
                self._notify_finished(running["id"])

    def _ensure_channel_worker(self, channel: int) -> None:
        """Lazily spawn a channel's worker loop if it isn't running yet."""
        if channel in self._worker_tasks:
            return
        self._wake[channel] = asyncio.Event()
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
            self._after_finish(job_id)
            return

        db.mark_running(self.conn, job_id, pid=proc.pid, pgid=proc.pid)
        self._current_proc[channel] = proc
        self._current_job_id[channel] = job_id

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
            self._cancelled.discard(job_id)
            self._release(channel, job_id)
            self._after_finish(job_id)
            return

        await log_task

        if job_id in self._cancelled:
            status = "cancelled"
            self._cancelled.discard(job_id)
        else:
            status = "done" if exit_code == 0 else "failed"
        db.mark_finished(self.conn, job_id, status=status, exit_code=exit_code)
        self._release(channel, job_id)
        self._after_finish(job_id)

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
        if not self._maybe_auto_retry(job_id):
            self._notify_finished(job_id)

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

    async def _telegram_listener(self) -> None:
        """Supervisor: one poller task per connected bot that has `listen`
        on, started and stopped as the config changes (re-read every few
        seconds). Steps aside while `qur connect` pairs. No error may
        escape."""
        pollers: dict[str, asyncio.Task] = {}
        try:
            while True:
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
        last_err: str | None = None
        while True:
            cfg = self._listening_bot(token)
            if cfg is None:
                return
            try:
                updates = await asyncio.to_thread(notify.get_updates, cfg, offset)
                last_err = None
            except Exception as e:
                err = f"{type(e).__name__}: {e}"
                if err != last_err:  # don't spam the journal while offline
                    print(f"[queuerd] telegram listener (bot {notify.bot_id(token)}): {err}", file=sys.stderr)
                    last_err = err
                await asyncio.sleep(LISTEN_RETRY_SECS)
                continue
            for update in updates:
                offset = self._tg_offsets[token] = update["update_id"] + 1
                try:
                    await self._handle_update(cfg, update)
                except Exception as e:
                    print(f"[queuerd] telegram update failed: {type(e).__name__}: {e}", file=sys.stderr)
            await asyncio.sleep(LISTEN_GAP_SECS)  # never hot-loop on instant empty replies

    async def _handle_update(self, cfg: dict[str, Any], update: dict[str, Any]) -> None:
        """A Requeue or Details button press that arrived on the bot `cfg`
        (its flat view). Only presses from a chat paired with that bot count,
        and replies go out through that same bot."""
        cq = update.get("callback_query")
        if not cq:
            return
        msg = cq.get("message") or {}
        chat_id = (msg.get("chat") or {}).get("id")
        parsed = notify.parse_callback(cq.get("data"))
        if parsed is None or chat_id not in cfg["chat_ids"]:
            return
        action, qur_id, job_id = parsed
        reply = lambda text: asyncio.to_thread(notify.answer_callback, cfg, cq["id"], text)  # noqa: E731
        if qur_id != cfg["qur_id"]:
            await reply("this message came from another machine or an old id")
            return
        to_chat = {**cfg, "chat_ids": [chat_id]}

        if action == "sh":
            assert self.conn is not None
            job = db.get_job(self.conn, job_id)
            if job is None:
                await reply(f"job {job_id} does not exist")
                return
            await reply("")
            await asyncio.to_thread(notify.send, to_chat, notify.format_details(job, cfg), None, "HTML")
            return

        try:
            new_id = await self._requeue_job(job_id)
        except ValueError as e:
            await reply(str(e))
            return
        await reply(f"requeued as job {new_id}")
        # Requeue is spent; Details stays
        await asyncio.to_thread(notify.edit_markup, cfg, chat_id, msg["message_id"], notify.details_markup(cfg, job_id))
        text = f"🔁 job {job_id} requeued as job {new_id}\n{notify._who(cfg)}"
        await asyncio.to_thread(notify.send, to_chat, text)

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
        max_retries = args.get("max_retries") or 0
        if not argv:
            raise ValueError("empty command")
        if not isinstance(max_retries, int) or max_retries < 0:
            raise ValueError("retries must be a non-negative integer")
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
        python_path = args.get("python_path")
        if python_path:
            env_extra["PYTHONPATH"] = python_path
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
            max_retries=max_retries,
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
        """Without `id`: kill the running job on `channel` (default 1), as
        before. With `id`: cancel that job wherever it is -- running, frozen
        by `--now`, or still queued (kept in history as `cancelled`). If
        `channel` is also given it must match the job's channel."""
        assert self.conn is not None
        job_id = args.get("id")
        channel = args.get("channel")

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
            db.cancel_queued(self.conn, job_id)
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
            listener = asyncio.create_task(self._telegram_listener())
            try:
                await asyncio.gather(server.serve_forever(), *self._worker_tasks.values())
            finally:
                listener.cancel()


def main() -> None:
    daemon = Daemon()
    try:
        asyncio.run(daemon.serve())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
