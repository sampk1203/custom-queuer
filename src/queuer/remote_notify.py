"""Machine-aware Telegram texts and button handling for workers (QUEUER_REMOTE_PLAN.md, phases 4 and 5).

Phase 4: `notify.format_job` / `format_paused` take a `machine` label that replaces the "this host
[id]" part, so a worker's job reads like the host's own jobs but is labelled with the worker's id.
The text helpers send nothing; `remote_watch` does.

Phase 5: the Status / List keys, `/status vm3`, `/list vm3` and every button under a worker's
messages. A button's callback_data is `ACTION:MACHINE:N` where MACHINE is this host's qur id
(local) or a worker's id from hosts.json (`rq:vm3:42`); old callbacks are therefore still valid.
A worker's views are fetched on demand over ssh (`status --json`, `list --json`, `show --json`,
`logs -n`) in a thread with a timeout, after the callback was answered ("fetching..."); Requeue
and Cancel run `qur requeue ID` / `qur cancel ID` there (fixed argv, ids are ints). Everything a
worker returns is checked for shape, escaped and redacted like local text; a worker that does not
answer gives a plain "vm3 unreachable", never a stack trace.
"""

from __future__ import annotations

import asyncio
import html
import re
import socket
import sqlite3
import time
from collections import Counter
from datetime import datetime
from typing import Any, Awaitable, Callable

from queuer import db, hostcfg, notify, redact, remote

FETCH_TIMEOUT_SECS = 20.0  # one ssh call for a button press
SECTION_TIMEOUT_SECS = 12.0  # per worker in the host's Status
MAX_JOB_ID = 2**62
REQUEUE_GUARD_SECS = 30  # same double-tap guard as a local Requeue
LOCAL_WORDS = frozenset({"here", "local", "host"})


def label(host: dict[str, Any]) -> str:
    """How a worker is named in messages: its id from hosts.json."""
    return str(host["id"])


def job_text(job: dict[str, Any], machine: str) -> str:
    return notify.format_job(job, {}, machine=machine)


def paused_text(job: dict[str, Any], machine: str) -> str:
    return notify.format_paused(job, {}, machine=machine)


def burst_text(machine: str, jobs: list[dict[str, Any]]) -> str:
    """"14 jobs finished on vm3 while the host was away: 11 done, 3 failed"."""
    counts = Counter(str(j.get("status")) for j in jobs)
    order = ["done", "failed", "cancelled", "interrupted", "skipped"]
    parts = [f"{counts[s]} {s}" for s in order if counts.get(s)]
    parts += [f"{n} {s}" for s, n in sorted(counts.items()) if s not in order]
    return f"{len(jobs)} jobs finished on {machine} while the host was away: " + ", ".join(parts)


def missed_text(machine: str) -> str:
    return f"⚠ some events on {machine} were missed (the worker pruned them before this host polled)"


def unreachable_text(machine: str, since_iso: str) -> str:
    try:
        when = datetime.fromisoformat(since_iso).astimezone().strftime("%H:%M")
    except ValueError:
        when = since_iso
    return f"⚠ {machine} unreachable since {when}"


def reachable_text(machine: str) -> str:
    return f"✅ {machine} reachable again"


def incompatible_text(machine: str, detail: str) -> str:
    return f"⚠ {machine}: incompatible queuer version, not polling it until fixed ({detail})"


# ---------------------------------------------------------------------------
# phase 5: Telegram per machine
# ---------------------------------------------------------------------------

_requeued: dict[tuple[str, int], tuple[float, int | None]] = {}  # (machine, job) -> (when, new id)
_NEW_ID_RE = re.compile(r"new job (\d+)")


def enabled_hosts() -> list[dict[str, Any]]:
    """The enabled workers in hosts.json (empty when the file is missing or unusable)."""
    try:
        hosts = hostcfg.load()["hosts"]
    except Exception:  # nothing about a Telegram press may raise on a bad config
        return []
    local = notify.load()
    local_id = local["qur_id"] if local else None
    # a worker whose id equals this machine's qur id is unusable here: its buttons would act on local jobs
    return [{**h, "name": n} for n, h in sorted(hosts.items()) if h["enabled"] and h["id"] != local_id]


def host_by_id(machine: str) -> dict[str, Any] | None:
    """The worker whose id is `machine` (an id of a disabled worker still resolves: old buttons)."""
    try:
        hosts = hostcfg.load()["hosts"]
    except Exception:
        return None
    for n, h in sorted(hosts.items()):
        if h["id"] == machine:
            return {**h, "name": n}
    return None


def job_buttons(machine: str, job_id: int, epoch: str | None = None) -> dict[str, Any]:
    """Requeue / Details / Logs under a worker's finished-job message; `epoch` ties them to the worker's database."""
    return notify.job_markup({"qur_id": machine, "epoch": epoch}, job_id)


def epoch_ok(daemon: Any, machine: str, tag: str | None) -> bool:
    """False when a button carries an epoch tag that is not the one the watcher last saw on that worker
    (the worker's database was reset: its job ids may now mean other jobs). Untagged old buttons pass."""
    if tag is None:
        return True
    host = host_by_id(machine)
    if host is None or daemon.conn is None:
        return True  # an unknown machine is refused further on, with its own message
    row = daemon.conn.execute("SELECT epoch FROM remote_hosts WHERE name = ?", (host["name"],)).fetchone()
    return not (row and row[0] and not str(row[0]).startswith(tag))


def picker_markup(local_id: str, hosts: list[dict[str, Any]], action: str) -> dict[str, Any]:
    """One button per machine (this host first), 3 per row; `action` is "ps" (status) or "pl" (list)."""
    btns = [{"text": f"🖥 {socket.gethostname()}"[:30], "callback_data": f"{action}:{local_id}:0"}]
    btns += [{"text": str(h["id"]), "callback_data": f"{action}:{h['id']}:0"} for h in hosts if h["id"] != local_id]
    return {"inline_keyboard": [btns[i:i + 3] for i in range(0, len(btns), 3)]}


def _first_line(text: str, limit: int = 150) -> str:
    line = next((ln.strip() for ln in text.splitlines() if ln.strip()), "")
    line = redact.redact_text(line)
    return line if len(line) <= limit else line[: limit - 1] + "…"


def problem_text(machine: str, err: Exception) -> str:
    """One plain line for a worker that cannot be used right now."""
    if isinstance(err, (remote.HostUnreachable, remote.AuthFailed)):
        return f"⚠ {machine} unreachable"
    if isinstance(err, remote.RemoteError):
        return f"⚠ {machine}: {_first_line(str(err)) or 'the command failed'}"
    if isinstance(err, asyncio.TimeoutError):
        return f"⚠ {machine} unreachable (no answer)"
    return f"⚠ {machine}: {_first_line(str(err)) or type(err).__name__}"


async def _run(host: dict[str, Any], argv: list[str], timeout: float = FETCH_TIMEOUT_SECS) -> tuple[int, bytes, str]:
    return await asyncio.to_thread(remote.ssh_run, host, argv, timeout=timeout)


async def _json(host: dict[str, Any], argv: list[str], timeout: float = FETCH_TIMEOUT_SECS) -> Any:
    return await asyncio.to_thread(remote.ssh_json, host, argv, timeout=timeout)


def _is_id(value: Any) -> bool:
    return type(value) is int and 0 <= value <= MAX_JOB_ID


def _clean_job(job: Any) -> dict[str, Any] | None:
    """A job row from a worker if it has what the formatters and buttons rely on, else None."""
    if not isinstance(job, dict) or not _is_id(job.get("id")) or not isinstance(job.get("status"), str):
        return None
    return job


def clean_overview(data: Any) -> dict[str, Any]:
    """`status --json` / `list --json` from a worker with int channel keys and only well-formed jobs.
    Raises remote.RemoteError when the answer has the wrong shape."""
    if not isinstance(data, dict) or not isinstance(data.get("channels"), dict):
        raise remote.RemoteError(0, "unexpected answer")
    channels: dict[int, dict[str, Any]] = {}
    for key, chan in data["channels"].items():
        if not isinstance(chan, dict) or not str(key).isdecimal():
            continue
        out: dict[str, Any] = {"paused": bool(chan.get("paused"))}
        for one in ("running", "frozen"):
            out[one] = _clean_job(chan.get(one))
        for many in ("queue", "priority_queue", "backlog"):
            rows = chan.get(many) or []
            out[many] = [j for j in (_clean_job(r) for r in rows) if j] if isinstance(rows, list) else []
        channels[int(key)] = out
    return {"channels": channels}


def summarize(data: dict[str, Any]) -> tuple[int, int]:
    """(running, queued) over every channel of a cleaned overview."""
    running = sum(1 for c in data["channels"].values() if c.get("running") or c.get("frozen"))
    queued = sum(len(c.get("queue") or []) + len(c.get("priority_queue") or []) for c in data["channels"].values())
    return running, queued


def workers_text(rows: list[tuple[str, tuple[int, int] | str]]) -> str:
    """The worker sections under the host's Status: counts, or why the worker is not answering."""
    lines = []
    for machine, what in rows:
        body = what if isinstance(what, str) else f"▶️ {what[0]} running · ⏳ {what[1]} queued"
        lines.append(f"<b>{html.escape(machine)}</b> · {html.escape(body)}")
    return "\n".join(lines)


async def _section(host: dict[str, Any]) -> tuple[str, tuple[int, int] | str]:
    machine = label(host)
    try:
        data = clean_overview(await asyncio.wait_for(_json(host, ["list", "--json"], SECTION_TIMEOUT_SECS), SECTION_TIMEOUT_SECS + 5))
    except (remote.RemoteTransportError, asyncio.TimeoutError) as e:
        down = isinstance(e, (remote.HostUnreachable, remote.AuthFailed, asyncio.TimeoutError))
        return machine, "⚠ unreachable" if down else "⚠ " + (_first_line(str(e)) or "error")
    return machine, summarize(data)


async def _say(to_chat: dict[str, Any], text: str, markup: dict[str, Any] | None = None, mode: str | None = None) -> None:
    await asyncio.to_thread(notify.send, to_chat, text, markup, mode)


async def menu(daemon: Any, cfg: dict[str, Any], to_chat: dict[str, Any], action: str, arg: str | None) -> bool:
    """The Status / List keys and `/status [machine]`, `/list [machine]`. False = no worker is
    configured and none was named: the caller does what it always did."""
    hosts = enabled_hosts()
    if arg is None and not hosts:
        return False
    local_id = cfg["qur_id"]
    if arg is None:
        title = "📊 Status: pick a machine" if action == "status" else "📋 List: pick a machine"
        await _say(to_chat, title, picker_markup(local_id, hosts, "ps" if action == "status" else "pl"))
        return True
    host = None if arg == local_id or arg.lower() in LOCAL_WORDS else host_by_id(arg)
    if host is None and not (arg == local_id or arg.lower() in LOCAL_WORDS):
        known = ", ".join([local_id, *[str(h["id"]) for h in hosts]])
        await _say(to_chat, f"unknown machine {_first_line(arg, 40)!r} (known: {known})")
        return True
    if host is None:
        await _local_view(daemon, cfg, to_chat, action, hosts)
    else:
        await _worker_view(host, cfg, to_chat, action)
    return True


async def _local_view(daemon: Any, cfg: dict[str, Any], to_chat: dict[str, Any], action: str, hosts: list[dict[str, Any]]) -> None:
    if action == "status":
        data = await daemon._cmd_status({})
        text = notify.format_status(data, cfg)
        if hosts:
            rows = await asyncio.gather(*(_section(h) for h in hosts))
            text = f"{text}\n\n{workers_text(list(rows))}"[: notify.TG_MAX_CHARS - 40]
        await _say(to_chat, text, None, "HTML")
        return
    data = await daemon._cmd_list({})
    if not data["channels"]:
        await _say(to_chat, "nothing has ever been queued")
        return
    await _say(to_chat, "📋 List: pick a channel", notify.list_picker_markup(cfg, sorted(data["channels"])))


async def _worker_view(host: dict[str, Any], cfg: dict[str, Any], to_chat: dict[str, Any], action: str) -> None:
    machine = label(host)
    try:
        data = clean_overview(await _json(host, ["status" if action == "status" else "list", "--json"]))
    except (remote.RemoteTransportError, asyncio.TimeoutError) as e:
        await _say(to_chat, problem_text(machine, e))
        return
    if action == "status":
        await _say(to_chat, notify.format_status(data, cfg, machine), None, "HTML")
    elif not data["channels"]:
        await _say(to_chat, f"nothing has ever been queued on {machine}")
    else:
        await _say(to_chat, f"📋 List · {machine}: pick a channel", notify.list_picker_markup({"qur_id": machine}, sorted(data["channels"])))


Reply = Callable[[str], Awaitable[Any]]


async def handle_callback(daemon: Any, cfg: dict[str, Any], to_chat: dict[str, Any], cq: dict[str, Any], msg: dict[str, Any],
                          action: str, machine: str, job_id: int, reply: Reply) -> bool:
    """A button press for machine `machine` (a worker's id; for "ps" / "pl" also this host's).
    False = `machine` is unknown (the caller says so)."""
    local = machine == cfg["qur_id"]
    host = None if local else host_by_id(machine)
    if host is None and not local:
        return False
    if action in ("ps", "pl"):
        hosts = enabled_hosts()
        await reply("" if local and action == "pl" else "fetching…")
        if local:
            await _local_view(daemon, cfg, to_chat, "status" if action == "ps" else "list", hosts)
        else:
            assert host is not None
            await _worker_view(host, cfg, to_chat, "status" if action == "ps" else "list")
        return True
    assert host is not None
    if job_id > MAX_JOB_ID:
        await reply("not a valid job id")
        return True
    label_ = label(host)
    try:
        if action == "lc":
            await reply("fetching…")
            data = clean_overview(await _json(host, ["list", "--json"]))
            chan = data["channels"].get(job_id)
            if chan is None:
                await _say(to_chat, f"channel {job_id} has no jobs on {label_}")
                return True
            await _say(to_chat, notify.format_list(data, cfg, job_id, label_), notify.list_markup({"qur_id": label_}, chan), "HTML")
        elif action == "sh":
            await reply("fetching…")
            job = await _show(host, job_id)
            if job:
                await _say(to_chat, notify.format_details(job, cfg, label_), None, "HTML")
            else:
                await _say(to_chat, f"job {job_id} does not exist on {label_}")
        elif action == "lg":
            await reply("fetching…")
            job, (code, out, _err) = await asyncio.gather(_show(host, job_id), _run(host, ["logs", str(job_id), "-n", str(notify.LOG_TAIL_LINES)]))
            if job is None:
                await _say(to_chat, f"job {job_id} does not exist on {label_}")
                return True
            tail = notify.tidy_tail(out[-notify.LOG_READ_BYTES:], cut_start=len(out) > notify.LOG_READ_BYTES) if code == 0 else None
            await _say(to_chat, notify.format_log(job, tail, cfg, label_), None, "HTML")
        elif action == "cn":
            code, out, err = await _run(host, ["cancel", str(job_id)])
            if code != 0:
                await reply(_first_line(err) or "could not cancel")
                return True
            await reply(f"cancelled job {job_id}")  # the worker's own "cancelled" event follows through the watcher
            spent = notify.without_button(msg.get("reply_markup"), cq["data"])
            await asyncio.to_thread(notify.edit_markup, cfg, msg["chat"]["id"], msg["message_id"], spent)
        else:  # "rq" / "rl"
            await _requeue(host, cfg, to_chat, cq, msg, action, job_id, reply, daemon)
    except (remote.RemoteTransportError, asyncio.TimeoutError) as e:
        text = problem_text(label_, e)
        if action in ("cn", "rq", "rl"):
            await reply(text[:200])
        else:
            await _say(to_chat, text)  # the callback was already answered
    return True


async def _show(host: dict[str, Any], job_id: int) -> dict[str, Any] | None:
    """One job row from the worker, None when it has no such job."""
    try:
        data = await _json(host, ["show", str(job_id), "--json"])
    except remote.RemoteError as e:
        if e.exit_code == 2:  # the documented "no such job"; exit 1 is any other failure and must show as an error
            return None
        raise
    return _clean_job(data)


def _guard_key(machine: str, job_id: int) -> str:
    return f"requeued:{machine}:{job_id}"


def _guard_get(daemon: Any, machine: str, job_id: int) -> tuple[bool, int | None]:
    """(a requeue of this job was done within REQUEUE_GUARD_SECS, the new job id). The record lives in the
    daemon's database, so a daemon restart between two taps does not lose it."""
    conn = getattr(daemon, "conn", None)
    if conn is None:
        return False, None
    try:
        raw = db.get_state(conn, _guard_key(machine, job_id))
        if raw is None:
            return False, None
        when, _, new = raw.partition(":")
        if time.time() - float(when) > REQUEUE_GUARD_SECS:
            conn.execute("DELETE FROM daemon_state WHERE key = ?", (_guard_key(machine, job_id),))
            conn.commit()
            return False, None
        return True, int(new) if new.isdigit() else None
    except (sqlite3.Error, ValueError):
        return False, None


def _guard_set(daemon: Any, machine: str, job_id: int, new_id: int | None) -> None:
    conn = getattr(daemon, "conn", None)
    if conn is None:
        return
    try:
        db.set_state(conn, _guard_key(machine, job_id), f"{time.time()}:{new_id if new_id is not None else ''}")
    except sqlite3.Error:
        pass


async def _requeue(host: dict[str, Any], cfg: dict[str, Any], to_chat: dict[str, Any], cq: dict[str, Any], msg: dict[str, Any],
                   action: str, job_id: int, reply: Reply, daemon: Any = None) -> None:
    machine = label(host)
    now = time.monotonic()
    for key in [k for k, (t, _n) in _requeued.items() if now - t > REQUEUE_GUARD_SECS]:
        del _requeued[key]
    done = _requeued.get((machine, job_id))
    if done is None:
        persisted, persisted_new = _guard_get(daemon, machine, job_id)
        if persisted:
            done = (now, persisted_new)
    if done is not None:  # two quick taps reach us as two presses of the same old keyboard
        await reply(f"already requeued as job {done[1]}" if done[1] is not None else "already requeued")
        return
    code, out, err = await _run(host, ["requeue", str(job_id)])
    if code != 0:
        await reply(_first_line(err) or "could not requeue")  # e.g. the job has not finished yet
        return
    found = _NEW_ID_RE.search(out.decode("utf-8", "replace"))
    new_id = int(found.group(1)) if found else None
    _requeued[(machine, job_id)] = (now, new_id)
    _guard_set(daemon, machine, job_id, new_id)
    shown = f"job {new_id}" if new_id is not None else "a new job"
    await reply(f"requeued as {shown}")
    if action == "rl":  # from a list: drop only this job's button
        spent = notify.without_button(msg.get("reply_markup"), cq["data"])
    else:  # from a finished-job message: Details and Logs stay
        spent = notify.details_markup({"qur_id": machine, "epoch": notify.callback_epoch(cq.get("data"))}, job_id)
    await asyncio.to_thread(notify.edit_markup, cfg, msg["chat"]["id"], msg["message_id"], spent)
    await _say(to_chat, f"🔁 job {job_id} requeued as {shown}\n{machine}")
