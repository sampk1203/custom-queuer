"""Host watcher: polls workers over ssh and announces their jobs (QUEUER_REMOTE_PLAN.md, phase 4).

Lives in `queuerd` on the host: one asyncio task per enabled host in hosts.json, each calling
`qur remote events --after CURSOR --json` every `poll_secs` through `remote.py` (in a thread).

- The cursor, the worker's database epoch and reachability live in the `remote_hosts` table. A
  new host starts at the worker's current `last_seq`, so its history never floods Telegram.
- A different epoch, or a `last_seq` below the cursor, means the worker database was reset: the
  cursor jumps to the end with a warning, no notification storm. If the worker pruned events we
  had not read (`earliest_seq > cursor + 1`) one "some events were missed" note goes out.
- For each event: the job snapshot (with the worker's stats: peak RSS, CPU, duration, exit code,
  folder) is upserted into `remote_jobs`, then the host announces it under the worker's id
  UNLESS the worker already did (`telegram_sent`). The cursor moves after each successful send
  (at-least-once; a duplicate after a crash is acceptable).
- More than QUEUER_REMOTE_BURST (8) announceable events in one poll -> one summary message, then
  individual messages only for failed jobs.
- Unreachable for QUEUER_REMOTE_UNREACHABLE_SECS (600, counted from the first failed poll of this
  daemon run, so a host that slept is not reported) -> one alert, one "reachable again";
  backoff 15 s -> 5 min meanwhile. A protocol mismatch marks the host `incompatible`, alerts once
  and only probes it cheaply until it is fixed.

Events describe a worker's *own* jobs only, so two qurs watching each other cannot loop.
"""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from typing import Any

from queuer import db, hostcfg, jobenv, notify, remote, remote_notify, remote_pull

PAGE = 100  # events per ssh call
MAX_PAGES = 20  # per poll (2000 events = the worker's default retention)
RELOAD_SECS = 30  # how often hosts.json is re-read for added / removed / disabled hosts
MAX_BACKOFF_SECS = 300
PULL_ATTEMPTS = 3  # auto-pull tries per run (one per poll), then "giving up"
SEND_ATTEMPTS = 3  # a message that still fails after this many polls is dropped so the cursor can move
TRANSPORT_ERRORS = (remote.HostUnreachable, remote.AuthFailed, remote.RemoteQurMissing, remote.RemoteError)


def _env_int(name: str, default: int) -> int:
    try:
        return max(1, int(os.environ.get(name, "")))
    except ValueError:
        return default


class RemoteWatcher:
    def __init__(self, daemon: Any) -> None:
        self.daemon = daemon
        self.burst = _env_int("QUEUER_REMOTE_BURST", 8)
        self.unreachable_after = _env_int("QUEUER_REMOTE_UNREACHABLE_SECS", 600)
        self._pulling: dict[str, asyncio.Task[None]] = {}  # one auto-pull task per host
        self._first_fail: dict[str, str] = {}
        self._send_fails: dict[tuple[str, int], int] = {}
        self._summarised: dict[str, int] = {}

    # ---- database (the daemon's connection; only touched on the event-loop thread) ----

    @property
    def conn(self) -> sqlite3.Connection:
        assert self.daemon.conn is not None
        return self.daemon.conn

    def _row(self, name: str) -> dict[str, Any] | None:
        r = self.conn.execute("SELECT * FROM remote_hosts WHERE name = ?", (name,)).fetchone()
        return dict(r) if r else None

    def _save(self, name: str, **fields: Any) -> None:
        self.conn.execute("INSERT OR IGNORE INTO remote_hosts (name) VALUES (?)", (name,))
        if fields:
            sets = ", ".join(f"{k} = ?" for k in fields)
            self.conn.execute(f"UPDATE remote_hosts SET {sets} WHERE name = ?", (*fields.values(), name))  # nosec B608 - names are our own
        self.conn.commit()

    def _upsert_job(self, name: str, job: dict[str, Any]) -> None:
        if job.get("env_extra"):  # an older or hostile worker may still send values: keep names only
            job = {**job, "env_extra": jobenv.mask_env_extra(job["env_extra"])}
        job = db.limit_payload(job)  # a worker row of any size must not bloat the host database
        self.conn.execute(
            "INSERT INTO remote_jobs (host, remote_id, status, snapshot, seen_at) VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(host, remote_id) DO UPDATE SET status = excluded.status, snapshot = excluded.snapshot, seen_at = excluded.seen_at",
            (name, int(job["id"]), job.get("status"), json.dumps(job), db._now()),
        )
        self.conn.commit()

    # ---- sending ----

    async def send(self, text: str, job_id: int | None = None, machine: str | None = None, epoch: str | None = None) -> bool:
        """Send through every connected bot; True when delivered (or when there is nothing to send to).
        With `job_id` and `machine` the message carries the worker's Requeue / Details / Logs buttons
        (phase 5), on bots that listen for button presses."""
        cfg = notify.load(self.daemon.notify_path)
        if cfg is None or not any(b["chat_ids"] for b in notify.bot_cfgs(cfg)):
            return True
        try:
            markup_for = None
            if job_id is not None and machine:
                markup_for = lambda bcfg: remote_notify.job_buttons(machine, job_id, epoch) if notify.listening(bcfg) else None  # noqa: E731
            errors = await asyncio.to_thread(notify.send_all, cfg, text, markup_for)
        except Exception as e:  # nothing here may take the daemon down
            errors = [f"{type(e).__name__}: {e}"]
        for err in errors:
            print(f"[queuerd] remote notification failed: {err}", file=sys.stderr)
        return not errors

    # ---- ssh ----

    async def _events(self, host: dict[str, Any], after: int, limit: int) -> dict[str, Any]:
        data = await asyncio.to_thread(
            remote.ssh_json, host, ["remote", "events", "--after", str(after), "--limit", str(limit), "--json"], timeout=60
        )
        if not isinstance(data, dict) or "epoch" not in data:
            raise remote.RemoteError(0, "the worker's event feed has an unexpected shape")
        return data

    # ---- one poll ----

    async def poll_once(self, name: str) -> bool:
        """One poll of one host. True = the worker answered (or there is nothing to do)."""
        host = hostcfg.get(name)
        if host is None or not host["enabled"]:
            return True
        machine = remote_notify.label(host)
        st = self._row(name)
        if st and st["incompatible"]:
            try:
                await asyncio.to_thread(remote.ssh_json, host, ["remote", "info", "--json"], timeout=30)
            except remote.ProtocolMismatch:
                return False
            except TRANSPORT_ERRORS as e:
                return await self._failure(name, machine, st, e)
            self._save(name, incompatible=0)  # fixed: carry on below
            st = self._row(name)
        try:
            if st is None or st["epoch"] is None:  # first contact: start at the end; only our own runs are announced
                first, events = await self._read(host, 0)
                self._save(name, epoch=first["epoch"], cursor=0)
                await self._ok(name, machine, self._row(name))
                await self._catch_up(name, machine, first, events)
                return True
            cursor = int(st["cursor"])
            first, events = await self._read(host, cursor)
        except remote.ProtocolMismatch as e:
            self._save(name, incompatible=1, last_error=str(e))
            await self.send(remote_notify.incompatible_text(machine, str(e)))
            return False
        except TRANSPORT_ERRORS as e:
            return await self._failure(name, machine, st, e)
        await self._ok(name, machine, st)
        if first["epoch"] != st["epoch"] or int(first["last_seq"]) < cursor:
            print(f"[queuerd] worker {name}: its database was reset; resuming at its end", file=sys.stderr)
            self._save(name, epoch=first["epoch"], cursor=0)
            try:
                first, events = await self._read(host, 0)
            except TRANSPORT_ERRORS:
                self._save(name, epoch=first["epoch"], cursor=int(first["last_seq"]))
                return True
            await self._catch_up(name, machine, first, events)
            return True
        if first["earliest_seq"] and int(first["earliest_seq"]) > cursor + 1:
            await self.send(remote_notify.missed_text(machine))
        await self._process(name, machine, events)
        return True

    async def _read(self, host: dict[str, Any], after: int) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        """Events after `after` (up to MAX_PAGES pages): (the first page's header, all events)."""
        events: list[dict[str, Any]] = []
        first: dict[str, Any] | None = None
        for _ in range(MAX_PAGES):
            data = await self._events(host, after, PAGE)
            first = first or data
            events += data["events"]
            if len(data["events"]) < PAGE:
                break
            after = int(data["events"][-1]["seq"])
        assert first is not None
        return first, events

    async def _catch_up(self, name: str, machine: str, first: dict[str, Any], events: list[dict[str, Any]]) -> None:
        """First contact / database reset: skip the worker's history, but still process the final events of
        jobs this host sent itself (remote_runs), so a `qur run --pull` that finished before the watcher
        reached the worker is announced and pulled. Then the cursor goes to the end."""
        mine = {int(r["remote_id"]) for r in self.conn.execute(
            "SELECT remote_id FROM remote_runs WHERE host = ? AND remote_id IS NOT NULL AND pulled_path IS NULL", (name,))}
        wanted = [
            e for e in events
            if e.get("kind") == "final" and ((e.get("job") or {}).get("id") in mine or (e.get("job") or {}).get("root_id") in mine)
        ]
        if wanted:
            await self._process(name, machine, wanted)
        self._save(name, cursor=int(first["last_seq"]))

    async def _process(self, name: str, machine: str, events: list[dict[str, Any]]) -> None:
        announce = [e for e in events if not e.get("telegram_sent")]
        burst = len(announce) > self.burst
        if burst:
            finals = [e["job"] for e in announce if e["kind"] == "final"]
            last = int(announce[-1]["seq"])
            if finals and self._summarised.get(name) != last:
                if not await self.send(remote_notify.burst_text(machine, finals)):
                    return
                self._summarised[name] = last
        for ev in events:
            seq, job = int(ev["seq"]), ev.get("job") or {}
            if ev["kind"] == "final" and "id" in job:
                self._upsert_job(name, job)
            if not ev.get("telegram_sent") and not (burst and ev["kind"] == "final" and job.get("status") != "failed"):
                paused = ev["kind"] == "paused"
                text = remote_notify.paused_text(job, machine) if paused else remote_notify.job_text(job, machine)
                buttons = isinstance(job.get("id"), int) and not paused
                if not await self.send(text, job["id"] if buttons else None, machine, (self._row(name) or {}).get("epoch")):
                    n = self._send_fails[(name, seq)] = self._send_fails.get((name, seq), 0) + 1
                    if n < SEND_ATTEMPTS:
                        return  # retried at the next poll; the cursor stays here
                    print(f"[queuerd] giving up on the message for {name} event {seq}", file=sys.stderr)
            self._send_fails.pop((name, seq), None)
            self._save(name, cursor=seq)

    # ---- auto-pull (phase 6) ----

    def _pending_pulls(self, name: str) -> list[dict[str, Any]]:
        """Runs recorded with --pull whose job ended done or failed (as the watcher saw it), not yet
        pulled and with attempts left. Each row: the remote_runs columns plus `status` and `snapshot`."""
        runs = self.conn.execute(
            "SELECT * FROM remote_runs WHERE host = ? AND pull = 1 AND pulled_path IS NULL AND pull_attempts < ? "
            "AND remote_id IS NOT NULL ORDER BY remote_id",
            (name, PULL_ATTEMPTS),
        ).fetchall()
        if not runs:
            return []
        jobs = [dict(j) for j in self.conn.execute(
            "SELECT remote_id, status, snapshot FROM remote_jobs WHERE host = ? AND status IN ('done', 'failed')", (name,))]
        snaps: dict[int, dict[str, Any]] = {}
        for j in jobs:
            try:
                snaps[j["remote_id"]] = json.loads(j["snapshot"] or "{}")
            except ValueError:
                snaps[j["remote_id"]] = {}
        for j in jobs:
            snap = snaps[j["remote_id"]]
            root = snap.get("root_id")
            if root is None:  # a worker that sends no root_id: walk the retry chain through the cached rows
                cur, seen = snap, {j["remote_id"]}
                while cur.get("origin") == "retry" and isinstance(cur.get("parent_id"), int) and cur["parent_id"] not in seen:
                    root = cur["parent_id"]
                    seen.add(root)
                    cur = snaps.get(root, {})
            j["root"] = root
        out = []
        for r in runs:
            # the run records the FIRST attempt's id; with --retries the final event belongs to the last
            # attempt, which names the first one as `root_id`
            hits = [j for j in jobs if j["remote_id"] == r["remote_id"] or j["root"] == r["remote_id"]]
            if hits:
                last = max(hits, key=lambda j: j["remote_id"])
                out.append({**dict(r), "job_status": last["status"], "snapshot": last["snapshot"], "final_id": last["remote_id"]})
        return out

    def _start_pulls(self, name: str) -> None:
        """Start the host's pull task unless one is still running (one pull per host at a time; a
        long download never blocks the poll loop)."""
        task = self._pulling.get(name)
        if task is not None and not task.done():
            return
        rows = self._pending_pulls(name)
        if rows:
            self._pulling[name] = asyncio.create_task(self._pull_worker(name, rows))

    async def _pull_worker(self, name: str, rows: list[dict[str, Any]]) -> None:
        host = hostcfg.get(name)
        if host is None:
            return
        machine = remote_notify.label(host)
        for run in rows:
            job_id = int(run.get("final_id") or run["remote_id"])
            try:
                snap = json.loads(run.get("snapshot") or "{}")
            except ValueError:
                snap = {}
            job_dir = run.get("remote_dir") or snap.get("job_dir")
            try:
                res = await asyncio.to_thread(
                    remote_pull.pull_tarball, host, job_id, job_dir,
                    exclude=remote_pull.effective_excludes(host), unique=True,
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:  # any failure counts as an attempt; otherwise the same pull restarts every poll forever
                tries = int(run["pull_attempts"]) + 1
                self.conn.execute("UPDATE remote_runs SET pull_attempts = ?, updated_at = ? WHERE run_key = ?", (tries, db._now(), run["run_key"]))
                self.conn.commit()
                tail = "will retry at the next poll" if tries < PULL_ATTEMPTS else "giving up"
                await self.send(f"⚠ pulling {machine}:{job_id} failed ({remote_notify._first_line(str(e)) or type(e).__name__}); {tail}")
                continue
            self.conn.execute("UPDATE remote_runs SET pulled_path = ?, updated_at = ? WHERE run_key = ?", (str(res.path), db._now(), run["run_key"]))
            self.conn.commit()
            await self.send(f"📦 {machine}:{job_id} pulled to {res.path} ({remote_pull.human_size(res.size)})")

    # ---- reachability ----

    async def _failure(self, name: str, machine: str, st: dict[str, Any] | None, err: Exception) -> bool:
        now = datetime.now(timezone.utc)
        # downtime counts from the first failure seen by THIS daemon run, not from `last_ok_at`: that would
        # include the time the host itself was asleep or off and alert at once after waking
        since = self._first_fail.setdefault(name, now.isoformat())
        self._save(name, last_error=str(err)[:500], reachable=0)
        alerted = bool((st or {}).get("unreachable_alerted"))
        try:
            down = (now - datetime.fromisoformat(since).astimezone(timezone.utc)).total_seconds()
        except ValueError:
            down = 0
        if not alerted and down >= self.unreachable_after and await self.send(remote_notify.unreachable_text(machine, since)):
            self._save(name, unreachable_alerted=1)
        return False

    async def _ok(self, name: str, machine: str, st: dict[str, Any] | None) -> None:
        self._first_fail.pop(name, None)
        if st and st["unreachable_alerted"]:
            await self.send(remote_notify.reachable_text(machine))
        self._save(name, reachable=1, unreachable_alerted=0, last_error=None, last_ok_at=db._now())

    # ---- loops ----

    async def _host_loop(self, name: str) -> None:
        delay = 0.0
        while True:
            try:
                ok = await self.poll_once(name)
                if ok:
                    self._start_pulls(name)  # also retries earlier failures
            except asyncio.CancelledError:
                raise
            except Exception as e:  # one bad host never stops the others
                print(f"[queuerd] watcher for {name}: {type(e).__name__}: {e}", file=sys.stderr)
                ok = False
            host = hostcfg.get(name)
            poll = host["poll_secs"] if host else hostcfg.DEFAULT_POLL_SECS
            delay = poll if ok else min(MAX_BACKOFF_SECS, max(poll, delay * 2))
            await asyncio.sleep(delay)

    async def run(self) -> None:
        tasks: dict[str, asyncio.Task[None]] = {}
        try:
            while True:
                try:
                    hosts = hostcfg.load()["hosts"]
                except Exception as e:
                    print(f"[queuerd] hosts.json: {type(e).__name__}: {e}", file=sys.stderr)
                    hosts = {}
                want = {n for n, h in hosts.items() if h["enabled"]}
                for n in want - set(tasks):
                    tasks[n] = asyncio.create_task(self._host_loop(n))
                for n in set(tasks) - want:
                    tasks.pop(n).cancel()
                    pull = self._pulling.pop(n, None)
                    if pull is not None:
                        pull.cancel()
                    self._first_fail.pop(n, None)
                await asyncio.sleep(RELOAD_SECS)
        finally:
            for t in tasks.values():
                t.cancel()


def cached_jobs(host: str | None = None) -> list[dict[str, Any]]:
    """Cached worker jobs (what the watcher saw), newest first, read-only; never creates the database.
    Each row: host, remote_id, status, seen_at, snapshot (dict), stale (host currently unreachable)."""
    from queuer.paths import _default_db_path

    path = _default_db_path()
    if not path.exists():
        return []
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        try:
            down = {r["name"] for r in conn.execute("SELECT name FROM remote_hosts WHERE reachable = 0")}
            sql = "SELECT host, remote_id, status, snapshot, seen_at FROM remote_jobs"
            rows = conn.execute(sql + (" WHERE host = ?" if host else "") + " ORDER BY seen_at DESC, remote_id DESC",
                                (host,) if host else ()).fetchall()
        finally:
            conn.close()
    except sqlite3.Error:
        return []
    out = []
    for r in rows:
        try:
            snap = json.loads(r["snapshot"] or "{}")
        except ValueError:
            snap = {}
        out.append({"host": r["host"], "remote_id": r["remote_id"], "status": r["status"], "seen_at": r["seen_at"],
                    "snapshot": snap, "stale": r["host"] in down})
    return out

