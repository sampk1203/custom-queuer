"""Phase 4: the host watcher (polling, cursor, announcements, reachability). No network: ssh_json is faked."""

import asyncio
import json
from types import SimpleNamespace

import pytest

from queuer import db, hostcfg, remote
from queuer.remote_watch import RemoteWatcher, cached_jobs


def job(i, status="done", **kw):
    return {"id": i, "status": status, "channel": 1, "raw_cmd": json.dumps(["lmp", "-in", "x"]), "exit_code": 0 if status == "done" else 1,
            "started_at": "2026-10-20T10:00:00+00:00", "finished_at": "2026-10-20T11:58:00+00:00",
            "max_rss_kb": 31 * 1024 * 1024, "cpu_user_secs": 7000.0, "cpu_sys_secs": 80.0, "job_dir": "/w/j", **kw}


class Worker:
    """A scripted worker: events list, epoch, optional failure."""

    def __init__(self):
        self.epoch, self.events, self.fail, self.earliest = "e1", [], None, None

    def ssh_json(self, host, argv, **kw):
        if self.fail:
            raise self.fail
        if argv[:2] == ["remote", "info"]:
            return {"protocol": 1}
        after, limit = int(argv[argv.index("--after") + 1]), int(argv[argv.index("--limit") + 1])
        ev = [e for e in self.events if e["seq"] > after][:limit]
        seqs = [e["seq"] for e in self.events]
        return {"protocol": 1, "epoch": self.epoch, "earliest_seq": self.earliest or (min(seqs) if seqs else 0),
                "last_seq": max(seqs) if seqs else 0, "events": ev}

    def add(self, seq, status="done", sent=False, kind="final"):
        self.events.append({"seq": seq, "kind": kind, "at": "x", "telegram_sent": sent, "job": job(seq, status)})


@pytest.fixture
def env(tmp_path, monkeypatch):
    conn = db.connect(tmp_path / "q.db")
    db.init_db(conn)
    daemon = SimpleNamespace(conn=conn, notify_path=tmp_path / "notify.json")
    cfg = hostcfg.load()
    cfg["hosts"]["vm3"] = {"ssh": "vm3", "id": "vm3"}
    hostcfg.save(cfg)
    w = Worker()
    monkeypatch.setattr(remote, "ssh_json", w.ssh_json)
    watcher = RemoteWatcher(daemon)
    sent = []

    async def send(text, job_id=None, machine=None, epoch=None):
        sent.append(text)
        return True

    watcher.send = send
    return SimpleNamespace(w=w, watcher=watcher, sent=sent, conn=conn)


def poll(env):
    return asyncio.run(env.watcher.poll_once("vm3"))


def test_new_host_starts_at_end_without_flooding(env):
    env.w.add(1)
    env.w.add(2)
    assert poll(env) and env.sent == []
    assert env.watcher._row("vm3")["cursor"] == 2


def test_finished_job_announced_once_with_worker_id_and_stats(env):
    poll(env)
    env.w.add(1)
    poll(env)
    poll(env)
    assert len(env.sent) == 1
    assert "vm3" in env.sent[0] and "job 1" in env.sent[0] and "31.0 GB" in env.sent[0]
    assert env.watcher._row("vm3")["cursor"] == 1
    assert cached_jobs is not None
    snap = json.loads(env.conn.execute("SELECT snapshot FROM remote_jobs WHERE host='vm3' AND remote_id=1").fetchone()[0])
    assert snap["max_rss_kb"] and snap["cpu_user_secs"] and snap["job_dir"] == "/w/j"


def test_worker_that_already_announced_stays_silent_but_is_cached(env):
    poll(env)
    env.w.add(1, sent=True)
    poll(env)
    assert env.sent == []
    assert env.conn.execute("SELECT COUNT(*) FROM remote_jobs").fetchone()[0] == 1


def test_paused_event_message(env):
    poll(env)
    env.w.add(1, "failed", kind="paused")
    poll(env)
    assert len(env.sent) == 1 and "paused" in env.sent[0] and "vm3" in env.sent[0]


def test_failed_send_keeps_cursor_and_retries(env):
    poll(env)
    env.w.add(1)
    results = iter([False, True])

    async def flaky(text, job_id=None, machine=None, epoch=None):
        ok = next(results)
        env.sent.append(text)
        return ok

    env.watcher.send = flaky
    poll(env)
    assert env.watcher._row("vm3")["cursor"] == 0
    poll(env)
    assert env.watcher._row("vm3")["cursor"] == 1


def test_database_reset_moves_cursor_without_messages(env):
    env.w.add(1)
    env.w.add(2)
    poll(env)
    env.w.epoch, env.w.events = "e2", []
    env.w.add(1)
    poll(env)
    assert env.sent == [] and env.watcher._row("vm3")["epoch"] == "e2" and env.watcher._row("vm3")["cursor"] == 1


def test_pruned_events_send_one_note(env):
    poll(env)
    env.w.add(5)
    env.w.earliest = 5
    poll(env)
    assert any("missed" in t for t in env.sent) and any("job 5" in t for t in env.sent)


def test_burst_gives_summary_then_failures_only(env):
    poll(env)
    for i in range(1, 13):
        env.w.add(i, "failed" if i in (3, 9) else "done")
    poll(env)
    assert "12 jobs finished on vm3" in env.sent[0] and "10 done" in env.sent[0] and "2 failed" in env.sent[0]
    assert len(env.sent) == 3 and env.watcher._row("vm3")["cursor"] == 12


def test_unreachable_alert_once_then_recovery_once(env):
    poll(env)
    env.watcher.unreachable_after = 0
    env.w.fail = remote.HostUnreachable("no route")
    assert poll(env) is False
    poll(env)
    poll(env)
    assert [t for t in env.sent if "unreachable" in t].__len__() == 1
    env.w.fail = None
    poll(env)
    poll(env)
    assert [t for t in env.sent if "reachable again" in t].__len__() == 1
    assert env.watcher._row("vm3")["reachable"] == 1


def test_protocol_mismatch_marks_incompatible_and_alerts_once(env):
    poll(env)
    env.w.fail = remote.ProtocolMismatch("worker speaks protocol 2")
    poll(env)
    poll(env)
    assert env.watcher._row("vm3")["incompatible"] == 1
    assert len([t for t in env.sent if "incompatible" in t]) == 1


def test_disabled_host_is_not_polled(env):
    cfg = hostcfg.load()
    cfg["hosts"]["vm3"]["enabled"] = False
    hostcfg.save(cfg)
    env.w.fail = remote.HostUnreachable("x")
    assert poll(env) is True and env.watcher._row("vm3") is None


# --- host-side views, labels, daemon check ------------------------------------

from typer.testing import CliRunner  # noqa: E402

from queuer import notify  # noqa: E402
from queuer.cli import app  # noqa: E402

runner = CliRunner()


def test_machine_label_replaces_this_hosts_name():
    text = notify.format_job(job(7), {}, machine="vm3")
    assert "vm3" in text and "job 7" in text and "[" not in text.splitlines()[-1].split("·")[0]
    assert "vm3" in notify.format_paused(job(7, "failed", channel=2), {}, machine="vm3")


def _cache(job_id=5, status="done", reachable=1):
    from queuer.paths import _default_db_path

    conn = db.connect(_default_db_path())
    db.init_db(conn)
    conn.execute("INSERT OR REPLACE INTO remote_hosts (name, epoch, cursor, reachable) VALUES ('vm3', 'e', 1, ?)", (reachable,))
    conn.execute("INSERT OR REPLACE INTO remote_jobs (host, remote_id, status, snapshot, seen_at) VALUES ('vm3', ?, ?, ?, '2026-10-20T10:00:00+00:00')",
                 (job_id, status, json.dumps(job(job_id, status))))
    conn.commit()
    conn.close()


def test_remote_list_reads_the_cache_and_marks_stale():
    _cache(reachable=0)
    r = runner.invoke(app, ["remote", "list", "--json"])
    rows = json.loads(r.output)
    assert r.exit_code == 0 and rows[0]["host"] == "vm3" and rows[0]["remote_id"] == 5 and rows[0]["stale"] is True
    assert rows[0]["snapshot"]["max_rss_kb"]
    assert runner.invoke(app, ["remote", "list", "vm9", "--json"]).output.strip() == "[]"


def test_show_falls_back_to_the_cached_snapshot_when_the_worker_is_down(fake_ssh):
    paths = fake_ssh.add_worker("vm3")
    cfg = hostcfg.load()
    cfg["hosts"]["vm3"] = {"ssh": "vm3", "jobs_dir": str(paths["QUEUER_JOBS_DIR"])}
    hostcfg.save(cfg)
    _cache()
    fake_ssh.set_mode("vm3", "unreachable")
    r = runner.invoke(app, ["show", "vm3:5", "--json"])
    data = json.loads(r.output)
    assert r.exit_code == 0 and data["stale"] is True and data["id"] == 5 and data["max_rss_kb"]
    r = runner.invoke(app, ["show", "vm3:5"])
    assert r.exit_code == 0 and "cached" in r.output and "job folder" in r.output
    r = runner.invoke(app, ["show", "vm3:99"])
    assert r.exit_code == 1 and "no cached snapshot" in r.output


def test_hosts_check_from_daemon_uses_the_daemon(monkeypatch):
    from queuer import client

    cfg = hostcfg.load()
    cfg["hosts"]["vm3"] = {"ssh": "vm3"}
    hostcfg.save(cfg)
    calls = []
    monkeypatch.setattr(client, "send", lambda cmd, **a: calls.append((cmd, a)) or {"ok": True, "detail": "the daemon reached vm3"})
    r = runner.invoke(app, ["hosts", "check", "vm3", "--from-daemon", "--json"])
    assert r.exit_code == 0 and calls == [("remote_check", {"host": "vm3"})]
    monkeypatch.setattr(client, "send", lambda cmd, **a: (_ for _ in ()).throw(client.DaemonNotRunning("queuerd is not running")))
    assert runner.invoke(app, ["hosts", "check", "vm3", "--from-daemon"]).exit_code == 1


# --- P0-3 gap tests (see 6.todo): fixed; the markers are gone ---------------

def _run_row(env, key="K1", remote_id=1, pull=1):
    env.conn.execute(
        "INSERT INTO remote_runs (run_key, host, kind, state, remote_id, pull, created_at, updated_at) "
        "VALUES (?, 'vm3', 'run', 'queued', ?, ?, 'x', 'x')", (key, remote_id, pull))
    env.conn.commit()


def test_pull_fires_for_a_run_whose_job_was_retried(env):
    poll(env)
    _run_row(env, remote_id=1)  # `qur run --pull --retries 1`: queued as job 1 ...
    # ... attempt 1 failed silently (it will be retried), attempt 2 is job 2 and ends done
    retry = job(2, "done", parent_id=1, origin="retry")
    env.w.events.append({"seq": 1, "kind": "final", "at": "x", "telegram_sent": True, "job": retry})
    poll(env)
    assert [r["run_key"] for r in env.watcher._pending_pulls("vm3")] == ["K1"]


def test_first_contact_still_sees_a_run_that_already_finished(env):
    env.w.add(1, "done", sent=True)  # finished before the watcher ever reached this worker
    env.w.add(2, "done")  # unrelated history
    _run_row(env, remote_id=1)
    poll(env)  # first contact
    assert [r["run_key"] for r in env.watcher._pending_pulls("vm3")] == ["K1"]
    assert not any("job 2" in t for t in env.sent)  # but still no flood of old history


def test_no_unreachable_alert_on_the_first_failed_poll_after_the_daemon_was_idle(env):
    from datetime import datetime, timedelta, timezone

    poll(env)
    slept = (datetime.now(timezone.utc) - timedelta(hours=8)).isoformat()
    env.conn.execute("UPDATE remote_hosts SET last_ok_at = ? WHERE name = 'vm3'", (slept,))
    env.conn.commit()
    env.w.fail = remote.HostUnreachable("network not up yet")
    assert poll(env) is False
    assert [t for t in env.sent if "unreachable" in t] == []
