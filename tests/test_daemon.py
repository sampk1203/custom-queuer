"""Integration tests for queuerd -- drives a real daemon instance over its
actual Unix socket, exercising the worker loop, process supervision, and
every command in the protocol. Uses temp db/socket/log paths per test via
the `daemon` fixture, never touches real state.

Requires: uv add --dev pytest-asyncio
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pytest
import pytest_asyncio

from queuer import db as db_module
from queuer import notify
from queuer.daemon import Daemon

CWD = "/tmp"


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

async def send(sock_path: Path, cmd: str, **args) -> dict:
    reader, writer = await asyncio.open_unix_connection(str(sock_path))
    writer.write((json.dumps({"cmd": cmd, "args": args}) + "\n").encode())
    await writer.drain()
    line = await reader.readline()
    writer.close()
    await writer.wait_closed()
    return json.loads(line.decode())


async def wait_for_status(sock_path: Path, job_id: int, statuses: set[str], timeout: float = 5.0) -> dict:
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        resp = await send(sock_path, "show", id=job_id)
        if resp["ok"] and resp["data"]["status"] in statuses:
            return resp["data"]
        await asyncio.sleep(0.05)
    raise AssertionError(f"job {job_id} did not reach {statuses} within {timeout}s")


def _ch(resp_data: dict, channel: int) -> dict:
    """`list`/`status` responses key channels by their JSON string form
    (e.g. "1", "2") since the wire protocol is JSON -- this looks a
    channel's section up by its int id."""
    return resp_data["channels"][str(channel)]


@pytest.fixture(autouse=True)
def _fast_listener_idle(monkeypatch):
    # autouse fixtures run before `daemon`, so its Telegram listener (idle
    # unless a test configures one) re-checks the config quickly
    monkeypatch.setattr("queuer.daemon.LISTEN_IDLE_SECS", 0.05)


@pytest_asyncio.fixture
async def daemon(tmp_path):
    d = Daemon(
        db_path=tmp_path / "queuer.db",
        socket_path=tmp_path / "queuer.sock",
        log_dir=tmp_path / "logs",
    )
    task = asyncio.create_task(d.serve())
    for _ in range(100):
        if d.socket_path.exists():
            break
        await asyncio.sleep(0.02)
    else:
        task.cancel()
        raise RuntimeError("daemon socket never appeared")

    yield d

    procs = list(d._current_proc.values())
    for proc in procs:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    # Reap the killed children while the loop is still open, so their transports are
    # closed here rather than garbage-collected after the loop is gone.
    for proc in procs:
        try:
            await asyncio.wait_for(proc.wait(), timeout=5)
        except (asyncio.TimeoutError, ProcessLookupError):
            pass

    # Stop the daemon through its normal shutdown, not task.cancel(): jobs still in flight
    # (an `echo` added at the very end of a test) get their pipes drained and closed by the
    # daemon itself. A bare cancel left their subprocess transports open, to be
    # garbage-collected after the loop closed (PytestUnraisableExceptionWarning: "Event loop
    # is closed", blamed on whichever later test happened to trigger the GC).
    d._stopping.set()
    try:
        await asyncio.wait_for(task, timeout=15)
    except asyncio.TimeoutError:
        pass  # wait_for already cancelled the task
    except asyncio.CancelledError:
        pass


# ---------------------------------------------------------------------------
# F10: _read_proc_usage (pure /proc parsing, no daemon needed)
# ---------------------------------------------------------------------------

def test_read_proc_usage_for_live_process():
    from queuer.daemon import _read_proc_usage

    proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(0.5)"])
    try:
        time.sleep(0.1)
        sample = _read_proc_usage(proc.pid)
        assert sample is not None
        rss_kb, user_secs, sys_secs = sample
        assert rss_kb is None or rss_kb > 0
        assert user_secs >= 0
        assert sys_secs >= 0
    finally:
        proc.terminate()
        proc.wait()


def test_read_proc_usage_for_gone_pid_returns_none():
    from queuer.daemon import _read_proc_usage

    proc = subprocess.Popen(["true"])
    proc.wait()  # reaped: its /proc entry is gone
    assert _read_proc_usage(proc.pid) is None


def test_read_proc_usage_for_this_process_has_a_peak_rss():
    from queuer.daemon import _read_proc_usage

    sample = _read_proc_usage(os.getpid())
    assert sample is not None
    rss_kb, user_secs, sys_secs = sample
    assert rss_kb is not None and rss_kb > 0  # VmHWM exists for a live process
    assert user_secs >= 0 and sys_secs >= 0


def test_read_proc_usage_handles_a_command_name_with_spaces_and_parens(monkeypatch, tmp_path):
    from queuer import daemon as daemon_mod

    # comm = "a) (b c", utime = 250 ticks, stime = 50 ticks (fields 14 and 15)
    stat_line = "4242 (a) (b c) S 1 4242 4242 0 -1 4194560 100 0 0 0 250 50 0 0 20 0 1 0 1000 1000 100 18446744073709551615"
    status = "Name:\ta\nVmPeak:\t   9000 kB\nVmHWM:\t    5120 kB\nVmRSS:\t    4000 kB\n"
    files = {"/proc/4242/stat": stat_line.encode(), "/proc/4242/status": status.encode()}
    real_open = open

    def fake_open(path, *a, **kw):
        if str(path) in files:
            p = tmp_path / str(path).replace("/", "_")
            p.write_bytes(files[str(path)])
            return real_open(p, *a, **kw)
        return real_open(path, *a, **kw)

    monkeypatch.setattr("builtins.open", fake_open)
    monkeypatch.setattr(daemon_mod, "_CLK_TCK", 100)
    assert daemon_mod._read_proc_usage(4242) == (5120, 2.5, 0.5)


def test_read_proc_usage_without_a_vmhwm_line_gives_no_rss(monkeypatch, tmp_path):
    from queuer import daemon as daemon_mod

    stat_line = "7 (x) S 1 7 7 0 -1 0 0 0 0 0 100 0 0 0 20 0 1 0 1 1 1 1"
    files = {"/proc/7/stat": stat_line.encode(), "/proc/7/status": b"Name:\tx\nState:\tZ (zombie)\n"}
    real_open = open

    def fake_open(path, *a, **kw):
        if str(path) in files:
            p = tmp_path / str(path).replace("/", "_")
            p.write_bytes(files[str(path)])
            return real_open(p, *a, **kw)
        return real_open(path, *a, **kw)

    monkeypatch.setattr("builtins.open", fake_open)
    monkeypatch.setattr(daemon_mod, "_CLK_TCK", 100)
    assert daemon_mod._read_proc_usage(7) == (None, 1.0, 0.0)


# ---------------------------------------------------------------------------
# add / basic execution
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_job_runs_to_completion(daemon):
    resp = await send(daemon.socket_path, "add", argv=["echo", "hello"], cwd=CWD)
    assert resp["ok"]
    job_id = resp["data"]["id"]

    job = await wait_for_status(daemon.socket_path, job_id, {"done", "failed"})
    assert job["status"] == "done"
    assert job["exit_code"] == 0

    log_path = Path(job["log_path"])
    assert log_path.exists()
    assert log_path.read_text() == "hello\n"


@pytest.mark.asyncio
async def test_raw_argv_stored_separately_from_resolved_argv(daemon):
    # `argv` (what actually runs) gets the interpreter pinned to an
    # absolute path; `raw_argv` (what the user typed) must be stored
    # untouched and shown as-is in `raw_cmd`, not clobbered by the pin.
    resp = await send(
        daemon.socket_path, "add",
        argv=["/bin/echo", "hello"],
        raw_argv=["echo", "hello"],
        cwd=CWD,
    )
    assert resp["ok"]
    job = await wait_for_status(daemon.socket_path, resp["data"]["id"], {"done", "failed"})
    assert json.loads(job["raw_cmd"]) == ["echo", "hello"]
    assert json.loads(job["resolved_cmd"])[0] == "/bin/echo"


@pytest.mark.asyncio
async def test_raw_argv_falls_back_to_argv_when_absent(daemon):
    # older clients that don't send raw_argv -- unchanged behavior.
    resp = await send(daemon.socket_path, "add", argv=["/bin/echo", "hi"], cwd=CWD)
    assert resp["ok"]
    job = await wait_for_status(daemon.socket_path, resp["data"]["id"], {"done", "failed"})
    assert json.loads(job["raw_cmd"]) == ["/bin/echo", "hi"]


@pytest.mark.asyncio
async def test_add_rejects_missing_executable(daemon):
    resp = await send(daemon.socket_path, "add", argv=["/bin/this-does-not-exist-xyz"], cwd=CWD)
    assert resp["ok"] is False


@pytest.mark.asyncio
async def test_add_rejects_empty_argv(daemon):
    resp = await send(daemon.socket_path, "add", argv=[], cwd=CWD)
    assert resp["ok"] is False


@pytest.mark.asyncio
async def test_nonzero_exit_marks_failed(daemon):
    resp = await send(daemon.socket_path, "add", argv=["sh", "-c", "exit 7"], cwd=CWD)
    job_id = resp["data"]["id"]
    job = await wait_for_status(daemon.socket_path, job_id, {"done", "failed"})
    assert job["status"] == "failed"
    assert job["exit_code"] == 7


# ---------------------------------------------------------------------------
# channels: default, explicit, isolation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_add_defaults_to_channel_one(daemon):
    resp = await send(daemon.socket_path, "add", argv=["echo", "hi"], cwd=CWD)
    assert resp["data"]["channel"] == 1
    job_id = resp["data"]["id"]
    job = await wait_for_status(daemon.socket_path, job_id, {"done", "failed"})
    assert job["channel"] == 1


@pytest.mark.asyncio
async def test_add_to_explicit_channel(daemon):
    resp = await send(daemon.socket_path, "add", argv=["echo", "hi"], cwd=CWD, channel=3)
    assert resp["data"]["channel"] == 3
    job_id = resp["data"]["id"]
    job = await wait_for_status(daemon.socket_path, job_id, {"done", "failed"})
    assert job["channel"] == 3


@pytest.mark.asyncio
async def test_channel_absent_from_list_until_first_job(daemon):
    listing = await send(daemon.socket_path, "list")
    assert listing["data"]["channels"] == {}

    await send(daemon.socket_path, "add", argv=["echo", "hi"], cwd=CWD, channel=2)
    await asyncio.sleep(0.1)

    listing = await send(daemon.socket_path, "list")
    assert "2" in listing["data"]["channels"]
    assert "3" not in listing["data"]["channels"]  # never touched, never appears


@pytest.mark.asyncio
async def test_job_ids_global_across_channels(daemon):
    a = await send(daemon.socket_path, "add", argv=["echo", "a"], cwd=CWD, channel=1)
    b = await send(daemon.socket_path, "add", argv=["echo", "b"], cwd=CWD, channel=2)
    c = await send(daemon.socket_path, "add", argv=["echo", "c"], cwd=CWD, channel=1)
    assert a["data"]["id"] < b["data"]["id"] < c["data"]["id"]


@pytest.mark.asyncio
async def test_channels_run_in_parallel_not_serially(daemon):
    # two 1.5s jobs on separate channels, added back to back -- if channels
    # were still one shared serial queue this would take ~3s; parallel
    # channels should finish in ~1.5s.
    start = time.monotonic()
    r1 = await send(daemon.socket_path, "add", argv=["sleep", "1.5"], cwd=CWD, channel=1)
    r2 = await send(daemon.socket_path, "add", argv=["sleep", "1.5"], cwd=CWD, channel=2)

    job1 = await wait_for_status(daemon.socket_path, r1["data"]["id"], {"done", "failed"}, timeout=5.0)
    job2 = await wait_for_status(daemon.socket_path, r2["data"]["id"], {"done", "failed"}, timeout=5.0)
    elapsed = time.monotonic() - start

    assert job1["status"] == "done"
    assert job2["status"] == "done"
    assert elapsed < 2.5  # well under the ~3s a serial run would take


@pytest.mark.asyncio
async def test_channel_is_serial_internally(daemon):
    # two jobs on the *same* channel must still run one at a time
    r1 = await send(daemon.socket_path, "add", argv=["sleep", "0.5"], cwd=CWD, channel=1)
    r2 = await send(daemon.socket_path, "add", argv=["echo", "second"], cwd=CWD, channel=1)
    await asyncio.sleep(0.1)

    listing = await send(daemon.socket_path, "list")
    chan = _ch(listing["data"], 1)
    assert chan["running"]["id"] == r1["data"]["id"]
    assert [j["id"] for j in chan["queue"]] == [r2["data"]["id"]]


# ---------------------------------------------------------------------------
# list / status / show
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_list_shows_running_and_queued(daemon):
    r1 = await send(daemon.socket_path, "add", argv=["sleep", "1"], cwd=CWD)
    r2 = await send(daemon.socket_path, "add", argv=["echo", "second"], cwd=CWD)
    await asyncio.sleep(0.2)  # let the worker pick up job 1

    listing = await send(daemon.socket_path, "list")
    assert listing["ok"]
    chan = _ch(listing["data"], 1)
    assert chan["running"]["id"] == r1["data"]["id"]
    queued_ids = [j["id"] for j in chan["queue"]]
    assert r2["data"]["id"] in queued_ids


@pytest.mark.asyncio
async def test_list_separates_channels(daemon):
    r1 = await send(daemon.socket_path, "add", argv=["sleep", "1"], cwd=CWD, channel=1)
    r2 = await send(daemon.socket_path, "add", argv=["sleep", "1"], cwd=CWD, channel=2)
    await asyncio.sleep(0.2)

    listing = await send(daemon.socket_path, "list")
    assert _ch(listing["data"], 1)["running"]["id"] == r1["data"]["id"]
    assert _ch(listing["data"], 2)["running"]["id"] == r2["data"]["id"]


@pytest.mark.asyncio
async def test_status_idle_when_nothing_ever_queued(daemon):
    resp = await send(daemon.socket_path, "status")
    assert resp["ok"]
    assert resp["data"]["channels"] == {}


@pytest.mark.asyncio
async def test_status_shows_per_channel_running(daemon):
    r1 = await send(daemon.socket_path, "add", argv=["sleep", "1"], cwd=CWD, channel=1)
    await asyncio.sleep(0.2)
    resp = await send(daemon.socket_path, "status")
    assert _ch(resp["data"], 1)["running"]["id"] == r1["data"]["id"]


@pytest.mark.asyncio
async def test_channel_status_counts_only(daemon):
    """The cheap command `wait --channel` polls: counts, not full job rows."""
    await send(daemon.socket_path, "add", argv=["sleep", "1"], cwd=CWD, channel=1)
    await send(daemon.socket_path, "add", argv=["echo", "second"], cwd=CWD, channel=1)
    await asyncio.sleep(0.2)  # let the worker pick up job 1

    resp = await send(daemon.socket_path, "channel_status", channel=1)
    assert resp["ok"]
    assert resp["data"] == {"channel": 1, "paused": False, "running": 1, "frozen": 0, "queued": 1, "priority_queued": 0}
    assert "argv" not in resp["data"]  # counts only, no job rows


@pytest.mark.asyncio
async def test_channel_status_unknown_channel_is_idle(daemon):
    resp = await send(daemon.socket_path, "channel_status", channel=99)
    assert resp["ok"]
    assert resp["data"] == {"channel": 99, "paused": False, "running": 0, "frozen": 0, "queued": 0, "priority_queued": 0}


@pytest.mark.asyncio
async def test_states_returns_only_the_state_columns_in_request_order(daemon):
    """The batch command `wait JOBS` polls: a few columns per job, one request for all of them."""
    a = await _add(daemon, ["sleep", "30"])
    await wait_for_status(daemon.socket_path, a, {"running"})
    b = await _add(daemon, ["echo", "queued"])
    resp = await send(daemon.socket_path, "states", ids=[b, a, 999, b])
    assert resp["ok"]
    jobs = resp["data"]["jobs"]
    assert [j["id"] for j in jobs] == [b, a]  # request order, the duplicate dropped, 999 left out
    assert resp["data"]["missing"] == [999]
    assert set(jobs[0]) == set(db_module.STATE_COLUMNS)
    assert jobs[0]["status"] == "queued" and jobs[1]["status"] == "running"
    assert "raw_cmd" not in jobs[0] and "env_extra" not in jobs[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("ids", [None, [], "1", [1, "2"], [True], [0], [-3], [1.5]])
async def test_states_rejects_bad_ids(daemon, ids):
    resp = await send(daemon.socket_path, "states", ids=ids)
    assert resp["ok"] is False and "ids" in resp["error"]


@pytest.mark.asyncio
async def test_states_caps_the_number_of_ids(daemon):
    resp = await send(daemon.socket_path, "states", ids=list(range(1, db_module.MAX_SWEEP + 2)))
    assert resp["ok"] is False and "too many" in resp["error"]
    resp = await send(daemon.socket_path, "states", ids=list(range(1, db_module.MAX_SWEEP + 1)))
    assert resp["ok"] and resp["data"]["jobs"] == [] and len(resp["data"]["missing"]) == db_module.MAX_SWEEP


@pytest.mark.asyncio
async def test_channel_status_requires_channel_arg(daemon):
    resp = await send(daemon.socket_path, "channel_status")
    assert resp["ok"] is False and "channel" in resp["error"]


@pytest.mark.asyncio
async def test_show_unknown_job_errors(daemon):
    resp = await send(daemon.socket_path, "show", id=99999)
    assert resp["ok"] is False


@pytest.mark.asyncio
async def test_show_includes_channel(daemon):
    resp = await send(daemon.socket_path, "add", argv=["echo", "hi"], cwd=CWD, channel=4)
    job_id = resp["data"]["id"]
    job = await wait_for_status(daemon.socket_path, job_id, {"done", "failed"})
    assert job["channel"] == 4


# ---------------------------------------------------------------------------
# cancel
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cancel_kills_running_job(daemon):
    resp = await send(daemon.socket_path, "add", argv=["sleep", "10"], cwd=CWD)
    job_id = resp["data"]["id"]
    await asyncio.sleep(0.2)

    cancel_resp = await send(daemon.socket_path, "cancel")
    assert cancel_resp["ok"]
    assert cancel_resp["data"]["cancelled"] == job_id

    job = await wait_for_status(daemon.socket_path, job_id, {"cancelled", "failed", "done"})
    assert job["status"] == "cancelled"


@pytest.mark.asyncio
async def test_cancel_when_idle_errors(daemon):
    resp = await send(daemon.socket_path, "cancel")
    assert resp["ok"] is False


@pytest.mark.asyncio
async def test_cancel_defaults_to_channel_one(daemon):
    resp = await send(daemon.socket_path, "add", argv=["sleep", "10"], cwd=CWD, channel=1)
    job_id = resp["data"]["id"]
    await asyncio.sleep(0.2)

    cancel_resp = await send(daemon.socket_path, "cancel")  # no channel arg
    assert cancel_resp["ok"]
    assert cancel_resp["data"]["cancelled"] == job_id


@pytest.mark.asyncio
async def test_cancel_scoped_to_one_channel_leaves_others_running(daemon):
    r1 = await send(daemon.socket_path, "add", argv=["sleep", "2"], cwd=CWD, channel=1)
    r2 = await send(daemon.socket_path, "add", argv=["sleep", "2"], cwd=CWD, channel=2)
    await asyncio.sleep(0.2)

    cancel_resp = await send(daemon.socket_path, "cancel", channel=2)
    assert cancel_resp["ok"]
    assert cancel_resp["data"]["cancelled"] == r2["data"]["id"]

    job2 = await wait_for_status(daemon.socket_path, r2["data"]["id"], {"cancelled", "failed", "done"})
    assert job2["status"] == "cancelled"

    # channel 1's job was never touched
    status = await send(daemon.socket_path, "show", id=r1["data"]["id"])
    assert status["data"]["status"] == "running"

    await send(daemon.socket_path, "cancel", channel=1)  # cleanup


@pytest.mark.asyncio
async def test_cancel_on_channel_with_nothing_running_errors(daemon):
    await send(daemon.socket_path, "add", argv=["sleep", "2"], cwd=CWD, channel=1)
    await asyncio.sleep(0.2)

    resp = await send(daemon.socket_path, "cancel", channel=2)  # channel 2 idle
    assert resp["ok"] is False

    await send(daemon.socket_path, "cancel", channel=1)  # cleanup


# ---------------------------------------------------------------------------
# rm
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_rm_queued_job(daemon):
    await send(daemon.socket_path, "add", argv=["sleep", "1"], cwd=CWD)
    r2 = await send(daemon.socket_path, "add", argv=["echo", "later"], cwd=CWD)
    await asyncio.sleep(0.1)

    rm_resp = await send(daemon.socket_path, "rm", id=r2["data"]["id"])
    assert rm_resp["ok"]

    listing = await send(daemon.socket_path, "list")
    ids = [j["id"] for j in _ch(listing["data"], 1)["queue"]]
    assert r2["data"]["id"] not in ids


@pytest.mark.asyncio
async def test_rm_running_job_errors(daemon):
    resp = await send(daemon.socket_path, "add", argv=["sleep", "2"], cwd=CWD)
    job_id = resp["data"]["id"]
    await asyncio.sleep(0.2)

    rm_resp = await send(daemon.socket_path, "rm", id=job_id)
    assert rm_resp["ok"] is False

    await send(daemon.socket_path, "cancel")  # cleanup so the test doesn't leave a stray sleep


@pytest.mark.asyncio
async def test_rm_nonexistent_job_errors(daemon):
    resp = await send(daemon.socket_path, "rm", id=99999)
    assert resp["ok"] is False


# ---------------------------------------------------------------------------
# clear
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_clear_removes_finished_backlog(daemon):
    resp = await send(daemon.socket_path, "add", argv=["/bin/echo", "hi"], cwd=CWD)
    job_id = resp["data"]["id"]
    await wait_for_status(daemon.socket_path, job_id, {"done", "failed"})

    listing = await send(daemon.socket_path, "list")
    assert any(j["id"] == job_id for j in _ch(listing["data"], 1)["backlog"])

    clear_resp = await send(daemon.socket_path, "clear")
    assert clear_resp["ok"]
    assert clear_resp["data"]["cleared"] == 1

    listing = await send(daemon.socket_path, "list")
    assert _ch(listing["data"], 1)["backlog"] == []


@pytest.mark.asyncio
async def test_clear_leaves_queued_and_running_jobs_alone(daemon):
    running = await send(daemon.socket_path, "add", argv=["sleep", "2"], cwd=CWD)
    await asyncio.sleep(0.2)
    queued = await send(daemon.socket_path, "add", argv=["sleep", "1"], cwd=CWD)

    clear_resp = await send(daemon.socket_path, "clear")
    assert clear_resp["ok"]
    assert clear_resp["data"]["cleared"] == 0

    listing = await send(daemon.socket_path, "list")
    ch = _ch(listing["data"], 1)
    assert ch["running"]["id"] == running["data"]["id"]
    assert [j["id"] for j in ch["queue"]] == [queued["data"]["id"]]

    await send(daemon.socket_path, "rm", id=queued["data"]["id"])
    await send(daemon.socket_path, "cancel")  # cleanup


@pytest.mark.asyncio
async def test_clear_scoped_to_one_channel(daemon):
    a = await send(daemon.socket_path, "add", argv=["/bin/echo", "a"], cwd=CWD, channel=1)
    b = await send(daemon.socket_path, "add", argv=["/bin/echo", "b"], cwd=CWD, channel=2)
    await wait_for_status(daemon.socket_path, a["data"]["id"], {"done", "failed"})
    await wait_for_status(daemon.socket_path, b["data"]["id"], {"done", "failed"})

    clear_resp = await send(daemon.socket_path, "clear", channel=1)
    assert clear_resp["data"]["cleared"] == 1

    listing = await send(daemon.socket_path, "list")
    assert _ch(listing["data"], 1)["backlog"] == []
    assert len(_ch(listing["data"], 2)["backlog"]) == 1


# ---------------------------------------------------------------------------
# requeue
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_requeue_finished_job(daemon):
    r1 = await send(daemon.socket_path, "add", argv=["echo", "original"], cwd=CWD)
    job_id = r1["data"]["id"]
    await wait_for_status(daemon.socket_path, job_id, {"done", "failed"})

    rq = await send(daemon.socket_path, "requeue", id=job_id)
    assert rq["ok"]
    new_id = rq["data"]["id"]
    assert new_id != job_id

    new_job = await wait_for_status(daemon.socket_path, new_id, {"done", "failed"})
    assert json.loads(new_job["raw_cmd"]) == ["echo", "original"]


@pytest.mark.asyncio
async def test_requeue_running_job_errors(daemon):
    resp = await send(daemon.socket_path, "add", argv=["sleep", "2"], cwd=CWD)
    job_id = resp["data"]["id"]
    await asyncio.sleep(0.2)

    rq = await send(daemon.socket_path, "requeue", id=job_id)
    assert rq["ok"] is False

    await send(daemon.socket_path, "cancel")  # cleanup


@pytest.mark.asyncio
async def test_requeue_nonexistent_job_errors(daemon):
    resp = await send(daemon.socket_path, "requeue", id=99999)
    assert resp["ok"] is False


@pytest.mark.asyncio
async def test_requeue_preserves_channel(daemon):
    r1 = await send(daemon.socket_path, "add", argv=["echo", "original"], cwd=CWD, channel=3)
    job_id = r1["data"]["id"]
    await wait_for_status(daemon.socket_path, job_id, {"done", "failed"})

    rq = await send(daemon.socket_path, "requeue", id=job_id)
    new_job = await wait_for_status(daemon.socket_path, rq["data"]["id"], {"done", "failed"})
    assert new_job["channel"] == 3


# ---------------------------------------------------------------------------
# auto-retry (--retries)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_failed_job_is_retried_up_to_max_retries(daemon):
    r = await send(daemon.socket_path, "add", argv=["false"], cwd=CWD, max_retries=2)
    first = r["data"]["id"]

    last = await wait_for_status(daemon.socket_path, first + 2, {"failed"})
    assert last["attempt"] == 2
    assert last["max_retries"] == 2

    # retry budget exhausted -- no 4th job
    await asyncio.sleep(0.5)
    assert (await send(daemon.socket_path, "show", id=first + 3))["ok"] is False

    # the first failure's note says where its retry went
    j1 = (await send(daemon.socket_path, "show", id=first))["data"]
    assert j1["attempt"] == 0
    assert j1["origin"] is None
    j2 = (await send(daemon.socket_path, "show", id=first + 1))["data"]
    assert (j2["origin"], j2["parent_id"]) == ("retry", first)
    assert (last["origin"], last["parent_id"]) == ("retry", first + 1)
    assert f"queued as job {first + 1}" in j1["note"]


@pytest.mark.asyncio
async def test_retry_stops_once_job_succeeds(daemon, tmp_path):
    marker = tmp_path / "ran-once"
    script = f"test -f {marker} || {{ touch {marker}; exit 1; }}"
    r = await send(daemon.socket_path, "add", argv=["sh", "-c", script], cwd=CWD, max_retries=5)
    first = r["data"]["id"]

    await wait_for_status(daemon.socket_path, first, {"failed"})
    j2 = await wait_for_status(daemon.socket_path, first + 1, {"done"})
    assert j2["attempt"] == 1

    await asyncio.sleep(0.5)
    assert (await send(daemon.socket_path, "show", id=first + 2))["ok"] is False


@pytest.mark.asyncio
async def test_no_retry_by_default(daemon):
    r = await send(daemon.socket_path, "add", argv=["false"], cwd=CWD)
    first = r["data"]["id"]
    await wait_for_status(daemon.socket_path, first, {"failed"})
    await asyncio.sleep(0.5)
    assert (await send(daemon.socket_path, "show", id=first + 1))["ok"] is False


@pytest.mark.asyncio
async def test_successful_job_is_not_retried(daemon):
    r = await send(daemon.socket_path, "add", argv=["true"], cwd=CWD, max_retries=3)
    first = r["data"]["id"]
    await wait_for_status(daemon.socket_path, first, {"done"})
    await asyncio.sleep(0.5)
    assert (await send(daemon.socket_path, "show", id=first + 1))["ok"] is False


@pytest.mark.asyncio
async def test_cancelled_job_is_not_retried(daemon):
    r = await send(daemon.socket_path, "add", argv=["sleep", "5"], cwd=CWD, max_retries=3)
    first = r["data"]["id"]
    await wait_for_status(daemon.socket_path, first, {"running"})
    await send(daemon.socket_path, "cancel")
    await wait_for_status(daemon.socket_path, first, {"cancelled"})
    await asyncio.sleep(0.5)
    assert (await send(daemon.socket_path, "show", id=first + 1))["ok"] is False


@pytest.mark.asyncio
async def test_timed_out_job_is_retried(daemon):
    r = await send(
        daemon.socket_path, "add", argv=["sleep", "30"], cwd=CWD, timeout_secs=1, max_retries=1
    )
    first = r["data"]["id"]
    j1 = await wait_for_status(daemon.socket_path, first, {"failed"}, timeout=10)
    assert "timed out" in j1["note"]
    j2 = await wait_for_status(daemon.socket_path, first + 1, {"failed"}, timeout=10)
    assert j2["attempt"] == 1


@pytest.mark.asyncio
async def test_retry_stays_on_same_channel(daemon):
    r = await send(daemon.socket_path, "add", argv=["false"], cwd=CWD, channel=3, max_retries=1)
    first = r["data"]["id"]
    j2 = await wait_for_status(daemon.socket_path, first + 1, {"failed"})
    assert j2["channel"] == 3


@pytest.mark.asyncio
async def test_negative_retries_rejected(daemon):
    r = await send(daemon.socket_path, "add", argv=["true"], cwd=CWD, max_retries=-1)
    assert r["ok"] is False


@pytest.mark.asyncio
async def test_manual_requeue_resets_attempt(daemon):
    r = await send(daemon.socket_path, "add", argv=["false"], cwd=CWD, max_retries=1)
    first = r["data"]["id"]
    last = await wait_for_status(daemon.socket_path, first + 1, {"failed"})
    assert last["attempt"] == 1

    rq = await send(daemon.socket_path, "requeue", id=last["id"])
    new_job = (await send(daemon.socket_path, "show", id=rq["data"]["id"]))["data"]
    assert new_job["attempt"] == 0
    assert (new_job["origin"], new_job["parent_id"]) == ("requeue", last["id"])
    assert new_job["max_retries"] == 1


# ---------------------------------------------------------------------------
# pause / resume
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_pause_prevents_new_job_from_starting(daemon):
    pause_resp = await send(daemon.socket_path, "pause")
    assert pause_resp["ok"]

    add_resp = await send(daemon.socket_path, "add", argv=["echo", "should wait"], cwd=CWD)
    job_id = add_resp["data"]["id"]

    await asyncio.sleep(0.5)
    show_resp = await send(daemon.socket_path, "show", id=job_id)
    assert show_resp["data"]["status"] == "queued"  # never picked up while paused


@pytest.mark.asyncio
async def test_resume_lets_paused_queue_continue(daemon):
    await send(daemon.socket_path, "pause")
    add_resp = await send(daemon.socket_path, "add", argv=["echo", "go"], cwd=CWD)
    job_id = add_resp["data"]["id"]
    await asyncio.sleep(0.3)

    resume_resp = await send(daemon.socket_path, "resume")
    assert resume_resp["ok"]

    job = await wait_for_status(daemon.socket_path, job_id, {"done", "failed"})
    assert job["status"] == "done"


@pytest.mark.asyncio
async def test_pause_state_survives_within_same_daemon_instance(daemon):
    await send(daemon.socket_path, "add", argv=["echo", "seed"], cwd=CWD)  # registers channel 1
    await send(daemon.socket_path, "pause")
    status1 = await send(daemon.socket_path, "list")
    assert _ch(status1["data"], 1)["paused"] is True

    await send(daemon.socket_path, "resume")
    status2 = await send(daemon.socket_path, "list")
    assert _ch(status2["data"], 1)["paused"] is False


@pytest.mark.asyncio
async def test_pause_scoped_to_one_channel(daemon):
    r1 = await send(daemon.socket_path, "add", argv=["sleep", "0.5"], cwd=CWD, channel=1)
    await send(daemon.socket_path, "pause", channel=2)
    r2 = await send(daemon.socket_path, "add", argv=["echo", "held"], cwd=CWD, channel=2)

    # channel 1 unaffected -- runs to completion
    job1 = await wait_for_status(daemon.socket_path, r1["data"]["id"], {"done", "failed"})
    assert job1["status"] == "done"

    # channel 2 stays queued -- it's paused
    await asyncio.sleep(0.3)
    show2 = await send(daemon.socket_path, "show", id=r2["data"]["id"])
    assert show2["data"]["status"] == "queued"

    await send(daemon.socket_path, "resume", channel=2)
    job2 = await wait_for_status(daemon.socket_path, r2["data"]["id"], {"done", "failed"})
    assert job2["status"] == "done"


# ---------------------------------------------------------------------------
# timeout enforcement
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_timeout_kills_long_job(daemon):
    resp = await send(daemon.socket_path, "add", argv=["sleep", "10"], cwd=CWD, timeout_secs=1)
    job_id = resp["data"]["id"]

    job = await wait_for_status(daemon.socket_path, job_id, {"failed", "done"}, timeout=5.0)
    assert job["status"] == "failed"
    assert job["note"] == "timed out"


@pytest.mark.asyncio
async def test_job_under_timeout_is_unaffected(daemon):
    resp = await send(daemon.socket_path, "add", argv=["sleep", "1"], cwd=CWD, timeout_secs=10)
    job_id = resp["data"]["id"]

    job = await wait_for_status(daemon.socket_path, job_id, {"failed", "done"}, timeout=5.0)
    assert job["status"] == "done"
    assert job["note"] is None


# ---------------------------------------------------------------------------
# F10: resource accounting (peak RSS, user/system CPU time)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_resource_usage_recorded_for_a_job_that_runs_long_enough(daemon, monkeypatch):
    monkeypatch.setattr("queuer.daemon.RESOURCE_SAMPLE_SECS", 0.02)
    resp = await send(
        daemon.socket_path, "add",
        argv=[sys.executable, "-c", "import time; x = bytearray(20 * 1024 * 1024); time.sleep(0.4)"],
        cwd=CWD,
    )
    job_id = resp["data"]["id"]

    job = await wait_for_status(daemon.socket_path, job_id, {"done", "failed"}, timeout=10.0)
    assert job["status"] == "done"
    assert job["max_rss_kb"] is not None
    assert job["max_rss_kb"] >= 20 * 1024  # at least the 20MB buffer we allocated, in KB
    assert job["cpu_user_secs"] is not None
    assert job["cpu_user_secs"] >= 0
    assert job["cpu_sys_secs"] is not None
    assert job["cpu_sys_secs"] >= 0


@pytest.mark.asyncio
async def test_resource_usage_recorded_on_timeout(daemon, monkeypatch):
    monkeypatch.setattr("queuer.daemon.RESOURCE_SAMPLE_SECS", 0.02)
    resp = await send(
        daemon.socket_path, "add",
        argv=[sys.executable, "-c", "import time; x = bytearray(10 * 1024 * 1024); time.sleep(10)"],
        cwd=CWD, timeout_secs=1,
    )
    job_id = resp["data"]["id"]

    job = await wait_for_status(daemon.socket_path, job_id, {"failed", "done"}, timeout=10.0)
    assert job["status"] == "failed"
    assert job["note"] == "timed out"
    assert job["max_rss_kb"] is not None
    assert job["max_rss_kb"] >= 10 * 1024


@pytest.mark.asyncio
async def test_resource_usage_absent_before_first_sample(daemon):
    # the default sampling interval is coarse relative to `echo`'s lifetime, so a
    # near-instant job is never sampled -- querying is never worse than "no data"
    resp = await send(daemon.socket_path, "add", argv=["echo", "hi"], cwd=CWD)
    job_id = resp["data"]["id"]
    job = await wait_for_status(daemon.socket_path, job_id, {"done", "failed"})
    assert job["status"] == "done"
    assert job["max_rss_kb"] is None or job["max_rss_kb"] >= 0
    # whatever it recorded (possibly nothing), it must not have crashed the job


@pytest.mark.asyncio
async def test_resource_usage_recorded_for_a_cancelled_job(daemon, monkeypatch):
    monkeypatch.setattr("queuer.daemon.RESOURCE_SAMPLE_SECS", 0.02)
    resp = await send(
        daemon.socket_path, "add",
        argv=[sys.executable, "-c", "import time; x = bytearray(10 * 1024 * 1024); time.sleep(30)"],
        cwd=CWD,
    )
    job_id = resp["data"]["id"]
    await wait_for_status(daemon.socket_path, job_id, {"running"})
    await asyncio.sleep(0.6)  # let the interpreter start, allocate, and be sampled
    assert (await send(daemon.socket_path, "cancel", id=job_id))["ok"]

    job = await wait_for_status(daemon.socket_path, job_id, {"cancelled", "failed", "done"}, timeout=10.0)
    assert job["status"] == "cancelled"
    assert job["max_rss_kb"] is not None and job["max_rss_kb"] >= 10 * 1024


@pytest.mark.asyncio
async def test_a_queued_job_has_no_resource_usage(daemon):
    blocker = (await send(daemon.socket_path, "add", argv=["sleep", "30"], cwd=CWD))["data"]["id"]
    waiting = (await send(daemon.socket_path, "add", argv=["echo", "x"], cwd=CWD))["data"]["id"]
    job = (await send(daemon.socket_path, "show", id=waiting))["data"]
    assert job["status"] == "queued"
    assert job["max_rss_kb"] is None and job["cpu_user_secs"] is None and job["cpu_sys_secs"] is None
    await send(daemon.socket_path, "cancel", id=blocker)  # cleanup
    await send(daemon.socket_path, "cancel", id=waiting)


@pytest.mark.asyncio
async def test_sample_usage_keeps_the_last_reading_when_the_process_is_gone(daemon, monkeypatch):
    readings = iter([(100, 1.0, 0.5), (4096, 2.0, 0.75), None, None])
    monkeypatch.setattr("queuer.daemon._read_proc_usage", lambda pid: next(readings, None))
    monkeypatch.setattr("queuer.daemon.RESOURCE_SAMPLE_SECS", 0.01)
    usage: dict = {"rss_kb": None, "cpu_user": None, "cpu_sys": None}
    task = asyncio.create_task(daemon._sample_usage(12345, usage))
    await asyncio.sleep(0.2)  # long enough for all four readings
    await daemon._stop_usage_sampling(task)
    assert usage == {"rss_kb": 4096, "cpu_user": 2.0, "cpu_sys": 0.75}
    assert task.done()


@pytest.mark.asyncio
async def test_sample_usage_with_no_reading_leaves_usage_empty(daemon, monkeypatch):
    monkeypatch.setattr("queuer.daemon._read_proc_usage", lambda pid: None)
    monkeypatch.setattr("queuer.daemon.RESOURCE_SAMPLE_SECS", 0.01)
    usage: dict = {"rss_kb": None, "cpu_user": None, "cpu_sys": None}
    task = asyncio.create_task(daemon._sample_usage(12345, usage))
    await asyncio.sleep(0.05)
    await daemon._stop_usage_sampling(task)
    assert usage == {"rss_kb": None, "cpu_user": None, "cpu_sys": None}


@pytest.mark.asyncio
async def test_sample_usage_slows_down_after_the_fast_window(daemon, monkeypatch):
    """5.todo item 9: fast (RESOURCE_SAMPLE_SECS) for only RESOURCE_SAMPLE_FAST_FOR_SECS,
    then slow (RESOURCE_SAMPLE_SLOW_SECS) -- not RESOURCE_SAMPLE_SECS the whole time. Real
    (tiny) sleeps, like the other _sample_usage tests -- no global asyncio.sleep patching."""
    calls: list[float] = []

    def fake_read(pid: int) -> tuple[int | None, float, float]:
        calls.append(time.monotonic())
        return (100, 1.0, 0.5)

    monkeypatch.setattr("queuer.daemon._read_proc_usage", fake_read)
    monkeypatch.setattr("queuer.daemon.RESOURCE_SAMPLE_SECS", 0.01)
    monkeypatch.setattr("queuer.daemon.RESOURCE_SAMPLE_FAST_FOR_SECS", 0.03)
    monkeypatch.setattr("queuer.daemon.RESOURCE_SAMPLE_SLOW_SECS", 0.2)
    usage: dict = {"rss_kb": None, "cpu_user": None, "cpu_sys": None}
    task = asyncio.create_task(daemon._sample_usage(12345, usage))
    await asyncio.sleep(0.5)
    await daemon._stop_usage_sampling(task)
    # had it stayed at the fast interval the whole time this would be ~50 calls
    assert 3 <= len(calls) <= 10
    gaps = [b - a for a, b in zip(calls, calls[1:], strict=False)]
    assert any(gap > 0.1 for gap in gaps)  # at least one slow-interval gap


# ---------------------------------------------------------------------------
# ordering: --before / --after
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_before_after_ordering(daemon):
    # keep something running so the queue doesn't drain while we set this up
    await send(daemon.socket_path, "add", argv=["sleep", "2"], cwd=CWD)
    await asyncio.sleep(0.1)

    a = await send(daemon.socket_path, "add", argv=["echo", "a"], cwd=CWD)
    b = await send(daemon.socket_path, "add", argv=["echo", "b"], cwd=CWD, before=a["data"]["id"])
    c = await send(daemon.socket_path, "add", argv=["echo", "c"], cwd=CWD, after=a["data"]["id"])

    listing = await send(daemon.socket_path, "list")
    ids = [j["id"] for j in _ch(listing["data"], 1)["queue"]]
    assert ids == [b["data"]["id"], a["data"]["id"], c["data"]["id"]]

    await send(daemon.socket_path, "cancel")  # cleanup the blocker


@pytest.mark.asyncio
async def test_before_nonqueued_id_errors(daemon):
    resp = await send(daemon.socket_path, "add", argv=["echo", "x"], cwd=CWD, before=99999)
    assert resp["ok"] is False


@pytest.mark.asyncio
async def test_before_id_from_different_channel_errors(daemon):
    a2 = await send(daemon.socket_path, "add", argv=["sleep", "1"], cwd=CWD, channel=2)
    resp = await send(
        daemon.socket_path, "add", argv=["echo", "x"], cwd=CWD, channel=1, before=a2["data"]["id"]
    )
    assert resp["ok"] is False

    await send(daemon.socket_path, "cancel", channel=2)  # cleanup


# ---------------------------------------------------------------------------
# startup recovery (separate daemon instance, pre-seeded DB)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_startup_recovery_marks_stale_running_job_interrupted(tmp_path):
    db_path = tmp_path / "queuer.db"

    # simulate a previous daemon instance that crashed mid-job
    conn = db_module.connect(db_path)
    db_module.init_db(conn)
    job_id = db_module.enqueue(
        conn,
        raw_cmd=["sleep", "999"],
        resolved_cmd=["/bin/sleep", "999"],
        cwd=CWD,
        log_path=str(tmp_path / "logs" / "1.log"),
    )
    db_module.mark_running(conn, job_id, pid=999999, pgid=999999)  # pid guaranteed not to exist
    conn.close()

    d = Daemon(db_path=db_path, socket_path=tmp_path / "queuer.sock", log_dir=tmp_path / "logs")
    task = asyncio.create_task(d.serve())
    try:
        for _ in range(100):
            if d.socket_path.exists():
                break
            await asyncio.sleep(0.02)

        resp = await send(d.socket_path, "show", id=job_id)
        assert resp["ok"]
        assert resp["data"]["status"] == "interrupted"
        assert resp["data"]["note"] == "daemon stopped"
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_startup_recovery_leaves_queued_jobs_untouched(tmp_path):
    db_path = tmp_path / "queuer.db"

    conn = db_module.connect(db_path)
    db_module.init_db(conn)
    queued_id = db_module.enqueue(
        conn,
        raw_cmd=["echo", "still queued"],
        resolved_cmd=["/bin/echo", "still queued"],
        cwd=CWD,
        log_path=str(tmp_path / "logs" / "1.log"),
    )
    conn.close()

    d = Daemon(db_path=db_path, socket_path=tmp_path / "queuer.sock", log_dir=tmp_path / "logs")
    task = asyncio.create_task(d.serve())
    try:
        for _ in range(100):
            if d.socket_path.exists():
                break
            await asyncio.sleep(0.02)

        # it should run to completion normally, not be flagged as interrupted
        job = await wait_for_status(d.socket_path, queued_id, {"done", "failed"})
        assert job["status"] == "done"
        assert job["note"] is None
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_startup_recovery_covers_every_channel(tmp_path):
    db_path = tmp_path / "queuer.db"

    conn = db_module.connect(db_path)
    db_module.init_db(conn)
    job1 = db_module.enqueue(
        conn, raw_cmd=["sleep", "999"], resolved_cmd=["/bin/sleep", "999"], cwd=CWD,
        log_path=str(tmp_path / "logs" / "1.log"), channel=1,
    )
    db_module.mark_running(conn, job1, pid=999999, pgid=999999)
    job2 = db_module.enqueue(
        conn, raw_cmd=["sleep", "999"], resolved_cmd=["/bin/sleep", "999"], cwd=CWD,
        log_path=str(tmp_path / "logs" / "2.log"), channel=2,
    )
    db_module.mark_running(conn, job2, pid=999998, pgid=999998)
    conn.close()

    d = Daemon(db_path=db_path, socket_path=tmp_path / "queuer.sock", log_dir=tmp_path / "logs")
    task = asyncio.create_task(d.serve())
    try:
        for _ in range(100):
            if d.socket_path.exists():
                break
            await asyncio.sleep(0.02)

        resp1 = await send(d.socket_path, "show", id=job1)
        resp2 = await send(d.socket_path, "show", id=job2)
        assert resp1["data"]["status"] == "interrupted"
        assert resp1["data"]["note"] == "daemon stopped"
        assert resp2["data"]["status"] == "interrupted"
        assert resp2["data"]["note"] == "daemon stopped"
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_startup_spawns_worker_for_preexisting_channel_and_runs_new_jobs(tmp_path):
    # a channel that only ever appears in the DB (never touched this
    # daemon instance) must still get a live worker on startup, not just
    # remain a dead entry in the channels table.
    db_path = tmp_path / "queuer.db"
    conn = db_module.connect(db_path)
    db_module.init_db(conn)
    db_module.register_channel(conn, 2)  # channel 2 "exists" but is idle
    conn.close()

    d = Daemon(db_path=db_path, socket_path=tmp_path / "queuer.sock", log_dir=tmp_path / "logs")
    task = asyncio.create_task(d.serve())
    try:
        for _ in range(100):
            if d.socket_path.exists():
                break
            await asyncio.sleep(0.02)

        resp = await send(d.socket_path, "add", argv=["echo", "hi"], cwd=CWD, channel=2)
        job = await wait_for_status(d.socket_path, resp["data"]["id"], {"done", "failed"})
        assert job["status"] == "done"
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


# ---------------------------------------------------------------------------
# log capping
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_log_truncation_marker_appears_when_capped(daemon, monkeypatch):
    import queuer.daemon as daemon_module
    monkeypatch.setattr(daemon_module, "MAX_LOG_BYTES", 200)  # tiny cap for a fast test

    resp = await send(
        daemon.socket_path,
        "add",
        argv=["sh", "-c", "for i in $(seq 1 200); do echo line $i; done"],
        cwd=CWD,
    )
    job_id = resp["data"]["id"]
    job = await wait_for_status(daemon.socket_path, job_id, {"done", "failed"})

    content = Path(job["log_path"]).read_bytes()
    assert len(content) < 400  # capped, not the full ~1700+ bytes of uncapped output
    assert b"[queuer] log truncated at" in content


@pytest.mark.asyncio
async def test_log_not_truncated_when_under_cap(daemon):
    resp = await send(daemon.socket_path, "add", argv=["echo", "short"], cwd=CWD)
    job_id = resp["data"]["id"]
    job = await wait_for_status(daemon.socket_path, job_id, {"done", "failed"})

    content = Path(job["log_path"]).read_bytes()
    assert b"truncated" not in content


# ---------------------------------------------------------------------------
# protocol robustness
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_unknown_command_returns_clean_error(daemon):
    resp = await send(daemon.socket_path, "not_a_real_command")
    assert resp["ok"] is False
    assert "unknown command" in resp["error"]


@pytest.mark.asyncio
async def test_malformed_json_does_not_crash_daemon(daemon):
    reader, writer = await asyncio.open_unix_connection(str(daemon.socket_path))
    writer.write(b"this is not json\n")
    await writer.drain()
    line = await reader.readline()
    writer.close()
    await writer.wait_closed()

    resp = json.loads(line.decode())
    assert resp["ok"] is False

    # daemon should still be alive and answer a normal request afterward
    followup = await send(daemon.socket_path, "status")
    assert followup["ok"]


@pytest.mark.asyncio
async def test_empty_request_does_not_crash_daemon(daemon):
    reader, writer = await asyncio.open_unix_connection(str(daemon.socket_path))
    writer.close()
    await writer.wait_closed()

    # daemon should still respond to a fresh connection afterward
    followup = await send(daemon.socket_path, "status")
    assert followup["ok"]


# ---------------------------------------------------------------------------
# `--now` priority lane: SIGSTOP the running job, run priority in FIFO,
# SIGCONT once the priority lane drains
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_now_freezes_running_job_and_runs_priority_job_first(daemon):
    r1 = await send(daemon.socket_path, "add", argv=["sleep", "2"], cwd=CWD)
    job1 = r1["data"]["id"]
    await wait_for_status(daemon.socket_path, job1, {"running"})

    r2 = await send(daemon.socket_path, "add", argv=["sleep", "0.5"], cwd=CWD, now=True)
    assert r2["ok"]
    job2 = r2["data"]["id"]

    # frozen job shows up as 'stopped', not 'running' -- and stays that
    # way while the priority job runs to completion
    stopped = await wait_for_status(daemon.socket_path, job1, {"stopped"})
    assert stopped["status"] == "stopped"

    finished2 = await wait_for_status(daemon.socket_path, job2, {"done", "failed"})
    assert finished2["status"] == "done"
    assert finished2["is_priority"] == 1

    # frozen job resumes on its own once the priority lane drains, and
    # completes normally (SIGCONT, not a fresh run)
    finished1 = await wait_for_status(daemon.socket_path, job1, {"done", "failed"}, timeout=8.0)
    assert finished1["status"] == "done"
    assert finished1["exit_code"] == 0


@pytest.mark.asyncio
async def test_now_on_idle_channel_just_runs_no_freeze(daemon):
    resp = await send(daemon.socket_path, "add", argv=["echo", "hi"], cwd=CWD, now=True)
    job_id = resp["data"]["id"]
    job = await wait_for_status(daemon.socket_path, job_id, {"done", "failed"})
    assert job["status"] == "done"
    assert job["is_priority"] == 1

    listing = await send(daemon.socket_path, "list")
    assert _ch(listing["data"], 1)["frozen"] is None


@pytest.mark.asyncio
async def test_normal_job_waits_for_now_job_running_on_idle_channel(daemon):
    # H1: a --now job on an idle channel used to run side by side with the next normal add
    r1 = await send(daemon.socket_path, "add", argv=["sleep", "0.8"], cwd=CWD, now=True)
    now_id = r1["data"]["id"]
    await wait_for_status(daemon.socket_path, now_id, {"running"})

    r2 = await send(daemon.socket_path, "add", argv=["echo", "normal"], cwd=CWD)
    normal_id = r2["data"]["id"]
    await asyncio.sleep(0.4)
    assert (await send(daemon.socket_path, "show", id=normal_id))["data"]["status"] == "queued"
    assert (await send(daemon.socket_path, "show", id=now_id))["data"]["status"] == "running"

    await wait_for_status(daemon.socket_path, now_id, {"done"}, timeout=8.0)
    normal = await wait_for_status(daemon.socket_path, normal_id, {"done"}, timeout=8.0)
    assert normal["status"] == "done"


@pytest.mark.asyncio
async def test_cancel_by_id_of_now_job_works_with_normal_job_queued(daemon):
    # H1: with two jobs running, the newer one owned the channel slot and `cancel ID` of the
    # --now job answered "not running"
    r1 = await send(daemon.socket_path, "add", argv=["sleep", "30"], cwd=CWD, now=True)
    now_id = r1["data"]["id"]
    await wait_for_status(daemon.socket_path, now_id, {"running"})
    r2 = await send(daemon.socket_path, "add", argv=["echo", "normal"], cwd=CWD)
    normal_id = r2["data"]["id"]
    await asyncio.sleep(0.3)
    assert (await send(daemon.socket_path, "show", id=normal_id))["data"]["status"] == "queued"

    resp = await send(daemon.socket_path, "cancel", id=now_id)
    assert resp["ok"], resp
    assert (await wait_for_status(daemon.socket_path, now_id, {"cancelled"}))["status"] == "cancelled"
    assert (await wait_for_status(daemon.socket_path, normal_id, {"done"}))["status"] == "done"


@pytest.mark.asyncio
async def test_resumed_channel_does_not_start_head_while_now_job_runs(daemon):
    # H1: resuming a paused channel (or a due --delay head) must also respect a running --now job
    await send(daemon.socket_path, "pause")
    r1 = await send(daemon.socket_path, "add", argv=["sleep", "0.8"], cwd=CWD, now=True)
    now_id = r1["data"]["id"]
    await wait_for_status(daemon.socket_path, now_id, {"running"})
    r2 = await send(daemon.socket_path, "add", argv=["echo", "normal"], cwd=CWD)
    normal_id = r2["data"]["id"]
    await send(daemon.socket_path, "resume")
    await asyncio.sleep(0.3)
    assert (await send(daemon.socket_path, "show", id=normal_id))["data"]["status"] == "queued"
    assert (await wait_for_status(daemon.socket_path, normal_id, {"done"}, timeout=8.0))["status"] == "done"


@pytest.mark.asyncio
async def test_multiple_now_jobs_stack_fifo_behind_each_other(daemon):
    r1 = await send(daemon.socket_path, "add", argv=["sleep", "2"], cwd=CWD)
    job1 = r1["data"]["id"]
    await wait_for_status(daemon.socket_path, job1, {"running"})

    r2 = await send(daemon.socket_path, "add", argv=["sleep", "0.3"], cwd=CWD, now=True)
    job2 = r2["data"]["id"]
    await wait_for_status(daemon.socket_path, job2, {"running"})

    # a second --now while one priority job is already running should NOT
    # trigger a second freeze -- it just queues FIFO in the priority lane
    r3 = await send(daemon.socket_path, "add", argv=["echo", "third"], cwd=CWD, now=True)
    job3 = r3["data"]["id"]

    listing = await send(daemon.socket_path, "list")
    chan = _ch(listing["data"], 1)
    assert chan["frozen"]["id"] == job1  # still exactly one frozen job
    assert [j["id"] for j in chan["priority_queue"]] == [job3]

    finished2 = await wait_for_status(daemon.socket_path, job2, {"done", "failed"})
    assert finished2["status"] == "done"
    finished3 = await wait_for_status(daemon.socket_path, job3, {"done", "failed"})
    assert finished3["status"] == "done"

    finished1 = await wait_for_status(daemon.socket_path, job1, {"done", "failed"}, timeout=8.0)
    assert finished1["status"] == "done"


@pytest.mark.asyncio
async def test_now_scoped_to_its_own_channel(daemon):
    r1 = await send(daemon.socket_path, "add", argv=["sleep", "1.5"], cwd=CWD, channel=1)
    job1 = r1["data"]["id"]
    await wait_for_status(daemon.socket_path, job1, {"running"})

    # a --now on a different, idle channel must not touch channel 1 at all
    r2 = await send(daemon.socket_path, "add", argv=["echo", "hi"], cwd=CWD, channel=2, now=True)
    job2 = r2["data"]["id"]
    finished2 = await wait_for_status(daemon.socket_path, job2, {"done", "failed"})
    assert finished2["status"] == "done"

    listing = await send(daemon.socket_path, "list")
    assert _ch(listing["data"], 1)["frozen"] is None
    assert _ch(listing["data"], 1)["running"]["id"] == job1

    finished1 = await wait_for_status(daemon.socket_path, job1, {"done", "failed"})
    assert finished1["status"] == "done"


@pytest.mark.asyncio
async def test_now_rejects_before_and_after(daemon):
    resp = await send(daemon.socket_path, "add", argv=["echo", "hi"], cwd=CWD, now=True, before=1)
    assert resp["ok"] is False


@pytest.mark.asyncio
async def test_cancel_targets_priority_job_while_frozen_job_untouched(daemon):
    r1 = await send(daemon.socket_path, "add", argv=["sleep", "3"], cwd=CWD)
    job1 = r1["data"]["id"]
    await wait_for_status(daemon.socket_path, job1, {"running"})

    r2 = await send(daemon.socket_path, "add", argv=["sleep", "3"], cwd=CWD, now=True)
    job2 = r2["data"]["id"]
    await wait_for_status(daemon.socket_path, job2, {"running"})

    # cancel while a priority job is in the foreground must kill the
    # priority job, not disturb the frozen one
    cancel_resp = await send(daemon.socket_path, "cancel", channel=1)
    assert cancel_resp["ok"]
    assert cancel_resp["data"]["cancelled"] == job2

    finished2 = await wait_for_status(daemon.socket_path, job2, {"cancelled", "failed", "done"})
    assert finished2["status"] == "cancelled"

    # frozen job resumes once the (now-empty) priority lane drains, and
    # is still alive/untouched by the cancel
    finished1 = await wait_for_status(daemon.socket_path, job1, {"done", "failed"}, timeout=8.0)
    assert finished1["status"] == "done"


# ---------------------------------------------------------------------------
# cancel by job id
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cancel_by_id_kills_running_job(daemon):
    job_id = (await send(daemon.socket_path, "add", argv=["sleep", "10"], cwd=CWD))["data"]["id"]
    await wait_for_status(daemon.socket_path, job_id, {"running"})

    resp = await send(daemon.socket_path, "cancel", id=job_id)
    assert resp["ok"]
    assert resp["data"] == {"cancelled": job_id, "channel": 1}
    job = await wait_for_status(daemon.socket_path, job_id, {"cancelled", "failed", "done"})
    assert job["status"] == "cancelled"


@pytest.mark.asyncio
async def test_cancel_by_id_finds_job_on_other_channel_without_channel_arg(daemon):
    r1 = await send(daemon.socket_path, "add", argv=["sleep", "10"], cwd=CWD, channel=1)
    r2 = await send(daemon.socket_path, "add", argv=["sleep", "10"], cwd=CWD, channel=2)
    await wait_for_status(daemon.socket_path, r1["data"]["id"], {"running"})
    await wait_for_status(daemon.socket_path, r2["data"]["id"], {"running"})

    resp = await send(daemon.socket_path, "cancel", id=r2["data"]["id"])  # no channel
    assert resp["ok"] and resp["data"]["channel"] == 2
    job2 = await wait_for_status(daemon.socket_path, r2["data"]["id"], {"cancelled", "failed", "done"})
    assert job2["status"] == "cancelled"
    assert (await send(daemon.socket_path, "show", id=r1["data"]["id"]))["data"]["status"] == "running"

    await send(daemon.socket_path, "cancel", id=r1["data"]["id"])  # cleanup


@pytest.mark.asyncio
async def test_cancel_by_id_one_of_several_queued_jobs(daemon):
    blocker = (await send(daemon.socket_path, "add", argv=["sleep", "10"], cwd=CWD))["data"]["id"]
    await wait_for_status(daemon.socket_path, blocker, {"running"})
    q1 = (await send(daemon.socket_path, "add", argv=["echo", "1"], cwd=CWD))["data"]["id"]
    q2 = (await send(daemon.socket_path, "add", argv=["echo", "2"], cwd=CWD))["data"]["id"]

    resp = await send(daemon.socket_path, "cancel", id=q1)
    assert resp["ok"] and resp["data"]["cancelled"] == q1

    job = (await send(daemon.socket_path, "show", id=q1))["data"]
    assert job["status"] == "cancelled" and job["note"] == "cancelled while queued"
    listing = (await send(daemon.socket_path, "list"))["data"]
    assert [j["id"] for j in _ch(listing, 1)["queue"]] == [q2]
    assert q1 in [j["id"] for j in _ch(listing, 1)["backlog"]]
    assert _ch(listing, 1)["running"]["id"] == blocker  # untouched

    await send(daemon.socket_path, "cancel", id=blocker)  # cleanup
    done = await wait_for_status(daemon.socket_path, q2, {"done", "failed"})
    assert done["status"] == "done"  # the other queued job still ran


@pytest.mark.asyncio
async def test_cancel_by_id_queued_priority_job(daemon):
    r1 = await send(daemon.socket_path, "add", argv=["sleep", "10"], cwd=CWD)
    await wait_for_status(daemon.socket_path, r1["data"]["id"], {"running"})
    r2 = await send(daemon.socket_path, "add", argv=["sleep", "10"], cwd=CWD, now=True)
    await wait_for_status(daemon.socket_path, r2["data"]["id"], {"running"})
    r3 = await send(daemon.socket_path, "add", argv=["echo", "p"], cwd=CWD, now=True)  # queued behind r2

    resp = await send(daemon.socket_path, "cancel", id=r3["data"]["id"])
    assert resp["ok"]
    assert (await send(daemon.socket_path, "show", id=r3["data"]["id"]))["data"]["status"] == "cancelled"
    listing = (await send(daemon.socket_path, "list"))["data"]
    assert _ch(listing, 1)["priority_queue"] == []

    await send(daemon.socket_path, "cancel", id=r2["data"]["id"])
    await send(daemon.socket_path, "cancel", id=r1["data"]["id"])


@pytest.mark.asyncio
async def test_cancel_by_id_frozen_job_leaves_priority_job_cancellable(daemon):
    r1 = await send(daemon.socket_path, "add", argv=["sleep", "30"], cwd=CWD)
    job1 = r1["data"]["id"]
    await wait_for_status(daemon.socket_path, job1, {"running"})
    r2 = await send(daemon.socket_path, "add", argv=["sleep", "30"], cwd=CWD, now=True)
    job2 = r2["data"]["id"]
    await wait_for_status(daemon.socket_path, job2, {"running"})
    assert (await send(daemon.socket_path, "show", id=job1))["data"]["status"] == "stopped"

    resp = await send(daemon.socket_path, "cancel", id=job1)
    assert resp["ok"] and resp["data"]["cancelled"] == job1
    j1 = await wait_for_status(daemon.socket_path, job1, {"cancelled", "failed", "done"}, timeout=8.0)
    assert j1["status"] == "cancelled"

    # the priority job is still running and still owns the channel's slot
    listing = (await send(daemon.socket_path, "list"))["data"]
    assert _ch(listing, 1)["frozen"] is None
    assert _ch(listing, 1)["running"]["id"] == job2

    # plain `cancel` must still find and kill it
    plain = await send(daemon.socket_path, "cancel")
    assert plain["ok"] and plain["data"]["cancelled"] == job2
    j2 = await wait_for_status(daemon.socket_path, job2, {"cancelled", "failed", "done"})
    assert j2["status"] == "cancelled"


@pytest.mark.asyncio
async def test_cancel_by_id_errors(daemon):
    done = (await send(daemon.socket_path, "add", argv=["true"], cwd=CWD))["data"]["id"]
    await wait_for_status(daemon.socket_path, done, {"done"})

    missing = await send(daemon.socket_path, "cancel", id=999)
    assert missing["ok"] is False and "does not exist" in missing["error"]

    finished = await send(daemon.socket_path, "cancel", id=done)
    assert finished["ok"] is False and "already done" in finished["error"]

    blocker = (await send(daemon.socket_path, "add", argv=["sleep", "10"], cwd=CWD))["data"]["id"]
    await wait_for_status(daemon.socket_path, blocker, {"running"})
    wrong = await send(daemon.socket_path, "cancel", id=blocker, channel=2)
    assert wrong["ok"] is False and "channel 1" in wrong["error"]
    assert (await send(daemon.socket_path, "show", id=blocker))["data"]["status"] == "running"

    right = await send(daemon.socket_path, "cancel", id=blocker, channel=1)
    assert right["ok"]


@pytest.mark.asyncio
async def test_cancelled_by_id_job_is_not_retried(daemon):
    r = await send(daemon.socket_path, "add", argv=["sleep", "10"], cwd=CWD, max_retries=3)
    job_id = r["data"]["id"]
    await wait_for_status(daemon.socket_path, job_id, {"running"})
    await send(daemon.socket_path, "cancel", id=job_id)
    await wait_for_status(daemon.socket_path, job_id, {"cancelled"})
    await asyncio.sleep(0.3)
    listing = (await send(daemon.socket_path, "list"))["data"]
    assert _ch(listing, 1)["queue"] == [] and _ch(listing, 1)["running"] is None


# ---------------------------------------------------------------------------
# notifications
# ---------------------------------------------------------------------------

TG_TOKEN = "123456789:AAH-test_token-0123456789abcdefghijk"


@pytest.fixture
def sent(monkeypatch, daemon):
    """Notifications configured; Telegram replaced by a recorder."""
    msgs: list[dict] = []
    monkeypatch.setattr(notify, "_api", lambda token, method, params=None, timeout=0: msgs.append(params))
    notify.save({"qur_id": "a3f9", "token": TG_TOKEN, "chat_ids": [111], "listen": False}, daemon.notify_path)
    return msgs


async def wait_msgs(msgs: list, n: int, timeout: float = 5.0) -> None:
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if len(msgs) >= n:
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"expected {n} message(s), got {len(msgs)}: {msgs}")


@pytest.mark.asyncio
async def test_notifies_when_job_done(daemon, sent):
    job_id = (await send(daemon.socket_path, "add", argv=["true"], cwd=CWD))["data"]["id"]
    await wait_msgs(sent, 1)
    assert sent[0]["chat_id"] == 111
    assert f"done · job {job_id}" in sent[0]["text"]
    assert "[a3f9]" in sent[0]["text"]
    assert "exit 0" in sent[0]["text"]


@pytest.mark.asyncio
async def test_notifies_when_job_fails_with_exit_code(daemon, sent):
    job_id = (await send(daemon.socket_path, "add", argv=["sh", "-c", "exit 3"], cwd=CWD))["data"]["id"]
    await wait_msgs(sent, 1)
    assert f"failed · job {job_id}" in sent[0]["text"] and "exit 3" in sent[0]["text"]


@pytest.mark.asyncio
async def test_notifies_on_timeout(daemon, sent):
    await send(daemon.socket_path, "add", argv=["sleep", "30"], cwd=CWD, timeout_secs=1)
    await wait_msgs(sent, 1, timeout=10.0)
    assert "failed" in sent[0]["text"] and "timed out" in sent[0]["text"]


@pytest.mark.asyncio
async def test_notifies_on_failed_start_without_leaking_raw_note(daemon, sent, tmp_path):
    # passes add-time validation (exists, executable) but cannot exec: bad shebang
    script = tmp_path / "badshebang.sh"
    script.write_text("#!/nonexistent/interpreter\n")
    script.chmod(0o755)
    job_id = (await send(daemon.socket_path, "add", argv=[str(script)], cwd=CWD))["data"]["id"]
    job = await wait_for_status(daemon.socket_path, job_id, {"failed"})
    assert job["note"].startswith("failed to start")
    await wait_msgs(sent, 1)
    assert f"failed · job {job_id}" in sent[0]["text"]
    assert "failed to start" in sent[0]["text"]
    # the raw OSError text (interpreter path) stays out; the command as typed is shown
    assert "nonexistent" not in sent[0]["text"] and "Errno" not in sent[0]["text"]
    assert f"$ {script}" in sent[0]["text"]


@pytest.mark.asyncio
async def test_notifies_when_running_job_cancelled(daemon, sent):
    job_id = (await send(daemon.socket_path, "add", argv=["sleep", "10"], cwd=CWD))["data"]["id"]
    await wait_for_status(daemon.socket_path, job_id, {"running"})
    await send(daemon.socket_path, "cancel", id=job_id)
    await wait_msgs(sent, 1)
    assert f"cancelled · job {job_id}" in sent[0]["text"]


@pytest.mark.asyncio
async def test_notifies_when_queued_job_cancelled(daemon, sent):
    blocker = (await send(daemon.socket_path, "add", argv=["sleep", "10"], cwd=CWD))["data"]["id"]
    await wait_for_status(daemon.socket_path, blocker, {"running"})
    queued = (await send(daemon.socket_path, "add", argv=["echo", "x"], cwd=CWD))["data"]["id"]
    await send(daemon.socket_path, "cancel", id=queued)
    await wait_msgs(sent, 1)
    assert f"cancelled · job {queued}" in sent[0]["text"]
    assert "cancelled while queued" in sent[0]["text"]
    await send(daemon.socket_path, "cancel", id=blocker)  # cleanup


@pytest.mark.asyncio
async def test_retried_failure_is_silent_until_the_final_attempt(daemon, sent):
    r = await send(daemon.socket_path, "add", argv=["false"], cwd=CWD, max_retries=1)
    first = r["data"]["id"]
    await wait_for_status(daemon.socket_path, first, {"failed"})
    await wait_msgs(sent, 1)
    await asyncio.sleep(0.4)  # nothing else may arrive
    assert len(sent) == 1
    assert f"job {first}" not in sent[0]["text"]  # the final job's, not the retried one's
    assert "failed" in sent[0]["text"] and "attempt 2/2" in sent[0]["text"]


@pytest.mark.asyncio
async def test_no_message_without_config(daemon, monkeypatch):
    calls: list = []
    monkeypatch.setattr(notify, "_api", lambda *a, **k: calls.append(a))
    job_id = (await send(daemon.socket_path, "add", argv=["true"], cwd=CWD))["data"]["id"]
    await wait_for_status(daemon.socket_path, job_id, {"done"})
    await asyncio.sleep(0.3)
    assert calls == []


@pytest.mark.asyncio
async def test_connect_takes_effect_without_daemon_restart(daemon, monkeypatch):
    calls: list = []
    monkeypatch.setattr(notify, "_api", lambda token, method, params=None, timeout=0: calls.append(params))
    first = (await send(daemon.socket_path, "add", argv=["true"], cwd=CWD))["data"]["id"]
    await wait_for_status(daemon.socket_path, first, {"done"})
    await asyncio.sleep(0.2)
    assert calls == []

    notify.save({"qur_id": "a3f9", "token": TG_TOKEN, "chat_ids": [111], "listen": False}, daemon.notify_path)
    await send(daemon.socket_path, "add", argv=["true"], cwd=CWD)
    await wait_msgs(calls, 1)


@pytest.mark.asyncio
async def test_telegram_failure_never_disturbs_the_queue(daemon, monkeypatch, capsys):
    def boom(*a, **k):
        raise notify.NotifyError("Telegram is down", 500)

    monkeypatch.setattr(notify, "_api", boom)
    notify.save({"qur_id": "a3f9", "token": TG_TOKEN, "chat_ids": [111], "listen": False}, daemon.notify_path)
    a = (await send(daemon.socket_path, "add", argv=["true"], cwd=CWD))["data"]["id"]
    b = (await send(daemon.socket_path, "add", argv=["true"], cwd=CWD))["data"]["id"]
    assert (await wait_for_status(daemon.socket_path, a, {"done"}))["status"] == "done"
    assert (await wait_for_status(daemon.socket_path, b, {"done"}))["status"] == "done"
    await asyncio.sleep(0.3)
    assert "notification failed" in capsys.readouterr().err


@pytest.mark.asyncio
async def test_unexpected_error_in_sender_never_disturbs_the_queue(daemon, monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("bug in sender")

    monkeypatch.setattr(notify, "send", boom)
    notify.save({"qur_id": "a3f9", "token": TG_TOKEN, "chat_ids": [111], "listen": False}, daemon.notify_path)
    await send(daemon.socket_path, "add", argv=["true"], cwd=CWD)
    b = (await send(daemon.socket_path, "add", argv=["true"], cwd=CWD))["data"]["id"]
    assert (await wait_for_status(daemon.socket_path, b, {"done"}))["status"] == "done"


@pytest.mark.asyncio
async def test_notifies_for_job_interrupted_by_daemon_restart(tmp_path, monkeypatch):
    msgs: list = []
    monkeypatch.setattr(notify, "_api", lambda token, method, params=None, timeout=0: msgs.append(params))
    notify_path = tmp_path / "notify.json"
    notify.save({"qur_id": "a3f9", "token": TG_TOKEN, "chat_ids": [111], "listen": False}, notify_path)

    db_path = tmp_path / "queuer.db"
    conn = db_module.connect(db_path)
    db_module.init_db(conn)
    job_id = db_module.enqueue(
        conn, raw_cmd=["sleep", "999"], resolved_cmd=["/bin/sleep", "999"], cwd=CWD,
        log_path=str(tmp_path / "logs" / "1.log"),
    )
    db_module.mark_running(conn, job_id, pid=999999, pgid=999999)
    conn.close()

    d = Daemon(db_path=db_path, socket_path=tmp_path / "queuer.sock", log_dir=tmp_path / "logs", notify_path=notify_path)
    task = asyncio.create_task(d.serve())
    try:
        await wait_msgs(msgs, 1)
        assert f"interrupted (daemon stopped) · job {job_id}" in msgs[0]["text"]
        assert "failed" not in msgs[0]["text"]
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


# ---------------------------------------------------------------------------
# Requeue button (Telegram listener)
# ---------------------------------------------------------------------------

class FakeBot:
    """Replaces notify._api. Records every call except getUpdates; serves
    scripted getUpdates batches, then empty ones."""

    def __init__(self, batches=()):
        self.batches = list(batches)
        self.calls: list[tuple[str, dict]] = []
        self.polls = 0

    def api(self, token, method, params=None, timeout=0):
        if method == "getUpdates":
            self.polls += 1
            return self.batches.pop(0) if self.batches else []
        self.calls.append((method, params))
        return {}

    def of(self, method):
        return [p for m, p in self.calls if m == method]


async def wait_calls(bot: "FakeBot", method: str, n: int, timeout: float = 5.0) -> None:
    class _Live:  # wait_msgs only needs len()
        def __len__(self):
            return len(bot.of(method))

    await wait_msgs(_Live(), n, timeout)  # type: ignore[arg-type]


def _press(update_id, job_id, qur_id="a3f9", chat_id=111, message_id=9, action="rq"):
    return {"update_id": update_id, "callback_query": {
        "id": f"cb{update_id}", "data": f"{action}:{qur_id}:{job_id}", "from": {"id": chat_id},
        "message": {"message_id": message_id, "chat": {"id": chat_id}},
    }}


def _tg_cfg(daemon, **extra):
    notify.save({"qur_id": "a3f9", "token": TG_TOKEN, **{"chat_ids": [111], **extra}}, daemon.notify_path)


async def _finished_job(daemon) -> int:
    job_id = (await send(daemon.socket_path, "add", argv=["sh", "-c", "exit 1"], cwd=CWD))["data"]["id"]
    await wait_for_status(daemon.socket_path, job_id, {"failed"})
    return job_id


@pytest.mark.asyncio
async def test_message_has_requeue_button_when_listening(daemon, monkeypatch):
    bot = FakeBot()
    monkeypatch.setattr(notify, "_api", bot.api)
    _tg_cfg(daemon)  # listen defaults on
    job_id = await _finished_job(daemon)
    await wait_calls(bot, "sendMessage", 1)
    row = bot.of("sendMessage")[0]["reply_markup"]["inline_keyboard"][0]
    assert [b["callback_data"] for b in row] == [f"rq:a3f9:{job_id}", f"sh:a3f9:{job_id}"]


@pytest.mark.asyncio
async def test_no_button_and_no_polling_when_listen_off(daemon, monkeypatch):
    bot = FakeBot()
    monkeypatch.setattr(notify, "_api", bot.api)
    _tg_cfg(daemon, listen=False)
    await _finished_job(daemon)
    await wait_calls(bot, "sendMessage", 1)
    await asyncio.sleep(0.3)
    assert "reply_markup" not in bot.of("sendMessage")[0]
    assert bot.polls == 0


@pytest.mark.asyncio
async def test_button_press_requeues_job_and_confirms(daemon, monkeypatch):
    job_id = await _finished_job(daemon)
    bot = FakeBot([[_press(5, job_id)]])
    monkeypatch.setattr(notify, "_api", bot.api)
    _tg_cfg(daemon)
    await wait_calls(bot, "sendMessage", 1)

    listing = (await send(daemon.socket_path, "list"))["data"]
    ids = [j["id"] for j in _ch(listing, 1)["backlog"]]
    new = max(ids)
    assert new != job_id
    job = await wait_for_status(daemon.socket_path, new, {"done", "failed"})
    assert job["origin"] == "requeue" and job["parent_id"] == job_id
    assert bot.of("answerCallbackQuery")[0]["text"] == f"requeued as job {new}"
    # Requeue is spent, Details stays
    kb = bot.of("editMessageReplyMarkup")[0]["reply_markup"]["inline_keyboard"]
    assert [b["callback_data"] for b in kb[0]] == [f"sh:a3f9:{job_id}", f"lg:a3f9:{job_id}"]
    assert bot.of("editMessageReplyMarkup")[0]["message_id"] == 9
    assert f"job {job_id} requeued as job {new}" in bot.of("sendMessage")[0]["text"]


@pytest.mark.asyncio
async def test_button_press_from_unpaired_chat_or_other_qur_is_not_acted_on(daemon, monkeypatch):
    job_id = await _finished_job(daemon)
    bot = FakeBot([[
        _press(1, job_id, chat_id=999),            # stranger
        _press(2, job_id, qur_id="zzzz"),          # other machine / old id
        {"update_id": 3, "callback_query": {"id": "x", "data": "garbage", "message": {"message_id": 1, "chat": {"id": 111}}}},
        {"update_id": 4, "message": {"text": "hi", "chat": {"id": 111}}},
    ]])
    monkeypatch.setattr(notify, "_api", bot.api)
    _tg_cfg(daemon)
    for _ in range(100):
        if bot.polls >= 2:
            break
        await asyncio.sleep(0.05)
    listing = (await send(daemon.socket_path, "list"))["data"]
    assert [j["id"] for j in _ch(listing, 1)["backlog"]] == [job_id]  # nothing requeued
    answers = bot.of("answerCallbackQuery")
    assert len(answers) == 1 and "another machine" in answers[0]["text"]  # stranger gets no reply at all


@pytest.mark.asyncio
async def test_button_press_on_unfinished_or_missing_job_toasts_error(daemon, monkeypatch):
    running = (await send(daemon.socket_path, "add", argv=["sleep", "10"], cwd=CWD))["data"]["id"]
    await wait_for_status(daemon.socket_path, running, {"running"})
    bot = FakeBot([[_press(1, running), _press(2, 4242)]])
    monkeypatch.setattr(notify, "_api", bot.api)
    _tg_cfg(daemon)
    await wait_calls(bot, "answerCallbackQuery", 2)
    texts = [a["text"] for a in bot.of("answerCallbackQuery")]
    assert "only finished jobs" in texts[0] and "does not exist" in texts[1]
    assert not bot.of("editMessageReplyMarkup") and not bot.of("sendMessage")


@pytest.mark.asyncio
async def test_listener_survives_poll_errors_and_pauses_for_pairing(daemon, monkeypatch):
    monkeypatch.setattr("queuer.daemon.LISTEN_RETRY_SECS", 0.05)
    job_id = await _finished_job(daemon)
    bot = FakeBot([notify.NotifyError("Conflict", 409)])
    real = bot.api

    def flaky(token, method, params=None, timeout=0):
        if method == "getUpdates" and bot.batches and isinstance(bot.batches[0], Exception):
            raise bot.batches.pop(0)
        return real(token, method, params, timeout)

    monkeypatch.setattr(notify, "_api", flaky)
    with notify.pairing_lock(daemon.notify_path):
        _tg_cfg(daemon)
        await asyncio.sleep(0.3)
        assert bot.polls == 0  # stepped aside while pairing
    bot.batches.append([_press(1, job_id)])
    bot.batches.insert(0, notify.NotifyError("Conflict", 409))  # first poll fails, then recovers
    await wait_calls(bot, "answerCallbackQuery", 1)
    assert "requeued as job" in bot.of("answerCallbackQuery")[0]["text"]


@pytest.mark.asyncio
async def test_details_button_sends_html_details_to_pressing_chat_only(daemon, monkeypatch):
    job_id = await _finished_job(daemon)
    bot = FakeBot([[_press(1, job_id, action="sh")]])
    monkeypatch.setattr(notify, "_api", bot.api)
    _tg_cfg(daemon, chat_ids=[111, 222])
    await wait_calls(bot, "sendMessage", 1)
    (m,) = bot.of("sendMessage")
    assert m["chat_id"] == 111 and m["parse_mode"] == "HTML"
    assert f"<b>Job #{job_id}</b> · failed" in m["text"] and "<b>resolved command</b>" in m["text"]
    assert "reply_markup" not in m
    assert "text" not in bot.of("answerCallbackQuery")[0]
    assert not bot.of("editMessageReplyMarkup")
    listing = (await send(daemon.socket_path, "list"))["data"]
    assert [j["id"] for j in _ch(listing, 1)["backlog"]] == [job_id]  # nothing requeued


@pytest.mark.asyncio
async def test_details_button_for_missing_job_toasts_error(daemon, monkeypatch):
    bot = FakeBot([[_press(1, 4242, action="sh")]])
    monkeypatch.setattr(notify, "_api", bot.api)
    _tg_cfg(daemon)
    await wait_calls(bot, "answerCallbackQuery", 1)
    assert "does not exist" in bot.of("answerCallbackQuery")[0]["text"] and not bot.of("sendMessage")


def _menu_msg(update_id, text, chat_id=111):
    return {"update_id": update_id, "message": {"message_id": 5, "chat": {"id": chat_id, "type": "private"}, "text": text}}


@pytest.mark.asyncio
@pytest.mark.asyncio
def _menu_msg(update_id, text, chat_id=111):
    return {"update_id": update_id, "message": {"message_id": 5, "chat": {"id": chat_id, "type": "private"}, "text": text}}


@pytest.mark.asyncio
async def test_logs_button_sends_log_tail_to_pressing_chat(daemon, monkeypatch):
    job_id = (await send(daemon.socket_path, "add", argv=["sh", "-c", "echo hello-log; exit 1"], cwd=CWD))["data"]["id"]
    await wait_for_status(daemon.socket_path, job_id, {"failed"})
    bot = FakeBot([[_press(1, job_id, action="lg")]])
    monkeypatch.setattr(notify, "_api", bot.api)
    _tg_cfg(daemon, chat_ids=[111, 222])
    await wait_calls(bot, "sendMessage", 1)
    (m,) = bot.of("sendMessage")
    assert m["chat_id"] == 111 and m["parse_mode"] == "HTML" and "<pre>hello-log</pre>" in m["text"]


@pytest.mark.asyncio
async def test_menu_status_list_picker_and_channel_list_with_requeue(daemon, monkeypatch):
    job_id = await _finished_job(daemon)
    bot = FakeBot([[_menu_msg(1, notify.MENU_STATUS), _menu_msg(2, notify.MENU_LIST), _menu_msg(3, "/list", chat_id=999),
                    _menu_msg(4, "random chatter"), _menu_msg(5, "/start"), _press(6, 1, action="lc")]])
    monkeypatch.setattr(notify, "_api", bot.api)
    _tg_cfg(daemon)
    await wait_calls(bot, "sendMessage", 4)
    msgs = bot.of("sendMessage")
    assert msgs[0]["text"].startswith("<b>Status</b>")
    assert [b["callback_data"] for b in msgs[1]["reply_markup"]["inline_keyboard"][0]] == ["lc:a3f9:1"]  # picker
    assert msgs[2]["reply_markup"]["is_persistent"]  # /start brings the keys back
    assert msgs[3]["text"].startswith("<b>List · channel 1</b>") and f"#{job_id}" in msgs[3]["text"]
    assert [b["callback_data"] for b in msgs[3]["reply_markup"]["inline_keyboard"][0]] == [
        f"sh:a3f9:{job_id}", f"lg:a3f9:{job_id}", f"rl:a3f9:{job_id}"]
    assert all(m["chat_id"] == 111 for m in msgs)  # stranger ignored
    listing = (await send(daemon.socket_path, "list"))["data"]
    assert [j["id"] for j in _ch(listing, 1)["backlog"]] == [job_id]  # queue untouched


@pytest.mark.asyncio
async def test_requeue_from_list_drops_only_that_button(daemon, monkeypatch):
    job_id = await _finished_job(daemon)
    press = _press(1, job_id, action="rl")
    press["callback_query"]["message"]["reply_markup"] = {"inline_keyboard": [[
        {"text": "a", "callback_data": f"rl:a3f9:{job_id}"}, {"text": "b", "callback_data": "rl:a3f9:999"}]]}
    bot = FakeBot([[press]])
    monkeypatch.setattr(notify, "_api", bot.api)
    _tg_cfg(daemon)
    await wait_calls(bot, "editMessageReplyMarkup", 1)
    kb = bot.of("editMessageReplyMarkup")[0]["reply_markup"]["inline_keyboard"]
    assert [b["callback_data"] for r in kb for b in r] == ["rl:a3f9:999"]
    listing = (await send(daemon.socket_path, "list"))["data"]
    assert any(j["origin"] == "requeue" for j in _ch(listing, 1)["backlog"] + _ch(listing, 1)["queue"] + [_ch(listing, 1)["running"] or {}])


async def _add(daemon, argv, channel=1) -> int:
    return (await send(daemon.socket_path, "add", argv=argv, cwd=CWD, channel=channel))["data"]["id"]


@pytest.mark.asyncio
async def test_list_shows_log_and_cancel_buttons_for_running_and_queued(daemon, monkeypatch):
    running = await _add(daemon, ["sh", "-c", "echo out-1; sleep 30"])
    queued = await _add(daemon, ["sleep", "30"])
    await wait_for_status(daemon.socket_path, running, {"running"})
    bot = FakeBot([[_press(1, 1, action="lc")]])
    monkeypatch.setattr(notify, "_api", bot.api)
    _tg_cfg(daemon)
    await wait_calls(bot, "sendMessage", 1)
    rows = bot.of("sendMessage")[0]["reply_markup"]["inline_keyboard"]
    assert [[b["callback_data"] for b in r] for r in rows] == [
        [f"sh:a3f9:{running}", f"lg:a3f9:{running}", f"cn:a3f9:{running}"], [f"sh:a3f9:{queued}", f"cn:a3f9:{queued}"]]


@pytest.mark.asyncio
async def test_details_button_works_on_running_and_queued_jobs(daemon, monkeypatch):
    running = await _add(daemon, ["sleep", "30"])
    queued = await _add(daemon, ["sleep", "31"])
    await wait_for_status(daemon.socket_path, running, {"running"})
    bot = FakeBot([[_press(1, running, action="sh"), _press(2, queued, action="sh")]])
    monkeypatch.setattr(notify, "_api", bot.api)
    _tg_cfg(daemon)
    await wait_calls(bot, "sendMessage", 2)
    a, b = (m["text"] for m in bot.of("sendMessage"))
    assert f"<b>Job #{running}</b> · running" in a and "<b>duration</b> " in a and "<b>duration</b> -" not in a
    assert f"<b>Job #{queued}</b> · queued" in b and "<b>started</b> -" in b
    assert (await send(daemon.socket_path, "show", id=running))["data"]["status"] == "running"  # read-only


@pytest.mark.asyncio
async def test_logs_button_works_on_a_running_job(daemon, monkeypatch):
    running = await _add(daemon, ["sh", "-c", "for i in 1 2 3 4 5 6 7 8 9 10 11 12; do echo row-$i; done; sleep 30"])
    await wait_for_status(daemon.socket_path, running, {"running"})
    for _ in range(100):  # wait until the output reached the log file
        if "row-12" in (daemon.log_dir / f"{running}.log").read_text():
            break
        await asyncio.sleep(0.05)
    bot = FakeBot([[_press(1, running, action="lg")]])
    monkeypatch.setattr(notify, "_api", bot.api)
    _tg_cfg(daemon)
    await wait_calls(bot, "sendMessage", 1)
    text = bot.of("sendMessage")[0]["text"]
    assert "· running" in text and all(f"row-{i}" in text for i in range(3, 13))  # at least the last 10 lines


@pytest.mark.asyncio
async def test_logs_button_ignores_db_log_path(daemon, monkeypatch, tmp_path):
    secret = tmp_path / "secret.txt"
    secret.write_text("TOP-SECRET")
    job_id = await _finished_job(daemon)
    daemon.conn.execute("UPDATE jobs SET log_path = ? WHERE id = ?", (str(secret), job_id))
    daemon.conn.commit()
    bot = FakeBot([[_press(1, job_id, action="lg")]])
    monkeypatch.setattr(notify, "_api", bot.api)
    _tg_cfg(daemon)
    await wait_calls(bot, "sendMessage", 1)
    assert "TOP-SECRET" not in bot.of("sendMessage")[0]["text"]


@pytest.mark.asyncio
async def test_finished_job_message_masks_secrets_in_the_command(daemon, monkeypatch):
    bot = FakeBot()
    monkeypatch.setattr(notify, "_api", bot.api)
    _tg_cfg(daemon)
    await _add(daemon, ["sh", "-c", "exit 0", "x", "--api-key=abc123", "TOKEN=xyz789"])
    await wait_calls(bot, "sendMessage", 1)
    text = bot.of("sendMessage")[0]["text"]
    assert "abc123" not in text and "xyz789" not in text
    assert "--api-key=***" in text and "TOKEN=***" in text


@pytest.mark.asyncio
async def test_details_and_logs_buttons_mask_secrets_but_local_data_is_untouched(daemon, monkeypatch):
    job_id = await _add(daemon, ["sh", "-c", "echo API_KEY=s3cr3t-value", "x", "--password", "hunter2"])
    await wait_for_status(daemon.socket_path, job_id, {"done"})
    bot = FakeBot([[_press(1, job_id, action="sh"), _press(2, job_id, action="lg")]])
    monkeypatch.setattr(notify, "_api", bot.api)
    _tg_cfg(daemon)
    await wait_calls(bot, "sendMessage", 2)
    texts = [m["text"] for m in bot.of("sendMessage")]
    assert all("s3cr3t-value" not in t and "hunter2" not in t for t in texts)
    assert any("--password ***" in t for t in texts)  # Details: the command
    assert any("<pre>API_KEY=***" in t for t in texts)  # Logs: the output
    assert "s3cr3t-value" in (daemon.log_dir / f"{job_id}.log").read_text()  # the local log is as printed
    assert "hunter2" in (await send(daemon.socket_path, "show", id=job_id))["data"]["raw_cmd"]  # so is `qur show`


@pytest.mark.asyncio
async def test_cancel_button_cancels_by_id_a_queued_job_not_the_running_one(daemon, monkeypatch):
    running = await _add(daemon, ["sleep", "30"])
    queued = await _add(daemon, ["sleep", "30"])
    await wait_for_status(daemon.socket_path, running, {"running"})
    press = _press(1, queued, action="cn")
    press["callback_query"]["message"]["reply_markup"] = {"inline_keyboard": [[
        {"text": "x", "callback_data": f"cn:a3f9:{queued}"}, {"text": "y", "callback_data": f"cn:a3f9:{running}"}]]}
    bot = FakeBot([[press]])
    monkeypatch.setattr(notify, "_api", bot.api)
    _tg_cfg(daemon)
    await wait_calls(bot, "editMessageReplyMarkup", 1)
    assert bot.of("answerCallbackQuery")[0]["text"] == f"cancelled job {queued}"
    kb = bot.of("editMessageReplyMarkup")[0]["reply_markup"]["inline_keyboard"]
    assert [b["callback_data"] for r in kb for b in r] == [f"cn:a3f9:{running}"]
    assert (await send(daemon.socket_path, "show", id=queued))["data"]["status"] == "cancelled"
    assert (await send(daemon.socket_path, "show", id=running))["data"]["status"] == "running"


@pytest.mark.asyncio
async def test_cancel_button_kills_a_running_job(daemon, monkeypatch):
    running = await _add(daemon, ["sleep", "30"])
    await wait_for_status(daemon.socket_path, running, {"running"})
    bot = FakeBot([[_press(1, running, action="cn")]])
    monkeypatch.setattr(notify, "_api", bot.api)
    _tg_cfg(daemon)
    job = await wait_for_status(daemon.socket_path, running, {"cancelled"})
    assert job["status"] == "cancelled"
    await wait_calls(bot, "answerCallbackQuery", 1)
    assert bot.of("answerCallbackQuery")[0]["text"] == f"cancelled job {running}"


@pytest.mark.asyncio
async def test_cancel_button_on_finished_or_missing_job_toasts_error(daemon, monkeypatch):
    done = await _finished_job(daemon)
    bot = FakeBot([[_press(1, done, action="cn"), _press(2, 4242, action="cn")]])
    monkeypatch.setattr(notify, "_api", bot.api)
    _tg_cfg(daemon)
    await wait_calls(bot, "answerCallbackQuery", 2)
    toasts = [c["text"] for c in bot.of("answerCallbackQuery")]
    assert "already failed" in toasts[0] and "does not exist" in toasts[1]
    assert not bot.of("editMessageReplyMarkup")


@pytest.mark.asyncio
async def test_cancel_and_other_buttons_refuse_strangers_and_other_machines(daemon, monkeypatch):
    running = await _add(daemon, ["sleep", "30"])
    await wait_for_status(daemon.socket_path, running, {"running"})
    bot = FakeBot([[_press(1, running, action="cn", chat_id=999), _press(2, running, action="cn", qur_id="beef"),
                    _press(3, running, action="lg", chat_id=999), _menu_msg(4, notify.MENU_LIST, chat_id=999)]])
    monkeypatch.setattr(notify, "_api", bot.api)
    _tg_cfg(daemon)
    await wait_calls(bot, "answerCallbackQuery", 1)
    await asyncio.sleep(0.3)
    assert "another machine" in bot.of("answerCallbackQuery")[0]["text"]  # the stranger got nothing at all
    assert len(bot.of("answerCallbackQuery")) == 1 and not bot.of("sendMessage")
    assert (await send(daemon.socket_path, "show", id=running))["data"]["status"] == "running"


# ---------------------------------------------------------------------------
# several bots
# ---------------------------------------------------------------------------

TG_TOKEN2 = "987654321:BBH-other_token-0123456789abcdefghijk"


class MultiBot:
    """Replaces notify._api for several bots: records (token, method, params)
    and serves scripted getUpdates batches per token."""

    def __init__(self, batches=None):
        self.batches = {t: list(b) for t, b in (batches or {}).items()}
        self.calls: list[tuple[str, str, dict]] = []
        self.polls: dict[str, int] = {}

    def api(self, token, method, params=None, timeout=0):
        if method == "getUpdates":
            self.polls[token] = self.polls.get(token, 0) + 1
            queue = self.batches.get(token) or []
            return queue.pop(0) if queue else []
        self.calls.append((token, method, params))
        return {}

    def of(self, method, token=None):
        return [(t, p) for t, m, p in self.calls if m == method and (token is None or t == token)]


async def _wait(cond, timeout=5.0):
    deadline = time.monotonic() + timeout
    while not cond():
        assert time.monotonic() < deadline, "timed out"
        await asyncio.sleep(0.02)


def _two_bots(daemon, *, listen1=True, listen2=True, chats1=(111,), chats2=(111, 222)):
    notify.save({"qur_id": "a3f9", "bots": [
        {"token": TG_TOKEN, "chat_ids": list(chats1), "listen": listen1},
        {"token": TG_TOKEN2, "chat_ids": list(chats2), "listen": listen2},
    ]}, daemon.notify_path)


@pytest.mark.asyncio
async def test_finished_job_is_sent_through_every_bot_to_each_of_its_chats(daemon, monkeypatch):
    bot = MultiBot()
    monkeypatch.setattr(notify, "_api", bot.api)
    _two_bots(daemon)
    job_id = await _finished_job(daemon)
    await _wait(lambda: len(bot.of("sendMessage")) == 3)
    sent = sorted((t, p["chat_id"]) for t, p in bot.of("sendMessage"))
    assert sent == sorted([(TG_TOKEN, 111), (TG_TOKEN2, 111), (TG_TOKEN2, 222)])
    for _t, p in bot.of("sendMessage"):
        assert p["reply_markup"]["inline_keyboard"][0][0]["callback_data"] == f"rq:a3f9:{job_id}"


@pytest.mark.asyncio
async def test_only_bots_with_listen_on_get_buttons_and_get_polled(daemon, monkeypatch):
    bot = MultiBot()
    monkeypatch.setattr(notify, "_api", bot.api)
    _two_bots(daemon, listen2=False)
    await _finished_job(daemon)
    await _wait(lambda: len(bot.of("sendMessage")) == 3)
    await _wait(lambda: bot.polls.get(TG_TOKEN, 0) >= 2)
    assert all("reply_markup" in p for _t, p in bot.of("sendMessage", TG_TOKEN))
    assert all("reply_markup" not in p for _t, p in bot.of("sendMessage", TG_TOKEN2))  # send-only bot
    assert TG_TOKEN2 not in bot.polls


@pytest.mark.asyncio
async def test_one_failing_bot_does_not_stop_the_others(daemon, monkeypatch):
    bot = MultiBot()

    def api(token, method, params=None, timeout=0):
        if token == TG_TOKEN and method == "sendMessage":
            raise notify.NotifyError("bot was blocked", 403)
        return bot.api(token, method, params, timeout)

    monkeypatch.setattr(notify, "_api", api)
    _two_bots(daemon, listen1=False, listen2=False)
    await _finished_job(daemon)
    await _wait(lambda: len(bot.of("sendMessage", TG_TOKEN2)) == 2)


@pytest.mark.asyncio
async def test_press_on_each_bot_is_answered_through_that_same_bot(daemon, monkeypatch):
    job_id = await _finished_job(daemon)
    bot = MultiBot({
        TG_TOKEN: [[_press(1, job_id, action="sh", chat_id=111)]],
        TG_TOKEN2: [[_press(1, job_id, action="sh", chat_id=222)]],
    })
    monkeypatch.setattr(notify, "_api", bot.api)
    _two_bots(daemon)
    await _wait(lambda: len(bot.of("sendMessage")) == 2)
    (t1, m1), = bot.of("sendMessage", TG_TOKEN)
    (t2, m2), = bot.of("sendMessage", TG_TOKEN2)
    assert m1["chat_id"] == 111 and m2["chat_id"] == 222 and m2["parse_mode"] == "HTML"
    assert len(bot.of("answerCallbackQuery", TG_TOKEN)) == 1 and len(bot.of("answerCallbackQuery", TG_TOKEN2)) == 1


@pytest.mark.asyncio
async def test_requeue_press_on_second_bot_requeues_once_and_edits_via_second_bot(daemon, monkeypatch):
    job_id = await _finished_job(daemon)
    bot = MultiBot({TG_TOKEN2: [[_press(7, job_id, chat_id=222, message_id=42)]]})
    monkeypatch.setattr(notify, "_api", bot.api)
    _two_bots(daemon)
    await _wait(lambda: len(bot.of("editMessageReplyMarkup")) == 1)
    (tok, edit), = bot.of("editMessageReplyMarkup")
    assert tok == TG_TOKEN2 and edit["chat_id"] == 222 and edit["message_id"] == 42
    listing = (await send(daemon.socket_path, "list"))["data"]
    new_ids = [j["id"] for j in _ch(listing, 1)["backlog"] if j["id"] != job_id]
    assert len(new_ids) == 1  # exactly one requeue, from one press
    assert not bot.of("editMessageReplyMarkup", TG_TOKEN)


@pytest.mark.asyncio
async def test_press_from_chat_paired_with_another_bot_only_is_ignored(daemon, monkeypatch):
    job_id = await _finished_job(daemon)
    # chat 222 is paired with bot 2 only; a press from it arriving on bot 1 must not count
    bot = MultiBot({TG_TOKEN: [[_press(1, job_id, chat_id=222)]]})
    monkeypatch.setattr(notify, "_api", bot.api)
    _two_bots(daemon)
    await _wait(lambda: bot.polls.get(TG_TOKEN, 0) >= 2)
    assert not bot.of("answerCallbackQuery") and not bot.of("editMessageReplyMarkup")
    listing = (await send(daemon.socket_path, "list"))["data"]
    assert [j["id"] for j in _ch(listing, 1)["backlog"]] == [job_id]


@pytest.mark.asyncio
async def test_each_bot_has_its_own_poller_and_offset(daemon, monkeypatch):
    seen: dict[str, list] = {}

    def api(token, method, params=None, timeout=0):
        if method == "getUpdates":
            seen.setdefault(token, []).append((params or {}).get("offset"))
            return [{"update_id": 10, "message": {"text": "x", "chat": {"id": 1}}}] if len(seen[token]) == 1 else []
        return {}

    monkeypatch.setattr(notify, "_api", api)
    _two_bots(daemon)
    await _wait(lambda: all(len(seen.get(t, [])) >= 2 for t in (TG_TOKEN, TG_TOKEN2)))
    assert seen[TG_TOKEN][:2] == [None, 11] and seen[TG_TOKEN2][:2] == [None, 11]


@pytest.mark.asyncio
async def test_bot_added_or_removed_while_running_starts_and_stops_its_poller(daemon, monkeypatch):
    bot = MultiBot()
    monkeypatch.setattr(notify, "_api", bot.api)
    notify.save({"qur_id": "a3f9", "token": TG_TOKEN, "chat_ids": [111]}, daemon.notify_path)  # old layout
    await _wait(lambda: bot.polls.get(TG_TOKEN, 0) >= 2)
    assert TG_TOKEN2 not in bot.polls
    _two_bots(daemon)  # add the second bot: no daemon restart
    await _wait(lambda: bot.polls.get(TG_TOKEN2, 0) >= 2)
    notify.save({"qur_id": "a3f9", "bots": [{"token": TG_TOKEN2, "chat_ids": [111]}]}, daemon.notify_path)  # drop the first
    await asyncio.sleep(0.4)
    frozen = bot.polls[TG_TOKEN]
    await asyncio.sleep(0.4)
    assert bot.polls[TG_TOKEN] == frozen and bot.polls[TG_TOKEN2] > 2


@pytest.mark.asyncio
async def test_pairing_pauses_every_poller_and_offsets_survive_it(daemon, monkeypatch):
    seen: dict[str, list] = {}

    def api(token, method, params=None, timeout=0):
        if method == "getUpdates":
            seen.setdefault(token, []).append((params or {}).get("offset"))
            return [{"update_id": 5, "message": {"text": "x", "chat": {"id": 1}}}] if len(seen[token]) == 1 else []
        return {}

    monkeypatch.setattr(notify, "_api", api)
    _two_bots(daemon)
    await _wait(lambda: all(len(seen.get(t, [])) >= 2 for t in (TG_TOKEN, TG_TOKEN2)))
    with notify.pairing_lock(daemon.notify_path):
        await asyncio.sleep(0.3)  # let the pollers notice and stop
        counts = {t: len(v) for t, v in seen.items()}
        await asyncio.sleep(0.3)
        assert {t: len(v) for t, v in seen.items()} == counts
    await _wait(lambda: all(len(seen[t]) > counts[t] for t in (TG_TOKEN, TG_TOKEN2)))
    assert all(v[-1] == 6 for v in seen.values())  # resumed after the handled update, not from scratch


@pytest.mark.asyncio
async def test_bot_with_no_chats_is_not_polled_and_sends_nothing(daemon, monkeypatch):
    bot = MultiBot()
    monkeypatch.setattr(notify, "_api", bot.api)
    _two_bots(daemon, chats1=())
    await _finished_job(daemon)
    await _wait(lambda: len(bot.of("sendMessage")) == 2)
    await asyncio.sleep(0.3)
    assert TG_TOKEN not in bot.polls and not bot.of("sendMessage", TG_TOKEN)


# ---------------------------------------------------------------------------
# hardening: file modes, bad requests
# ---------------------------------------------------------------------------

def _mode(path):
    return os.stat(path).st_mode & 0o777


@pytest.mark.asyncio
async def test_socket_db_and_log_modes_are_private_even_under_umask_000(tmp_path):
    old = os.umask(0o000)
    try:
        d = Daemon(db_path=tmp_path / "s" / "queuer.db", socket_path=tmp_path / "queuer.sock", log_dir=tmp_path / "logs")
        task = asyncio.create_task(d.serve())
        for _ in range(100):
            if d.socket_path.exists():
                break
            await asyncio.sleep(0.02)
        resp = await send(d.socket_path, "add", argv=["echo", "hi"], cwd=CWD)
        job = await wait_for_status(d.socket_path, resp["data"]["id"], {"done", "failed"})
        assert _mode(d.socket_path) == 0o600
        assert _mode(d.db_path) == 0o600
        assert _mode(d.db_path.parent) == 0o700
        assert _mode(d.log_dir) == 0o700
        assert _mode(job["log_path"]) == 0o600
    finally:
        os.umask(old)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


async def _raw(socket_path, payload: bytes) -> bytes:
    reader, writer = await asyncio.open_unix_connection(str(socket_path))
    writer.write(payload)
    await writer.drain()
    data = await reader.read()
    writer.close()
    return data


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [b"\xff\xfe\n", b"x" * 200_000 + b"\n", b"not json\n"],
    ids=["invalid-utf8", "oversized-line", "not-json"],
)
async def test_bad_request_gets_error_reply_not_a_crash(daemon, payload, capsys):
    data = await _raw(daemon.socket_path, payload)
    resp = json.loads(data)
    assert resp["ok"] is False and "invalid request" in resp["error"]
    assert "UnboundLocalError" not in capsys.readouterr().err
    # daemon still serves
    assert (await send(daemon.socket_path, "status"))["ok"]


# ---------------------------------------------------------------------------
# hardening: peer uid, private fallback socket dir
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_connection_from_another_uid_is_refused(daemon):
    real = daemon._uid
    daemon._uid = real + 1  # pretend we are someone else: our own connection is now "foreign"
    try:
        reply = await _raw(daemon.socket_path, b'{"cmd": "status"}\n')
    except ConnectionResetError:  # closed on us before we finished writing: also a refusal
        reply = b""
    assert reply == b""
    daemon._uid = real
    assert (await send(daemon.socket_path, "status"))["ok"]


def test_default_socket_path_falls_back_to_private_dir_not_bare_tmp(monkeypatch):
    from queuer.daemon import _default_socket_path

    for unset in (None, ""):
        if unset is None:
            monkeypatch.delenv("XDG_RUNTIME_DIR", raising=False)
        else:
            monkeypatch.setenv("XDG_RUNTIME_DIR", unset)
        monkeypatch.delenv("QUEUER_SOCKET_PATH", raising=False)
        path = _default_socket_path()
        assert path == Path(f"/tmp/queuer-{os.getuid()}/queuer.sock")
    monkeypatch.setenv("XDG_RUNTIME_DIR", "/run/user/1")
    assert _default_socket_path() == Path("/run/user/1/queuer.sock")


def test_ensure_private_dir(tmp_path, monkeypatch):
    from queuer.daemon import _ensure_private_dir

    d = tmp_path / "q"
    _ensure_private_dir(d)
    assert _mode(d) == 0o700
    os.chmod(d, 0o755)  # ours but loose -> tightened
    _ensure_private_dir(d)
    assert _mode(d) == 0o700
    link = tmp_path / "link"
    link.symlink_to(d)  # symlink planted in place of the dir -> refused
    with pytest.raises(RuntimeError):
        _ensure_private_dir(link)
    monkeypatch.setattr(os, "getuid", lambda: os.stat(d).st_uid + 1)  # someone else's dir -> refused
    with pytest.raises(RuntimeError):
        _ensure_private_dir(d)


@pytest.mark.asyncio
async def test_serve_creates_private_fallback_dir(tmp_path, monkeypatch):
    fb = tmp_path / "queuer-fallback"
    monkeypatch.setattr("queuer.daemon._fallback_socket_dir", lambda: fb)
    d = Daemon(db_path=tmp_path / "q.db", socket_path=fb / "queuer.sock", log_dir=tmp_path / "logs")
    task = asyncio.create_task(d.serve())
    try:
        for _ in range(100):
            if d.socket_path.exists():
                break
            await asyncio.sleep(0.02)
        assert _mode(fb) == 0o700 and _mode(d.socket_path) == 0o600
        assert (await send(d.socket_path, "status"))["ok"]
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


# ---------------------------------------------------------------------------
# hardening: one daemon per socket, state-folder modes
# ---------------------------------------------------------------------------

async def _stop(task):
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_second_daemon_refuses_and_leaves_first_untouched(daemon, tmp_path):
    from queuer.daemon import AlreadyRunning

    resp = await send(daemon.socket_path, "add", argv=["sleep", "30"], cwd=CWD)
    job_id = resp["data"]["id"]
    await wait_for_status(daemon.socket_path, job_id, {"running"})

    second = Daemon(db_path=daemon.db_path, socket_path=daemon.socket_path, log_dir=daemon.log_dir)
    with pytest.raises(AlreadyRunning):
        await second.serve()
    assert second.conn is None  # never touched the db (would have failed the running job)

    shown = await send(daemon.socket_path, "show", id=job_id)
    assert shown["data"]["status"] == "running"
    assert (await send(daemon.socket_path, "status"))["ok"]  # socket not stolen
    await send(daemon.socket_path, "cancel", id=job_id)


@pytest.mark.asyncio
async def test_stale_socket_file_does_not_block_start(tmp_path):
    import socket as sk

    sock = tmp_path / "queuer.sock"
    s = sk.socket(sk.AF_UNIX)
    s.bind(str(sock))
    s.close()  # leftover file, nobody listening
    d = Daemon(db_path=tmp_path / "q.db", socket_path=sock, log_dir=tmp_path / "logs")
    task = asyncio.create_task(d.serve())
    try:
        for _ in range(100):
            if (await _try_status(sock)):
                break
            await asyncio.sleep(0.02)
        else:
            pytest.fail("daemon did not come up over a stale socket")
    finally:
        await _stop(task)


async def _try_status(sock):
    try:
        return (await send(sock, "status"))["ok"]
    except OSError:
        return False


def test_main_exits_1_with_message_when_already_running(monkeypatch, capsys):
    from queuer import daemon as dmod

    async def boom(self):
        raise dmod.AlreadyRunning("queuerd is already running (x)")

    monkeypatch.setattr(dmod.Daemon, "serve", boom)
    with pytest.raises(SystemExit) as e:
        dmod.main()
    assert e.value.code == 1
    assert "already running" in capsys.readouterr().err


def test_main_ends_the_process_right_after_a_clean_shutdown(monkeypatch):
    # 5.todo item 14: no waiting for the executor's threads (a Telegram long poll) once serve() is done
    from queuer import daemon as dmod

    events = []

    async def fake_serve(self):
        events.append("served")

    def fake_exit(code):
        events.append(("exit", code))
        raise SystemExit(code)  # os._exit would end pytest too

    monkeypatch.setattr(dmod.Daemon, "serve", fake_serve)
    monkeypatch.setattr(dmod, "_hard_exit", fake_exit)
    with pytest.raises(SystemExit) as e:
        dmod.main()
    assert e.value.code == 0 and events == ["served", ("exit", 0)]


def test_main_does_not_hard_exit_when_serve_fails(monkeypatch):
    from queuer import daemon as dmod

    async def crash(self):
        raise RuntimeError("worker crashed")

    exits = []
    monkeypatch.setattr(dmod.Daemon, "serve", crash)
    monkeypatch.setattr(dmod, "_hard_exit", lambda code: exits.append(code))
    with pytest.raises(RuntimeError, match="worker crashed"):
        dmod.main()
    assert exits == []  # the error surfaces as before, with a traceback and a non-zero exit


def test_hard_exit_flushes_both_streams_then_calls_os_exit(monkeypatch):
    from queuer import daemon as dmod

    flushed = []

    class Stream:
        def __init__(self, name, broken=False):
            self.name, self.broken = name, broken

        def flush(self):
            flushed.append(self.name)
            if self.broken:
                raise ValueError("I/O operation on closed file")

    codes = []
    monkeypatch.setattr(dmod.sys, "stdout", Stream("out", broken=True))
    monkeypatch.setattr(dmod.sys, "stderr", Stream("err"))
    monkeypatch.setattr(dmod.os, "_exit", lambda code: codes.append(code))
    dmod._hard_exit(0)
    assert flushed == ["out", "err"] and codes == [0]  # a closed stdout does not stop the exit


@pytest.mark.asyncio
async def test_default_state_folders_are_tightened(tmp_path, monkeypatch):
    state = tmp_path / "state"
    state.mkdir(mode=0o755)
    os.chmod(state, 0o755)
    monkeypatch.setattr(db_module, "DEFAULT_DB_PATH", state / "queuer.db")
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.delenv("QUEUER_DB_PATH", raising=False)
    monkeypatch.delenv("QUEUER_LOG_DIR", raising=False)
    logs = tmp_path / "home" / ".local" / "state" / "queuer" / "logs"
    logs.mkdir(parents=True)
    os.chmod(logs, 0o755)
    d = Daemon(socket_path=tmp_path / "queuer.sock")
    assert d.log_dir == logs and d.db_path.parent == state
    task = asyncio.create_task(d.serve())
    try:
        for _ in range(100):
            if d.socket_path.exists():
                break
            await asyncio.sleep(0.02)
        assert _mode(state) == 0o700 and _mode(logs) == 0o700
    finally:
        await _stop(task)


@pytest.mark.asyncio
async def test_user_chosen_folders_are_never_chmoded_only_warned(tmp_path, capsys):
    shared_db, shared_logs = tmp_path / "shared-db", tmp_path / "shared-logs"
    for p in (shared_db, shared_logs):
        p.mkdir()
        os.chmod(p, 0o755)
    d = Daemon(db_path=shared_db / "q.db", socket_path=tmp_path / "queuer.sock", log_dir=shared_logs)
    task = asyncio.create_task(d.serve())
    try:
        for _ in range(100):
            if d.socket_path.exists():
                break
            await asyncio.sleep(0.02)
        assert _mode(shared_db) == 0o755 and _mode(shared_logs) == 0o755
        err = capsys.readouterr().err
        assert str(shared_db) in err and str(shared_logs) in err and "warning" in err
        assert _mode(shared_db / "q.db") == 0o600  # the files themselves stay private
    finally:
        await _stop(task)


# ---------------------------------------------------------------------------
# robustness: bad add fields, unwritable log, large uids
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize("bad", [
    {"timeout_secs": "abc"}, {"timeout_secs": -1}, {"timeout_secs": True}, {"timeout_secs": 1.5},
    {"channel": "x"}, {"channel": -2}, {"before": "a"}, {"after": [1]},
    {"argv": "echo hi"}, {"argv": ["echo", 1]}, {"cwd": 5},
])
async def test_add_rejects_bad_field_types_and_daemon_survives(daemon, bad):
    args = {"argv": ["echo", "x"], "cwd": CWD, **bad}
    resp = await send(daemon.socket_path, "add", **args)
    assert resp["ok"] is False
    await asyncio.sleep(1.3)  # a bad value used to kill the worker, and with it the daemon
    ok = await send(daemon.socket_path, "add", argv=["echo", "fine"], cwd=CWD)
    job = await wait_for_status(daemon.socket_path, ok["data"]["id"], {"done", "failed"})
    assert job["status"] == "done"


@pytest.mark.asyncio
@pytest.mark.skipif(not os.path.exists("/dev/full"), reason="needs /dev/full")
async def test_unwritable_log_does_not_kill_job_or_daemon(daemon, capsys, monkeypatch):
    real_open = os.open
    # make the log's fd point at /dev/full: every write fails with ENOSPC
    monkeypatch.setattr(os, "open", lambda p, *a, **k: real_open("/dev/full", os.O_WRONLY) if str(p).endswith("1.log") else real_open(p, *a, **k))
    resp = await send(daemon.socket_path, "add", argv=["echo", "x"], cwd=CWD)
    job = await wait_for_status(daemon.socket_path, resp["data"]["id"], {"done", "failed"})
    assert job["status"] == "done" and job["exit_code"] == 0
    assert "cannot write log" in capsys.readouterr().err
    assert (await send(daemon.socket_path, "status"))["ok"]


def test_peer_check_reads_uid_as_unsigned(daemon):
    import struct

    big = 4294967294  # e.g. a rootless-container uid; "3i" would read it as -2

    class Sock:
        def getsockopt(self, *a):
            return struct.pack("iII", 1, big, 1)

    class Writer:
        def get_extra_info(self, name):
            return Sock()

    daemon._uid = big
    assert daemon._peer_is_me(Writer()) is True
    daemon._uid = 1000
    assert daemon._peer_is_me(Writer()) is False



# ---- audit round: a bad request must never stall a channel or kill the daemon ----

@pytest.mark.asyncio
@pytest.mark.parametrize("extra", [
    {"virtual_env": 5}, {"python_path": ["a"]}, {"path": 7}, {"raw_argv": [1, 2]},
    {"virtual_env": "a\0b"}, {"cwd": "/nonexistent-dir-xyz"}, {"argv": ["echo", "a\0b"]},
])
async def test_add_rejects_bad_fields_and_channel_keeps_working(daemon, extra):
    args = {"argv": ["echo", "hi"], "cwd": CWD, "channel": 7, **extra}
    resp = await send(daemon.socket_path, "add", **args)
    assert resp["ok"] is False
    ok = await send(daemon.socket_path, "add", argv=["echo", "fine"], cwd=CWD, channel=7)
    job = await wait_for_status(daemon.socket_path, ok["data"]["id"], {"done", "failed"})
    assert job["status"] == "done"


@pytest.mark.asyncio
async def test_runner_crash_fails_the_job_and_worker_survives(daemon, monkeypatch, capsys):
    real = daemon._run_job
    calls = {"n": 0}

    async def boom(channel, job):
        calls["n"] += 1
        if calls["n"] == 1:
            raise TypeError("simulated")
        await real(channel, job)

    monkeypatch.setattr(daemon, "_run_job", boom)
    a = await send(daemon.socket_path, "add", argv=["echo", "a"], cwd=CWD, channel=5)
    j1 = await wait_for_status(daemon.socket_path, a["data"]["id"], {"done", "failed"})
    assert j1["status"] == "failed" and "internal error" in j1["note"]
    b = await send(daemon.socket_path, "add", argv=["echo", "b"], cwd=CWD, channel=5)
    j2 = await wait_for_status(daemon.socket_path, b["data"]["id"], {"done", "failed"})
    assert j2["status"] == "done"


@pytest.mark.asyncio
async def test_dead_worker_is_respawned_on_next_add(daemon):
    daemon._ensure_channel_worker(6)
    task = daemon._worker_tasks[6]
    task.cancel()
    await asyncio.sleep(0.05)
    r = await send(daemon.socket_path, "add", argv=["echo", "x"], cwd=CWD, channel=6)
    job = await wait_for_status(daemon.socket_path, r["data"]["id"], {"done", "failed"})
    assert job["status"] == "done"


class _FakeProc:
    """Just enough of asyncio's Process for Daemon._wait_running_time."""

    def __init__(self, code: int = 7) -> None:
        self._done = asyncio.Event()
        self._code = code

    async def wait(self) -> int:
        await self._done.wait()
        return self._code

    def finish(self) -> None:
        self._done.set()


def _freeze_slot(daemon, channel, job_id, proc):
    daemon._frozen[channel] = (job_id, proc)
    daemon._freeze_changed(channel)


def _unfreeze_slot(daemon, channel):
    daemon._frozen.pop(channel)
    daemon._freeze_changed(channel)


@pytest.mark.asyncio
async def test_wait_running_time_raises_after_the_timeout_without_polling(daemon):
    loop = asyncio.get_running_loop()
    start = loop.time()
    with pytest.raises(asyncio.TimeoutError):
        await daemon._wait_running_time(_FakeProc(), 1, 1, 0.4)
    assert 0.35 <= loop.time() - start < 1.5


@pytest.mark.asyncio
async def test_wait_running_time_returns_the_exit_code_when_the_job_ends_first(daemon):
    proc = _FakeProc(code=3)
    asyncio.get_running_loop().call_later(0.1, proc.finish)
    assert await daemon._wait_running_time(proc, 1, 1, 30) == 3


@pytest.mark.asyncio
async def test_wait_running_time_does_not_count_the_time_frozen_from_the_start(daemon):
    loop = asyncio.get_running_loop()
    proc = _FakeProc()
    _freeze_slot(daemon, 1, 1, proc)
    loop.call_later(0.5, _unfreeze_slot, daemon, 1)
    start = loop.time()
    with pytest.raises(asyncio.TimeoutError):
        await daemon._wait_running_time(proc, 1, 1, 0.4)
    # 0.5 s frozen (not counted) + 0.4 s of run time
    assert loop.time() - start >= 0.85


@pytest.mark.asyncio
async def test_wait_running_time_books_the_run_time_before_a_freeze_and_resumes_the_rest(daemon):
    loop = asyncio.get_running_loop()
    proc = _FakeProc()
    loop.call_later(0.2, _freeze_slot, daemon, 1, 1, proc)
    loop.call_later(0.7, _unfreeze_slot, daemon, 1)
    start = loop.time()
    with pytest.raises(asyncio.TimeoutError):
        await daemon._wait_running_time(proc, 1, 1, 0.5)
    # 0.2 s run + 0.5 s frozen + the remaining 0.3 s of run time
    assert loop.time() - start >= 0.95


@pytest.mark.asyncio
async def test_wait_running_time_returns_when_the_job_ends_while_frozen(daemon):
    loop = asyncio.get_running_loop()
    proc = _FakeProc(code=-9)
    _freeze_slot(daemon, 1, 1, proc)
    loop.call_later(0.1, proc.finish)
    assert await asyncio.wait_for(daemon._wait_running_time(proc, 1, 1, 0.2), timeout=3) == -9
    daemon._frozen.clear()  # a fake process must not be left for the daemon's shutdown to signal


@pytest.mark.asyncio
async def test_wait_running_time_ignores_another_jobs_freeze_on_the_channel(daemon):
    loop = asyncio.get_running_loop()
    proc = _FakeProc()
    _freeze_slot(daemon, 1, 99, _FakeProc())  # some other job is frozen: this one keeps running
    start = loop.time()
    with pytest.raises(asyncio.TimeoutError):
        await daemon._wait_running_time(proc, 1, 1, 0.3)
    assert loop.time() - start < 1.5
    daemon._frozen.clear()


@pytest.mark.asyncio
async def test_timeout_does_not_count_time_frozen_by_now(daemon):
    slow = await send(daemon.socket_path, "add", argv=["sleep", "1.5"], cwd=CWD, channel=9, timeout_secs=2)
    await asyncio.sleep(0.4)
    await send(daemon.socket_path, "add", argv=["sleep", "2.5"], cwd=CWD, channel=9, now=True)
    job = await wait_for_status(daemon.socket_path, slow["data"]["id"], {"done", "failed"}, timeout=15)
    # ran ~1.5s in total (+2.5s frozen): wall clock would have killed it at 2s
    assert job["status"] == "done"


@pytest.mark.asyncio
@pytest.mark.parametrize("cmd,args", [
    ("cancel", {"id": "x"}), ("cancel", {"channel": "x"}), ("show", {"id": "1"}), ("rm", {"id": None}),
    ("requeue", {"id": True}), ("pause", {"channel": "x"}), ("resume", {"channel": -1}), ("clear", {"channel": "x"}),
])
async def test_handlers_reject_wrong_types_cleanly(daemon, cmd, args):
    resp = await send(daemon.socket_path, cmd, **args)
    assert resp["ok"] is False
    assert "internal error" not in resp["error"]


@pytest.mark.asyncio
async def test_non_object_args_and_private_names_are_rejected(daemon):
    r, w = await asyncio.open_unix_connection(str(daemon.socket_path))
    w.write(b'{"cmd": "add", "args": [1]}\n')
    await w.drain()
    assert json.loads(await r.readline())["ok"] is False
    w.close()
    r, w = await asyncio.open_unix_connection(str(daemon.socket_path))
    w.write(b'{"cmd": "_x"}\n')
    await w.drain()
    assert json.loads(await r.readline())["ok"] is False
    w.close()


@pytest.mark.asyncio
async def test_silent_client_is_dropped(daemon, monkeypatch):
    from queuer import daemon as daemon_mod
    monkeypatch.setattr(daemon_mod, "CLIENT_READ_TIMEOUT_SECS", 0.3)
    r, w = await asyncio.open_unix_connection(str(daemon.socket_path))
    assert await asyncio.wait_for(r.read(), timeout=3) == b""  # server closed it
    w.close()
    assert (await send(daemon.socket_path, "status"))["ok"]


@pytest.mark.asyncio
@pytest.mark.skipif(not os.path.exists("/dev/full"), reason="needs a symlink-capable fs")
async def test_log_symlink_is_not_followed(daemon, tmp_path, capsys):
    victim = tmp_path / "victim.txt"
    victim.write_text("keep")
    (daemon.log_dir / "1.log").symlink_to(victim)
    r = await send(daemon.socket_path, "add", argv=["echo", "x"], cwd=CWD)
    job = await wait_for_status(daemon.socket_path, r["data"]["id"], {"done", "failed"})
    assert job["status"] == "done"
    assert victim.read_text() == "keep"


def test_telegram_offset_is_persisted(daemon):
    from queuer import db as dbm
    dbm.set_state(daemon.conn, "tg_offset:123456789", "42")
    assert dbm.get_state(daemon.conn, "tg_offset:123456789") == "42"
    assert dbm.get_state(daemon.conn, "nope") is None


# ---------------------------------------------------------------------------
# round 3: idle wake-ups (B11), stale presses and double taps (B6), client cap (S7)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_idle_worker_is_woken_by_add_and_resume_not_by_polling(tmp_path, monkeypatch):
    from queuer import daemon as daemon_mod
    monkeypatch.setattr(daemon_mod, "IDLE_POLL_SECS", 3600)  # a poll could never explain a pickup
    d = Daemon(db_path=tmp_path / "queuer.db", socket_path=tmp_path / "queuer.sock", log_dir=tmp_path / "logs")
    task = asyncio.create_task(d.serve())
    try:
        for _ in range(100):
            if d.socket_path.exists():
                break
            await asyncio.sleep(0.02)
        await asyncio.sleep(0.2)  # the worker is now idle in its long wait

        first = (await send(d.socket_path, "add", argv=["echo", "hi"], cwd=CWD))["data"]["id"]
        assert (await wait_for_status(d.socket_path, first, {"done", "failed"}))["status"] == "done"

        await send(d.socket_path, "pause")
        second = (await send(d.socket_path, "add", argv=["echo", "hi"], cwd=CWD))["data"]["id"]
        await asyncio.sleep(0.2)
        assert (await send(d.socket_path, "show", id=second))["data"]["status"] == "queued"
        await send(d.socket_path, "resume")
        assert (await wait_for_status(d.socket_path, second, {"done", "failed"}))["status"] == "done"
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


def test_idle_poll_is_a_slow_safety_net():
    from queuer import daemon as daemon_mod
    assert daemon_mod.IDLE_POLL_SECS >= 10


class RecordingBot(FakeBot):
    """FakeBot that also remembers the params of every getUpdates call."""

    def __init__(self, batches=()):
        super().__init__(batches)
        self.poll_params: list[dict] = []

    def api(self, token, method, params=None, timeout=0):
        if method == "getUpdates":
            self.poll_params.append(params)
        return super().api(token, method, params, timeout)


def _from_chat(press: dict, user_id: int = 111) -> dict:
    press["callback_query"]["from"] = {"id": user_id}
    return press


def _requeue_children(daemon, job_id: int) -> int:
    row = daemon.conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE parent_id = ? AND origin = 'requeue'", (job_id,)
    ).fetchone()
    return row[0]


@pytest.mark.asyncio
async def test_presses_that_piled_up_while_offline_are_dropped_not_run(daemon, monkeypatch):
    job_id = await _finished_job(daemon)
    db_module.set_state(daemon.conn, f"tg_alive:{notify.bot_id(TG_TOKEN)}", str(int(time.time()) - 3600))
    stale = _from_chat(_press(5, job_id))
    fresh = _press(6, job_id)
    # batch 1 = what waited while we were off, batch 2 = backlog gone, batch 3 = a real press
    bot = RecordingBot([[stale], [], [fresh]])
    monkeypatch.setattr(notify, "_api", bot.api)
    _tg_cfg(daemon)
    await wait_calls(bot, "editMessageReplyMarkup", 1)

    assert [p["timeout"] for p in bot.poll_params[:3]] == [0, 0, notify.POLL_SECS]  # drained without waiting, then normal
    assert _requeue_children(daemon, job_id) == 1  # only the fresh press ran
    texts = [a["text"] for a in bot.of("answerCallbackQuery")]
    assert any("offline" in t for t in texts)  # the stale press was told to press again
    assert any(t.startswith("requeued as job") for t in texts)


@pytest.mark.asyncio
async def test_short_outage_keeps_presses(daemon, monkeypatch):
    job_id = await _finished_job(daemon)
    db_module.set_state(daemon.conn, f"tg_alive:{notify.bot_id(TG_TOKEN)}", str(int(time.time()) - 10))
    bot = RecordingBot([[_press(5, job_id)]])
    monkeypatch.setattr(notify, "_api", bot.api)
    _tg_cfg(daemon)
    await wait_calls(bot, "editMessageReplyMarkup", 1)
    assert bot.poll_params[0]["timeout"] == notify.POLL_SECS
    assert _requeue_children(daemon, job_id) == 1


@pytest.mark.asyncio
async def test_first_start_polls_normally_and_records_being_alive(daemon, monkeypatch):
    bot = RecordingBot()
    monkeypatch.setattr(notify, "_api", bot.api)
    _tg_cfg(daemon)
    for _ in range(100):
        if bot.polls >= 1:
            break
        await asyncio.sleep(0.05)
    assert bot.poll_params[0]["timeout"] == notify.POLL_SECS  # no stamp yet: nothing is dropped
    key = f"tg_alive:{notify.bot_id(TG_TOKEN)}"
    await _wait(lambda: db_module.get_state(daemon.conn, key) is not None)
    assert abs(int(db_module.get_state(daemon.conn, key)) - time.time()) < 30


@pytest.mark.asyncio
async def test_double_tap_requeues_once(daemon, monkeypatch):
    job_id = await _finished_job(daemon)
    # two taps = two callback ids on the same old keyboard
    bot = FakeBot([[_press(1, job_id), _press(2, job_id)]])
    monkeypatch.setattr(notify, "_api", bot.api)
    _tg_cfg(daemon)
    await wait_calls(bot, "answerCallbackQuery", 2)
    texts = [a["text"] for a in bot.of("answerCallbackQuery")]
    assert texts[0].startswith("requeued as job")
    assert texts[1].startswith("already requeued as job")
    assert _requeue_children(daemon, job_id) == 1
    assert len(bot.of("editMessageReplyMarkup")) == 1


@pytest.mark.asyncio
async def test_requeue_guard_is_only_a_short_window_and_never_blocks_the_cli(daemon, monkeypatch):
    from queuer import daemon as daemon_mod
    monkeypatch.setattr(daemon_mod, "REQUEUE_GUARD_SECS", 0)  # window closed
    job_id = await _finished_job(daemon)
    bot = FakeBot([[_press(1, job_id)], [_press(2, job_id)]])
    monkeypatch.setattr(notify, "_api", bot.api)
    _tg_cfg(daemon)
    await wait_calls(bot, "answerCallbackQuery", 2)
    assert _requeue_children(daemon, job_id) == 2

    # `qur requeue` has no guard at all
    r1 = await send(daemon.socket_path, "requeue", id=job_id)
    r2 = await send(daemon.socket_path, "requeue", id=job_id)
    assert r1["ok"] and r2["ok"] and r1["data"]["id"] != r2["data"]["id"]


@pytest.mark.asyncio
async def test_too_many_connections_get_a_clean_error_and_the_count_recovers(daemon, monkeypatch):
    from queuer import daemon as daemon_mod
    monkeypatch.setattr(daemon_mod, "MAX_CLIENTS", 0)
    resp = await send(daemon.socket_path, "status")
    assert resp["ok"] is False and "too many connections" in resp["error"]
    monkeypatch.setattr(daemon_mod, "MAX_CLIENTS", 32)
    assert (await send(daemon.socket_path, "status"))["ok"]
    await asyncio.sleep(0.1)
    assert daemon._clients == 0  # every finished connection gave its slot back


# ---------------------------------------------------------------------------
# B5: a daemon stop records running jobs as "interrupted", never "failed"
# ---------------------------------------------------------------------------

def _make_daemon(tmp_path, **kw) -> Daemon:
    return Daemon(db_path=tmp_path / "queuer.db", socket_path=tmp_path / "queuer.sock", log_dir=tmp_path / "logs", **kw)


async def _start(d: Daemon) -> "asyncio.Task":
    d.socket_path.unlink(missing_ok=True)  # an earlier daemon of the same test may have left its socket file
    task = asyncio.create_task(d.serve())
    for _ in range(100):
        if d.socket_path.exists():
            break
        await asyncio.sleep(0.02)
    return task


def _alive(pid: int) -> bool:
    """Running, not just a zombie nobody has reaped yet (a container's PID 1 may not reap)."""
    try:
        os.kill(pid, 0)
        state = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0]
    except (ProcessLookupError, FileNotFoundError):
        return False
    return state != "Z"


@pytest.mark.asyncio
async def test_clean_stop_marks_running_job_interrupted_kills_it_and_keeps_the_queue(tmp_path):
    d = _make_daemon(tmp_path)
    task = await _start(d)
    running = (await send(d.socket_path, "add", argv=["sleep", "60"], cwd=CWD))["data"]["id"]
    waiting = (await send(d.socket_path, "add", argv=["true"], cwd=CWD))["data"]["id"]
    job = await wait_for_status(d.socket_path, running, {"running"})

    d._stopping.set()  # what the SIGTERM handler does
    await asyncio.wait_for(task, timeout=15)  # serve() returns by itself: no exception, no cancel

    conn = db_module.connect(d.db_path)
    try:
        rec = db_module.get_job(conn, running)
        assert rec["status"] == "interrupted"
        assert rec["note"] == "daemon stopped"
        assert rec["finished_at"] is not None
        assert db_module.get_job(conn, waiting)["status"] == "queued"  # the next job did NOT start
    finally:
        conn.close()
    assert not _alive(job["pid"])


@pytest.mark.asyncio
async def test_real_sigterm_stops_the_daemon_gracefully(tmp_path):
    d = _make_daemon(tmp_path)
    task = await _start(d)
    job_id = (await send(d.socket_path, "add", argv=["sleep", "60"], cwd=CWD))["data"]["id"]
    job = await wait_for_status(d.socket_path, job_id, {"running"})

    os.kill(os.getpid(), signal.SIGTERM)  # handler installed by serve(); must not kill the test run
    await asyncio.wait_for(task, timeout=15)

    conn = db_module.connect(d.db_path)
    try:
        assert db_module.get_job(conn, job_id)["status"] == "interrupted"
    finally:
        conn.close()
    assert not _alive(job["pid"])


@pytest.mark.asyncio
async def test_interrupted_job_is_not_retried_and_does_not_use_up_retries(tmp_path):
    d = _make_daemon(tmp_path)
    task = await _start(d)
    job_id = (await send(d.socket_path, "add", argv=["sleep", "60"], cwd=CWD, max_retries=3))["data"]["id"]
    await wait_for_status(d.socket_path, job_id, {"running"})
    d._stopping.set()
    await asyncio.wait_for(task, timeout=15)

    conn = db_module.connect(d.db_path)
    try:
        rows = conn.execute("SELECT id, status, attempt FROM jobs").fetchall()
    finally:
        conn.close()
    assert [(r["id"], r["status"], r["attempt"]) for r in rows] == [(job_id, "interrupted", 0)]  # no retry row


@pytest.mark.asyncio
async def test_clean_stop_kills_a_backgrounded_child_that_ignores_sigterm(tmp_path, monkeypatch):
    monkeypatch.setattr("queuer.daemon.DEFAULT_GRACE_SECS", 0.3)
    d = _make_daemon(tmp_path)
    task = await _start(d)
    marker = tmp_path / "child.pid"
    script = f"trap '' TERM; sleep 60 & echo $! > {marker}; wait"
    job_id = (await send(d.socket_path, "add", argv=["sh", "-c", script], cwd=CWD))["data"]["id"]
    await wait_for_status(d.socket_path, job_id, {"running"})
    for _ in range(100):
        if marker.exists() and marker.read_text().strip():
            break
        await asyncio.sleep(0.02)
    child = int(marker.read_text())

    d._stopping.set()
    await asyncio.wait_for(task, timeout=15)

    conn = db_module.connect(d.db_path)
    try:
        assert db_module.get_job(conn, job_id)["status"] == "interrupted"
    finally:
        conn.close()
    await asyncio.sleep(0.1)
    assert not _alive(child)  # SIGKILLed after the grace period, not left holding the log pipe


@pytest.mark.asyncio
async def test_clean_stop_interrupts_a_frozen_job_and_the_priority_job(tmp_path):
    d = _make_daemon(tmp_path)
    task = await _start(d)
    slow = (await send(d.socket_path, "add", argv=["sleep", "60"], cwd=CWD))["data"]["id"]
    await wait_for_status(d.socket_path, slow, {"running"})
    prio = (await send(d.socket_path, "add", argv=["sleep", "60"], cwd=CWD, now=True))["data"]["id"]
    await wait_for_status(d.socket_path, slow, {"stopped"})
    await wait_for_status(d.socket_path, prio, {"running"})

    d._stopping.set()
    await asyncio.wait_for(task, timeout=15)

    conn = db_module.connect(d.db_path)
    try:
        assert db_module.get_job(conn, slow)["status"] == "interrupted"
        assert db_module.get_job(conn, prio)["status"] == "interrupted"
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_clean_stop_sends_the_telegram_message_before_the_daemon_exits(tmp_path, monkeypatch):
    msgs: list = []
    monkeypatch.setattr(notify, "_api", lambda token, method, params=None, timeout=0: msgs.append(params))
    notify_path = tmp_path / "notify.json"
    notify.save({"qur_id": "a3f9", "token": TG_TOKEN, "chat_ids": [111], "listen": False}, notify_path)
    d = _make_daemon(tmp_path, notify_path=notify_path)
    task = await _start(d)
    job_id = (await send(d.socket_path, "add", argv=["sleep", "60"], cwd=CWD))["data"]["id"]
    await wait_for_status(d.socket_path, job_id, {"running"})

    d._stopping.set()
    await asyncio.wait_for(task, timeout=15)

    assert len(msgs) == 1  # already delivered when serve() returned, no sleeping needed
    assert f"interrupted (daemon stopped) · job {job_id}" in msgs[0]["text"]
    assert "failed" not in msgs[0]["text"]


@pytest.mark.asyncio
async def test_stopping_daemon_starts_no_new_job(tmp_path):
    d = _make_daemon(tmp_path)
    task = await _start(d)
    first = (await send(d.socket_path, "add", argv=["sleep", "60"], cwd=CWD))["data"]["id"]
    await wait_for_status(d.socket_path, first, {"running"})
    second = (await send(d.socket_path, "add", argv=["true"], cwd=CWD))["data"]["id"]
    d._stopping.set()
    d._wake_worker(1)  # even a wake-up (new job, resume, ...) must not start the next job
    await asyncio.wait_for(task, timeout=15)

    conn = db_module.connect(d.db_path)
    try:
        assert db_module.get_job(conn, second)["status"] == "queued"
        assert db_module.get_job(conn, second)["started_at"] is None
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_restart_continues_with_the_next_queued_job(tmp_path):
    d = _make_daemon(tmp_path)
    task = await _start(d)
    first = (await send(d.socket_path, "add", argv=["sleep", "60"], cwd=CWD))["data"]["id"]
    second = (await send(d.socket_path, "add", argv=["true"], cwd=CWD))["data"]["id"]
    await wait_for_status(d.socket_path, first, {"running"})
    d._stopping.set()
    await asyncio.wait_for(task, timeout=15)

    d2 = _make_daemon(tmp_path)  # "systemctl restart": same db, new daemon
    task2 = await _start(d2)
    try:
        assert (await wait_for_status(d2.socket_path, second, {"done"}))["status"] == "done"
        assert (await send(d2.socket_path, "show", id=first))["data"]["status"] == "interrupted"
    finally:
        task2.cancel()
        try:
            await task2
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_requeue_works_on_an_interrupted_job(tmp_path):
    d = _make_daemon(tmp_path)
    task = await _start(d)
    job_id = (await send(d.socket_path, "add", argv=["sleep", "60"], cwd=CWD))["data"]["id"]
    await wait_for_status(d.socket_path, job_id, {"running"})
    d._stopping.set()
    await asyncio.wait_for(task, timeout=15)

    d2 = _make_daemon(tmp_path)
    task2 = await _start(d2)
    try:
        resp = await send(d2.socket_path, "requeue", id=job_id)
        assert resp["ok"], resp
        new = (await send(d2.socket_path, "show", id=resp["data"]["id"]))["data"]
        assert new["status"] in ("queued", "running")
        assert new["parent_id"] == job_id
        await send(d2.socket_path, "cancel", id=new["id"])
        await wait_for_status(d2.socket_path, new["id"], {"cancelled"})
    finally:
        task2.cancel()
        try:
            await task2
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_startup_recovery_marks_a_frozen_job_interrupted_too(tmp_path):
    conn = db_module.connect(tmp_path / "queuer.db")
    db_module.init_db(conn)
    job_id = db_module.enqueue(
        conn, raw_cmd=["sleep", "999"], resolved_cmd=["/bin/sleep", "999"], cwd=CWD,
        log_path=str(tmp_path / "logs" / "1.log"),
    )
    db_module.mark_running(conn, job_id, pid=999999, pgid=999999)
    db_module.mark_stopped(conn, job_id)  # frozen by --now when the old daemon died
    conn.close()

    d = _make_daemon(tmp_path)
    task = await _start(d)
    try:
        resp = await send(d.socket_path, "show", id=job_id)
        assert resp["data"]["status"] == "interrupted"
        assert resp["data"]["note"] == "daemon stopped"
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_job_cancelled_by_the_user_stays_cancelled(daemon):
    job_id = (await send(daemon.socket_path, "add", argv=["sleep", "60"], cwd=CWD))["data"]["id"]
    await wait_for_status(daemon.socket_path, job_id, {"running"})
    await send(daemon.socket_path, "cancel", id=job_id)
    assert (await wait_for_status(daemon.socket_path, job_id, {"cancelled", "interrupted", "failed"}))["status"] == "cancelled"


@pytest.mark.asyncio
async def test_job_killed_by_signal_is_failed_when_no_stop_is_happening(daemon):
    # exit code -15 on its own (someone ran `kill`): the daemon is not stopping, so it IS a failure
    job_id = (await send(daemon.socket_path, "add", argv=["sh", "-c", "kill -TERM $$"], cwd=CWD))["data"]["id"]
    job = await wait_for_status(daemon.socket_path, job_id, {"failed", "interrupted"}, timeout=10)
    assert job["status"] == "failed"


@pytest.mark.asyncio
async def test_cut_short_by_stop_rules(tmp_path, monkeypatch):
    monkeypatch.setattr("queuer.daemon.STOP_RACE_SECS", 0.3)
    d = _make_daemon(tmp_path)
    assert await d._cut_short_by_stop(0) is False  # finished by itself
    assert await d._cut_short_by_stop(1) is False  # an ordinary failure, no stop
    assert await d._cut_short_by_stop(-15) is False  # signal death, but no stop arrives within the window

    asyncio.get_running_loop().call_later(0.05, d._stopping.set)  # our own SIGTERM lands a moment later
    assert await d._cut_short_by_stop(-15) is True
    assert await d._cut_short_by_stop(143) is True
    assert await d._cut_short_by_stop(1) is True  # stopping: any non-zero exit is the stop's doing
    assert await d._cut_short_by_stop(0) is False  # but a clean exit 0 stays done


# ---------------------------------------------------------------------------
# F2: dependencies (--depends-on / --after-any)
# ---------------------------------------------------------------------------

async def _add(daemon, argv, **kw) -> int:
    resp = await send(daemon.socket_path, "add", argv=argv, cwd=CWD, **kw)
    assert resp["ok"], resp
    return resp["data"]["id"]


@pytest.mark.asyncio
async def test_depends_on_runs_after_the_dependency_succeeds(daemon):
    a = await _add(daemon, ["sleep", "0.5"])
    b = await _add(daemon, ["echo", "b"], depends_on=[a])
    await asyncio.sleep(0.2)
    assert (await send(daemon.socket_path, "show", id=b))["data"]["status"] == "queued"
    assert (await wait_for_status(daemon.socket_path, b, {"done", "failed", "skipped"}))["status"] == "done"
    ja = (await send(daemon.socket_path, "show", id=a))["data"]
    jb = (await send(daemon.socket_path, "show", id=b))["data"]
    assert ja["finished_at"] <= jb["started_at"]


@pytest.mark.asyncio
async def test_depends_on_failed_job_is_skipped_and_the_queue_moves_on(daemon):
    a = await _add(daemon, ["false"])
    b = await _add(daemon, ["echo", "b"], depends_on=[a])
    c = await _add(daemon, ["echo", "c"])
    jb = await wait_for_status(daemon.socket_path, b, {"skipped", "done"})
    assert jb["status"] == "skipped" and jb["note"] == f"dependency {a} failed"
    assert jb["started_at"] is None
    assert (await wait_for_status(daemon.socket_path, c, {"done", "failed"}))["status"] == "done"


@pytest.mark.asyncio
async def test_skips_cascade_to_dependants_on_other_channels_too(daemon):
    a = await _add(daemon, ["false"])
    b = await _add(daemon, ["echo", "b"], depends_on=[a], channel=2)
    c = await _add(daemon, ["echo", "c"], depends_on=[b], channel=3)
    d = await _add(daemon, ["echo", "d"], after_any=[b], channel=4)
    jc = await wait_for_status(daemon.socket_path, c, {"skipped", "done"})
    assert jc["status"] == "skipped" and jc["note"] == f"dependency {b} failed"
    assert (await wait_for_status(daemon.socket_path, d, {"done", "skipped"}))["status"] == "done"  # b ended: that is enough


@pytest.mark.asyncio
async def test_after_any_runs_whatever_the_outcome(daemon):
    a = await _add(daemon, ["false"])
    b = await _add(daemon, ["echo", "b"], after_any=[a])
    assert (await wait_for_status(daemon.socket_path, b, {"done", "skipped"}))["status"] == "done"


@pytest.mark.asyncio
async def test_depends_on_cancelled_job_skips_the_dependant(daemon):
    a = await _add(daemon, ["sleep", "30"])
    b = await _add(daemon, ["echo", "b"], depends_on=[a], channel=2)
    await wait_for_status(daemon.socket_path, a, {"running"})
    await send(daemon.socket_path, "cancel", id=a)
    jb = await wait_for_status(daemon.socket_path, b, {"skipped", "done"})
    assert jb["status"] == "skipped" and jb["note"] == f"dependency {a} failed"


@pytest.mark.asyncio
async def test_all_listed_dependencies_must_succeed(daemon):
    a = await _add(daemon, ["true"])
    b = await _add(daemon, ["false"], channel=2)
    c = await _add(daemon, ["echo", "c"], depends_on=[a, b], channel=3)
    assert (await wait_for_status(daemon.socket_path, c, {"skipped", "done"}))["status"] == "skipped"


@pytest.mark.asyncio
async def test_waiting_job_blocks_the_jobs_behind_it_strict_fifo(daemon):
    a = await _add(daemon, ["sleep", "1"], channel=2)
    b = await _add(daemon, ["echo", "b"], depends_on=[a], channel=1)
    c = await _add(daemon, ["echo", "c"], channel=1)
    await asyncio.sleep(0.4)
    chan = _ch((await send(daemon.socket_path, "list"))["data"], 1)
    assert chan["running"] is None  # nothing jumped ahead of the waiting job
    assert [j["id"] for j in chan["queue"]] == [b, c]
    assert chan["queue"][0]["waiting_on"] == [a] and chan["queue"][0]["depends_on"] == [a]
    assert chan["queue"][1]["waiting_on"] == []
    # a dependency finishing on ANOTHER channel wakes this one at once (not at the 30 s recheck)
    jb = await wait_for_status(daemon.socket_path, b, {"done", "failed", "skipped"})
    jc = await wait_for_status(daemon.socket_path, c, {"done", "failed"})
    assert jb["status"] == "done" and jc["status"] == "done"
    assert jb["finished_at"] <= jc["started_at"]


@pytest.mark.asyncio
async def test_add_rejects_unknown_dependency_and_bad_arguments(daemon):
    for kw, text in (
        ({"depends_on": [999]}, "does not exist"),
        ({"after_any": [999]}, "does not exist"),
        ({"depends_on": "5"}, "list of job ids"),
        ({"depends_on": [0]}, "list of job ids"),
        ({"after_any": [True]}, "list of job ids"),
        ({"depends_on": list(range(1, 1002))}, "too many"),
    ):
        resp = await send(daemon.socket_path, "add", argv=["true"], cwd=CWD, **kw)
        assert resp["ok"] is False and text in resp["error"], (kw, resp)
    a = await _add(daemon, ["true"])
    resp = await send(daemon.socket_path, "add", argv=["true"], cwd=CWD, now=True, depends_on=[a])
    assert resp["ok"] is False and "--now" in resp["error"]


@pytest.mark.asyncio
async def test_add_rejects_a_dependency_that_could_never_run(daemon):
    a = await _add(daemon, ["sleep", "5"])  # running
    b = await _add(daemon, ["echo", "b"])   # queued behind a
    resp = await send(daemon.socket_path, "add", argv=["true"], cwd=CWD, before=b, depends_on=[b])
    assert resp["ok"] is False and "behind" in resp["error"]
    resp = await send(daemon.socket_path, "add", argv=["true"], cwd=CWD, before=b, channel=1, after_any=[b])
    assert resp["ok"] is False and "behind" in resp["error"]
    chan = _ch((await send(daemon.socket_path, "list"))["data"], 1)
    assert [j["id"] for j in chan["queue"]] == [b]  # nothing was added
    assert chan["running"]["id"] == a


@pytest.mark.asyncio
async def test_add_with_an_already_failed_dependency_is_skipped_at_once(daemon):
    a = await _add(daemon, ["false"])
    await wait_for_status(daemon.socket_path, a, {"failed"})
    resp = await send(daemon.socket_path, "add", argv=["echo", "x"], cwd=CWD, depends_on=[a])
    assert resp["ok"] and resp["data"]["skipped"] == f"dependency {a} failed"
    job = (await send(daemon.socket_path, "show", id=resp["data"]["id"]))["data"]
    assert job["status"] == "skipped" and job["started_at"] is None
    # a plain add carries no "skipped" key
    assert "skipped" not in (await send(daemon.socket_path, "add", argv=["true"], cwd=CWD))["data"]


@pytest.mark.asyncio
async def test_add_with_an_already_finished_dependency_just_runs(daemon):
    a = await _add(daemon, ["true"])
    await wait_for_status(daemon.socket_path, a, {"done"})
    b = await _add(daemon, ["echo", "b"], depends_on=[a])
    assert (await wait_for_status(daemon.socket_path, b, {"done", "skipped"}))["status"] == "done"


@pytest.mark.asyncio
async def test_id_in_both_lists_needs_success(daemon):
    a = await _add(daemon, ["false"])
    b = await _add(daemon, ["echo", "b"], depends_on=[a], after_any=[a])
    assert (await wait_for_status(daemon.socket_path, b, {"skipped", "done"}))["status"] == "skipped"


@pytest.mark.asyncio
async def test_rm_of_a_job_others_depend_on_is_refused(daemon):
    await _add(daemon, ["sleep", "30"])
    a = await _add(daemon, ["echo", "a"])
    b = await _add(daemon, ["echo", "b"], depends_on=[a])
    resp = await send(daemon.socket_path, "rm", id=a)
    assert resp["ok"] is False and str(b) in resp["error"]
    assert (await send(daemon.socket_path, "show", id=a))["data"]["status"] == "queued"
    assert (await send(daemon.socket_path, "rm", id=b))["ok"]
    assert (await send(daemon.socket_path, "rm", id=a))["ok"]


@pytest.mark.asyncio
async def test_cancelling_a_queued_dependency_skips_its_dependants(daemon):
    await _add(daemon, ["sleep", "30"])
    a = await _add(daemon, ["echo", "a"])
    b = await _add(daemon, ["echo", "b"], depends_on=[a])
    c = await _add(daemon, ["echo", "c"], after_any=[a], channel=2)
    await send(daemon.socket_path, "cancel", id=a)
    jb = await wait_for_status(daemon.socket_path, b, {"skipped"})
    assert jb["note"] == f"dependency {a} failed"
    assert (await wait_for_status(daemon.socket_path, c, {"done"}))["status"] == "done"


@pytest.mark.asyncio
async def test_removing_a_waiting_head_wakes_the_channel(daemon):
    a = await _add(daemon, ["sleep", "30"], channel=2)
    b = await _add(daemon, ["echo", "b"], depends_on=[a], channel=1)
    c = await _add(daemon, ["echo", "c"], channel=1)
    await asyncio.sleep(0.2)
    assert (await send(daemon.socket_path, "show", id=c))["data"]["status"] == "queued"  # held behind b
    assert (await send(daemon.socket_path, "rm", id=b))["ok"]
    assert (await wait_for_status(daemon.socket_path, c, {"done", "failed"}))["status"] == "done"


@pytest.mark.asyncio
async def test_show_reports_dependencies(daemon):
    a = await _add(daemon, ["sleep", "30"])
    b = await _add(daemon, ["sleep", "30"], channel=2)
    c = await _add(daemon, ["true"], channel=3, depends_on=[a], after_any=[b])
    job = (await send(daemon.socket_path, "show", id=c))["data"]
    assert (job["depends_on"], job["after_any"], job["waiting_on"]) == ([a], [b], [a, b])
    plain = (await send(daemon.socket_path, "show", id=a))["data"]
    assert plain["depends_on"] == [] and plain["waiting_on"] == []


@pytest.mark.asyncio
async def test_job_left_running_by_a_dead_daemon_skips_its_dependants_after_restart(tmp_path):
    # the dependency was running when the old daemon died: the next daemon marks it
    # interrupted, and a job that needed it to succeed is skipped, not run
    d = _make_daemon(tmp_path)
    conn = db_module.connect(d.db_path)
    db_module.init_db(conn)
    a = db_module.enqueue(conn, raw_cmd=["x"], resolved_cmd=["/bin/true"], cwd=CWD, log_path="pending")
    b = db_module.enqueue(conn, raw_cmd=["x"], resolved_cmd=["/bin/true"], cwd=CWD, log_path=str(tmp_path / "b.log"),
                          channel=2, deps={a: "success"})
    c = db_module.enqueue(conn, raw_cmd=["x"], resolved_cmd=["/bin/true"], cwd=CWD, log_path=str(tmp_path / "c.log"),
                          channel=3, deps={a: "any"})
    db_module.mark_running(conn, a, pid=1, pgid=1)
    conn.close()
    task = await _start(d)
    try:
        jb = await wait_for_status(d.socket_path, b, {"skipped", "done"})
        assert jb["status"] == "skipped" and jb["note"] == f"dependency {a} failed"
        assert (await wait_for_status(d.socket_path, a, {"interrupted"}))["status"] == "interrupted"
        assert (await wait_for_status(d.socket_path, c, {"done", "failed"}))["status"] == "done"
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_clean_stop_skips_dependants_of_the_interrupted_job_but_not_unrelated_ones(tmp_path):
    d = _make_daemon(tmp_path)
    task = await _start(d)
    a = (await send(d.socket_path, "add", argv=["sleep", "60"], cwd=CWD))["data"]["id"]
    b = (await send(d.socket_path, "add", argv=["true"], cwd=CWD, channel=2, depends_on=[a]))["data"]["id"]
    c = (await send(d.socket_path, "add", argv=["true"], cwd=CWD, channel=3, after_any=[a]))["data"]["id"]
    e = (await send(d.socket_path, "add", argv=["true"], cwd=CWD, channel=1))["data"]["id"]
    await wait_for_status(d.socket_path, a, {"running"})
    d._stopping.set()
    await asyncio.wait_for(task, timeout=15)

    conn = db_module.connect(d.db_path)
    try:
        assert db_module.get_job(conn, a)["status"] == "interrupted"
        assert db_module.get_job(conn, b)["status"] == "skipped"
        assert db_module.get_job(conn, b)["note"] == f"dependency {a} failed"
        assert db_module.get_job(conn, c)["status"] == "queued"  # --after-any: it runs on the next start
        assert db_module.get_job(conn, e)["status"] == "queued"
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# F3: sweeps (`add` with `jobs`)
# ---------------------------------------------------------------------------

def _spec(argv, index=None, raw=None):
    return {"argv": argv, "raw_argv": raw or argv, "index": index}


@pytest.mark.asyncio
async def test_sweep_creates_one_job_per_entry_in_order_with_qur_index(daemon):
    jobs = [_spec(["sh", "-c", "echo idx=$QUR_INDEX"], index=i) for i in (3, 4, 5)]
    resp = await send(daemon.socket_path, "add", jobs=jobs, cwd=CWD, channel=2)
    assert resp["ok"], resp
    ids = resp["data"]["ids"]
    assert resp["data"]["id"] == ids[0] and ids == list(range(ids[0], ids[0] + 3)) and resp["data"]["channel"] == 2
    for job_id, n in zip(ids, (3, 4, 5), strict=True):
        job = await wait_for_status(daemon.socket_path, job_id, {"done", "failed"})
        assert job["status"] == "done" and job["channel"] == 2
        assert Path(job["log_path"]).read_text() == f"idx={n}\n"
    done = [(await send(daemon.socket_path, "show", id=i))["data"] for i in ids]
    assert done[0]["finished_at"] <= done[1]["started_at"] <= done[2]["started_at"]  # in order, one at a time


@pytest.mark.asyncio
async def test_array_is_expanded_by_the_daemon_into_one_job_per_number(daemon):
    resp = await send(daemon.socket_path, "add", argv=["sh", "-c", "echo idx=$QUR_INDEX"], array=[3, 5], cwd=CWD, channel=2)
    assert resp["ok"], resp
    ids = resp["data"]["ids"]
    assert resp["data"]["id"] == ids[0] and ids == list(range(ids[0], ids[0] + 3)) and resp["data"]["channel"] == 2
    for job_id, n in zip(ids, (3, 4, 5), strict=True):
        job = await wait_for_status(daemon.socket_path, job_id, {"done", "failed"})
        assert job["status"] == "done" and job["channel"] == 2
        assert Path(job["log_path"]).read_text() == f"idx={n}\n"
        assert json.loads(job["raw_cmd"]) == ["sh", "-c", "echo idx=$QUR_INDEX"]  # raw_argv left out: falls back to argv


@pytest.mark.asyncio
async def test_array_keeps_a_separate_raw_argv_when_one_is_sent(daemon):
    resp = await send(daemon.socket_path, "add", argv=["echo", "hi"], raw_argv=["echo", "{}"], array=[1, 2], cwd=CWD, dry_run=True)
    assert resp["ok"], resp
    got = resp["data"]["jobs"]
    assert [j["index"] for j in got] == [1, 2] and all(j["raw_argv"] == ["echo", "{}"] for j in got)
    assert _job_count(daemon) == 0


@pytest.mark.asyncio
async def test_array_resolves_its_command_once(daemon, monkeypatch):
    from queuer import daemon as daemon_mod

    calls = []
    real = daemon_mod.resolve_command

    def counting(*a, **kw):
        calls.append(a[0])
        return real(*a, **kw)

    monkeypatch.setattr(daemon_mod, "resolve_command", counting)
    resp = await send(daemon.socket_path, "add", argv=["echo", "hi"], array=[1, 50], cwd=CWD, dry_run=True)
    assert resp["ok"], resp
    assert len(resp["data"]["jobs"]) == 50 and len(calls) == 1


@pytest.mark.asyncio
async def test_array_still_rejects_an_executable_that_does_not_exist(daemon):
    resp = await send(daemon.socket_path, "add", argv=["no-such-exe-array"], array=[1, 3], cwd=CWD)
    assert resp["ok"] is False and "not found" in resp["error"]
    assert _job_count(daemon) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("array", ["1-3", 5, [1], [1, 2, 3], [3, 1], [-1, 2], ["a", 2], [True, 2], [1.5, 2], [None, 2], {"a": 1}])
async def test_array_rejects_a_bad_range(daemon, array):
    resp = await send(daemon.socket_path, "add", argv=["echo", "hi"], array=array, cwd=CWD)
    assert resp["ok"] is False and "array" in resp["error"]
    assert _job_count(daemon) == 0


@pytest.mark.asyncio
async def test_array_is_capped_like_jobs_and_cannot_be_combined_with_jobs(daemon):
    resp = await send(daemon.socket_path, "add", argv=["echo", "hi"], array=[0, db_module.MAX_SWEEP], cwd=CWD)
    assert resp["ok"] is False and "too many" in resp["error"]
    # (the largest allowed range is not sent as a dry run: its 1000-job report is bigger than send()'s line limit)
    resp = await send(daemon.socket_path, "add", argv=["echo", "hi"], array=[0, 9], cwd=CWD, dry_run=True)
    assert resp["ok"] and len(resp["data"]["jobs"]) == 10
    resp = await send(daemon.socket_path, "add", jobs=[_spec(["echo", "x"], index=1)], array=[1, 2], argv=["echo", "x"], cwd=CWD)
    assert resp["ok"] is False and "cannot be combined" in resp["error"]


@pytest.mark.asyncio
async def test_sweep_job_without_raw_argv_falls_back_to_its_argv(daemon):
    jobs = [{"argv": ["echo", "a"], "index": 1}, {"argv": ["echo", "b"], "index": 2}]
    ids = (await send(daemon.socket_path, "add", jobs=jobs, cwd=CWD))["data"]["ids"]
    assert [json.loads(db_module.get_job(daemon.conn, i)["raw_cmd"]) for i in ids] == [["echo", "a"], ["echo", "b"]]


@pytest.mark.asyncio
async def test_sweep_each_job_keeps_its_own_raw_command(daemon):
    jobs = [_spec(["echo", v], index=n, raw=["echo", v]) for n, v in enumerate(["a", "b"], 1)]
    ids = (await send(daemon.socket_path, "add", jobs=jobs, cwd=CWD))["data"]["ids"]
    raws = [json.loads((await send(daemon.socket_path, "show", id=i))["data"]["raw_cmd"]) for i in ids]
    assert raws == [["echo", "a"], ["echo", "b"]]


@pytest.mark.asyncio
async def test_sweep_without_index_sets_no_qur_index(daemon):
    ids = (await send(daemon.socket_path, "add", jobs=[_spec(["true"])], cwd=CWD))["data"]["ids"]
    job = (await send(daemon.socket_path, "show", id=ids[0]))["data"]
    assert job["env_extra"] is None


@pytest.mark.asyncio
async def test_sweep_is_all_or_nothing(daemon):
    jobs = [_spec(["true"], 1), _spec(["/nonexistent/prog"], 2), _spec(["true"], 3)]
    resp = await send(daemon.socket_path, "add", jobs=jobs, cwd=CWD)
    assert resp["ok"] is False and "not found" in resp["error"]
    assert (await send(daemon.socket_path, "list"))["data"]["channels"] == {}  # nothing was created


@pytest.mark.asyncio
async def test_sweep_rejected_by_the_database_leaves_no_partial_jobs(daemon):
    a = await _add(daemon, ["sleep", "30"])
    resp = await send(daemon.socket_path, "add", jobs=[_spec(["true"], 1), _spec(["true"], 2)], cwd=CWD,
                      depends_on=[999])
    assert resp["ok"] is False and "does not exist" in resp["error"]
    chan = _ch((await send(daemon.socket_path, "list"))["data"], 1)
    assert chan["running"]["id"] == a and chan["queue"] == []


@pytest.mark.asyncio
async def test_sweep_argument_validation(daemon):
    for jobs, text in (
        ([], "non-empty"), ("x", "non-empty"), ([1], "non-empty"),
        ([_spec(["true"], index=-1)], "index"), ([_spec(["true"], index="1")], "index"),
        ([_spec(["true"])] * 1001, "too many"),
        ([{"argv": []}], "empty command"),
    ):
        resp = await send(daemon.socket_path, "add", jobs=jobs, cwd=CWD)
        assert resp["ok"] is False and text in resp["error"], (jobs[:1], resp)


@pytest.mark.asyncio
async def test_sweep_of_the_cap_is_accepted(daemon):
    resp = await send(daemon.socket_path, "add", jobs=[_spec(["true"], i) for i in range(1000)], cwd=CWD, channel=9)
    assert resp["ok"] and len(resp["data"]["ids"]) == 1000
    await send(daemon.socket_path, "pause", channel=9)


@pytest.mark.asyncio
async def test_sweep_dependency_applies_to_every_job(daemon):
    a = await _add(daemon, ["sleep", "0.5"], channel=2)
    resp = await send(daemon.socket_path, "add", jobs=[_spec(["true"], 1), _spec(["true"], 2)], cwd=CWD, depends_on=[a])
    ids = resp["data"]["ids"]
    await asyncio.sleep(0.2)
    chan = _ch((await send(daemon.socket_path, "list"))["data"], 1)
    assert [j["id"] for j in chan["queue"]] == ids and all(j["depends_on"] == [a] for j in chan["queue"])
    for i in ids:
        assert (await wait_for_status(daemon.socket_path, i, {"done", "skipped"}))["status"] == "done"


@pytest.mark.asyncio
async def test_sweep_with_a_failed_dependency_is_skipped_as_a_whole(daemon):
    a = await _add(daemon, ["false"])
    await wait_for_status(daemon.socket_path, a, {"failed"})
    resp = await send(daemon.socket_path, "add", jobs=[_spec(["true"], 1), _spec(["true"], 2)], cwd=CWD, depends_on=[a])
    assert resp["ok"] and resp["data"]["skipped"] == f"dependency {a} failed"
    for i in resp["data"]["ids"]:
        assert (await send(daemon.socket_path, "show", id=i))["data"]["status"] == "skipped"


@pytest.mark.asyncio
async def test_sweep_after_keeps_the_order_and_before_too(daemon):
    await _add(daemon, ["sleep", "30"])
    x = await _add(daemon, ["true"])
    y = await _add(daemon, ["true"])
    r = await send(daemon.socket_path, "add", jobs=[_spec(["true"], 1), _spec(["true"], 2)], cwd=CWD, after=x)
    ids_after = r["data"]["ids"]
    r = await send(daemon.socket_path, "add", jobs=[_spec(["true"], 1), _spec(["true"], 2)], cwd=CWD, before=y)
    ids_before = r["data"]["ids"]
    chan = _ch((await send(daemon.socket_path, "list"))["data"], 1)
    assert [j["id"] for j in chan["queue"]] == [x, *ids_after, *ids_before, y]


@pytest.mark.asyncio
async def test_single_add_is_unchanged_by_the_sweep_support(daemon):
    resp = await send(daemon.socket_path, "add", argv=["true"], cwd=CWD)
    assert set(resp["data"]) == {"id", "channel"}


# ---------------------------------------------------------------------------
# F6: --on-fail pause|continue
# ---------------------------------------------------------------------------

async def _paused(daemon, channel: int = 1) -> bool:
    return _ch((await send(daemon.socket_path, "list"))["data"], channel)["paused"]


@pytest.mark.asyncio
async def test_on_fail_pause_pauses_the_channel_and_keeps_the_rest_queued(daemon):
    a = await _add(daemon, ["false"], on_fail="pause")
    b = await _add(daemon, ["echo", "b"])
    await wait_for_status(daemon.socket_path, a, {"failed"})
    await asyncio.sleep(0.3)
    assert await _paused(daemon) is True
    assert (await send(daemon.socket_path, "show", id=b))["data"]["status"] == "queued"
    await send(daemon.socket_path, "resume")
    assert (await wait_for_status(daemon.socket_path, b, {"done", "failed"}))["status"] == "done"


@pytest.mark.asyncio
async def test_on_fail_pause_only_pauses_its_own_channel(daemon):
    a = await _add(daemon, ["false"], on_fail="pause", channel=1)
    c = await _add(daemon, ["echo", "c"], channel=2)
    await wait_for_status(daemon.socket_path, a, {"failed"})
    assert (await wait_for_status(daemon.socket_path, c, {"done"}))["status"] == "done"
    assert await _paused(daemon, 1) is True and await _paused(daemon, 2) is False


@pytest.mark.asyncio
async def test_default_and_continue_do_not_pause(daemon):
    a = await _add(daemon, ["false"])
    b = await _add(daemon, ["false"], on_fail="continue")
    c = await _add(daemon, ["echo", "c"])
    assert (await wait_for_status(daemon.socket_path, c, {"done", "failed"}))["status"] == "done"
    assert await _paused(daemon) is False
    assert (await send(daemon.socket_path, "show", id=a))["data"]["on_fail"] == "continue"
    assert (await send(daemon.socket_path, "show", id=b))["data"]["on_fail"] == "continue"


@pytest.mark.asyncio
async def test_on_fail_pause_ignores_success_cancel_and_skip(daemon):
    ok = await _add(daemon, ["true"], on_fail="pause")
    await wait_for_status(daemon.socket_path, ok, {"done"})
    long = await _add(daemon, ["sleep", "30"], on_fail="pause")
    await wait_for_status(daemon.socket_path, long, {"running"})
    await send(daemon.socket_path, "cancel", id=long)
    await wait_for_status(daemon.socket_path, long, {"cancelled"})
    await asyncio.sleep(0.3)
    assert await _paused(daemon) is False
    bad = await _add(daemon, ["false"], channel=2)
    skipped = await _add(daemon, ["true"], channel=3, depends_on=[bad], on_fail="pause")
    await wait_for_status(daemon.socket_path, skipped, {"skipped"})
    await asyncio.sleep(0.2)
    assert await _paused(daemon, 3) is False


@pytest.mark.asyncio
async def test_on_fail_pause_after_timeout_pauses(daemon):
    a = await _add(daemon, ["sleep", "30"], timeout_secs=1, on_fail="pause")
    job = await wait_for_status(daemon.socket_path, a, {"failed"}, timeout=10)
    assert "timed out" in job["note"]
    await asyncio.sleep(0.3)
    assert await _paused(daemon) is True


@pytest.mark.asyncio
async def test_on_fail_pause_waits_for_the_final_failure_when_retrying(daemon):
    a = await _add(daemon, ["false"], on_fail="pause", max_retries=1)
    retry = a + 1
    await wait_for_status(daemon.socket_path, retry, {"failed"})
    await asyncio.sleep(0.3)
    assert await _paused(daemon) is True
    assert (await send(daemon.socket_path, "show", id=retry))["data"]["on_fail"] == "pause"  # the copy keeps it
    assert (await send(daemon.socket_path, "show", id=retry + 1))["ok"] is False  # and no third attempt


@pytest.mark.asyncio
async def test_on_fail_is_validated_and_survives_requeue(daemon):
    for bad in ("stop", 5, ["pause"]):
        resp = await send(daemon.socket_path, "add", argv=["true"], cwd=CWD, on_fail=bad)
        assert resp["ok"] is False and "on_fail" in resp["error"], bad
    a = await _add(daemon, ["false"], on_fail="pause")
    await wait_for_status(daemon.socket_path, a, {"failed"})
    new_id = (await send(daemon.socket_path, "requeue", id=a))["data"]["id"]
    assert (await send(daemon.socket_path, "show", id=new_id))["data"]["on_fail"] == "pause"


@pytest.mark.asyncio
async def test_on_fail_applies_to_every_job_of_a_sweep(daemon):
    resp = await send(daemon.socket_path, "add", jobs=[_spec(["false"], 1), _spec(["true"], 2)], cwd=CWD, on_fail="pause")
    first, second = resp["data"]["ids"]
    await wait_for_status(daemon.socket_path, first, {"failed"})
    await asyncio.sleep(0.3)
    assert await _paused(daemon) is True
    assert (await send(daemon.socket_path, "show", id=second))["data"]["on_fail"] == "pause"
    assert (await send(daemon.socket_path, "show", id=second))["data"]["status"] == "queued"


@pytest.mark.asyncio
async def test_on_fail_pause_sends_a_telegram_message(daemon, sent):
    job_id = await _add(daemon, ["false"], on_fail="pause")
    await wait_msgs(sent, 2)
    texts = [m["text"] for m in sent]
    assert any(f"failed · job {job_id}" in t for t in texts)
    assert any(f"channel 1 paused after job {job_id} failed" in t for t in texts)


@pytest.mark.asyncio
async def test_continue_failure_sends_no_pause_message(daemon, sent):
    await _add(daemon, ["false"])
    await wait_msgs(sent, 1)
    await asyncio.sleep(0.3)
    assert len(sent) == 1 and "paused" not in sent[0]["text"]


# ---------------------------------------------------------------------------
# F7: delayed start (--delay / --at -> not_before)
# ---------------------------------------------------------------------------

from datetime import datetime, timedelta, timezone  # noqa: E402


def _in(secs: float) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=secs)).isoformat()


@pytest.mark.asyncio
async def test_delayed_job_waits_for_its_time_then_runs(daemon):
    started = time.monotonic()
    a = await _add(daemon, ["echo", "later"], not_before=_in(1.0))
    await asyncio.sleep(0.4)
    assert (await send(daemon.socket_path, "show", id=a))["data"]["status"] == "queued"
    assert (await wait_for_status(daemon.socket_path, a, {"done", "failed"}))["status"] == "done"
    assert time.monotonic() - started >= 0.95


@pytest.mark.asyncio
async def test_delayed_job_starts_at_not_before_not_at_the_idle_recheck(daemon, monkeypatch):
    # the recheck is far longer than the delay: only a computed sleep can start it on time
    monkeypatch.setattr("queuer.daemon.IDLE_POLL_SECS", 30)
    a = await _add(daemon, ["echo", "x"], not_before=_in(0.8))
    job = await wait_for_status(daemon.socket_path, a, {"done", "failed"}, timeout=5)
    started = datetime.fromisoformat(job["started_at"])
    assert datetime.fromisoformat(job["not_before"]) <= started <= datetime.fromisoformat(job["not_before"]) + timedelta(seconds=1.5)


@pytest.mark.asyncio
async def test_a_delayed_head_blocks_the_jobs_behind_it_strict_fifo(daemon):
    a = await _add(daemon, ["echo", "a"], not_before=_in(1.0))
    b = await _add(daemon, ["echo", "b"])
    await asyncio.sleep(0.4)
    assert (await send(daemon.socket_path, "show", id=b))["data"]["status"] == "queued"
    ja = await wait_for_status(daemon.socket_path, a, {"done"})
    jb = await wait_for_status(daemon.socket_path, b, {"done"})
    assert ja["finished_at"] <= jb["started_at"]


@pytest.mark.asyncio
async def test_a_delayed_job_does_not_block_another_channel(daemon):
    a = await _add(daemon, ["echo", "a"], not_before=_in(30), channel=1)
    b = await _add(daemon, ["echo", "b"], channel=2)
    assert (await wait_for_status(daemon.socket_path, b, {"done"}))["status"] == "done"
    assert (await send(daemon.socket_path, "show", id=a))["data"]["status"] == "queued"


@pytest.mark.asyncio
async def test_a_not_before_in_the_past_runs_at_once(daemon):
    a = await _add(daemon, ["echo", "a"], not_before=_in(-60))
    assert (await wait_for_status(daemon.socket_path, a, {"done", "failed"}))["status"] == "done"


@pytest.mark.asyncio
async def test_cancelling_a_delayed_head_frees_the_channel_at_once(daemon):
    a = await _add(daemon, ["echo", "a"], not_before=_in(60))
    b = await _add(daemon, ["echo", "b"])
    await asyncio.sleep(0.2)
    assert (await send(daemon.socket_path, "cancel", id=a))["ok"]
    assert (await send(daemon.socket_path, "show", id=a))["data"]["status"] == "cancelled"
    assert (await wait_for_status(daemon.socket_path, b, {"done", "failed"}))["status"] == "done"


@pytest.mark.asyncio
async def test_removing_a_delayed_head_frees_the_channel_at_once(daemon):
    a = await _add(daemon, ["echo", "a"], not_before=_in(60))
    b = await _add(daemon, ["echo", "b"])
    await asyncio.sleep(0.2)
    assert (await send(daemon.socket_path, "rm", id=a))["ok"]
    assert (await wait_for_status(daemon.socket_path, b, {"done", "failed"}))["status"] == "done"


@pytest.mark.asyncio
async def test_delay_and_dependency_both_have_to_be_met(daemon):
    dep = await _add(daemon, ["sleep", "1.2"])
    a = await _add(daemon, ["echo", "a"], depends_on=[dep], not_before=_in(0.3), channel=2)
    await asyncio.sleep(0.6)  # its time has come but the dependency has not ended
    assert (await send(daemon.socket_path, "show", id=a))["data"]["status"] == "queued"
    ja = await wait_for_status(daemon.socket_path, a, {"done", "failed", "skipped"})
    jd = await wait_for_status(daemon.socket_path, dep, {"done"})
    assert ja["status"] == "done" and ja["started_at"] >= jd["finished_at"]


@pytest.mark.asyncio
async def test_a_failed_dependency_skips_a_delayed_job_without_waiting_for_its_time(daemon):
    dep = await _add(daemon, ["false"])
    a = await _add(daemon, ["echo", "a"], depends_on=[dep], not_before=_in(60), channel=2)
    assert (await wait_for_status(daemon.socket_path, a, {"skipped"}, timeout=5))["status"] == "skipped"


@pytest.mark.asyncio
async def test_a_paused_channel_does_not_start_a_delayed_job_when_its_time_comes(daemon):
    await send(daemon.socket_path, "pause")
    a = await _add(daemon, ["echo", "a"], not_before=_in(0.3))
    await asyncio.sleep(0.8)
    assert (await send(daemon.socket_path, "show", id=a))["data"]["status"] == "queued"
    await send(daemon.socket_path, "resume")
    assert (await wait_for_status(daemon.socket_path, a, {"done"}))["status"] == "done"


@pytest.mark.asyncio
async def test_timeout_counts_run_time_not_the_delay(daemon):
    a = await _add(daemon, ["sleep", "0.3"], not_before=_in(1.0), timeout_secs=1)
    assert (await wait_for_status(daemon.socket_path, a, {"done", "failed"}, timeout=6))["status"] == "done"


@pytest.mark.asyncio
async def test_not_before_is_stored_as_utc_and_shown_by_show_and_list(daemon):
    local = datetime.now(timezone(timedelta(hours=5, minutes=30))) + timedelta(hours=1)
    a = await _add(daemon, ["echo", "a"], not_before=local.isoformat(), channel=3)
    job = (await send(daemon.socket_path, "show", id=a))["data"]
    assert job["not_before"] == local.astimezone(timezone.utc).isoformat()
    queue = _ch((await send(daemon.socket_path, "list"))["data"], 3)["queue"]
    assert queue[0]["not_before"] == job["not_before"]


@pytest.mark.asyncio
async def test_delayed_start_survives_a_daemon_restart(tmp_path):
    d = _make_daemon(tmp_path)
    task = await _start(d)
    a = (await send(d.socket_path, "add", argv=["echo", "a"], cwd=CWD, not_before=_in(1.5)))["data"]["id"]
    await asyncio.sleep(0.2)
    d._stopping.set()
    await asyncio.wait_for(task, timeout=15)
    stored_nb = None
    conn = db_module.connect(d.db_path)
    try:
        job = db_module.get_job(conn, a)
        assert job is not None and job["status"] == "queued"
        stored_nb = job["not_before"]
    finally:
        conn.close()
    assert stored_nb

    d2 = _make_daemon(tmp_path)  # "systemctl restart": same db, new daemon
    task2 = await _start(d2)
    try:
        assert (await send(d2.socket_path, "show", id=a))["data"]["status"] == "queued"
        done = await wait_for_status(d2.socket_path, a, {"done", "failed"})
        assert done["status"] == "done"
        assert datetime.fromisoformat(done["started_at"]) >= datetime.fromisoformat(stored_nb)
    finally:
        task2.cancel()
        try:
            await task2
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", [5, "tomorrow", "2026-10-16T10:00:00", ["x"], True])
async def test_add_rejects_a_bad_not_before(daemon, bad):
    resp = await send(daemon.socket_path, "add", argv=["echo", "a"], cwd=CWD, not_before=bad)
    assert resp["ok"] is False and "not_before" in resp["error"]
    assert (await send(daemon.socket_path, "list"))["data"]["channels"] == {}


@pytest.mark.asyncio
async def test_add_rejects_not_before_with_now(daemon):
    resp = await send(daemon.socket_path, "add", argv=["echo", "a"], cwd=CWD, now=True, not_before=_in(60))
    assert resp["ok"] is False and "--now" in resp["error"]


@pytest.mark.asyncio
async def test_not_before_applies_to_every_job_of_a_sweep(daemon):
    nb = _in(60)
    resp = await send(daemon.socket_path, "add", jobs=[_spec(["echo", "1"], 1), _spec(["echo", "2"], 2)], cwd=CWD, not_before=nb)
    for job_id in resp["data"]["ids"]:
        assert (await send(daemon.socket_path, "show", id=job_id))["data"]["not_before"] == datetime.fromisoformat(nb).astimezone(timezone.utc).isoformat()


@pytest.mark.asyncio
async def test_requeue_does_not_copy_the_start_time(daemon):
    a = await _add(daemon, ["echo", "a"], not_before=_in(0.3))
    await wait_for_status(daemon.socket_path, a, {"done"})
    new_id = (await send(daemon.socket_path, "requeue", id=a))["data"]["id"]
    assert (await send(daemon.socket_path, "show", id=new_id))["data"]["not_before"] is None
    assert (await wait_for_status(daemon.socket_path, new_id, {"done"}))["status"] == "done"


def test_secs_until_handles_missing_past_future_and_garbage():
    assert Daemon._secs_until(None) == 0.0 and Daemon._secs_until("") == 0.0
    assert Daemon._secs_until("not a time") == 0.0
    assert Daemon._secs_until(_in(-5)) < 0
    assert 8 < Daemon._secs_until(_in(10)) <= 10
    naive_future = (datetime.now(timezone.utc) + timedelta(seconds=10)).replace(tzinfo=None).isoformat()
    assert 8 < Daemon._secs_until(naive_future) <= 10  # a stored naive value is read as UTC


# ---------------------------------------------------------------------------
# F1/B13: environment passthrough
# ---------------------------------------------------------------------------

async def _run_env_job(daemon, **add_args) -> tuple[dict, dict[str, str]]:
    resp = await send(daemon.socket_path, "add", argv=["env"], cwd=CWD, **add_args)
    assert resp["ok"], resp
    job = await wait_for_status(daemon.socket_path, resp["data"]["id"], {"done", "failed"})
    assert job["status"] == "done"
    seen = dict(line.split("=", 1) for line in Path(job["log_path"]).read_text().splitlines() if "=" in line)
    return job, seen


@pytest.mark.asyncio
async def test_job_gets_passthrough_explicit_env_and_unbuffered_default(daemon, monkeypatch):
    monkeypatch.setenv("DAEMON_ONLY_SECRET", "must-not-reach-jobs")
    job, seen = await _run_env_job(
        daemon,
        env_pass={"CUDA_VISIBLE_DEVICES": "1", "LANG": "C.UTF-8"},
        env={"MY_SETTING": "hello", "OMP_NUM_THREADS": "4"},
    )
    assert seen["CUDA_VISIBLE_DEVICES"] == "1" and seen["LANG"] == "C.UTF-8"
    assert seen["MY_SETTING"] == "hello" and seen["OMP_NUM_THREADS"] == "4"
    assert seen["PYTHONUNBUFFERED"] == "1"
    assert "DAEMON_ONLY_SECRET" not in seen
    assert "QUR_ENV_PASSTHROUGH" not in seen  # the bookkeeping marker never reaches the job
    stored = json.loads(job["env_extra"])
    assert stored["QUR_ENV_PASSTHROUGH"] == "CUDA_VISIBLE_DEVICES,LANG"
    assert "PYTHONUNBUFFERED" not in stored  # the default is applied at start, not stored


@pytest.mark.asyncio
async def test_explicit_env_overrides_passthrough_and_unbuffered(daemon):
    job, seen = await _run_env_job(
        daemon, env_pass={"OMP_NUM_THREADS": "8"}, env={"OMP_NUM_THREADS": "2", "PYTHONUNBUFFERED": "0"}
    )
    assert seen["OMP_NUM_THREADS"] == "2" and seen["PYTHONUNBUFFERED"] == "0"
    assert "QUR_ENV_PASSTHROUGH" not in json.loads(job["env_extra"])  # nothing was left that was only passed through


@pytest.mark.asyncio
async def test_sweep_qur_index_wins_over_a_clashing_env(daemon):
    jobs = [_spec(["sh", "-c", "echo idx=$QUR_INDEX x=$X"], index=i) for i in (7, 8)]
    resp = await send(daemon.socket_path, "add", jobs=jobs, cwd=CWD, env={"QUR_INDEX": "99", "X": "1"})
    assert resp["ok"], resp
    for job_id, n in zip(resp["data"]["ids"], (7, 8), strict=True):
        job = await wait_for_status(daemon.socket_path, job_id, {"done", "failed"})
        assert Path(job["log_path"]).read_text() == f"idx={n} x=1\n"


@pytest.mark.asyncio
async def test_env_is_kept_by_requeue_and_retry(daemon):
    resp = await send(
        daemon.socket_path, "add", argv=["sh", "-c", "echo $MY_SETTING; exit 1"], cwd=CWD,
        env={"MY_SETTING": "kept"}, env_pass={"LANG": "C"}, max_retries=1,
    )
    first = resp["data"]["id"]
    await wait_for_status(daemon.socket_path, first, {"failed"})
    retry = await wait_for_status(daemon.socket_path, first + 1, {"failed"})
    assert Path(retry["log_path"]).read_text() == "kept\n"
    assert json.loads(retry["env_extra"])["QUR_ENV_PASSTHROUGH"] == "LANG"
    new_id = (await send(daemon.socket_path, "requeue", id=first))["data"]["id"]
    again = await wait_for_status(daemon.socket_path, new_id, {"failed"})
    assert Path(again["log_path"]).read_text() == "kept\n"
    assert json.loads(again["env_extra"]) == json.loads(retry["env_extra"])


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", [
    {"env": {"1A": "x"}}, {"env": {"A-B": "x"}}, {"env": {"A": "x\0y"}}, {"env": {"A": 1}}, {"env": ["A=1"]},
    {"env_pass": {"A B": "x"}}, {"env_pass": {"A": "\0"}}, {"env": {"QUR_ENV_PASSTHROUGH": "x"}},
])
async def test_add_rejects_bad_env_and_creates_nothing(daemon, bad):
    resp = await send(daemon.socket_path, "add", argv=["true"], cwd=CWD, **bad)
    assert resp["ok"] is False
    assert (await send(daemon.socket_path, "list"))["data"]["channels"] == {}


@pytest.mark.asyncio
async def test_add_without_env_stores_nothing_extra(daemon):
    resp = await send(daemon.socket_path, "add", argv=["true"], cwd=CWD, env={}, env_pass={})
    assert (await send(daemon.socket_path, "show", id=resp["data"]["id"]))["data"]["env_extra"] is None


# ---------------------------------------------------------------------------
# F9/U3: history that scales + filters
# ---------------------------------------------------------------------------

async def _run_quick(daemon, n: int, channel: int = 1, argv=None) -> list[int]:
    ids = []
    for _ in range(n):
        resp = await send(daemon.socket_path, "add", argv=argv or ["true"], cwd=CWD, channel=channel)
        await wait_for_status(daemon.socket_path, resp["data"]["id"], {"done", "failed"})
        ids.append(resp["data"]["id"])
    return ids


def test_default_keep_reads_queuer_keep(monkeypatch, capsys):
    from queuer.daemon import _default_keep

    monkeypatch.delenv("QUEUER_KEEP", raising=False)
    assert _default_keep() == 200
    monkeypatch.setenv("QUEUER_KEEP", "50")
    assert _default_keep() == 50
    monkeypatch.setenv("QUEUER_KEEP", "  ")
    assert _default_keep() == 200
    assert capsys.readouterr().err == ""
    for bad in ("0", "-3", "abc", "1.5"):
        monkeypatch.setenv("QUEUER_KEEP", bad)
        assert _default_keep() == 200
        assert "QUEUER_KEEP" in capsys.readouterr().err


@pytest.mark.asyncio
async def test_daemon_keeps_200_by_default_and_list_still_shows_10(daemon):
    assert daemon.keep == 200
    ids = await _run_quick(daemon, 12)
    listing = (await send(daemon.socket_path, "list"))["data"]
    assert [j["id"] for j in _ch(listing, 1)["backlog"]] == ids[::-1][:10]  # list stays short
    hist = (await send(daemon.socket_path, "history", limit=100))["data"]["jobs"]
    assert [j["id"] for j in hist] == ids[::-1]  # nothing was pruned at 10 any more


@pytest.mark.asyncio
async def test_daemon_prunes_to_its_keep_per_channel(daemon):
    daemon.keep = 3
    ch1 = await _run_quick(daemon, 5, channel=1)
    ch2 = await _run_quick(daemon, 2, channel=2)
    hist = (await send(daemon.socket_path, "history", limit=100))["data"]["jobs"]
    assert {j["id"] for j in hist} == set(ch1[-3:]) | set(ch2)


@pytest.mark.asyncio
async def test_prune_never_drops_a_dependency_of_a_queued_job(daemon):
    daemon.keep = 2
    first = (await send(daemon.socket_path, "add", argv=["true"], cwd=CWD, channel=1))["data"]["id"]
    await wait_for_status(daemon.socket_path, first, {"done"})
    # a delayed dependant on another channel keeps `first` alive while the history rolls over
    later = _in(3600)
    dep = (await send(daemon.socket_path, "add", argv=["true"], cwd=CWD, channel=2,
                      depends_on=[first], not_before=later))["data"]["id"]
    await _run_quick(daemon, 4, channel=1)
    assert (await send(daemon.socket_path, "show", id=first))["ok"]
    assert (await send(daemon.socket_path, "cancel", id=dep))["ok"]
    await _run_quick(daemon, 1, channel=1)
    assert not (await send(daemon.socket_path, "show", id=first))["ok"]


async def _history_fixture(daemon) -> dict[str, int]:
    """done/failed/cancelled-while-queued/interrupted jobs on two channels, with known commands."""
    out: dict[str, int] = {}
    out["done1"] = (await _run_quick(daemon, 1, argv=["echo", "Alpha"]))[0]
    out["fail1"] = (await _run_quick(daemon, 1, argv=["sh", "-c", "exit 3"]))[0]
    out["done2"] = (await _run_quick(daemon, 1, channel=2, argv=["echo", "beta"]))[0]
    blocker = (await send(daemon.socket_path, "add", argv=["sleep", "30"], cwd=CWD, channel=3))["data"]["id"]
    queued = (await send(daemon.socket_path, "add", argv=["true"], cwd=CWD, channel=3))["data"]["id"]
    await send(daemon.socket_path, "cancel", id=queued)
    out["cancelled"] = queued
    await send(daemon.socket_path, "cancel", id=blocker)
    await wait_for_status(daemon.socket_path, blocker, {"cancelled"})
    out["blocker"] = blocker
    return out


@pytest.mark.asyncio
async def test_history_filters_through_the_socket(daemon):
    ids = await _history_fixture(daemon)

    async def hist(**kw):
        resp = await send(daemon.socket_path, "history", **kw)
        assert resp["ok"], resp
        return [j["id"] for j in resp["data"]["jobs"]]

    everything = await hist()
    assert set(everything) == set(ids.values()) and len(everything) == 5
    assert await hist(statuses=["failed"]) == [ids["fail1"]]
    assert set(await hist(statuses=["failed", "cancelled"])) == {ids["fail1"], ids["cancelled"], ids["blocker"]}
    assert set(await hist(channel=2)) == {ids["done2"]}
    assert await hist(grep="alpha") == [ids["done1"]]  # case-insensitive, on the raw command
    assert await hist(grep="exit 3") == [ids["fail1"]]
    assert len(await hist(limit=2)) == 2
    assert await hist(since=_in(60)) == []  # nothing finished in the future
    assert len(await hist(since=_in(-3600))) == 5
    assert await hist(statuses=["done"], channel=1, grep="alpha", since=_in(-3600)) == [ids["done1"]]
    assert await hist(statuses=["done"], grep="beta", channel=1) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", [
    {"statuses": ["queued"]}, {"statuses": "failed"}, {"statuses": [1]}, {"channel": 0}, {"channel": "1"},
    {"limit": 0}, {"limit": -1}, {"limit": True}, {"limit": 10**6}, {"since": "yesterday"}, {"since": 5},
    {"grep": 5},
])
async def test_history_rejects_bad_arguments(daemon, bad):
    resp = await send(daemon.socket_path, "history", **bad)
    assert resp["ok"] is False and resp["error"]


@pytest.mark.asyncio
async def test_history_since_accepts_naive_and_offset_timestamps(daemon):
    (job_id,) = await _run_quick(daemon, 1)
    finished = datetime.fromisoformat((await send(daemon.socket_path, "show", id=job_id))["data"]["finished_at"])
    before = (finished - timedelta(seconds=5)).astimezone(timezone(timedelta(hours=5, minutes=30)))
    for stamp in (before.isoformat(), before.astimezone(timezone.utc).replace(tzinfo=None).isoformat()):
        resp = await send(daemon.socket_path, "history", since=stamp)
        assert [j["id"] for j in resp["data"]["jobs"]] == [job_id]
    after = (finished + timedelta(seconds=5)).isoformat()
    assert (await send(daemon.socket_path, "history", since=after))["data"]["jobs"] == []


# ---------------------------------------------------------------------------
# B14: --retry-delay / --retry-backoff
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_retry_waits_for_the_retry_delay(daemon):
    a = await _add(daemon, ["false"], max_retries=1, retry_delay_secs=1)
    first = await wait_for_status(daemon.socket_path, a, {"failed"})
    retry = (await send(daemon.socket_path, "show", id=a + 1))["data"]
    assert retry["status"] == "queued" and retry["not_before"]  # waiting, not started
    assert retry["retry_delay_secs"] == 1
    done = await wait_for_status(daemon.socket_path, a + 1, {"failed"}, timeout=8)
    gap = (datetime.fromisoformat(done["started_at"]) - datetime.fromisoformat(first["finished_at"])).total_seconds()
    assert gap >= 0.9
    assert (await send(daemon.socket_path, "show", id=a + 2))["ok"] is False  # budget used up


@pytest.mark.asyncio
async def test_retry_without_a_delay_still_starts_at_once(daemon):
    a = await _add(daemon, ["false"], max_retries=1)
    await wait_for_status(daemon.socket_path, a + 1, {"failed"}, timeout=3)
    assert (await send(daemon.socket_path, "show", id=a + 1))["data"]["not_before"] is None


@pytest.mark.asyncio
async def test_retry_backoff_doubles_the_wait(daemon):
    a = await _add(daemon, ["false"], max_retries=2, retry_delay_secs=1, retry_backoff=True)
    await wait_for_status(daemon.socket_path, a + 1, {"failed"}, timeout=8)
    second = (await send(daemon.socket_path, "show", id=a + 1))["data"]
    third = (await send(daemon.socket_path, "show", id=a + 2))["data"]
    assert third["status"] == "queued" and third["retry_backoff"] == 1
    wait = (datetime.fromisoformat(third["not_before"]) - datetime.fromisoformat(second["finished_at"])).total_seconds()
    assert 1.8 <= wait <= 2.3  # 1 s, then 2 s
    await wait_for_status(daemon.socket_path, a + 2, {"failed"}, timeout=8)


@pytest.mark.asyncio
async def test_a_waiting_retry_holds_its_channel_strict_fifo(daemon):
    a = await _add(daemon, ["false"], max_retries=1, retry_delay_secs=1)
    await wait_for_status(daemon.socket_path, a, {"failed"})
    c = await _add(daemon, ["echo", "c"])  # queued behind the waiting retry
    await asyncio.sleep(0.4)
    assert (await send(daemon.socket_path, "show", id=c))["data"]["status"] == "queued"
    retry = await wait_for_status(daemon.socket_path, a + 1, {"failed"}, timeout=8)
    jc = await wait_for_status(daemon.socket_path, c, {"done"})
    assert retry["finished_at"] <= jc["started_at"]


@pytest.mark.asyncio
async def test_a_dependant_of_a_failed_job_is_skipped_at_once_despite_the_retry_delay(daemon):
    a = await _add(daemon, ["false"], max_retries=1, retry_delay_secs=3600)
    b = await _add(daemon, ["echo", "b"], channel=2, depends_on=[a])
    job = await wait_for_status(daemon.socket_path, b, {"skipped"}, timeout=3)
    assert f"dependency {a} failed" in job["note"]


@pytest.mark.asyncio
async def test_manual_requeue_is_not_delayed_and_keeps_the_retry_delay(daemon):
    a = await _add(daemon, ["false"], max_retries=1, retry_delay_secs=3600, channel=3)
    await wait_for_status(daemon.socket_path, a, {"failed"})
    new_id = (await send(daemon.socket_path, "requeue", id=a))["data"]["id"]
    new = (await send(daemon.socket_path, "show", id=new_id))["data"]
    assert new["not_before"] is None and new["retry_delay_secs"] == 3600 and new["origin"] == "requeue"


@pytest.mark.asyncio
async def test_retry_delay_is_validated_by_the_daemon(daemon):
    for kw in (
        dict(max_retries=1, retry_delay_secs=0),
        dict(max_retries=1, retry_delay_secs=-1),
        dict(max_retries=1, retry_delay_secs="5m"),
        dict(max_retries=1, retry_delay_secs=True),
        dict(max_retries=1, retry_delay_secs=31 * 86400),
        dict(max_retries=1, retry_backoff=True),  # backoff needs a delay
        dict(max_retries=1, retry_delay_secs=5, retry_backoff="yes"),
        dict(retry_delay_secs=5),  # a delay needs --retries
    ):
        r = await send(daemon.socket_path, "add", argv=["true"], cwd=CWD, **kw)
        assert r["ok"] is False and "retr" in r["error"], kw
    assert (await send(daemon.socket_path, "list"))["data"]["channels"] == {}  # nothing was queued


# ---------------------------------------------------------------------------
# F8: qur edit + edit lock
# ---------------------------------------------------------------------------

async def _edit(daemon, job_id, **kw) -> dict:
    return await send(daemon.socket_path, "edit", id=job_id, **kw)


def _log_text(daemon, job_id) -> str:
    return (daemon.log_dir / f"{job_id}.log").read_text()


@pytest.mark.asyncio
async def test_edit_command_then_the_job_runs_the_new_one(daemon):
    blocker = await _add(daemon, ["sleep", "0.5"])
    b = await _add(daemon, ["echo", "old"])
    r = await _edit(daemon, b, argv=["echo", "new"], raw_argv=["echo", "new"])
    assert r["ok"] and r["data"]["changed"] == ["command"]
    await wait_for_status(daemon.socket_path, blocker, {"done"})
    await wait_for_status(daemon.socket_path, b, {"done"})
    assert _log_text(daemon, b).strip() == "new"
    shown = (await send(daemon.socket_path, "show", id=b))["data"]
    assert json.loads(shown["raw_cmd"]) == ["echo", "new"] and json.loads(shown["resolved_cmd"])[1:] == ["new"]


@pytest.mark.asyncio
async def test_edit_each_simple_field(daemon, tmp_path):
    a = await _add(daemon, ["sleep", "30"])
    await wait_for_status(daemon.socket_path, a, {"running"})
    b = await _add(daemon, ["echo", "b"], timeout_secs=5)
    other = tmp_path / "work"
    other.mkdir()
    r = await _edit(
        daemon, b, argv=["echo", "b"], cwd=str(other), timeout_secs=0, max_retries=2, on_fail="pause",
        not_before=_in(3600), env={"FOO": "bar"},
    )
    assert r["ok"], r
    assert set(r["data"]["changed"]) == {"command", "cwd", "timeout", "retries", "on-fail", "start time", "env"}
    job = (await send(daemon.socket_path, "show", id=b))["data"]
    assert job["cwd"] == str(other) and job["timeout_secs"] is None
    assert job["max_retries"] == 2 and job["on_fail"] == "pause" and job["not_before"] is not None
    assert json.loads(job["env_extra"])["FOO"] == "bar"
    r = await _edit(daemon, b, not_before=None, timeout_secs=9)
    assert r["ok"]
    job = (await send(daemon.socket_path, "show", id=b))["data"]
    assert job["not_before"] is None and job["timeout_secs"] == 9


@pytest.mark.asyncio
async def test_edit_env_keeps_sweep_index_and_drops_passthrough_marker_for_overridden_names(daemon):
    a = await _add(daemon, ["sleep", "30"])
    await wait_for_status(daemon.socket_path, a, {"running"})
    r = await send(daemon.socket_path, "add", cwd=CWD, jobs=[{"argv": ["echo", "x"], "index": 3}],
                   env_pass={"LANG": "C"})
    b = r["data"]["id"]
    assert (await _edit(daemon, b, env={"QUR_INDEX": "99", "LANG": "en"}))["ok"]
    env = json.loads((await send(daemon.socket_path, "show", id=b))["data"]["env_extra"])
    assert env["QUR_INDEX"] == "3" and env["LANG"] == "en"
    assert "LANG" not in env.get("QUR_ENV_PASSTHROUGH", "")


@pytest.mark.asyncio
async def test_edit_validation_errors_and_nothing_partial(daemon, tmp_path):
    a = await _add(daemon, ["sleep", "30"])
    await wait_for_status(daemon.socket_path, a, {"running"})
    b = await _add(daemon, ["echo", "b"], timeout_secs=5)
    before = (await send(daemon.socket_path, "show", id=b))["data"]
    cases = [
        (dict(argv=["/no/such/exe"]), "executable"),
        (dict(argv=["echo", "x"], cwd=str(tmp_path / "missing")), "working directory"),
        (dict(argv=[]), "non-empty"),
        (dict(argv=["echo", "a\0b"]), "NUL"),
        (dict(cwd=str(tmp_path)), "argv"),  # a new cwd without the re-resolved command
        (dict(timeout_secs=-1), "timeout"),
        (dict(timeout_secs="5"), "timeout"),
        (dict(max_retries=-1), "retries"),
        (dict(on_fail="explode"), "on_fail"),
        (dict(channel=0), "channel"),
        (dict(not_before="tomorrow"), "not_before"),
        (dict(not_before="2030-01-01T00:00:00"), "UTC offset"),
        (dict(depends_on=[999]), "does not exist"),
        (dict(depends_on=[b]), "itself"),
        (dict(env={"1X": "v"}), "invalid environment"),
        (dict(clear_deps=True, depends_on=[a]), "clear_deps"),
        (dict(), "nothing to change"),
        (dict(timeout_secs=5), "nothing to change"),  # same value
    ]
    for kw, word in cases:
        r = await _edit(daemon, b, **kw)
        assert r["ok"] is False and word in r["error"], (kw, r)
    # a valid change plus an invalid one: neither is applied
    r = await _edit(daemon, b, max_retries=3, timeout_secs=-5)
    assert r["ok"] is False
    assert (await send(daemon.socket_path, "show", id=b))["data"] == {**before, "editing": False}


@pytest.mark.asyncio
async def test_only_queued_jobs_can_be_edited(daemon):
    a = await _add(daemon, ["sleep", "30"])
    await wait_for_status(daemon.socket_path, a, {"running"})
    r = await _edit(daemon, a, timeout_secs=3)
    assert r["ok"] is False and "running, not queued" in r["error"]
    d = await _add(daemon, ["true"], channel=2)
    await wait_for_status(daemon.socket_path, d, {"done"})
    r = await _edit(daemon, d, timeout_secs=3)
    assert r["ok"] is False and "only queued jobs" in r["error"]
    r = await _edit(daemon, 9999, timeout_secs=3)
    assert r["ok"] is False and "does not exist" in r["error"]
    assert (await send(daemon.socket_path, "edit_lock", id=a, pid=os.getpid()))["ok"] is False


@pytest.mark.asyncio
async def test_edit_lock_blocks_the_start_until_unlocked(daemon):
    a = await _add(daemon, ["sleep", "0.3"])
    b = await _add(daemon, ["echo", "b"])
    r = await send(daemon.socket_path, "edit_lock", id=b, pid=os.getpid())
    assert r["ok"] and r["data"]["id"] == b
    await wait_for_status(daemon.socket_path, a, {"done"})
    await asyncio.sleep(0.6)  # the channel is free and b is the head, but locked
    shown = (await send(daemon.socket_path, "show", id=b))["data"]
    assert shown["status"] == "queued" and shown["editing"] is True and shown["edit_pid"] == os.getpid()
    listed = _ch((await send(daemon.socket_path, "list"))["data"], 1)["queue"][0]
    assert listed["id"] == b and listed["editing"] is True
    assert (await send(daemon.socket_path, "edit_unlock", id=b, pid=os.getpid()))["data"] == {"released": True}
    await wait_for_status(daemon.socket_path, b, {"done"})


@pytest.mark.asyncio
async def test_edit_with_the_lock_applies_releases_and_the_edited_job_runs(daemon):
    a = await _add(daemon, ["sleep", "0.3"])
    b = await _add(daemon, ["echo", "old"])
    assert (await send(daemon.socket_path, "edit_lock", id=b, pid=os.getpid()))["ok"]
    await wait_for_status(daemon.socket_path, a, {"done"})
    await asyncio.sleep(0.3)
    assert (await send(daemon.socket_path, "show", id=b))["data"]["status"] == "queued"
    r = await _edit(daemon, b, pid=os.getpid(), argv=["echo", "edited"])
    assert r["ok"], r
    await wait_for_status(daemon.socket_path, b, {"done"})
    assert _log_text(daemon, b).strip() == "edited"
    assert (await send(daemon.socket_path, "show", id=b))["data"]["editing"] is False


@pytest.mark.asyncio
async def test_strict_fifo_jobs_behind_a_locked_head_wait_too(daemon):
    a = await _add(daemon, ["sleep", "0.3"])
    b = await _add(daemon, ["echo", "b"])
    c = await _add(daemon, ["echo", "c"])
    assert (await send(daemon.socket_path, "edit_lock", id=b, pid=os.getpid()))["ok"]
    await wait_for_status(daemon.socket_path, a, {"done"})
    await asyncio.sleep(0.5)
    for j in (b, c):
        assert (await send(daemon.socket_path, "show", id=j))["data"]["status"] == "queued"
    await send(daemon.socket_path, "edit_unlock", id=b, pid=os.getpid())
    jb = await wait_for_status(daemon.socket_path, b, {"done"})
    jc = await wait_for_status(daemon.socket_path, c, {"done"})
    assert jb["finished_at"] <= jc["started_at"]


@pytest.mark.asyncio
async def test_a_locked_head_does_not_block_other_channels(daemon):
    b = await _add(daemon, ["echo", "b"], not_before=_in(0.5))  # still waiting, so the lock can be taken first
    assert (await send(daemon.socket_path, "edit_lock", id=b, pid=os.getpid()))["ok"]
    other = await _add(daemon, ["echo", "o"], channel=2)
    await wait_for_status(daemon.socket_path, other, {"done"})
    await asyncio.sleep(1.0)  # b's time has passed; only the lock holds it
    assert (await send(daemon.socket_path, "show", id=b))["data"]["status"] == "queued"


@pytest.mark.asyncio
async def test_lock_of_a_dead_editor_is_cleared_and_the_job_runs(daemon, monkeypatch):
    monkeypatch.setattr("queuer.daemon.EDIT_LOCK_RECHECK_SECS", 0.1)
    p = subprocess.Popen(["true"])
    p.wait()
    b = await _add(daemon, ["echo", "b"], not_before=_in(0.5))  # head: waits, so it can be locked first
    assert (await send(daemon.socket_path, "edit_lock", id=b, pid=os.getpid()))["ok"]
    daemon.conn.execute("UPDATE edit_locks SET holder_pid = ?", (p.pid,))  # the editor "crashed"
    daemon.conn.commit()
    await wait_for_status(daemon.socket_path, b, {"done"}, timeout=5)
    assert daemon.conn.execute("SELECT COUNT(*) FROM edit_locks").fetchone()[0] == 0


@pytest.mark.asyncio
async def test_expired_lock_is_cleared_and_the_job_runs(daemon, monkeypatch):
    monkeypatch.setattr("queuer.daemon.EDIT_LOCK_RECHECK_SECS", 0.1)
    b = await _add(daemon, ["echo", "b"], not_before=_in(0.5))
    assert (await send(daemon.socket_path, "edit_lock", id=b, pid=os.getpid()))["ok"]
    daemon.conn.execute("UPDATE edit_locks SET expires_at = ?", (_in(-1),))
    daemon.conn.commit()
    await wait_for_status(daemon.socket_path, b, {"done"}, timeout=5)


@pytest.mark.asyncio
async def test_edit_after_the_lock_went_stale_fails_cleanly(daemon):
    a = await _add(daemon, ["sleep", "30"])
    await wait_for_status(daemon.socket_path, a, {"running"})
    b = await _add(daemon, ["echo", "b"])
    assert (await send(daemon.socket_path, "edit_lock", id=b, pid=os.getpid()))["ok"]
    daemon.conn.execute("UPDATE edit_locks SET expires_at = ?", (_in(-1),))
    daemon.conn.commit()
    r = await _edit(daemon, b, pid=os.getpid(), timeout_secs=3)
    assert r["ok"] is False and "lock" in r["error"]
    assert (await send(daemon.socket_path, "show", id=b))["data"]["timeout_secs"] is None


@pytest.mark.asyncio
async def test_a_second_editor_is_refused_and_plain_edit_respects_the_lock(daemon):
    a = await _add(daemon, ["sleep", "30"])
    await wait_for_status(daemon.socket_path, a, {"running"})
    b = await _add(daemon, ["echo", "b"])
    assert (await send(daemon.socket_path, "edit_lock", id=b, pid=os.getpid()))["ok"]
    r = await send(daemon.socket_path, "edit_lock", id=b, pid=1)
    assert r["ok"] is False and "already being edited" in r["error"]
    r = await _edit(daemon, b, timeout_secs=3)  # flag edit without the lock holder's pid
    assert r["ok"] is False and "being edited" in r["error"]
    assert (await send(daemon.socket_path, "edit_unlock", id=b, pid=1))["data"] == {"released": False}  # not theirs
    assert (await _edit(daemon, b, pid=os.getpid(), timeout_secs=3))["ok"]


@pytest.mark.asyncio
async def test_lock_is_released_when_the_daemon_restarts(tmp_path):
    db_path = tmp_path / "queuer.db"
    conn = db_module.connect(db_path)
    db_module.init_db(conn)
    job_id = db_module.enqueue(conn, raw_cmd=["true"], resolved_cmd=["/bin/true"], cwd=CWD, log_path=str(tmp_path / "1.log"))
    db_module.take_edit_lock(conn, job_id, os.getpid())
    conn.close()
    d = Daemon(db_path=db_path, socket_path=tmp_path / "queuer.sock", log_dir=tmp_path / "logs")
    task = asyncio.create_task(d.serve())
    try:
        for _ in range(100):
            if d.socket_path.exists():
                break
            await asyncio.sleep(0.02)
        job = await wait_for_status(d.socket_path, job_id, {"done"}, timeout=5)  # not held back by the old lock
        assert job["editing"] is False
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_rm_and_cancel_win_over_an_open_edit(daemon):
    a = await _add(daemon, ["sleep", "30"])
    await wait_for_status(daemon.socket_path, a, {"running"})
    b = await _add(daemon, ["echo", "b"])
    c = await _add(daemon, ["echo", "c"])
    for j in (b, c):
        assert (await send(daemon.socket_path, "edit_lock", id=j, pid=os.getpid()))["ok"]
    assert (await send(daemon.socket_path, "rm", id=b))["ok"]
    assert (await send(daemon.socket_path, "cancel", id=c))["ok"]
    r = await _edit(daemon, b, pid=os.getpid(), timeout_secs=3)
    assert r["ok"] is False and "does not exist" in r["error"]
    r = await _edit(daemon, c, pid=os.getpid(), timeout_secs=3)
    assert r["ok"] is False and "cancelled, not queued" in r["error"]


@pytest.mark.asyncio
async def test_edit_moves_a_job_to_the_end_of_another_channel_and_it_runs_there(daemon):
    a = await _add(daemon, ["sleep", "30"])
    await wait_for_status(daemon.socket_path, a, {"running"})
    b = await _add(daemon, ["echo", "b"])
    c = await _add(daemon, ["echo", "c"])
    x = await _add(daemon, ["sleep", "0.3"], channel=2)
    r = await _edit(daemon, b, channel=2)
    assert r["ok"] and r["data"]["changed"] == ["channel"]
    listing = (await send(daemon.socket_path, "list"))["data"]
    assert [j["id"] for j in _ch(listing, 1)["queue"]] == [c]
    jb = await wait_for_status(daemon.socket_path, b, {"done"})
    assert jb["channel"] == 2
    assert (await wait_for_status(daemon.socket_path, x, {"done"}))["finished_at"] <= jb["started_at"]  # went to the END
    assert (await _edit(daemon, c, channel=1))["ok"] is False  # same channel: nothing to change


@pytest.mark.asyncio
async def test_edit_dependencies_replace_clear_and_are_checked(daemon):
    a = await _add(daemon, ["sleep", "30"], channel=2)
    d1 = await _add(daemon, ["sleep", "30"], channel=3)
    await wait_for_status(daemon.socket_path, a, {"running"})
    b = await _add(daemon, ["echo", "b"], depends_on=[a])
    r = await _edit(daemon, b, after_any=[d1])  # replaces only the after-any kind
    assert r["ok"]
    job = (await send(daemon.socket_path, "show", id=b))["data"]
    assert job["depends_on"] == [a] and job["after_any"] == [d1]
    assert (await _edit(daemon, b, depends_on=[d1]))["ok"]
    job = (await send(daemon.socket_path, "show", id=b))["data"]
    assert job["depends_on"] == [d1] and job["after_any"] == []
    assert (await _edit(daemon, b, clear_deps=True, not_before=_in(3600)))["ok"]  # the delay keeps b from starting
    job = (await send(daemon.socket_path, "show", id=b))["data"]
    assert job["depends_on"] == [] and job["after_any"] == []
    behind = await _add(daemon, ["echo", "z"])
    r = await _edit(daemon, b, depends_on=[behind])  # queued behind b on the same channel
    assert r["ok"] is False and "behind this job" in r["error"]


@pytest.mark.asyncio
async def test_edit_to_depend_on_an_already_failed_job_skips_it(daemon):
    f = await _add(daemon, ["false"], channel=2)
    await wait_for_status(daemon.socket_path, f, {"failed"})
    blocker = await _add(daemon, ["sleep", "30"])
    await wait_for_status(daemon.socket_path, blocker, {"running"})
    b = await _add(daemon, ["echo", "b"])
    assert (await _edit(daemon, b, depends_on=[f]))["ok"]
    job = await wait_for_status(daemon.socket_path, b, {"skipped"}, timeout=3)
    assert f"dependency {f} failed" in job["note"]


@pytest.mark.asyncio
async def test_edit_retries_zero_drops_the_retry_delay(daemon):
    a = await _add(daemon, ["sleep", "30"])
    await wait_for_status(daemon.socket_path, a, {"running"})
    b = await _add(daemon, ["echo", "b"], max_retries=2, retry_delay_secs=60)
    assert (await _edit(daemon, b, max_retries=0))["ok"]
    job = (await send(daemon.socket_path, "show", id=b))["data"]
    assert job["max_retries"] == 0 and job["retry_delay_secs"] is None and job["retry_backoff"] == 0


@pytest.mark.asyncio
async def test_edit_not_before_delays_the_job(daemon):
    a = await _add(daemon, ["sleep", "0.2"])
    b = await _add(daemon, ["echo", "b"])
    assert (await _edit(daemon, b, not_before=_in(1.0)))["ok"]
    await wait_for_status(daemon.socket_path, a, {"done"})
    await asyncio.sleep(0.4)
    assert (await send(daemon.socket_path, "show", id=b))["data"]["status"] == "queued"
    await wait_for_status(daemon.socket_path, b, {"done"}, timeout=4)


# ---------------------------------------------------------------------------
# B9 (DECIDED: keep): the channel waits for the whole process tree
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_backgrounded_child_keeps_the_channel_busy_until_it_ends(daemon):
    """The job's main process exits at once but its backgrounded child still holds the
    log pipe: the job stays `running` and the next job on the channel waits for the child."""
    a = await _add(daemon, ["sh", "-c", "sleep 1.2 & echo started"])
    b = await _add(daemon, ["echo", "after"])
    await wait_for_status(daemon.socket_path, a, {"running"})
    await asyncio.sleep(0.5)  # the shell is long gone, the child is not
    assert (await send(daemon.socket_path, "show", id=a))["data"]["status"] == "running"
    assert (await send(daemon.socket_path, "show", id=b))["data"]["status"] == "queued"
    shown_a = await wait_for_status(daemon.socket_path, a, {"done"}, timeout=10)
    await wait_for_status(daemon.socket_path, b, {"done"})
    started = datetime.fromisoformat(shown_a["started_at"])
    finished = datetime.fromisoformat(shown_a["finished_at"])
    assert (finished - started).total_seconds() >= 1.0  # it waited for the child, not just the shell


@pytest.mark.asyncio
async def test_export_returns_all_rows_with_dependencies(daemon):
    ids = await _history_fixture(daemon)
    blocker = (await send(daemon.socket_path, "add", argv=["sleep", "30"], cwd=CWD, channel=4))["data"]["id"]
    waiting = (await send(daemon.socket_path, "add", argv=["true"], cwd=CWD, channel=4, depends_on=[blocker]))["data"]["id"]
    await wait_for_status(daemon.socket_path, blocker, {"running"})

    resp = await send(daemon.socket_path, "export")
    assert resp["ok"], resp
    jobs = resp["data"]["jobs"]
    assert [j["id"] for j in jobs] == sorted(j["id"] for j in jobs)
    assert {j["id"] for j in jobs} == set(ids.values()) | {blocker, waiting}
    by_id = {j["id"]: j for j in jobs}
    assert by_id[blocker]["status"] == "running" and by_id[waiting]["status"] == "queued"
    assert by_id[waiting]["depends_on"] == [blocker]
    assert json.loads(by_id[ids["done1"]]["raw_cmd"]) == ["echo", "Alpha"]

    only = await send(daemon.socket_path, "export", statuses=["queued"], channel=4)
    assert [j["id"] for j in only["data"]["jobs"]] == [waiting]
    await send(daemon.socket_path, "cancel", id=blocker)


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", [
    {"statuses": ["bogus"]}, {"statuses": "done"}, {"statuses": [1]}, {"channel": 0}, {"channel": "1"},
])
async def test_export_rejects_bad_arguments(daemon, bad):
    resp = await send(daemon.socket_path, "export", **bad)
    assert resp["ok"] is False and resp["error"]


# ---------------------------------------------------------------------------
# Q6: fuzzing the JSON request handler
# ---------------------------------------------------------------------------

_FUZZ_KEYS = [
    "argv", "raw_argv", "jobs", "array", "cwd", "path", "virtual_env", "python_path", "env", "env_pass", "channel",
    "before", "after", "timeout_secs", "now", "max_retries", "depends_on", "after_any", "not_before",
    "on_fail", "retry_delay_secs", "retry_backoff", "id", "pid", "statuses", "since", "grep", "limit",
    "clear_deps", "clear_delay", "retries", "timeout", "delay", "all", "grace", "signal",
]


def _fuzz_value(rng):
    """A random JSON value of a random type -- none of them can name a real executable."""
    junk_argv = ["no-such-exe-fuzz"]
    pool = [
        None, True, False, 0, 1, -1, 2**63, 10**30, 1.5, float("1e300"), "", "x", "\x00", "../" * 40, "é" * 50,
        "a" * 5000, [], {}, [1], [None], [[]], [{}], ["a", 1], {"a": 1}, {"": ""}, {"jobs": []},
        junk_argv, [junk_argv], [{"argv": junk_argv}], [{"argv": 5, "raw_argv": None, "index": "z"}],
        {"A": "b"}, {"1bad": "x"}, {"OK": 5}, [1, 2, 3], ["1-3", "x"], "2026-10-01", "queued",
        ["done", "queued"], ["bogus"],
    ]
    return rng.choice(pool)


@pytest.mark.asyncio
async def test_fuzzed_requests_get_a_clean_reply_and_never_hurt_the_daemon(daemon):
    import random

    rng = random.Random(20261017)  # fixed seed: a failure reproduces
    commands = sorted(n[len("_cmd_"):] for n in dir(Daemon) if n.startswith("_cmd_"))
    assert "add" in commands and "history" in commands and "export" in commands
    internal = []
    for _ in range(400):
        cmd = rng.choice(commands)
        args = {rng.choice(_FUZZ_KEYS): _fuzz_value(rng) for _ in range(rng.randint(0, 6))}
        resp = await send(daemon.socket_path, cmd, **args)
        assert isinstance(resp, dict) and isinstance(resp.get("ok"), bool), (cmd, args, resp)
        if resp["ok"] is False:
            assert isinstance(resp.get("error"), str) and resp["error"], (cmd, args, resp)
            if resp["error"].startswith("internal error"):
                internal.append((cmd, args, resp["error"]))
    # an "internal error" is the last-resort guard catching a handler that did not validate
    assert internal == []
    assert (await send(daemon.socket_path, "status"))["ok"] is True  # still alive and answering


@pytest_asyncio.fixture
async def small_request_limit(monkeypatch):
    """Must be requested BEFORE `daemon`: the limit is read when the server starts."""
    monkeypatch.setattr("queuer.daemon.MAX_REQUEST_BYTES", 100_000)


@pytest.mark.asyncio
async def test_request_over_the_limit_gets_request_too_large(small_request_limit, daemon, capsys):
    # H4: a line past the limit used to get the unhelpful "invalid request"
    data = await _raw(daemon.socket_path, b"x" * 110_000 + b"\n")
    resp = json.loads(data)
    assert resp["ok"] is False and "request too large" in resp["error"]
    assert "Traceback" not in capsys.readouterr().err
    assert (await send(daemon.socket_path, "status"))["ok"]


@pytest.mark.asyncio
async def test_big_each_sweep_fits_through_the_real_socket(daemon):
    # H4: 1000 jobs with 150-character commands is ~200 KB of JSON: over the old 64 KiB stream limit
    jobs = [{"argv": ["echo", f"{i:04d}" + "x" * 146], "index": i} for i in range(1000)]
    request = {"cmd": "add", "args": {"jobs": jobs, "cwd": CWD, "path": "/bin:/usr/bin", "dry_run": True}}
    payload = (json.dumps(request) + "\n").encode()
    assert len(payload) > 64 * 1024
    reader, writer = await asyncio.open_unix_connection(str(daemon.socket_path), limit=16 * 1024 * 1024)  # the reply is big too
    writer.write(payload)
    await writer.drain()
    resp = json.loads((await reader.readline()).decode())
    writer.close()
    await writer.wait_closed()
    assert resp["ok"] is True, resp
    assert len(resp["data"]["jobs"]) == 1000
    assert _job_count(daemon) == 0  # a dry run queues nothing


@pytest.mark.asyncio
@pytest.mark.parametrize("raw", [
    b"[]\n", b"null\n", b"5\n", b'"x"\n', b"true\n",
    b'{"cmd": 5}\n', b'{"cmd": null}\n', b'{"cmd": "_cmd_add"}\n', b'{"cmd": "__init__"}\n',
    b'{"cmd": "add", "args": []}\n', b'{"cmd": "add", "args": "x"}\n', b'{"args": {}}\n', b"{}\n",
])
async def test_malformed_request_shapes_get_ok_false(daemon, raw):
    reader, writer = await asyncio.open_unix_connection(str(daemon.socket_path))
    writer.write(raw)
    await writer.drain()
    resp = json.loads((await reader.readline()).decode())
    writer.close()
    await writer.wait_closed()
    assert resp["ok"] is False and resp["error"]
    assert (await send(daemon.socket_path, "status"))["ok"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("cmd", ["edit_lock", "edit_unlock", "edit"])
@pytest.mark.parametrize("pid", [2**22 + 1, 2**31, 2**40, 2**63, 10**30])
async def test_huge_editor_pid_is_refused_and_leaves_no_lock(daemon, cmd, pid):
    # H2: pid 2**40 used to be stored as a lock, then crash the worker (and the daemon) when
    # that job became the head
    blocker = (await send(daemon.socket_path, "add", argv=["sleep", "30"], cwd=CWD))["data"]["id"]
    await wait_for_status(daemon.socket_path, blocker, {"running"})
    queued = (await send(daemon.socket_path, "add", argv=["echo", "q"], cwd=CWD))["data"]["id"]
    resp = await send(daemon.socket_path, cmd, id=queued, pid=pid, timeout_secs=5)
    assert resp["ok"] is False and not resp["error"].startswith("internal error")
    assert daemon.conn.execute("SELECT COUNT(*) FROM edit_locks").fetchone()[0] == 0
    await send(daemon.socket_path, "cancel", id=blocker)
    assert (await wait_for_status(daemon.socket_path, queued, {"done"}))["status"] == "done"


@pytest.mark.asyncio
async def test_worker_survives_a_failing_step_and_carries_on(daemon, monkeypatch, capsys):
    # H2: one DB error in the worker loop body used to end the worker and shut the daemon down
    monkeypatch.setattr("queuer.daemon.WORKER_ERROR_SECS", 0.05)
    real = db_module.get_paused
    calls = {"n": 0}

    def flaky(conn, channel=1):
        calls["n"] += 1
        if calls["n"] == 1:
            raise sqlite3.OperationalError("database is locked")
        return real(conn, channel)

    monkeypatch.setattr(db_module, "get_paused", flaky)
    daemon._wake_worker(1)
    r = await send(daemon.socket_path, "add", argv=["echo", "after-error"], cwd=CWD)
    job = await wait_for_status(daemon.socket_path, r["data"]["id"], {"done", "failed"})
    assert job["status"] == "done"
    assert calls["n"] >= 2
    assert "worker error" in capsys.readouterr().err
    assert not daemon._worker_tasks[1].done()


@pytest.mark.asyncio
async def test_database_error_in_a_command_gets_a_clean_reply(daemon, monkeypatch, capsys):
    async def locked(args):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(daemon, "_cmd_status", locked)
    resp = await send(daemon.socket_path, "status")
    assert resp["ok"] is False and "database is locked" in resp["error"]
    assert "Traceback" not in capsys.readouterr().err
    monkeypatch.delattr(daemon, "_cmd_status")  # the instance override only; the class method is back
    assert (await send(daemon.socket_path, "status"))["ok"] is True


@pytest.mark.asyncio
async def test_overflow_in_a_command_gets_a_clean_reply(daemon, capsys):
    resp = await send(daemon.socket_path, "add", argv=["echo", "x"], cwd=CWD, channel=2**70)
    assert resp["ok"] is False and not resp["error"].startswith("internal error")
    assert "Traceback" not in capsys.readouterr().err
    assert (await send(daemon.socket_path, "status"))["ok"] is True


@pytest.mark.asyncio
async def test_huge_ints_in_every_int_field_never_hurt_the_daemon(daemon):
    # H2 / Q6: ints past 2**31, 2**63 and 10**30 in every numeric field of every command
    commands = sorted(n[len("_cmd_"):] for n in dir(Daemon) if n.startswith("_cmd_"))
    keys = ["id", "pid", "channel", "before", "after", "timeout_secs", "max_retries", "retry_delay_secs",
            "limit", "index", "grace", "since"]
    internal = []
    for cmd in commands:
        for big in (2**31, 2**40, 2**63, 10**30):
            for key in keys:
                resp = await send(daemon.socket_path, cmd, argv=["echo", "x"], cwd=CWD, **{key: big})
                assert isinstance(resp.get("ok"), bool), (cmd, key, big, resp)
                if resp["ok"] is False and resp["error"].startswith("internal error"):
                    internal.append((cmd, key, big, resp["error"]))
    assert internal == []
    assert (await send(daemon.socket_path, "status"))["ok"] is True


@pytest.mark.asyncio
async def test_serve_is_told_when_a_worker_made_after_startup_ends_by_itself(tmp_path):
    d = Daemon(db_path=tmp_path / "q.db", socket_path=tmp_path / "q.sock", log_dir=tmp_path / "logs")

    async def boom() -> None:
        raise RuntimeError("worker bug")

    crashed = asyncio.create_task(boom())
    crashed.add_done_callback(d._worker_done)
    await asyncio.sleep(0.05)
    assert d._worker_ended.is_set()
    assert isinstance(crashed.exception(), RuntimeError)

    d._worker_ended.clear()
    cancelled = asyncio.create_task(asyncio.sleep(30))
    cancelled.add_done_callback(d._worker_done)
    cancelled.cancel()
    await asyncio.sleep(0.05)
    assert not d._worker_ended.is_set()  # a cancelled worker is respawned on the next add, not a stop


# ---------------------------------------------------------------------------
# F16: add --dry-run (dry_run=True on the daemon's add)
# ---------------------------------------------------------------------------

def _job_count(daemon) -> int:
    return daemon.conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]


async def _dry(daemon, argv=None, **kw) -> dict:
    return await send(daemon.socket_path, "add", argv=argv or ["echo", "hi"], cwd=CWD, dry_run=True, **kw)


@pytest.mark.asyncio
async def test_dry_run_queues_nothing_and_does_not_use_up_an_id(daemon):
    resp = await _dry(daemon, channel=3)
    assert resp["ok"], resp
    data = resp["data"]
    assert data["dry_run"] is True and data["channel"] == 3 and data["cwd"] == CWD
    assert data["jobs"][0]["raw_argv"] == ["echo", "hi"]
    assert data["jobs"][0]["argv"][-1] == "hi" and os.path.isabs(data["jobs"][0]["argv"][0])
    assert data["jobs"][0]["position"] == 1 and data["queued_before"] == 0
    assert data["running_id"] is None and data["paused"] is False
    assert _job_count(daemon) == 0
    assert 3 not in db_module.list_channels(daemon.conn)  # the channel was not registered either
    assert await _add(daemon, ["echo", "real"]) == 1  # the dry run did not consume job id 1


@pytest.mark.asyncio
async def test_dry_run_reports_the_queue_position_and_the_running_job(daemon):
    a = await _add(daemon, ["sleep", "30"])
    await wait_for_status(daemon.socket_path, a, {"running"})
    b = await _add(daemon, ["sleep", "30"])
    c = await _add(daemon, ["sleep", "30"])
    before_count = _job_count(daemon)
    positions = {i: db_module.get_job(daemon.conn, i)["position"] for i in (b, c)}
    data = (await _dry(daemon))["data"]
    assert data["jobs"][0]["position"] == 3 and data["queued_before"] == 2 and data["running_id"] == a
    data = (await _dry(daemon, before=c))["data"]
    assert data["jobs"][0]["position"] == 2
    data = (await _dry(daemon, after=b))["data"]
    assert data["jobs"][0]["position"] == 2
    data = (await _dry(daemon, now=True))["data"]
    assert data["priority"] is True and data["jobs"][0]["position"] == 1 and data["running_id"] == a
    # nothing changed: same rows, the running job was not frozen, the queue order is intact
    assert _job_count(daemon) == before_count
    assert (await send(daemon.socket_path, "show", id=a))["data"]["status"] == "running"
    assert [j["id"] for j in db_module.get_queue(daemon.conn, 1)] == [b, c]
    assert {i: db_module.get_job(daemon.conn, i)["position"] for i in (b, c)} == positions  # the rolled-back dry runs renumbered nothing


@pytest.mark.asyncio
async def test_dry_run_reports_a_paused_channel(daemon):
    await send(daemon.socket_path, "pause", channel=2)
    assert (await _dry(daemon, channel=2))["data"]["paused"] is True
    assert (await _dry(daemon, channel=1))["data"]["paused"] is False


@pytest.mark.asyncio
async def test_dry_run_gives_the_same_errors_as_a_real_add_and_leaves_nothing_behind(daemon):
    a = await _add(daemon, ["sleep", "30"])
    for kw, text in (
        (dict(cwd="/no/such/dir"), "working directory does not exist"),
        (dict(argv=["definitely-not-a-command-xyz"]), "not found"),
        (dict(before=999), "not currently queued"),
        (dict(depends_on=[999]), "does not exist"),
        (dict(channel=-1), "channel must be a positive integer"),
        (dict(now=True, before=a), "--now cannot be combined"),
        (dict(retry_delay_secs=60), "--retry-delay needs --retries"),
    ):
        args = {"argv": ["echo", "hi"], "cwd": CWD, "dry_run": True, **kw}
        resp = await send(daemon.socket_path, "add", **args)
        assert resp["ok"] is False and text in resp["error"], (kw, resp)
    assert _job_count(daemon) == 1
    assert await _add(daemon, ["echo", "after"]) == a + 1


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", ["yes", 1, [], {}])
async def test_dry_run_must_be_a_boolean(daemon, bad):
    resp = await send(daemon.socket_path, "add", argv=["echo", "hi"], cwd=CWD, dry_run=bad)
    assert resp["ok"] is False and "dry_run must be true or false" in resp["error"]
    assert _job_count(daemon) == 0


@pytest.mark.asyncio
async def test_dry_run_false_or_missing_queues_for_real(daemon):
    resp = await send(daemon.socket_path, "add", argv=["echo", "hi"], cwd=CWD, dry_run=False)
    assert resp["ok"] and "dry_run" not in resp["data"] and resp["data"]["id"] == 1
    assert _job_count(daemon) == 1


@pytest.mark.asyncio
async def test_dry_run_dependency_verdict(daemon):
    a = await _add(daemon, ["false"])
    await wait_for_status(daemon.socket_path, a, {"failed"})
    data = (await _dry(daemon, depends_on=[a]))["data"]
    assert data["jobs"][0]["dep_verdict"] == "failed" and data["jobs"][0]["dep_id"] == a
    data = (await _dry(daemon, after_any=[a]))["data"]
    assert data["jobs"][0]["dep_verdict"] == "ready"
    assert (await _dry(daemon))["data"]["jobs"][0]["dep_verdict"] == "ready"
    b = await _add(daemon, ["sleep", "30"])
    data = (await _dry(daemon, depends_on=[b]))["data"]
    assert data["jobs"][0]["dep_verdict"] == "waiting" and data["jobs"][0]["dep_id"] == b
    assert _job_count(daemon) == 2
    assert db_module.dep_info(daemon.conn, 3) == {"depends_on": [], "after_any": [], "waiting_on": []}


@pytest.mark.asyncio
async def test_dry_run_rejects_a_dependency_queued_behind_the_job(daemon):
    a = await _add(daemon, ["sleep", "30"])
    await wait_for_status(daemon.socket_path, a, {"running"})
    b = await _add(daemon, ["sleep", "30"])
    resp = await _dry(daemon, before=b, depends_on=[b])
    assert resp["ok"] is False and "never start" in resp["error"]
    assert [j["id"] for j in db_module.get_queue(daemon.conn, 1)] == [b]


@pytest.mark.asyncio
async def test_dry_run_sweep_reports_every_job_and_creates_none(daemon):
    jobs = [{"argv": ["echo", v], "raw_argv": ["echo", v], "index": i} for i, v in enumerate("abc", 1)]
    resp = await send(daemon.socket_path, "add", jobs=jobs, cwd=CWD, dry_run=True)
    assert resp["ok"], resp
    got = resp["data"]["jobs"]
    assert [j["index"] for j in got] == [1, 2, 3] and [j["position"] for j in got] == [1, 2, 3]
    assert "QUR_INDEX" not in resp["data"]["env"]
    assert _job_count(daemon) == 0
    assert await _add(daemon, ["echo", "x"]) == 1


@pytest.mark.asyncio
async def test_dry_run_returns_the_environment_the_job_would_get(daemon):
    data = (await _dry(
        daemon, env={"FOO": "bar"}, env_pass={"OMP_NUM_THREADS": "4"}, path="/usr/bin:/bin", virtual_env="/v",
    ))["data"]
    env = data["env"]
    assert env["FOO"] == "bar" and env["OMP_NUM_THREADS"] == "4" and env["PATH"] == "/usr/bin:/bin"
    assert env["VIRTUAL_ENV"] == "/v" and env["QUR_ENV_PASSTHROUGH"] == "OMP_NUM_THREADS"


@pytest.mark.asyncio
async def test_dry_run_does_not_start_anything_or_send_notifications(daemon):
    await _dry(daemon, argv=["touch", "/tmp/queuer-dry-run-should-not-exist"])
    await asyncio.sleep(0.3)
    assert not os.path.exists("/tmp/queuer-dry-run-should-not-exist")
    assert daemon._current_proc == {}


# ---------------------------------------------------------------------------
# F12: --on-finish / --on-error hooks and the global post-job hook
# ---------------------------------------------------------------------------

async def _until(predicate, timeout: float = 5.0) -> None:
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.05)
    raise AssertionError("condition not met in time")


def _lines(path: Path) -> list[str]:
    return path.read_text().splitlines() if path.exists() else []


@pytest.mark.asyncio
async def test_on_finish_runs_after_a_done_job_with_the_qur_variables(daemon, tmp_path):
    out = tmp_path / "hook.out"
    hook = f'echo "$QUR_HOOK $QUR_JOB_ID $QUR_STATUS [$QUR_EXIT_CODE] $QUR_CHANNEL $QUR_CWD $QUR_LOG $FOO" > {out}'
    a = await _add(daemon, ["echo", "hi"], channel=2, on_finish=hook, env={"FOO": "bar"})
    await wait_for_status(daemon.socket_path, a, {"done"})
    await _until(out.exists)
    await _until(lambda: out.read_text().endswith("\n"))
    assert out.read_text().strip() == f"on-finish {a} done [0] 2 {CWD} {daemon.log_dir / f'{a}.log'} bar"


@pytest.mark.asyncio
async def test_hook_gets_the_command_and_runs_in_the_job_cwd(daemon, tmp_path):
    out = tmp_path / "hook.out"
    hook = f'echo "$QUR_COMMAND|$(pwd)|[$QUR_DURATION_SECS]" > {out}'
    a = await _add(daemon, ["sh", "-c", "exit 0"], on_finish=hook)
    await wait_for_status(daemon.socket_path, a, {"done"})
    await _until(lambda: out.exists() and out.read_text().endswith("\n"))
    command, cwd, duration = out.read_text().strip().split("|")
    assert command == "sh -c 'exit 0'" and cwd == CWD and duration.strip("[]").isdigit()


@pytest.mark.asyncio
async def test_on_error_runs_only_for_a_failed_job_and_on_finish_for_both(daemon, tmp_path):
    fin, err = tmp_path / "fin", tmp_path / "err"
    ok = await _add(daemon, ["true"], on_finish=f"echo $QUR_JOB_ID >> {fin}", on_error=f"echo $QUR_JOB_ID >> {err}")
    await wait_for_status(daemon.socket_path, ok, {"done"})
    await _until(lambda: _lines(fin) == [str(ok)])
    bad = await _add(daemon, ["false"], on_finish=f"echo $QUR_JOB_ID >> {fin}", on_error=f"echo $QUR_JOB_ID >> {err}")
    await wait_for_status(daemon.socket_path, bad, {"failed"})
    await _until(lambda: _lines(fin) == [str(ok), str(bad)] and _lines(err) == [str(bad)])


@pytest.mark.asyncio
async def test_hooks_run_in_order_global_then_finish_then_error(daemon, tmp_path):
    out = tmp_path / "order"
    daemon.post_job_hook = f"echo $QUR_HOOK >> {out}"
    a = await _add(daemon, ["false"], on_error=f"echo $QUR_HOOK >> {out}", on_finish=f"echo $QUR_HOOK >> {out}")
    await wait_for_status(daemon.socket_path, a, {"failed"})
    await _until(lambda: _lines(out) == ["post-job", "on-finish", "on-error"])


@pytest.mark.asyncio
async def test_global_hook_runs_for_jobs_without_their_own_hooks(daemon, tmp_path):
    out = tmp_path / "g"
    daemon.post_job_hook = f"echo $QUR_JOB_ID:$QUR_STATUS >> {out}"
    a = await _add(daemon, ["true"])
    b = await _add(daemon, ["false"])
    await wait_for_status(daemon.socket_path, b, {"failed"})
    await _until(lambda: _lines(out) == [f"{a}:done", f"{b}:failed"])


@pytest.mark.asyncio
async def test_hooks_run_only_after_the_final_outcome_of_a_retried_job(daemon, tmp_path):
    out = tmp_path / "retry"
    a = await _add(daemon, ["false"], max_retries=1, on_error=f"echo $QUR_JOB_ID >> {out}")
    await wait_for_status(daemon.socket_path, a + 1, {"failed"})
    await _until(lambda: _lines(out) == [str(a + 1)])  # the retry's failure, not the first one
    await asyncio.sleep(0.3)
    assert _lines(out) == [str(a + 1)]


@pytest.mark.asyncio
async def test_cancelled_running_job_runs_on_finish_but_not_on_error(daemon, tmp_path):
    fin, err = tmp_path / "fin", tmp_path / "err"
    a = await _add(daemon, ["sleep", "30"], on_finish=f"echo $QUR_STATUS >> {fin}", on_error=f"echo x >> {err}")
    await wait_for_status(daemon.socket_path, a, {"running"})
    assert (await send(daemon.socket_path, "cancel", id=a))["ok"]
    await wait_for_status(daemon.socket_path, a, {"cancelled"})
    await _until(lambda: _lines(fin) == ["cancelled"])
    await asyncio.sleep(0.3)
    assert not err.exists()


@pytest.mark.asyncio
async def test_no_hooks_for_a_job_cancelled_while_queued_or_skipped(daemon, tmp_path):
    out = tmp_path / "none"
    daemon.post_job_hook = f"echo $QUR_STATUS >> {out}"
    blocker = await _add(daemon, ["sleep", "30"])
    await wait_for_status(daemon.socket_path, blocker, {"running"})
    queued = await _add(daemon, ["echo", "q"], on_finish=f"echo fin >> {out}")
    assert (await send(daemon.socket_path, "cancel", id=queued))["ok"]
    failed = await _add(daemon, ["false"], channel=2)
    await wait_for_status(daemon.socket_path, failed, {"failed"})
    skipped = await _add(daemon, ["echo", "s"], channel=2, depends_on=[failed], on_finish=f"echo fin >> {out}")
    await wait_for_status(daemon.socket_path, skipped, {"skipped"})
    await _until(lambda: _lines(out) == ["failed"])  # only the failed job's global hook
    await asyncio.sleep(0.3)
    assert _lines(out) == ["failed"]


@pytest.mark.asyncio
async def test_a_slow_hook_does_not_hold_the_channel(daemon, tmp_path):
    a = await _add(daemon, ["true"], on_finish="sleep 30")
    b = await _add(daemon, ["echo", "next"])
    await wait_for_status(daemon.socket_path, a, {"done"})
    await wait_for_status(daemon.socket_path, b, {"done"}, timeout=3)
    assert daemon._hook_tasks  # the hook is still running in the background
    tasks = list(daemon._hook_tasks)
    for task in tasks:  # cancelling kills its process (see _run_hook)
        task.cancel()
    await asyncio.wait(tasks, timeout=3)


@pytest.mark.asyncio
async def test_a_hook_that_runs_too_long_is_killed_and_the_log_says_so(daemon, monkeypatch):
    monkeypatch.setattr("queuer.daemon.HOOK_TIMEOUT_SECS", 0.3)
    a = await _add(daemon, ["true"], on_finish="sleep 30")
    await wait_for_status(daemon.socket_path, a, {"done"})
    await _until(lambda: "timed out" in _log_text(daemon, a))
    assert "[queuer] hook on-finish: sleep 30" in _log_text(daemon, a)


@pytest.mark.asyncio
async def test_hook_output_and_exit_code_are_appended_to_the_job_log(daemon):
    a = await _add(daemon, ["echo", "job-output"], on_finish="echo from-hook; echo oops >&2; exit 3")
    await wait_for_status(daemon.socket_path, a, {"done"})
    await _until(lambda: "hook on-finish exited" in _log_text(daemon, a))
    log = _log_text(daemon, a)
    assert log.index("job-output") < log.index("[queuer] hook on-finish: echo from-hook")
    assert "from-hook\n" in log and "oops\n" in log and "[queuer] hook on-finish exited 3" in log
    assert (await send(daemon.socket_path, "show", id=a))["data"]["status"] == "done"  # a failing hook changes nothing


@pytest.mark.asyncio
async def test_hook_output_is_capped(daemon, monkeypatch):
    monkeypatch.setattr("queuer.daemon.HOOK_OUTPUT_BYTES", 100)
    a = await _add(daemon, ["true"], on_finish="head -c 100000 /dev/zero | tr '\\0' x")
    await wait_for_status(daemon.socket_path, a, {"done"})
    await _until(lambda: "hook on-finish exited 0" in _log_text(daemon, a))
    assert 100 <= _log_text(daemon, a).count("x") <= 120  # 100 kept (the hook's own header has a few x's)


@pytest.mark.asyncio
async def test_a_hook_that_cannot_start_is_logged_and_harmless(daemon, tmp_path):
    work = tmp_path / "gone"
    work.mkdir()
    resp = await send(daemon.socket_path, "add", argv=["sleep", "0.3"], cwd=str(work), on_finish="echo hi")
    a = resp["data"]["id"]
    await wait_for_status(daemon.socket_path, a, {"running"})
    work.rmdir()  # the hook's cwd disappears before it starts
    await wait_for_status(daemon.socket_path, a, {"done"})
    await _until(lambda: "could not start" in _log_text(daemon, a))
    assert (await _add(daemon, ["echo", "still-alive"])) > a


@pytest.mark.asyncio
async def test_hooks_are_validated_by_the_daemon_and_blank_means_none(daemon):
    for kw in (
        {"on_finish": 5}, {"on_error": ["echo"]}, {"on_finish": "a\0b"}, {"on_error": "x" * (db_module.MAX_HOOK_LEN + 1)},
    ):
        resp = await send(daemon.socket_path, "add", argv=["echo", "hi"], cwd=CWD, **kw)
        assert resp["ok"] is False and "command" in resp["error"], kw
    assert daemon.conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 0
    a = await _add(daemon, ["echo", "hi"], on_finish="   ", on_error=None)
    job = (await send(daemon.socket_path, "show", id=a))["data"]
    assert job["on_finish_cmd"] is None and job["on_error_cmd"] is None


@pytest.mark.asyncio
async def test_sweep_jobs_share_the_hooks(daemon):
    jobs = [{"argv": ["echo", v], "raw_argv": ["echo", v], "index": i} for i, v in enumerate("ab", 1)]
    resp = await send(daemon.socket_path, "add", jobs=jobs, cwd=CWD, on_finish="echo x", on_error="echo y")
    assert resp["ok"], resp
    for job_id in resp["data"]["ids"]:
        job = (await send(daemon.socket_path, "show", id=job_id))["data"]
        assert job["on_finish_cmd"] == "echo x" and job["on_error_cmd"] == "echo y"


@pytest.mark.asyncio
async def test_dry_run_reports_the_hooks_and_stores_nothing(daemon):
    data = (await _dry(daemon, on_finish="echo done", on_error="echo bad"))["data"]
    assert data["on_finish"] == "echo done" and data["on_error"] == "echo bad"
    data = (await _dry(daemon))["data"]
    assert data["on_finish"] is None and data["on_error"] is None
    assert _job_count(daemon) == 0


@pytest.mark.asyncio
async def test_requeue_keeps_the_hooks(daemon):
    a = await _add(daemon, ["true"], on_finish="echo f", on_error="echo e", channel=3)
    await wait_for_status(daemon.socket_path, a, {"done"})
    new_id = (await send(daemon.socket_path, "requeue", id=a))["data"]["id"]
    new = (await send(daemon.socket_path, "show", id=new_id))["data"]
    assert new["on_finish_cmd"] == "echo f" and new["on_error_cmd"] == "echo e"


@pytest.mark.asyncio
async def test_edit_sets_changes_and_removes_hooks(daemon):
    blocker = await _add(daemon, ["sleep", "30"])
    await wait_for_status(daemon.socket_path, blocker, {"running"})
    b = await _add(daemon, ["echo", "b"])
    resp = await _edit(daemon, b, on_finish="echo f", on_error="echo e")
    assert resp["ok"] and set(resp["data"]["changed"]) == {"on-finish", "on-error"}
    job = (await send(daemon.socket_path, "show", id=b))["data"]
    assert job["on_finish_cmd"] == "echo f" and job["on_error_cmd"] == "echo e"
    resp = await _edit(daemon, b, on_finish="echo f")  # same value: nothing to change
    assert resp["ok"] is False and "nothing to change" in resp["error"]
    resp = await _edit(daemon, b, on_finish="", on_error=None)
    assert resp["ok"] and set(resp["data"]["changed"]) == {"on-finish", "on-error"}
    job = (await send(daemon.socket_path, "show", id=b))["data"]
    assert job["on_finish_cmd"] is None and job["on_error_cmd"] is None
    resp = await _edit(daemon, b, on_finish=7)
    assert resp["ok"] is False and "on-finish command" in resp["error"]


def test_global_hook_comes_from_the_environment_or_the_constructor(tmp_path, monkeypatch):
    paths = dict(db_path=tmp_path / "q.db", socket_path=tmp_path / "q.sock", log_dir=tmp_path / "logs")
    monkeypatch.delenv("QUEUER_POST_JOB_HOOK", raising=False)
    assert Daemon(**paths).post_job_hook is None
    monkeypatch.setenv("QUEUER_POST_JOB_HOOK", "echo from-env")
    assert Daemon(**paths).post_job_hook == "echo from-env"
    assert Daemon(**paths, post_job_hook="echo explicit").post_job_hook == "echo explicit"
    monkeypatch.setenv("QUEUER_POST_JOB_HOOK", "   ")
    assert Daemon(**paths).post_job_hook is None


def test_an_unusable_global_hook_is_ignored_with_a_warning(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("QUEUER_POST_JOB_HOOK", "x" * (db_module.MAX_HOOK_LEN + 1))
    d = Daemon(db_path=tmp_path / "q.db", socket_path=tmp_path / "q.sock", log_dir=tmp_path / "logs")
    assert d.post_job_hook is None
    assert "ignoring QUEUER_POST_JOB_HOOK" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# F11: configurable stop signal and grace
# ---------------------------------------------------------------------------

def _int_trap(out: Path) -> list[str]:
    """A job that records SIGINT and exits 0; it is only stopped by INT (SIGTERM kills it, no record)."""
    return ["sh", "-c", f'trap "echo got-int >> {out}; exit 0" INT; while :; do sleep 0.1; done']


_IGNORE_TERM = ["sh", "-c", "trap '' TERM; while :; do sleep 0.1; done"]


@pytest.mark.asyncio
async def test_cancel_with_a_signal_sends_that_signal(daemon, tmp_path):
    out = tmp_path / "sig"
    a = await _add(daemon, _int_trap(out))
    await wait_for_status(daemon.socket_path, a, {"running"})
    await asyncio.sleep(0.3)
    resp = await send(daemon.socket_path, "cancel", id=a, signal="int")
    assert resp["ok"], resp
    job = await wait_for_status(daemon.socket_path, a, {"cancelled"})
    assert _lines(out) == ["got-int"] and job["exit_code"] == 0


@pytest.mark.asyncio
async def test_cancel_grace_zero_kills_a_job_that_ignores_sigterm_at_once(daemon):
    a = await _add(daemon, _IGNORE_TERM)
    await wait_for_status(daemon.socket_path, a, {"running"})
    await asyncio.sleep(0.3)
    t0 = time.monotonic()
    assert (await send(daemon.socket_path, "cancel", id=a, grace=0))["ok"]
    await wait_for_status(daemon.socket_path, a, {"cancelled"}, timeout=3)
    assert time.monotonic() - t0 < 3  # the default grace is 5 s


@pytest.mark.asyncio
async def test_a_longer_grace_lets_the_job_finish_its_shutdown(daemon, tmp_path):
    out = tmp_path / "late"
    slow = ["sh", "-c", f'trap "sleep 1; echo saved >> {out}; exit 0" TERM; while :; do sleep 0.1; done']
    a = await _add(daemon, slow)
    await wait_for_status(daemon.socket_path, a, {"running"})
    await asyncio.sleep(0.3)
    assert (await send(daemon.socket_path, "cancel", id=a, grace=10))["ok"]
    await wait_for_status(daemon.socket_path, a, {"cancelled"}, timeout=6)
    assert _lines(out) == ["saved"]


@pytest.mark.asyncio
async def test_the_jobs_own_stop_settings_are_used_by_a_plain_cancel(daemon, tmp_path):
    out = tmp_path / "own"
    a = await _add(daemon, _int_trap(out), stop_signal="INT", stop_grace=10)
    job = (await send(daemon.socket_path, "show", id=a))["data"]
    assert job["stop_signal"] == "SIGINT" and job["stop_grace_secs"] == 10
    await wait_for_status(daemon.socket_path, a, {"running"})
    await asyncio.sleep(0.3)
    assert (await send(daemon.socket_path, "cancel", id=a))["ok"]
    await wait_for_status(daemon.socket_path, a, {"cancelled"})
    assert _lines(out) == ["got-int"]


@pytest.mark.asyncio
async def test_cancel_flags_override_the_jobs_own_stop_settings(daemon, tmp_path):
    a = await _add(daemon, _IGNORE_TERM, stop_grace=60)
    await wait_for_status(daemon.socket_path, a, {"running"})
    await asyncio.sleep(0.3)
    t0 = time.monotonic()
    assert (await send(daemon.socket_path, "cancel", id=a, grace=0))["ok"]
    await wait_for_status(daemon.socket_path, a, {"cancelled"}, timeout=3)
    assert time.monotonic() - t0 < 3


# M1: the leader exits on SIGTERM but a child in the same process group ignores it
_LEADER_DIES_CHILD_IGNORES_TERM = ["bash", "-c", '(trap "" TERM; exec sleep 600) & wait']


@pytest.mark.asyncio
async def test_cancel_kills_a_child_that_ignores_sigterm_after_the_leader_exited(daemon):
    a = await _add(daemon, _LEADER_DIES_CHILD_IGNORES_TERM)
    await wait_for_status(daemon.socket_path, a, {"running"})
    await asyncio.sleep(0.5)
    t0 = time.monotonic()
    assert (await send(daemon.socket_path, "cancel", id=a, grace=1))["ok"]
    job = await wait_for_status(daemon.socket_path, a, {"cancelled"}, timeout=6)
    assert job["status"] == "cancelled" and time.monotonic() - t0 < 6  # grace + 1 s, not for ever


@pytest.mark.asyncio
async def test_a_second_cancel_still_escalates_to_sigkill(daemon):
    a = await _add(daemon, _LEADER_DIES_CHILD_IGNORES_TERM, stop_grace=60)
    await wait_for_status(daemon.socket_path, a, {"running"})
    await asyncio.sleep(0.5)
    first = asyncio.create_task(send(daemon.socket_path, "cancel", id=a))  # waits out the 60 s grace
    await asyncio.sleep(0.5)
    assert (await send(daemon.socket_path, "cancel", id=a, grace=0))["ok"]
    await wait_for_status(daemon.socket_path, a, {"cancelled"}, timeout=6)
    assert (await first)["ok"]


@pytest.mark.asyncio
async def test_timeout_kills_a_child_that_ignores_sigterm_after_the_leader_exited(daemon):
    a = await _add(daemon, _LEADER_DIES_CHILD_IGNORES_TERM, timeout_secs=1, stop_grace=1)
    job = await wait_for_status(daemon.socket_path, a, {"failed"}, timeout=8)
    assert job["note"] == "timed out"


@pytest.mark.asyncio
async def test_a_timeout_uses_the_jobs_stop_signal(daemon, tmp_path):
    out = tmp_path / "timeout"
    a = await _add(daemon, _int_trap(out), timeout_secs=1, stop_signal="SIGINT")
    job = await wait_for_status(daemon.socket_path, a, {"failed"}, timeout=8)
    assert job["note"] == "timed out" and _lines(out) == ["got-int"]


@pytest.mark.asyncio
async def test_cancel_signal_is_ignored_for_a_queued_job(daemon):
    run = await _add(daemon, ["sleep", "30"])
    queued = await _add(daemon, ["true"])
    await wait_for_status(daemon.socket_path, run, {"running"})
    assert (await send(daemon.socket_path, "cancel", id=queued, signal="INT", grace=1))["ok"]
    assert (await send(daemon.socket_path, "show", id=queued))["data"]["status"] == "cancelled"
    await send(daemon.socket_path, "cancel", id=run)


@pytest.mark.asyncio
async def test_bad_stop_settings_are_rejected(daemon):
    for kw in ({"stop_signal": "STOP"}, {"stop_signal": "bogus"}, {"stop_grace": -1}, {"stop_grace": "5"}, {"stop_grace": True}):
        resp = await send(daemon.socket_path, "add", argv=["true"], cwd=CWD, **kw)
        assert not resp["ok"], kw
    run = await _add(daemon, ["sleep", "30"])
    await wait_for_status(daemon.socket_path, run, {"running"})
    for kw in ({"signal": "STOP"}, {"signal": "bogus"}, {"grace": -1}, {"grace": "5"}):
        resp = await send(daemon.socket_path, "cancel", id=run, **kw)
        assert not resp["ok"], kw
    assert (await send(daemon.socket_path, "show", id=run))["data"]["status"] == "running"
    await send(daemon.socket_path, "cancel", id=run)


@pytest.mark.asyncio
async def test_dry_run_reports_the_stop_settings_and_queues_nothing(daemon):
    resp = await send(daemon.socket_path, "add", argv=["true"], cwd=CWD, dry_run=True, stop_signal="int", stop_grace=30)
    assert resp["ok"] and resp["data"]["stop_signal"] == "SIGINT" and resp["data"]["stop_grace"] == 30
    assert not (await send(daemon.socket_path, "show", id=resp["data"]["jobs"][0]["position"]))["ok"]  # nothing was queued


@pytest.mark.asyncio
async def test_edit_changes_and_clears_the_stop_settings(daemon):
    run = await _add(daemon, ["sleep", "30"])
    queued = await _add(daemon, ["true"])
    await wait_for_status(daemon.socket_path, run, {"running"})
    resp = await send(daemon.socket_path, "edit", id=queued, stop_signal="usr1", stop_grace=7)
    assert resp["ok"] and set(resp["data"]["changed"]) == {"stop signal", "stop grace"}
    job = (await send(daemon.socket_path, "show", id=queued))["data"]
    assert job["stop_signal"] == "SIGUSR1" and job["stop_grace_secs"] == 7
    resp = await send(daemon.socket_path, "edit", id=queued, stop_signal=None, stop_grace=None)
    assert resp["ok"]
    job = (await send(daemon.socket_path, "show", id=queued))["data"]
    assert job["stop_signal"] is None and job["stop_grace_secs"] is None
    bad = await send(daemon.socket_path, "edit", id=queued, stop_signal="STOP")
    assert not bad["ok"]
    await send(daemon.socket_path, "cancel", id=run)


@pytest.mark.asyncio
async def test_a_requeue_keeps_the_stop_settings(daemon):
    a = await _add(daemon, ["false"], stop_signal="HUP", stop_grace=3)
    await wait_for_status(daemon.socket_path, a, {"failed"})
    new_id = (await send(daemon.socket_path, "requeue", id=a))["data"]["id"]
    job = (await send(daemon.socket_path, "show", id=new_id))["data"]
    assert job["stop_signal"] == "SIGHUP" and job["stop_grace_secs"] == 3


@pytest.mark.asyncio
async def test_usage_sampler_keeps_the_last_peak_rss_when_a_later_sample_has_none(tmp_path, monkeypatch):
    """A killed job that is not reaped yet is a zombie: /proc still has its CPU times but no
    VmHWM. That sample must not wipe the peak RSS read while the job was alive."""
    samples = iter([(2048, 1.0, 0.5), (None, 2.0, 1.0)])
    last = (None, 2.0, 1.0)
    monkeypatch.setattr("queuer.daemon._read_proc_usage", lambda pid: next(samples, last))
    monkeypatch.setattr("queuer.daemon.RESOURCE_SAMPLE_SECS", 0.01)
    d = Daemon(db_path=tmp_path / "q.db", socket_path=tmp_path / "q.sock", log_dir=tmp_path / "logs")
    usage = {"rss_kb": None, "cpu_user": None, "cpu_sys": None}
    task = asyncio.create_task(d._sample_usage(1, usage))
    await _until(lambda: usage["cpu_user"] == 2.0)
    await d._stop_usage_sampling(task)
    assert usage == {"rss_kb": 2048, "cpu_user": 2.0, "cpu_sys": 1.0}


# ---------------------------------------------------------------------------
# 5.todo M5: resource accounting covers the job's whole process group
# ---------------------------------------------------------------------------

def _fake_proc(root: Path, procs: dict[int, tuple[int, int, int, int, int, int | None]]) -> str:
    """A fake /proc under `root`: pid -> (pgrp, utime, stime, cutime, cstime, VmRSS kB or None)."""
    root.mkdir(parents=True, exist_ok=True)
    for pid, (pgrp, ut, st, cut, cst, rss) in procs.items():
        d = root / str(pid)
        d.mkdir()
        (d / "stat").write_text(f"{pid} (a b) S 1 {pgrp} {pgrp} 0 -1 0 0 0 0 0 {ut} {st} {cut} {cst} 20 0 1 0 1 1 1 1\n")
        status = "Name:\ta\n" + ("" if rss is None else f"VmRSS:\t{rss} kB\n")
        (d / "status").write_text(status)
    (root / "self").mkdir()  # not a pid: must be ignored
    (root / "cpuinfo").write_text("x")
    return str(root)


def test_read_group_usage_sums_only_the_members_of_the_group(tmp_path, monkeypatch):
    from queuer import daemon as daemon_mod

    monkeypatch.setattr(daemon_mod, "_CLK_TCK", 100)
    root = _fake_proc(tmp_path / "proc", {
        100: (100, 100, 10, 50, 5, 1000),  # the leader: it has already reaped children (cutime/cstime)
        101: (100, 20, 2, 0, 0, 2000),  # a rank
        102: (100, 5, 1, 0, 0, None),  # a zombie rank: CPU times but no VmRSS line
        200: (200, 9999, 9999, 0, 0, 99999),  # another group: never counted
    })
    rss, user, sys_ = daemon_mod._read_group_usage(100, root)
    assert rss == 3000
    assert user == pytest.approx((100 + 50 + 20 + 5) / 100)
    assert sys_ == pytest.approx((10 + 5 + 2 + 1) / 100)


def test_read_group_usage_without_members_or_proc_is_none(tmp_path):
    from queuer import daemon as daemon_mod

    root = _fake_proc(tmp_path / "proc", {100: (100, 1, 1, 0, 0, 10)})
    assert daemon_mod._read_group_usage(999, root) is None
    assert daemon_mod._read_group_usage(100, str(tmp_path / "missing")) is None


def test_read_group_usage_with_only_zombies_has_cpu_but_no_rss(tmp_path, monkeypatch):
    from queuer import daemon as daemon_mod

    monkeypatch.setattr(daemon_mod, "_CLK_TCK", 100)
    root = _fake_proc(tmp_path / "proc", {7: (7, 300, 0, 0, 0, None)})
    assert daemon_mod._read_group_usage(7, root) == (None, 3.0, 0.0)


def test_read_group_usage_sees_children_that_the_leader_alone_does_not():
    """A real `sh` that starts a 40 MB python child in its own group: the group sum has the
    child, the launcher's own VmHWM does not (this is `mpirun`/`bash -c` in miniature)."""
    from queuer.daemon import _read_group_usage, _read_proc_usage

    script = '"$0" -c "import time; x = bytearray(40 * 1024 * 1024); time.sleep(3)" & wait'
    proc = subprocess.Popen(["sh", "-c", script, sys.executable], start_new_session=True)
    try:
        group = None
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            group = _read_group_usage(proc.pid)
            if group is not None and group[0] is not None and group[0] >= 40 * 1024:
                break
            time.sleep(0.05)
        assert group is not None and group[0] is not None and group[0] >= 40 * 1024
        leader = _read_proc_usage(proc.pid)
        assert leader is not None and (leader[0] is None or leader[0] < 40 * 1024)
    finally:
        os.killpg(proc.pid, signal.SIGKILL)
        proc.wait()


@pytest.mark.asyncio
async def test_sample_usage_keeps_the_peak_group_sum_and_the_largest_cpu(daemon, monkeypatch):
    """The peak is the largest SUM seen; a rank that exits (smaller sum, smaller CPU) lowers nothing."""
    groups = iter([(5000, 3.0, 1.0), (3000, 2.0, 0.5), None])
    monkeypatch.setattr("queuer.daemon._read_proc_usage", lambda pid: (100, 1.0, 0.5))
    monkeypatch.setattr("queuer.daemon._read_group_usage", lambda pgid: next(groups, None))
    monkeypatch.setattr("queuer.daemon.RESOURCE_SAMPLE_SECS", 0.01)
    monkeypatch.setattr("queuer.daemon.RESOURCE_GROUP_SCAN_SECS", 0.0)
    usage: dict = {"rss_kb": None, "cpu_user": None, "cpu_sys": None}
    task = asyncio.create_task(daemon._sample_usage(12345, usage, whole_group=True))
    await asyncio.sleep(0.15)
    await daemon._stop_usage_sampling(task)
    assert usage == {"rss_kb": 5000, "cpu_user": 3.0, "cpu_sys": 1.0}


@pytest.mark.asyncio
async def test_sample_usage_records_the_group_when_the_leader_is_gone(daemon, monkeypatch):
    """The leader exited but a child that ignores SIGTERM is still in the group (see M1)."""
    monkeypatch.setattr("queuer.daemon._read_proc_usage", lambda pid: None)
    monkeypatch.setattr("queuer.daemon._read_group_usage", lambda pgid: (4096, 2.0, 0.25))
    monkeypatch.setattr("queuer.daemon.RESOURCE_SAMPLE_SECS", 0.01)
    usage: dict = {"rss_kb": None, "cpu_user": None, "cpu_sys": None}
    task = asyncio.create_task(daemon._sample_usage(12345, usage, whole_group=True))
    await asyncio.sleep(0.05)
    await daemon._stop_usage_sampling(task)
    assert usage == {"rss_kb": 4096, "cpu_user": 2.0, "cpu_sys": 0.25}


@pytest.mark.asyncio
async def test_sample_usage_scans_the_group_only_when_asked_and_not_every_poll(daemon, monkeypatch):
    scans: list[int] = []
    monkeypatch.setattr("queuer.daemon._read_proc_usage", lambda pid: (100, 1.0, 0.5))
    monkeypatch.setattr("queuer.daemon._read_group_usage", lambda pgid: scans.append(pgid) or (200, 1.0, 0.5))
    monkeypatch.setattr("queuer.daemon.RESOURCE_SAMPLE_SECS", 0.01)

    usage: dict = {"rss_kb": None, "cpu_user": None, "cpu_sys": None}
    task = asyncio.create_task(daemon._sample_usage(12345, usage))  # default: leader only
    await asyncio.sleep(0.1)
    await daemon._stop_usage_sampling(task)
    assert scans == [] and usage["rss_kb"] == 100

    monkeypatch.setattr("queuer.daemon.RESOURCE_GROUP_SCAN_SECS", 60.0)
    usage = {"rss_kb": None, "cpu_user": None, "cpu_sys": None}
    task = asyncio.create_task(daemon._sample_usage(12345, usage, whole_group=True))
    await asyncio.sleep(0.15)  # about 15 polls, one scan (the first)
    await daemon._stop_usage_sampling(task)
    assert scans == [12345] and usage["rss_kb"] == 200


@pytest.mark.asyncio
async def test_a_jobs_peak_rss_adds_up_its_subprocesses(daemon, monkeypatch):
    """`sh` starts two 30 MB python children (an `mpirun -np 2` in miniature): the recorded
    peak is about their SUM, which the launcher alone (a few MB) could never show."""
    monkeypatch.setattr("queuer.daemon.RESOURCE_SAMPLE_SECS", 0.02)
    monkeypatch.setattr("queuer.daemon.RESOURCE_GROUP_SCAN_SECS", 0.02)
    child = "import time; x = bytearray(30 * 1024 * 1024); time.sleep(1.5)"
    script = f'"$0" -c "{child}" & "$0" -c "{child}" & wait'
    job_id = await _add(daemon, ["sh", "-c", script, sys.executable])
    job = await wait_for_status(daemon.socket_path, job_id, {"done", "failed"}, timeout=15.0)
    assert job["status"] == "done"
    assert job["max_rss_kb"] is not None and job["max_rss_kb"] >= 55 * 1024
    assert job["cpu_user_secs"] is not None and job["cpu_sys_secs"] is not None


# ---------------------------------------------------------------------------
# 5.todo M4: a fuller job environment and QUEUER_JOB_UMASK
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_job_gets_user_logname_shell_and_tmpdir_from_the_daemons_environment(daemon, monkeypatch):
    monkeypatch.setenv("USER", "alice")
    monkeypatch.setenv("LOGNAME", "alice-login")
    monkeypatch.setenv("SHELL", "/bin/zsh")
    monkeypatch.setenv("TMPDIR", "/var/tmp/alice")
    monkeypatch.setenv("DAEMON_ONLY_SECRET", "must-not-reach-jobs")
    _, seen = await _run_env_job(daemon)
    assert seen["USER"] == "alice" and seen["LOGNAME"] == "alice-login"
    assert seen["SHELL"] == "/bin/zsh" and seen["TMPDIR"] == "/var/tmp/alice"
    assert "PATH" in seen and "HOME" in seen
    assert "DAEMON_ONLY_SECRET" not in seen  # still not the whole environment


@pytest.mark.asyncio
async def test_a_name_the_daemon_does_not_have_is_not_invented_and_env_overrides(daemon, monkeypatch):
    monkeypatch.delenv("TMPDIR", raising=False)
    monkeypatch.setenv("USER", "alice")
    _, seen = await _run_env_job(daemon, env={"USER": "bob"})
    assert "TMPDIR" not in seen
    assert seen["USER"] == "bob"  # --env wins over the daemon's own value


def test_hooks_get_the_same_base_environment(monkeypatch):
    monkeypatch.setenv("USER", "alice")
    monkeypatch.setenv("SHELL", "/bin/zsh")
    job = {
        "id": 3, "status": "done", "exit_code": 0, "channel": 1, "cwd": "/tmp", "log_path": "/tmp/3.log",
        "raw_cmd": json.dumps(["true"]), "env_extra": None, "started_at": None, "finished_at": None,
    }
    env = Daemon._hook_env("on-finish", job)
    assert env["USER"] == "alice" and env["SHELL"] == "/bin/zsh" and env["QUR_JOB_ID"] == "3"


def test_job_umask_from_the_environment(monkeypatch, capsys):
    from queuer import daemon as daemon_mod

    monkeypatch.delenv("QUEUER_JOB_UMASK", raising=False)
    assert daemon_mod._env_job_umask() is None
    for raw, want in (("022", 0o22), ("0o027", 0o27), (" 002 ", 0o2), ("0", 0), ("777", 0o777)):
        monkeypatch.setenv("QUEUER_JOB_UMASK", raw)
        assert daemon_mod._env_job_umask() == want, raw
    monkeypatch.setenv("QUEUER_JOB_UMASK", "")
    assert daemon_mod._env_job_umask() is None and capsys.readouterr().err == ""  # empty = unset, quietly
    for bad in ("9", "abc", "1000", "-1", "0.2"):
        monkeypatch.setenv("QUEUER_JOB_UMASK", bad)
        assert daemon_mod._env_job_umask() is None, bad
        assert "ignoring QUEUER_JOB_UMASK" in capsys.readouterr().err


def test_daemon_takes_the_job_umask_from_the_argument_or_the_environment(tmp_path, monkeypatch):
    paths = {"db_path": tmp_path / "q.db", "socket_path": tmp_path / "q.sock", "log_dir": tmp_path / "logs"}
    monkeypatch.setenv("QUEUER_JOB_UMASK", "027")
    assert Daemon(**paths).job_umask == 0o27
    assert Daemon(**paths, job_umask=0o22).job_umask == 0o22  # the argument wins
    monkeypatch.delenv("QUEUER_JOB_UMASK")
    assert Daemon(**paths).job_umask is None


_PRINT_UMASK = [sys.executable, "-c", "import os; print(oct(os.umask(0)))"]


@pytest.mark.asyncio
async def test_a_job_keeps_the_daemons_umask_by_default(daemon):
    current = os.umask(0)
    os.umask(current)
    daemon.job_umask = None
    job_id = await _add(daemon, _PRINT_UMASK)
    job = await wait_for_status(daemon.socket_path, job_id, {"done", "failed"})
    assert job["status"] == "done"
    assert Path(job["log_path"]).read_text().strip() == oct(current)


@pytest.mark.asyncio
async def test_job_umask_applies_to_the_job(daemon):
    daemon.job_umask = 0o027
    job_id = await _add(daemon, _PRINT_UMASK)
    job = await wait_for_status(daemon.socket_path, job_id, {"done", "failed"})
    assert job["status"] == "done"
    assert Path(job["log_path"]).read_text().strip() == "0o27"


@pytest.mark.asyncio
async def test_job_umask_applies_to_hooks_too(daemon, tmp_path):
    daemon.job_umask = 0o027
    out = tmp_path / "hook-umask.txt"
    await _add(daemon, ["true"], on_finish=f"umask > {out}")
    await _until(lambda: out.exists() and out.read_text().strip() != "")
    assert out.read_text().strip() == "0027"



# ---------------------------------------------------------------------------
# 5.todo L3, L4, L5, L7, L9
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rm_and_cancel_refuse_a_job_that_is_starting_right_now(daemon):
    """L3: a job whose process is being created still says `queued`; removing or cancelling it
    then would leave an untracked process behind."""
    a = await _add(daemon, ["sleep", "30"])
    await wait_for_status(daemon.socket_path, a, {"running"})
    b = await _add(daemon, ["echo", "b"])
    daemon._starting.add(b)
    try:
        for cmd in ("rm", "cancel"):
            r = await send(daemon.socket_path, cmd, id=b)
            assert r["ok"] is False and "starting right now" in r["error"]
        assert (await send(daemon.socket_path, "show", id=b))["data"]["status"] == "queued"
    finally:
        daemon._starting.discard(b)
    assert (await send(daemon.socket_path, "rm", id=b))["ok"]  # the window is over: it works again
    assert (await send(daemon.socket_path, "cancel", id=a))["ok"]


@pytest.mark.asyncio
async def test_a_row_that_vanishes_during_start_up_does_not_leave_a_process_behind(daemon, monkeypatch):
    """L3 belt and braces: mark_running fails after the process exists -> its group is killed."""
    killed: list[int] = []
    real_kill = daemon._kill_group_for_shutdown

    async def spy(proc, *a, **k):
        killed.append(proc.pid)
        await real_kill(proc, *a, **k)

    def boom(*a, **k):
        raise ValueError("job is not queued")

    monkeypatch.setattr(daemon, "_kill_group_for_shutdown", spy)
    monkeypatch.setattr(db_module, "mark_running", boom)
    await _add(daemon, ["sleep", "30"])
    for _ in range(100):
        if killed:
            break
        await asyncio.sleep(0.05)
    assert killed, "the untracked process was not killed"
    with pytest.raises(ProcessLookupError):
        os.killpg(killed[0], 0)


@pytest.mark.asyncio
async def test_a_job_never_reads_the_daemons_stdin(daemon):
    """L4: stdin is /dev/null, not whatever the daemon was started with."""
    job = await _add(daemon, ["sh", "-c", "readlink /proc/self/fd/0"])
    await wait_for_status(daemon.socket_path, job, {"done"})
    assert _log_text(daemon, job).strip() == "/dev/null"


@pytest.mark.asyncio
async def test_an_old_log_with_the_same_id_is_kept_not_truncated(daemon):
    """L5: after a database reset job ids start at 1 again; the old 1.log must survive."""
    daemon.log_dir.mkdir(parents=True, exist_ok=True)
    (daemon.log_dir / "1.log").write_text("output of the old job 1\n")
    job = await _add(daemon, ["echo", "new"])
    assert job == 1
    await wait_for_status(daemon.socket_path, job, {"done"})
    assert _log_text(daemon, job).strip() == "new"
    assert (daemon.log_dir / "1.log.old").read_text() == "output of the old job 1\n"


def test_keep_old_log_ignores_missing_empty_and_symlinked_logs(tmp_path):
    missing = tmp_path / "1.log"
    Daemon._keep_old_log(missing)
    assert not missing.exists() and not (tmp_path / "1.log.old").exists()
    empty = tmp_path / "2.log"
    empty.write_text("")
    Daemon._keep_old_log(empty)
    assert empty.exists() and not (tmp_path / "2.log.old").exists()
    target = tmp_path / "target"
    target.write_text("x")
    link = tmp_path / "3.log"
    link.symlink_to(target)
    Daemon._keep_old_log(link)
    assert link.is_symlink() and not (tmp_path / "3.log.old").exists()  # the O_NOFOLLOW open refuses it later


def test_keep_old_log_replaces_an_older_old_file(tmp_path):
    log = tmp_path / "4.log"
    (tmp_path / "4.log.old").write_text("older")
    log.write_text("old")
    Daemon._keep_old_log(log)
    assert not log.exists() and (tmp_path / "4.log.old").read_text() == "old"


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", [
    {"channel": 10**6}, {"channel": 2**70}, {"channel": True},
    {"max_retries": True}, {"max_retries": 10**6}, {"max_retries": 2**70}, {"max_retries": "3"},
    {"timeout_secs": 10**12}, {"timeout_secs": 2**70},
    {"array": [0, 10**10]}, {"array": [2**70, 2**70]},
    {"jobs": [{"argv": ["echo", "x"], "index": 10**10}]},
])
async def test_add_refuses_numbers_beyond_the_upper_bounds(daemon, bad):
    """L7: no worker task / registry row for a silly channel, no OverflowError, no `True` as 1."""
    args = {"argv": ["echo", "x"], "cwd": CWD, **bad}
    resp = await send(daemon.socket_path, "add", **args)
    assert resp["ok"] is False and "internal" not in resp["error"]
    status = (await send(daemon.socket_path, "status"))["data"]
    assert set(status["channels"]) <= {"1"}  # nothing was created for the refused request


@pytest.mark.asyncio
async def test_add_accepts_the_upper_bounds_themselves(daemon):
    resp = await send(daemon.socket_path, "add", argv=["echo", "x"], cwd=CWD,
                      channel=db_module.MAX_CHANNEL, max_retries=db_module.MAX_RETRIES,
                      timeout_secs=db_module.MAX_TIMEOUT_SECS)
    assert resp["ok"], resp


@pytest.mark.asyncio
async def test_add_without_cwd_or_argv_says_which_is_missing(daemon):
    r = await send(daemon.socket_path, "add", argv=["echo", "x"])
    assert r["ok"] is False and r["error"] == "cwd is required"
    r = await send(daemon.socket_path, "add", cwd=CWD)
    assert r["ok"] is False and r["error"] == "argv is required"


@pytest.mark.asyncio
async def test_edit_refuses_numbers_beyond_the_upper_bounds(daemon):
    blocker = await _add(daemon, ["sleep", "30"])
    await wait_for_status(daemon.socket_path, blocker, {"running"})
    b = await _add(daemon, ["echo", "b"])
    for bad in ({"channel": 10**6}, {"max_retries": 10**6}, {"timeout_secs": 10**12}):
        r = await send(daemon.socket_path, "edit", id=b, **bad)
        assert r["ok"] is False and "internal" not in r["error"], bad
    assert (await send(daemon.socket_path, "cancel", id=blocker))["ok"]


@pytest.mark.asyncio
async def test_requests_naming_a_huge_channel_are_refused_cleanly(daemon):
    for cmd in ("pause", "resume", "cancel"):
        r = await send(daemon.socket_path, cmd, channel=10**6)
        assert r["ok"] is False and "internal" not in r["error"], cmd


@pytest.mark.asyncio
async def test_signal_handlers_stay_until_the_shutdown_has_run(daemon):
    """L9: a second SIGTERM / Ctrl-C while the jobs are being stopped must not use the default action."""
    seen: dict[str, bool] = {}
    real = daemon._shutdown

    async def spy(server):
        handlers = asyncio.get_running_loop()._signal_handlers  # type: ignore[attr-defined]
        seen["term"] = signal.SIGTERM in handlers
        seen["int"] = signal.SIGINT in handlers
        await real(server)

    daemon._shutdown = spy  # type: ignore[method-assign]
    daemon._stopping.set()
    for _ in range(200):
        if seen:
            break
        await asyncio.sleep(0.05)
    assert seen == {"term": True, "int": True}
