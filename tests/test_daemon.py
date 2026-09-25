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
import time
from pathlib import Path

import pytest
import pytest_asyncio

from queuer import db as db_module
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

    for proc in list(d._current_proc.values()):
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass

    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


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
    r1 = await send(daemon.socket_path, "add", argv=["sleep", "1"], cwd=CWD)
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
async def test_startup_recovery_marks_stale_running_job_failed(tmp_path):
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
        assert resp["data"]["status"] == "failed"
        assert resp["data"]["note"] == "interrupted by daemon restart"
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
        assert resp1["data"]["status"] == "failed"
        assert resp1["data"]["note"] == "interrupted by daemon restart"
        assert resp2["data"]["status"] == "failed"
        assert resp2["data"]["note"] == "interrupted by daemon restart"
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
