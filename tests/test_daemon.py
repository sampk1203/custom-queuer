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

    if d._current_proc is not None:
        try:
            os.killpg(d._current_proc.pid, signal.SIGKILL)
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
# list / status / show
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_list_shows_running_and_queued(daemon):
    r1 = await send(daemon.socket_path, "add", argv=["sleep", "1"], cwd=CWD)
    r2 = await send(daemon.socket_path, "add", argv=["echo", "second"], cwd=CWD)
    await asyncio.sleep(0.2)  # let the worker pick up job 1

    listing = await send(daemon.socket_path, "list")
    assert listing["ok"]
    assert listing["data"]["running"]["id"] == r1["data"]["id"]
    queued_ids = [j["id"] for j in listing["data"]["queue"]]
    assert r2["data"]["id"] in queued_ids


@pytest.mark.asyncio
async def test_status_idle_when_nothing_queued(daemon):
    resp = await send(daemon.socket_path, "status")
    assert resp["ok"]
    assert resp["data"]["running"] is None


@pytest.mark.asyncio
async def test_show_unknown_job_errors(daemon):
    resp = await send(daemon.socket_path, "show", id=99999)
    assert resp["ok"] is False


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
    ids = [j["id"] for j in listing["data"]["queue"]]
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
    await send(daemon.socket_path, "pause")
    status1 = await send(daemon.socket_path, "list")
    assert status1["data"]["paused"] is True

    await send(daemon.socket_path, "resume")
    status2 = await send(daemon.socket_path, "list")
    assert status2["data"]["paused"] is False


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
    ids = [j["id"] for j in listing["data"]["queue"]]
    assert ids == [b["data"]["id"], a["data"]["id"], c["data"]["id"]]

    await send(daemon.socket_path, "cancel")  # cleanup the blocker


@pytest.mark.asyncio
async def test_before_nonqueued_id_errors(daemon):
    resp = await send(daemon.socket_path, "add", argv=["echo", "x"], cwd=CWD, before=99999)
    assert resp["ok"] is False


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
