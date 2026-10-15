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

    j1 = await wait_for_status(daemon.socket_path, first, {"failed"})
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
    a = (await send(daemon.socket_path, "add", argv=["true"], cwd=CWD))["data"]["id"]
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
        assert f"failed · job {job_id}" in msgs[0]["text"]
        assert "interrupted by daemon restart" in msgs[0]["text"]
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
        "id": f"cb{update_id}", "data": f"{action}:{qur_id}:{job_id}",
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
    w.write(b'{"cmd": "add", "args": [1]}\n'); await w.drain()
    assert json.loads(await r.readline())["ok"] is False
    w.close()
    r, w = await asyncio.open_unix_connection(str(daemon.socket_path))
    w.write(b'{"cmd": "_x"}\n'); await w.drain()
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
