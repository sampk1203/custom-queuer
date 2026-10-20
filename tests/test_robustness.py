"""Edge cases added in the P6 robustness pass: things the earlier phases fixed or documented but never pinned
down with a test of their own (idempotent update, upload lock, hosts.json permissions, worker text on a terminal,
the stall watchdog under odd conditions)."""

from __future__ import annotations

import fcntl
import io
import os
import threading
import time

import pytest
from tests.test_remote_worker import (  # noqa: F401 - fixtures
    conn,
    jobs,
    make_job,
    received,
    tar_bytes,
)
from typer.testing import CliRunner

from queuer import hostcfg, notify, remote, selfcheck
from queuer import remote_worker as rw
from queuer.cli import app
from queuer.remote_worker import RemoteError

runner = CliRunner()


# --- remote_worker -----------------------------------------------------------------------------------------


def test_update_with_the_same_payload_twice_leaves_no_second_backup(jobs, conn):  # noqa: F811
    folder = received(jobs)["path"]
    make_job(conn, folder)
    payload = [("in.txt", "changed"), ("new.txt", "x")]
    first = rw.update(1, tar_bytes(payload))
    assert sorted(first["added"]) == ["new.txt"] and first["replaced"] == ["in.txt"]
    backups = sorted(os.listdir(os.path.join(folder, ".qur-prev")))
    second = rw.update(1, tar_bytes(payload))  # what `qur rerun --resume` sends again
    assert second["replaced"] == [] and second["backup"] is None
    assert sorted(second["unchanged"]) == ["in.txt", "new.txt"]
    assert sorted(os.listdir(os.path.join(folder, ".qur-prev"))) == backups


def test_a_second_upload_of_the_same_run_key_is_refused_while_the_first_runs(jobs):  # noqa: F811
    lock_dir = rw._default_db_path().parent / "locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    fd = os.open(lock_dir / "recv-KEY1.lock", os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(fd, fcntl.LOCK_EX)  # the "first upload", still running
    try:
        with pytest.raises(RemoteError, match="still in progress"):
            rw.receive("run", tar_bytes([("a", "1")]), run_key="KEY1")
        assert list(jobs.iterdir()) == []  # and it did not touch the folders
        assert rw.receive("run", tar_bytes([("a", "1")]), run_key="OTHER")["files"] == 1  # other keys are not blocked
    finally:
        os.close(fd)
    assert rw.receive("run", tar_bytes([("a", "1")]), run_key="KEY1")["files"] == 1  # lock gone with its holder


def test_two_parallel_uploads_of_one_run_key_end_with_one_folder(jobs):  # noqa: F811
    results: list[object] = []

    def slow_stream():
        data = tar_bytes([("a", "x" * 1000)]).getvalue()

        class Slow(io.BytesIO):
            def read(self, n=-1):
                time.sleep(0.3)
                return super().read(n)

        return Slow(data)

    def go():
        try:
            results.append(rw.receive("run", slow_stream(), run_key="PAR"))
        except RemoteError as e:
            results.append(e)

    threads = [threading.Thread(target=go) for _ in range(2)]
    for t in threads:
        t.start()
        time.sleep(0.1)
    for t in threads:
        t.join()
    assert sum(isinstance(r, dict) for r in results) >= 1
    assert len([p for p in jobs.iterdir() if p.is_dir()]) == 1


def test_receive_rejects_a_run_key_with_whitespace(jobs):  # noqa: F811
    with pytest.raises(ValueError):
        rw.receive("run", tar_bytes([("a", "1")]), run_key="bad key")


# --- hosts.json --------------------------------------------------------------------------------------------


def _write_host(path, mode):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('{"version": 1, "hosts": {"vm3": {"ssh": "vm3"}}}')
    path.chmod(mode)


@pytest.mark.parametrize("mode", [0o666, 0o662, 0o620])
def test_a_hosts_file_others_can_write_is_not_used(tmp_path, monkeypatch, mode):
    path = tmp_path / "iso-hosts" / "hosts.json"
    _write_host(path, mode)
    assert hostcfg.load()["hosts"] == {}  # refused, like a missing file (its ssh_options and setup are executed)
    assert "vm3" in hostcfg.load(check=False)["hosts"]  # `qur hosts list/edit` can still repair it


def test_a_hosts_file_with_mode_600_is_used(tmp_path):
    path = tmp_path / "iso-hosts" / "hosts.json"
    _write_host(path, 0o600)
    assert "vm3" in hostcfg.load()["hosts"]


def test_a_symlinked_hosts_file_is_not_used(tmp_path):
    real = tmp_path / "elsewhere.json"
    _write_host(real, 0o600)
    link = tmp_path / "iso-hosts" / "hosts.json"
    link.parent.mkdir(parents=True, exist_ok=True)
    link.symlink_to(real)
    assert hostcfg.load()["hosts"] == {}


def test_doctor_reports_a_loose_hosts_file_and_does_not_ask_any_worker(tmp_path, monkeypatch):
    from queuer import remote_cli

    _write_host(tmp_path / "iso-hosts" / "hosts.json", 0o666)
    monkeypatch.setattr(remote_cli, "check_host", lambda *a, **k: pytest.fail("no worker may be contacted"))
    checks = selfcheck.check_hosts()
    assert [(c.name, c.status) for c in checks] == [("hosts.json permissions", selfcheck.FAIL)]


# --- the worker id must not be this machine's own qur id ------------------------------------------------------


def test_hosts_add_refuses_the_local_qur_id(monkeypatch):
    monkeypatch.setattr(notify, "load", lambda *a, **k: {"qur_id": "laptop"})
    r = runner.invoke(app, ["hosts", "add", "laptop", "--ssh", "vm3"])
    assert r.exit_code != 0 and "own qur id" in r.output
    assert hostcfg.load()["hosts"] == {}


# --- worker text on a terminal -----------------------------------------------------------------------------


def test_safe_text_strips_escapes_and_control_characters():
    nasty = "ok\x1b]8;;https://evil.example\x07click\x1b]8;;\x07 \x1b[31mred\x1b[0m\x00\x08 end‮"
    out = remote.safe_text(nasty)
    assert "\x1b" not in out and "\x07" not in out and "\x00" not in out and "\x08" not in out
    assert "click" in out and "end" in out
    assert remote.safe_text("error: x\ntraceback\there") == "error: x traceback here"  # lines are not glued together
    assert remote.safe_text("a\r\nb") == "a b"
    assert remote.safe_text("a\nb\x1b[2J", multiline=True) == "a\nb[2J"  # the ESC goes, the harmless rest stays
    assert remote.safe_text(42) == "42"


# --- the stall watchdog under odd conditions --------------------------------------------------------------


def test_stall_watchdog_keeps_the_exit_code_and_stderr_of_a_normal_run():
    code, out, err = remote._run_capped(["sh", "-c", "echo out; echo err >&2; exit 3"], stall_secs=2)
    assert (code, out, err) == (3, b"out\n", b"err\n")


def test_stall_watchdog_kills_the_process_it_gave_up_on():
    start = time.monotonic()
    with pytest.raises(remote.StalledTransfer):
        remote._run_capped(["sh", "-c", "sleep 60"], stall_secs=1)
    assert time.monotonic() - start < 10  # killed, not waited for


def test_a_sub_second_stall_limit_still_works():
    with pytest.raises(remote.StalledTransfer):
        remote._run_capped(["sh", "-c", "sleep 30"], stall_secs=0.3)


def test_stall_watchdog_still_enforces_the_output_cap():
    with pytest.raises(remote.RemoteError, match="more than"):
        remote._run_capped(["sh", "-c", "head -c 3000000 /dev/zero"], max_out=1024 * 1024, stall_secs=5)


def test_download_with_steady_progress_is_not_stalled(tmp_path, monkeypatch):
    monkeypatch.setattr(remote, "ssh_argv", lambda host, argv, **kw: ["sh", "-c", "for i in 1 2 3 4; do head -c 1000 /dev/zero; sleep 0.5; done"])
    dest = tmp_path / "d" / "x.tar.gz"
    assert remote.ssh_recv_to_file({"ssh": "vm3"}, ["x"], dest, stall_secs=1) == 4000
    assert dest.stat().st_size == 4000
