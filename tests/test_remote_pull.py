"""Phase 6: pull a job folder as a tarball (transfer, validation, extract, auto-pull). No network."""

import asyncio
import gzip
import io
import json
import tarfile
from pathlib import Path
from types import SimpleNamespace

import pytest
import typer
from typer.testing import CliRunner

from queuer import db, hostcfg, remote, remote_pull
from queuer.remote_watch import PULL_ATTEMPTS, RemoteWatcher


def make_tar(path: Path, files: dict[str, bytes]) -> None:
    with tarfile.open(path, "w:gz") as tf:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))


@pytest.fixture
def host(tmp_path, monkeypatch):
    monkeypatch.setenv("QUEUER_HOSTS_PATH", str(tmp_path / "hosts.json"))
    cfg = hostcfg.load()
    cfg["hosts"]["vm3"] = {"ssh": "vm3", "id": "vm3", "pull_dir": str(tmp_path / "results"), "pull_exclude": ["*.dump"]}
    hostcfg.save(cfg)
    return hostcfg.get("vm3")


@pytest.fixture
def fake_pack(monkeypatch, tmp_path):
    """Replace the ssh stream with a local tarball; records the argv it was asked for."""
    calls = []
    payload = tmp_path / "payload.tar.gz"
    make_tar(payload, {"in.lammps": b"run 10\n", ".qur-prev/20261020/in.old": b"old"})

    def recv(h, argv, dest, *, validate=None, **kw):
        calls.append(list(argv))
        part = dest.with_name(dest.name + ".part")
        dest.parent.mkdir(parents=True, exist_ok=True)
        part.write_bytes(payload.read_bytes())
        try:
            if validate:
                validate(part)
        except Exception:
            part.unlink()
            raise
        part.replace(dest)
        return dest.stat().st_size

    monkeypatch.setattr(remote, "ssh_recv_to_file", recv)
    return SimpleNamespace(calls=calls, payload=payload)


def test_validate_accepts_good_rejects_truncated_and_garbage(tmp_path):
    good = tmp_path / "g.tar.gz"
    make_tar(good, {"a": b"x" * 5000})
    remote_pull.validate_tarball(good)
    bad = tmp_path / "t.tar.gz"
    bad.write_bytes(good.read_bytes()[:-20])
    with pytest.raises(remote_pull.PullError):
        remote_pull.validate_tarball(bad)
    junk = tmp_path / "j.tar.gz"
    junk.write_bytes(b"not a tarball")
    with pytest.raises(remote_pull.PullError):
        remote_pull.validate_tarball(junk)
    notar = tmp_path / "n.tar.gz"
    notar.write_bytes(gzip.compress(b"plain text, not a tar" * 100))
    with pytest.raises(remote_pull.PullError):
        remote_pull.validate_tarball(notar)


def test_recv_to_file_validate_failure_leaves_nothing(tmp_path, monkeypatch):
    payload = tmp_path / "bad.bin"
    payload.write_bytes(b"corrupt")
    monkeypatch.setattr(remote, "ssh_argv", lambda h, argv, **kw: ["bash", "-c", f"cat {payload}"])
    dest = tmp_path / "out" / "x.tar.gz"
    with pytest.raises(remote_pull.PullError):
        remote.ssh_recv_to_file({"ssh": "vm3"}, ["remote", "pack", "1"], dest, validate=remote_pull.validate_tarball)
    assert not dest.exists() and not list(dest.parent.iterdir())


def test_pull_tarball_names_excludes_and_refuses_overwrite(host, fake_pack, tmp_path):
    res = remote_pull.pull_tarball(host, 42, "/home/u/qur-jobs/lmp-20261020-101500-k3f9",
                                   exclude=remote_pull.effective_excludes(host, ["*.big"]), max_mb=2000)
    assert res.path == tmp_path / "results" / "lmp-20261020-101500-k3f9.tar.gz" and res.path.is_file()
    assert fake_pack.calls[0] == ["remote", "pack", "42", "--exclude", "*.dump", "--exclude", "*.big", "--max-mb", "2000"]
    with pytest.raises(remote_pull.PullError, match="already exists"):
        remote_pull.pull_tarball(host, 42, "/x/lmp-20261020-101500-k3f9")
    again = remote_pull.pull_tarball(host, 43, "/x/lmp-20261020-101500-k3f9", unique=True)
    assert again.path.name == "lmp-20261020-101500-k3f9-43.tar.gz"
    assert remote_pull.pull_tarball(host, 42, "/x/lmp-20261020-101500-k3f9", force=True).path == res.path
    assert not list((tmp_path / "results").glob("*.part"))


def test_pull_tarball_extract_keeps_prev_dir(host, fake_pack, tmp_path):
    res = remote_pull.pull_tarball(host, 42, "/w/run1", extract=True)
    assert res.extracted == tmp_path / "results" / "run1"
    assert (res.extracted / "in.lammps").read_text() == "run 10\n"
    assert (res.extracted / ".qur-prev" / "20261020" / "in.old").exists()
    with pytest.raises(remote_pull.PullError, match="already exists"):
        remote_pull.pull_tarball(host, 42, "/w/run1", force=True, extract=True)


def test_pull_tarball_looks_up_folder_and_maps_exit_codes(host, monkeypatch):
    monkeypatch.setattr(remote, "ssh_json", lambda h, argv, **kw: {"id": 7, "job_dir": None})
    with pytest.raises(remote_pull.PullError, match="no job folder"):
        remote_pull.pull_tarball(host, 7)

    def too_big(h, argv, dest, **kw):
        raise remote.RemoteError(3, "too big")

    monkeypatch.setattr(remote, "ssh_recv_to_file", too_big)
    with pytest.raises(remote_pull.PullError, match="--max-mb"):
        remote_pull.pull_tarball(host, 7, "/w/run1")


def test_pull_file_paths(host, monkeypatch, tmp_path):
    seen = []

    def recv(h, argv, dest, **kw):
        seen.append(list(argv))
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"data")
        return 4

    monkeypatch.setattr(remote, "ssh_recv_to_file", recv)
    path, size = remote_pull.pull_file(host, 42, "out/log.lammps")
    assert path == tmp_path / "results" / "log.lammps" and size == 4
    assert seen[0] == ["remote", "cat", "--job", "42", "--max-mb", "5", "--", "out/log.lammps"]
    with pytest.raises(remote_pull.PullError, match="already exists"):
        remote_pull.pull_file(host, 42, "out/log.lammps")
    into = tmp_path / "dir"
    into.mkdir()
    assert remote_pull.pull_file(host, 42, "log.lammps", to=into)[0] == into / "log.lammps"
    for bad in ("../x", "/etc/passwd"):
        with pytest.raises(remote_pull.PullError):
            remote_pull.pull_file(host, 42, bad)


def test_pull_command(host, fake_pack, tmp_path, monkeypatch):
    app = typer.Typer()
    app.command()(remote_pull.pull_cmd)
    runner = CliRunner()
    monkeypatch.setattr(remote_pull, "lookup_job_dir", lambda h, i: "/w/run9")
    r = runner.invoke(app, ["vm3:42", "--exclude", "*.big", "--no-default-excludes"])
    assert r.exit_code == 0, r.output
    assert "--exclude" in fake_pack.calls[-1] and "*.dump" not in fake_pack.calls[-1]
    assert runner.invoke(app, ["nope:1"]).exit_code != 0
    assert runner.invoke(app, ["vm3:1", "--file", "a", "--extract"]).exit_code != 0
    r = runner.invoke(app, ["vm3:42"])  # second pull of the same folder without --force
    assert r.exit_code == 1 and "already exists" in r.output


# ---- auto-pull in the watcher ----


@pytest.fixture
def wenv(tmp_path, host, monkeypatch):
    conn = db.connect(tmp_path / "q.db")
    db.init_db(conn)
    w = RemoteWatcher(SimpleNamespace(conn=conn, notify_path=tmp_path / "n.json"))
    sent = []

    async def send(text, job_id=None, machine=None):
        sent.append(text)
        return True

    w.send = send

    def add(key, remote_id, status="done", pull=1):
        now = db._now()
        conn.execute("INSERT INTO remote_runs (run_key, host, kind, state, remote_id, pull, created_at, updated_at) VALUES (?, 'vm3', 'run', 'queued', ?, ?, ?, ?)",
                     (key, remote_id, pull, now, now))
        conn.execute("INSERT INTO remote_jobs (host, remote_id, status, snapshot, seen_at) VALUES ('vm3', ?, ?, ?, ?)",
                     (remote_id, status, json.dumps({"id": remote_id, "job_dir": f"/w/run{remote_id}"}), now))
        conn.commit()

    return SimpleNamespace(w=w, sent=sent, conn=conn, add=add)


def run_pulls(env):
    rows = env.w._pending_pulls("vm3")
    asyncio.run(env.w._pull_worker("vm3", rows))


def test_auto_pull_success_records_path_and_messages_once(wenv, monkeypatch, tmp_path):
    wenv.add("k1", 5)
    wenv.add("k2", 6, status="cancelled")  # never pulled
    wenv.add("k3", 7, pull=0)  # not asked for
    got = []

    def fake(host, job_id, job_dir, **kw):
        got.append((job_id, job_dir, kw))
        return remote_pull.PullResult(tmp_path / "r" / "run5.tar.gz", 1234, None)

    monkeypatch.setattr(remote_pull, "pull_tarball", fake)
    run_pulls(wenv)
    assert [g[0] for g in got] == [5] and got[0][1] == "/w/run5" and got[0][2]["unique"] is True and got[0][2]["exclude"] == ["*.dump"]
    assert len(wenv.sent) == 1 and "pulled to" in wenv.sent[0] and "vm3" in wenv.sent[0] and "1.2 KB" in wenv.sent[0]
    assert wenv.conn.execute("SELECT pulled_path FROM remote_runs WHERE run_key='k1'").fetchone()[0].endswith("run5.tar.gz")
    assert wenv.w._pending_pulls("vm3") == []


def test_auto_pull_failure_retries_up_to_limit(wenv, monkeypatch):
    wenv.add("k1", 5, status="failed")

    def boom(*a, **kw):
        raise remote_pull.PullError("disk full")

    monkeypatch.setattr(remote_pull, "pull_tarball", boom)
    for _ in range(PULL_ATTEMPTS + 2):
        run_pulls(wenv)
    assert len(wenv.sent) == PULL_ATTEMPTS and "will retry" in wenv.sent[0] and "giving up" in wenv.sent[-1]
    assert wenv.conn.execute("SELECT pull_attempts FROM remote_runs").fetchone()[0] == PULL_ATTEMPTS


def test_extract_stops_at_the_size_limit(tmp_path):
    tar = tmp_path / "big.tar.gz"
    make_tar(tar, {"zeros.bin": b"\0" * (3 * 1024 * 1024)})
    with pytest.raises(remote_pull.PullError, match="too big"):
        remote_pull.extract_tarball(tar, max_mb=1)
    assert not (tmp_path / "big").exists()  # the half-made folder is removed
    assert (remote_pull.extract_tarball(tar, max_mb=10) / "zeros.bin").stat().st_size == 3 * 1024 * 1024


def test_ssh_output_beyond_the_cap_is_refused_and_stderr_is_trimmed():
    with pytest.raises(remote.RemoteError, match="more than"):
        remote._run_capped(["sh", "-c", "head -c 3000000 /dev/zero"], max_out=1024 * 1024)
    code, out, err = remote._run_capped(["sh", "-c", "echo hi; head -c 200000 /dev/zero >&2"])
    assert code == 0 and out == b"hi\n" and len(err) <= remote.MAX_STDERR_BYTES


# --- P6-6: a transfer that moves no byte is stopped ------------------------------------------------------


def test_stalled_output_is_killed_but_a_slow_steady_stream_is_not():
    with pytest.raises(remote.StalledTransfer):
        remote._run_capped(["sh", "-c", "sleep 30"], stall_secs=1)
    code, out, _ = remote._run_capped(["sh", "-c", "for i in 1 2 3; do echo x; sleep 0.6; done"], stall_secs=1)
    assert code == 0 and out == b"x\nx\nx\n"


def test_stall_timeout_does_not_replace_the_overall_timeout():
    import subprocess

    with pytest.raises(subprocess.TimeoutExpired):
        remote._run_capped(["sh", "-c", "while :; do echo x; sleep 0.2; done"], stall_secs=5, timeout=1)


def test_upload_stall_only_counts_while_the_payload_is_still_being_read(tmp_path):
    payload = tmp_path / "p.tar.gz"
    payload.write_bytes(b"\0" * (1024 * 1024))  # more than a pipe holds: a consumer that never reads cannot take it
    with open(payload, "rb") as f, pytest.raises(remote.StalledTransfer):
        remote._run_capped(["sh", "-c", "sleep 30"], stdin=f, stall_secs=1)
    small = tmp_path / "s"
    small.write_bytes(b"hello")
    with open(small, "rb") as f:  # fully read, then the worker "works" for longer than stall_secs: fine
        code, out, _ = remote._run_capped(["sh", "-c", "cat >/dev/null; sleep 2; echo done"], stdin=f, stall_secs=1)
    assert code == 0 and out == b"done\n"


def test_download_and_upload_report_a_hung_worker_as_unreachable_and_leave_no_part_file(tmp_path, monkeypatch):
    monkeypatch.setattr(remote, "ssh_argv", lambda host, argv, **kw: ["sh", "-c", "sleep 30"])
    host = {"ssh": "vm3"}
    dest = tmp_path / "out" / "x.tar.gz"
    with pytest.raises(remote.HostUnreachable, match="stalled"):
        remote.ssh_recv_to_file(host, ["remote", "pack", "1"], dest, stall_secs=1)
    assert not dest.exists() and list(dest.parent.iterdir()) == []
    payload = tmp_path / "p"
    payload.write_bytes(b"\0" * (1024 * 1024))
    with pytest.raises(remote.HostUnreachable, match="stalled"):
        remote.ssh_send_file(host, payload, ["remote", "receive"], stall_secs=1)


def test_ssh_send_tar_is_gone():
    assert not hasattr(remote, "ssh_send_tar") and not hasattr(remote, "SECRET_EXCLUDES")
