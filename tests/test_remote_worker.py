"""Phase 1: worker primitives (`qur remote ...`): receive, pack, prepare, update, cat, gc, the event feed
and run keys. Function level, no ssh and no daemon."""

import io
import os
import sqlite3
import tarfile
import time

import pytest

from queuer import db
from queuer import remote_worker as rw
from queuer.remote_worker import RemoteError


@pytest.fixture
def jobs(tmp_path, monkeypatch):
    root = tmp_path / "jobs"
    root.mkdir()
    monkeypatch.setenv("QUEUER_JOBS_DIR", str(root))
    return root


@pytest.fixture
def conn():
    c = rw._open_db()
    yield c
    c.close()


def tar_bytes(members):
    """members: list of (name, data | ('link', target) | ('hard', target) | ('dev',))."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for name, spec in members:
            info = tarfile.TarInfo(name)
            if isinstance(spec, tuple) and spec[0] in ("link", "hard"):
                info.type = tarfile.SYMTYPE if spec[0] == "link" else tarfile.LNKTYPE
                info.linkname = spec[1]
                tf.addfile(info)
            elif isinstance(spec, tuple):
                info.type = tarfile.CHRTYPE
                tf.addfile(info)
            else:
                data = spec.encode() if isinstance(spec, str) else spec
                info.size = len(data)
                tf.addfile(info, io.BytesIO(data))
    return io.BytesIO(buf.getvalue())


def make_job(conn, folder, status="done", cmd=("true",)):
    jid = db.enqueue(conn, raw_cmd=list(cmd), resolved_cmd=list(cmd), cwd=str(folder), log_path="/dev/null", job_dir=str(folder))
    conn.execute("UPDATE jobs SET status = ? WHERE id = ?", (status, jid))
    conn.commit()
    return jid


def received(jobs, files=(("in.txt", "hello"),), **kw):
    return rw.receive("run", tar_bytes(list(files)), **kw)


# --- receive -----------------------------------------------------------------

def test_receive_makes_a_new_unique_folder_with_marker(jobs):
    a, b = received(jobs), received(jobs)
    assert a["path"] != b["path"] and a["files"] == 1 and a["bytes"] == 5
    assert os.path.dirname(a["path"]) == str(jobs) and os.path.basename(a["path"]).startswith("run-")
    assert open(os.path.join(a["path"], "in.txt")).read() == "hello"
    assert os.path.exists(os.path.join(a["path"], rw.RECEIVED_FILE))


@pytest.mark.parametrize("members", [
    [("../x", "evil")],
    [("/abs/x", "evil")],
    [("a/../../x", "evil")],
    [("ln", ("link", "/etc/passwd"))],
    [("ln", ("link", "../../outside"))],
    [("hl", ("hard", "/etc/passwd"))],
    [("dev", ("dev",))],
    [(rw.RECEIVED_FILE, "x")],
    [(rw.PREV_DIR + "/x", "x")],
])
def test_receive_rejects_hostile_archives(jobs, members):
    with pytest.raises(RemoteError) as e:
        rw.receive("run", tar_bytes(members), jobs_dir_override=str(jobs))
    assert e.value.code in (rw.EXIT_BAD_ARGS,)
    assert not (jobs.parent / "x").exists()


def test_receive_never_writes_through_a_symlinked_directory(jobs, tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    members = [("d", ("link", str(outside))), ("d/pwn", "x")]
    with pytest.raises(RemoteError):
        rw.receive("run", tar_bytes(members))
    assert not (outside / "pwn").exists()


def test_receive_size_cap_and_bad_name(jobs):
    with pytest.raises(RemoteError) as e:
        rw.receive("run", tar_bytes([("big", b"x" * 3_000_000)]), max_mb=1)
    assert e.value.code == rw.EXIT_TOO_BIG
    with pytest.raises(RemoteError):
        rw.receive("../evil", tar_bytes([("a", "b")]))


def test_receive_with_run_key_is_idempotent_and_redoes_half_received(jobs):
    first = received(jobs, run_key="k1")
    again = received(jobs, run_key="k1", files=(("other", "zzz"),))
    assert again["path"] == first["path"] and not os.path.exists(os.path.join(first["path"], "other"))
    os.unlink(os.path.join(first["path"], rw.RECEIVED_FILE))  # a transfer that died before the end
    redone = received(jobs, run_key="k1", files=(("other", "zzz"),))
    assert os.path.exists(os.path.join(redone["path"], "other")) and not os.path.exists(os.path.join(redone["path"], "in.txt"))


# --- pack --------------------------------------------------------------------

def test_pack_round_trip_skips_keys_and_does_not_follow_links(jobs, conn, tmp_path):
    r = received(jobs, files=[("out.dat", "data"), ("id_rsa", "SECRET"), (".ssh/config", "x"), ("k.pem", "p"), ("sub/a.key", "k")])
    secret = tmp_path / "secret.txt"
    secret.write_text("TOPSECRET")
    os.symlink(secret, os.path.join(r["path"], "escape"))
    jid = make_job(conn, r["path"])
    out = io.BytesIO()
    rw.pack(jid, out)
    out.seek(0)
    with tarfile.open(fileobj=out, mode="r:gz") as tf:
        names = tf.getnames()
    assert "out.dat" in names and not any(n in names for n in ("id_rsa", ".ssh/config", "k.pem", "sub/a.key"))
    assert "escape" not in names  # a link leaving the folder is left out, not followed
    assert b"TOPSECRET" not in out.getvalue()


def _packed_names(jid, **kw):
    out = io.BytesIO()
    result = rw.pack(jid, out, **kw)
    out.seek(0)
    with tarfile.open(fileobj=out, mode="r:gz") as tf:
        return set(tf.getnames()), result


def test_pack_denylist_matches_key_content_not_just_the_id_prefix(jobs, conn):
    r = received(jobs, files=[
        ("id_map.npy", "data"), ("id_table.csv", "a,b"), ("id_work", "-----BEGIN OPENSSH PRIVATE KEY-----\nx"),
        ("id_ed25519.pub", "ssh-ed25519 AAAA"), ("x.p12", "p"),
    ])
    names, res = _packed_names(make_job(conn, r["path"]))
    assert {"id_map.npy", "id_table.csv"} <= names
    assert not names & {"id_work", "id_ed25519.pub", "x.p12"}
    assert {"id_work", "id_ed25519.pub", "x.p12"} <= set(res["skipped"])


def test_pack_skips_credentials_unless_include_sensitive(jobs, conn):
    r = received(jobs, files=[("a.dat", "1"), (".env", "T=1"), (".netrc", "n"), (".aws/credentials", "c"), (".git/config", "g"), (".ssh/k", "k")])
    jid = make_job(conn, r["path"])
    names, res = _packed_names(jid)
    assert "a.dat" in names and not names & {".env", ".netrc", ".aws/credentials", ".git/config"}
    assert {".env", ".netrc"} <= set(res["skipped"])
    names, _ = _packed_names(jid, include_sensitive=True)
    assert {".env", ".netrc", ".aws/credentials", ".git/config"} <= names
    assert ".ssh/k" not in names  # keys never travel, whatever the switch


def test_pack_max_mb_refuses_before_writing_anything(jobs, conn):
    r = received(jobs, files=[("big", "x" * (2 * 1024 * 1024))])
    out = io.BytesIO()
    with pytest.raises(RemoteError) as e:
        rw.pack(make_job(conn, r["path"]), out, max_mb=1)
    assert e.value.code == rw.EXIT_TOO_BIG and out.getvalue() == b""


def test_pack_refusals(jobs, conn, tmp_path):
    with pytest.raises(RemoteError) as e:
        rw.pack(999, io.BytesIO())
    assert e.value.code == rw.EXIT_NOT_FOUND
    plain = db.enqueue(conn, raw_cmd=["true"], resolved_cmd=["true"], cwd="/", log_path="/dev/null")
    with pytest.raises(RemoteError) as e:
        rw.pack(plain, io.BytesIO())
    assert e.value.code == rw.EXIT_NOT_FOUND
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    with pytest.raises(RemoteError):
        rw.pack(make_job(conn, elsewhere), io.BytesIO())  # folder outside the jobs dir
    r = received(jobs, files=[("big", b"x" * 3_000_000)])
    with pytest.raises(RemoteError) as e:
        rw.pack(make_job(conn, r["path"]), io.BytesIO(), max_mb=1)
    assert e.value.code == rw.EXIT_TOO_BIG


# --- prepare -----------------------------------------------------------------

def test_prepare_require_link_copy(jobs, conn, tmp_path):
    folder = received(jobs)["path"]
    pot = tmp_path / "pot.txt"
    pot.write_text("potential")
    res, code = rw.prepare(folder, (str(pot),), (f"model.pt={pot}",), (f"{pot}=copied.txt",), None)
    assert code == 0 and res["missing"] == []
    assert os.path.islink(os.path.join(folder, "model.pt"))
    assert open(os.path.join(folder, "copied.txt")).read() == "potential"
    res, code = rw.prepare(folder, (str(tmp_path / "nope"), str(pot)), (), (), None)
    assert code == rw.EXIT_MISSING and res["missing"] == [str(tmp_path / "nope")]


def test_prepare_copy_from_another_job_and_no_overwrite(jobs, conn, tmp_path):
    old = received(jobs, files=[("restart.100", "state")])["path"]
    jid = make_job(conn, old)
    new = received(jobs)["path"]
    res, code = rw.prepare(new, (), (), (f"job:{jid}/restart.100=restart.in",), None)
    assert code == 0 and open(os.path.join(new, "restart.in")).read() == "state"
    res, code = rw.prepare(new, (), (), (f"job:{jid}/restart.100=restart.in",), None)  # same copy again: idempotent (P2-1)
    assert code == 0
    open(os.path.join(new, "restart.in"), "w").write("changed")
    with pytest.raises(RemoteError):
        rw.prepare(new, (), (), (f"job:{jid}/restart.100=restart.in",), None)  # never overwrites a different file
    with pytest.raises(RemoteError):
        rw.prepare(new, (), (), (f"job:{jid}/../../etc/passwd=x",), None)
    with pytest.raises(RemoteError):
        rw.prepare(new, (), ("../evil=/etc/passwd",), (), None)


# --- update / cat ------------------------------------------------------------

def test_update_replaces_with_backup_and_never_deletes(jobs, conn):
    folder = received(jobs, files=[("in.lammps", "old"), ("keep.txt", "k")])["path"]
    jid = make_job(conn, folder)
    res = rw.update(jid, tar_bytes([("in.lammps", "new"), ("added.txt", "a")]))
    assert res["replaced"] == ["in.lammps"] and res["added"] == ["added.txt"]
    assert open(os.path.join(folder, "in.lammps")).read() == "new"
    assert open(os.path.join(folder, res["backup"], "in.lammps")).read() == "old"
    assert os.path.exists(os.path.join(folder, "keep.txt"))


def test_update_refused_while_a_job_uses_the_folder_unless_forced(jobs, conn):
    folder = received(jobs)["path"]
    jid = make_job(conn, folder, status="running")
    with pytest.raises(RemoteError) as e:
        rw.update(jid, tar_bytes([("in.txt", "x")]))
    assert e.value.code == rw.EXIT_IN_USE
    assert rw.update(jid, tar_bytes([("in.txt", "x")]), force=True)["replaced"] == ["in.txt"]


@pytest.mark.parametrize("name", ["../x", "/abs", rw.RECEIVED_FILE, rw.PREV_DIR + "/a"])
def test_update_rejects_escapes_and_reserved_names(jobs, conn, name):
    folder = received(jobs)["path"]
    jid = make_job(conn, folder)
    with pytest.raises(RemoteError):
        rw.update(jid, tar_bytes([(name, "x")]))


def test_cat_reads_a_file_and_refuses_escapes_and_big_files(jobs, conn, tmp_path):
    folder = received(jobs, files=[("sub/a.txt", "hi"), ("big", b"x" * 2_000_000)])["path"]
    secret = tmp_path / "s"
    secret.write_text("no")
    os.symlink(secret, os.path.join(folder, "lnk"))
    jid = make_job(conn, folder)
    out = io.BytesIO()
    rw.cat(jid, "sub/a.txt", out)
    assert out.getvalue() == b"hi"
    for bad in ("../x", "/etc/passwd", "nope"):
        with pytest.raises(RemoteError):
            rw.cat(jid, bad, io.BytesIO())
    with pytest.raises(RemoteError) as e:
        rw.cat(jid, "big", io.BytesIO(), max_mb=1)
    assert e.value.code == rw.EXIT_TOO_BIG
    out = io.BytesIO()
    try:
        rw.cat(jid, "lnk", out)
    except RemoteError:
        pass
    assert out.getvalue() != b"no"


# --- gc ----------------------------------------------------------------------

def test_gc_only_touches_marker_folders_without_a_job(jobs, conn):
    orphan = received(jobs, run_key="orphan")["path"]
    used = received(jobs, run_key="used")["path"]
    make_job(conn, used)
    stranger = jobs / "not-ours"
    stranger.mkdir()
    old = time.time() - 30 * 86400
    for p in (orphan, used):
        os.utime(os.path.join(p, rw.RUN_KEY_FILE), (old, old))
    os.unlink(os.path.join(orphan, rw.RECEIVED_FILE))  # never finished receiving
    listed = rw.gc("7d")
    assert [o["path"] for o in listed["orphans"]] == [orphan] and listed["deleted"] == []
    assert os.path.exists(orphan)
    assert rw.gc("7d", delete=True)["deleted"] == [orphan]
    assert not os.path.exists(orphan) and os.path.exists(used) and stranger.exists()
    young = received(jobs, run_key="young")["path"]
    assert young not in [o["path"] for o in rw.gc("7d")["orphans"]]


def test_gc_delete_keeps_finished_unpulled_results(jobs, conn):
    """A finished folder whose job row was pruned looks orphaned but may hold results: listed as completed, kept."""
    done = received(jobs, run_key="done")["path"]
    old = time.time() - 30 * 86400
    os.utime(os.path.join(done, rw.RUN_KEY_FILE), (old, old))
    listed = rw.gc("7d", delete=True)
    assert listed["orphans"] == [] and [c["path"] for c in listed["completed"]] == [done] and listed["deleted"] == []
    assert os.path.exists(done)
    assert rw.gc("7d", delete=True, force_completed=True)["deleted"] == [done]
    assert not os.path.exists(done)


# --- run keys, events, info --------------------------------------------------

def test_run_key_is_unique_and_findable(conn):
    a = db.enqueue(conn, raw_cmd=["a"], resolved_cmd=["a"], cwd="/", log_path="/dev/null", run_key="K")
    assert rw.find_run_key("K")["id"] == a
    assert rw.find_run_key("other") == {}
    with pytest.raises(sqlite3.IntegrityError):
        db.enqueue(conn, raw_cmd=["b"], resolved_cmd=["b"], cwd="/", log_path="/dev/null", run_key="K")


def test_events_pagination_epoch_and_pruning(conn):
    for i in range(1, 6):
        db.add_event(conn, i, "final", {"id": i, "status": "done", "telegram_sent": i % 2 == 0})
    page = db.get_events(conn, after=0, limit=2)
    assert [e["seq"] for e in page["events"]] == [1, 2] and page["last_seq"] == 5 and page["earliest_seq"] == 1
    assert page["events"][1]["telegram_sent"] is True and "telegram_sent" not in page["events"][1]["job"]
    assert [e["seq"] for e in db.get_events(conn, after=4)["events"]] == [5]
    assert db.get_events(conn)["epoch"] == db.get_epoch(conn)
    db.prune_events(conn, keep=2, max_age_days=60)
    after = db.get_events(conn)
    assert after["earliest_seq"] == 4 and after["last_seq"] == 5
    with pytest.raises(ValueError):
        db.add_event(conn, 1, "bogus", {})


def test_info_has_the_appendix_a_fields(jobs):
    info = rw.info()
    for key in ("protocol", "queuer_version", "schema_version", "machine_id", "hostname", "jobs_dir", "epoch", "time", "daemon"):
        assert key in info
    assert info["protocol"] == 1 and info["jobs_dir"] == str(jobs)


# --- P0-3 gap tests (see 6.todo): fixed; the markers are gone ---------------

def test_prepare_twice_with_the_same_link_and_copy_is_the_same_result(jobs, conn, tmp_path):
    folder = received(jobs)["path"]
    pot = tmp_path / "pot.txt"
    pot.write_text("potential")
    args = (str(pot),), (f"model.pt={pot}",), (f"{pot}=copied.txt",), None
    first, code1 = rw.prepare(folder, *args)
    second, code2 = rw.prepare(folder, *args)  # what `qur run --resume` does after a half-finished first try
    assert code1 == 0 and code2 == 0
    assert second["missing"] == []
    assert os.path.islink(os.path.join(folder, "model.pt"))
    assert open(os.path.join(folder, "copied.txt")).read() == "potential"


# --- P1-1: a job folder is recognised by its marker, not by QUEUER_JOBS_DIR ---------------------------------

def test_commands_work_when_the_session_does_not_know_the_jobs_dir(jobs, conn, tmp_path, monkeypatch):
    """The host passes its jobs_dir to `receive`; an ssh session for pack / cat / prepare / update has no QUEUER_JOBS_DIR."""
    elsewhere = tmp_path / "custom-jobs"
    r = rw.receive("run", tar_bytes([("in.txt", "hello")]), jobs_dir_override=str(elsewhere))
    jid = make_job(conn, r["path"])
    pot = tmp_path / "pot.txt"
    pot.write_text("p")
    out = io.BytesIO()
    rw.pack(jid, out)
    assert out.getvalue()
    res, code = rw.prepare(r["path"], (str(pot),), (f"pot.txt={pot}",), (), None)
    assert code == 0
    assert rw.gc("0s", False, str(elsewhere))["orphans"] == []  # the folder belongs to job `jid`


def test_a_folder_without_the_received_marker_is_refused(jobs, conn, tmp_path):
    plain = tmp_path / "custom-jobs" / "mine"
    plain.mkdir(parents=True)
    with pytest.raises(RemoteError):
        rw.pack(make_job(conn, plain), io.BytesIO())
    with pytest.raises(RemoteError):
        rw.prepare(str(plain), (), (), (), None)


# --- P1-5: a remote command reads the database read-only and never migrates under a running daemon ------------

def _set_schema_version(version):
    c = db.connect(rw._default_db_path())
    db.init_db(c)
    c.execute("UPDATE schema_version SET version = ? WHERE id = 1", (version,))
    c.commit()
    c.close()


def _schema_version():
    c = db.connect(rw._default_db_path())
    try:
        return db.get_schema_version(c)
    finally:
        c.close()


def test_an_up_to_date_database_is_opened_read_only(jobs):
    rw._open_db().close()  # creates it (a worker that never ran queuerd)
    c = rw._open_db()
    try:
        assert db.get_epoch(c)  # reading the epoch does not write
        with pytest.raises(sqlite3.OperationalError):
            c.execute("INSERT INTO daemon_state (key, value) VALUES ('x', 'y')")
    finally:
        c.close()


def test_an_older_database_is_not_migrated_under_a_running_daemon(jobs, monkeypatch):
    rw._open_db().close()
    _set_schema_version(db.SCHEMA_VERSION - 1)
    monkeypatch.setattr(rw, "_daemon_running", lambda: True)
    with pytest.raises(RemoteError) as e:
        rw._open_db()
    assert e.value.code == rw.EXIT_BAD_ARGS and "restart" in str(e.value)
    assert _schema_version() == db.SCHEMA_VERSION - 1  # untouched


def test_an_older_database_is_migrated_when_no_daemon_runs(jobs, monkeypatch):
    rw._open_db().close()
    _set_schema_version(db.SCHEMA_VERSION - 1)
    monkeypatch.setattr(rw, "_daemon_running", lambda: False)
    rw._open_db().close()
    assert _schema_version() == db.SCHEMA_VERSION


def test_a_newer_database_is_a_remote_error_not_a_traceback(jobs):
    rw._open_db().close()
    _set_schema_version(db.SCHEMA_VERSION + 1)
    with pytest.raises(RemoteError):
        rw._open_db()
