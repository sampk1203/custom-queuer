"""Tests for `qur doctor` (F32/U8): every check, `--fix` touching permissions only, and
the outside world (systemctl, loginctl, Telegram, disk) replaced by stubs."""

from __future__ import annotations

import os
import socket
import sqlite3
import stat
import subprocess
from pathlib import Path

import pytest

from queuer import client, db, notify, selfcheck
from queuer.selfcheck import FAIL, OK, WARN, Check

TOKEN = "123456789:AAH-test_token-0123456789abcdefghijk"


@pytest.fixture(autouse=True)
def _no_redact_override(monkeypatch):
    monkeypatch.delenv("QUEUER_REDACT_PATH", raising=False)


def _mode(path) -> int:
    return stat.S_IMODE(os.lstat(path).st_mode)


def _by_name(checks: list[Check]) -> dict[str, Check]:
    return {c.name: c for c in checks}


def _db_at(path: Path) -> sqlite3.Connection:
    conn = db.connect(path)
    db.init_db(conn)
    return conn


# ---------------------------------------------------------------------------
# _check_node: the shared permission logic
# ---------------------------------------------------------------------------


def test_node_with_private_mode_is_ok(tmp_path):
    f = tmp_path / "f"
    f.write_text("x")
    f.chmod(0o600)
    c = selfcheck._check_node("f", f, "file", 0o600, fix=False)
    assert c is not None and c.status == OK and not c.fixed


def test_node_missing_returns_the_given_check_or_none(tmp_path):
    assert selfcheck._check_node("f", tmp_path / "nope", "file", 0o600, fix=False) is None
    gone = Check("f", WARN, "missing")
    assert selfcheck._check_node("f", tmp_path / "nope", "file", 0o600, fix=False, missing=gone) is gone


def test_loose_file_fails_and_fix_tightens_it(tmp_path):
    f = tmp_path / "f"
    f.write_text("x")
    f.chmod(0o644)
    c = selfcheck._check_node("f", f, "file", 0o600, fix=False)
    assert c is not None and c.status == FAIL and "644" in c.detail and "--fix" in c.hint
    assert _mode(f) == 0o644  # report only
    c = selfcheck._check_node("f", f, "file", 0o600, fix=True)
    assert c is not None and c.status == OK and c.fixed and "644" in c.detail
    assert _mode(f) == 0o600


def test_group_readable_counts_as_loose(tmp_path):
    f = tmp_path / "f"
    f.write_text("x")
    f.chmod(0o640)
    c = selfcheck._check_node("f", f, "file", 0o600, fix=False)
    assert c is not None and c.status == FAIL


def test_loose_dir_is_a_warning_when_loose_status_says_so(tmp_path):
    d = tmp_path / "d"
    d.mkdir()
    d.chmod(0o755)
    c = selfcheck._check_node("d", d, "dir", 0o700, fix=False, loose_status=WARN)
    assert c is not None and c.status == WARN


def test_fix_never_changes_a_folder_it_may_not_own(tmp_path):
    d = tmp_path / "d"
    d.mkdir()
    d.chmod(0o755)
    c = selfcheck._check_node("d", d, "dir", 0o700, fix=True, chmod_ok=False, loose_status=WARN)
    assert c is not None and c.status == WARN and not c.fixed
    assert "chmod 700" in c.hint
    assert _mode(d) == 0o755


def test_symlink_fails_and_fix_leaves_it_alone(tmp_path):
    target = tmp_path / "target"
    target.write_text("x")
    link = tmp_path / "link"
    link.symlink_to(target)
    c = selfcheck._check_node("l", link, "file", 0o600, fix=True)
    assert c is not None and c.status == FAIL and "symlink" in c.detail and not c.fixed
    assert link.is_symlink()


def test_wrong_kind_fails(tmp_path):
    d = tmp_path / "d"
    d.mkdir()
    c = selfcheck._check_node("d", d, "file", 0o600, fix=True)
    assert c is not None and c.status == FAIL and "regular file" in c.detail


def test_foreign_owner_fails_and_is_not_fixed(tmp_path, monkeypatch):
    f = tmp_path / "f"
    f.write_text("x")
    f.chmod(0o644)
    monkeypatch.setattr(os, "getuid", lambda: os.stat(f).st_uid + 1)
    c = selfcheck._check_node("f", f, "file", 0o600, fix=True)
    assert c is not None and c.status == FAIL and "owned by" in c.detail and not c.fixed
    assert _mode(f) == 0o644


# ---------------------------------------------------------------------------
# socket
# ---------------------------------------------------------------------------


def test_socket_missing_is_a_warning(monkeypatch):
    c = _by_name(selfcheck.check_socket(fix=False))["socket"]
    assert c.status == WARN and "daemon start" in c.hint


def test_socket_mode_checked_and_fixed(tmp_path, monkeypatch):
    path = tmp_path / "s.sock"
    monkeypatch.setenv("QUEUER_SOCKET_PATH", str(path))
    srv = socket.socket(socket.AF_UNIX)
    try:
        srv.bind(str(path))
        os.chmod(path, 0o666)
        c = _by_name(selfcheck.check_socket(fix=False))["socket"]
        assert c.status == FAIL
        c = _by_name(selfcheck.check_socket(fix=True))["socket"]
        assert c.status == OK and c.fixed
        assert _mode(path) == 0o600
        assert _by_name(selfcheck.check_socket(fix=False))["socket"].status == OK
    finally:
        srv.close()


def test_socket_that_is_a_plain_file_fails(tmp_path, monkeypatch):
    path = tmp_path / "s.sock"
    path.write_text("not a socket")
    monkeypatch.setenv("QUEUER_SOCKET_PATH", str(path))
    assert _by_name(selfcheck.check_socket(fix=True))["socket"].status == FAIL


def test_private_fallback_socket_dir_is_checked(tmp_path, monkeypatch):
    fallback = tmp_path / "queuer-fallback"
    fallback.mkdir()
    fallback.chmod(0o755)
    monkeypatch.setattr(selfcheck, "_fallback_socket_dir", lambda: fallback)
    monkeypatch.setenv("QUEUER_SOCKET_PATH", str(fallback / "queuer.sock"))
    assert _by_name(selfcheck.check_socket(fix=False))["socket dir"].status == FAIL
    c = _by_name(selfcheck.check_socket(fix=True))["socket dir"]
    assert c.status == OK and c.fixed and _mode(fallback) == 0o700


# ---------------------------------------------------------------------------
# config files
# ---------------------------------------------------------------------------


def _bot_cfg():
    return {"qur_id": "ab12", "bots": [{"token": TOKEN, "chat_ids": [42]}]}


def test_config_not_set_up_is_ok(tmp_path):
    names = _by_name(selfcheck.check_config(fix=False))
    assert names["notify.json"].status == OK and "not configured" in names["notify.json"].detail
    assert "redact.json" not in names  # optional file, silent when absent


def test_notify_json_mode_checked_and_fixed(tmp_path):
    path = notify.default_path()
    notify.save(_bot_cfg(), path)
    assert _by_name(selfcheck.check_config(fix=False))["notify.json"].status == OK
    path.chmod(0o644)
    assert _by_name(selfcheck.check_config(fix=False))["notify.json"].status == FAIL
    c = _by_name(selfcheck.check_config(fix=True))["notify.json"]
    assert c.status == OK and c.fixed and _mode(path) == 0o600


def test_notify_json_symlink_fails(tmp_path):
    path = notify.default_path()
    path.parent.mkdir(parents=True)
    real = tmp_path / "real.json"
    real.write_text("{}")
    path.symlink_to(real)
    c = _by_name(selfcheck.check_config(fix=True))["notify.json"]
    assert c.status == FAIL and "symlink" in c.detail


def test_redact_json_checked_when_present(tmp_path):
    path = notify.default_path().with_name("redact.json")  # next to the moved notify.json
    path.parent.mkdir(parents=True)
    path.write_text("[]")
    path.chmod(0o666)
    assert _by_name(selfcheck.check_config(fix=False))["redact.json"].status == FAIL
    assert _by_name(selfcheck.check_config(fix=True))["redact.json"].fixed
    assert _mode(path) == 0o600


def test_config_dir_fixed_only_at_the_default_location(tmp_path, monkeypatch):
    # moved with QUEUER_NOTIFY_PATH (the conftest default): reported, never chmod'ed
    moved = notify.default_path().parent
    moved.mkdir(parents=True)
    moved.chmod(0o755)
    c = _by_name(selfcheck.check_config(fix=True))["config dir"]
    assert c.status == WARN and not c.fixed and _mode(moved) == 0o755
    # the default ~/.config/queuer: fixed
    monkeypatch.delenv("QUEUER_NOTIFY_PATH")
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    default_dir = tmp_path / "xdg" / "queuer"
    default_dir.mkdir(parents=True)
    default_dir.chmod(0o755)
    c = _by_name(selfcheck.check_config(fix=True))["config dir"]
    assert c.status == OK and c.fixed and _mode(default_dir) == 0o700


# ---------------------------------------------------------------------------
# database
# ---------------------------------------------------------------------------


def test_database_missing_is_a_warning():
    names = _by_name(selfcheck.check_database(fix=False))
    assert names["db file"].status == WARN
    assert "db dir" not in names


def test_database_file_mode_checked_and_fixed(tmp_path):
    path = selfcheck._default_db_path()
    conn = _db_at(path)
    try:
        assert _by_name(selfcheck.check_database(fix=False))["db file"].status == OK
        path.chmod(0o644)
        assert _by_name(selfcheck.check_database(fix=False))["db file"].status == FAIL
        c = _by_name(selfcheck.check_database(fix=True))["db file"]
        assert c.status == OK and c.fixed and _mode(path) == 0o600
    finally:
        conn.close()


def test_wal_and_shm_files_are_checked_and_fixed():
    path = selfcheck._default_db_path()
    conn = _db_at(path)  # stays open: the -wal/-shm files exist while it does
    try:
        wal = Path(f"{path}-wal")
        assert wal.exists()
        assert _by_name(selfcheck.check_database(fix=False))["db wal/shm"].status == OK
        wal.chmod(0o644)
        c = _by_name(selfcheck.check_database(fix=False))["db wal/shm"]
        assert c.status == FAIL and "-wal" in c.detail
        c = _by_name(selfcheck.check_database(fix=True))["db wal/shm"]
        assert c.status == OK and c.fixed
        assert _mode(wal) == 0o600
    finally:
        conn.close()


def test_database_dir_fixed_only_at_the_default_location(tmp_path, monkeypatch):
    path = selfcheck._default_db_path()
    conn = _db_at(path)
    conn.close()
    path.parent.chmod(0o755)
    c = _by_name(selfcheck.check_database(fix=True))["db dir"]  # QUEUER_DB_PATH pointed elsewhere
    assert c.status == WARN and not c.fixed and _mode(path.parent) == 0o755
    monkeypatch.setattr(db, "DEFAULT_DB_PATH", path)  # now it is the default location
    c = _by_name(selfcheck.check_database(fix=True))["db dir"]
    assert c.status == OK and c.fixed and _mode(path.parent) == 0o700


# ---------------------------------------------------------------------------
# schema version
# ---------------------------------------------------------------------------


def _set_version(path, version):
    conn = sqlite3.connect(path)
    conn.execute("UPDATE schema_version SET version = ?", (version,))
    conn.commit()
    conn.close()


def test_schema_current_is_ok():
    conn = _db_at(selfcheck._default_db_path())
    conn.close()
    c = selfcheck.check_schema()
    assert c.status == OK and str(db.SCHEMA_VERSION) in c.detail


def test_schema_missing_database_is_a_warning_and_creates_nothing():
    c = selfcheck.check_schema()
    assert c.status == WARN
    assert not selfcheck._default_db_path().exists()


def test_schema_older_warns_newer_fails():
    path = selfcheck._default_db_path()
    _db_at(path).close()
    _set_version(path, db.SCHEMA_VERSION - 1)
    c = selfcheck.check_schema()
    assert c.status == WARN and "restart" in c.hint
    _set_version(path, db.SCHEMA_VERSION + 1)
    assert selfcheck.check_schema().status == FAIL


def test_schema_without_the_table_warns():
    path = selfcheck._default_db_path()
    path.parent.mkdir(parents=True)
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE unrelated (x)")  # a real file, but not queuer's
    conn.commit()
    conn.close()
    assert selfcheck.check_schema().status == WARN


def test_schema_unreadable_file_fails():
    path = selfcheck._default_db_path()
    path.parent.mkdir(parents=True)
    path.write_bytes(b"this is not a sqlite database" * 20)
    c = selfcheck.check_schema()
    assert c.status == FAIL and "cannot read" in c.detail


# ---------------------------------------------------------------------------
# logs and disk
# ---------------------------------------------------------------------------


def _roomy(monkeypatch, free=50 * 1024**3):
    monkeypatch.setattr(selfcheck, "_free_bytes", lambda p: free)


def test_logs_not_created_yet_is_ok(monkeypatch):
    _roomy(monkeypatch)
    names = _by_name(selfcheck.check_logs(fix=False))
    assert names["log dir"].status == OK and "does not exist" in names["log dir"].detail
    assert names["log writable"].status == OK
    assert names["disk space"].status == OK


def test_logs_not_writable_fails(monkeypatch):
    _roomy(monkeypatch)
    monkeypatch.setattr(selfcheck, "_writable", lambda p: False)
    c = _by_name(selfcheck.check_logs(fix=False))["log writable"]
    assert c.status == FAIL and "chmod" in c.hint


@pytest.mark.parametrize(
    ("free", "status"),
    [(50 * 1024**2, FAIL), (500 * 1024**2, WARN), (5 * 1024**3, OK)],
)
def test_disk_space_thresholds(monkeypatch, free, status):
    _roomy(monkeypatch, free)
    assert _by_name(selfcheck.check_logs(fix=False))["disk space"].status == status


def test_disk_probe_walks_up_to_an_existing_folder(tmp_path, monkeypatch):
    seen = []
    monkeypatch.setattr(selfcheck, "_free_bytes", lambda p: seen.append(p) or 10**12)
    monkeypatch.setenv("QUEUER_LOG_DIR", str(tmp_path / "a" / "b" / "logs"))
    selfcheck.check_logs(fix=False)
    assert seen == [tmp_path]


def test_log_dir_loose_mode_fixed_only_at_the_default_location(tmp_path, monkeypatch):
    _roomy(monkeypatch)
    moved = tmp_path / "mylogs"
    moved.mkdir()
    moved.chmod(0o755)
    monkeypatch.setenv("QUEUER_LOG_DIR", str(moved))
    c = _by_name(selfcheck.check_logs(fix=True))["log dir"]
    assert c.status == WARN and not c.fixed and _mode(moved) == 0o755
    monkeypatch.delenv("QUEUER_LOG_DIR")
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    default = tmp_path / "home" / ".local" / "state" / "queuer" / "logs"
    default.mkdir(parents=True)
    default.chmod(0o755)
    c = _by_name(selfcheck.check_logs(fix=True))["log dir"]
    assert c.status == OK and c.fixed and _mode(default) == 0o700


# ---------------------------------------------------------------------------
# daemon reachable
# ---------------------------------------------------------------------------


def test_daemon_down_fails_with_a_hint(monkeypatch):
    def boom(cmd, **kw):
        raise client.DaemonNotRunning("queuerd is not running -- start it")

    monkeypatch.setattr(client, "send", boom)
    c = selfcheck.check_daemon()
    assert c.status == FAIL and "not running" in c.detail and "qur daemon start" in c.hint


def test_daemon_error_reply_fails(monkeypatch):
    def boom(cmd, **kw):
        raise client.DaemonError("bad")

    monkeypatch.setattr(client, "send", boom)
    assert selfcheck.check_daemon().status == FAIL


def test_daemon_up_counts_running_jobs(monkeypatch):
    data = {"channels": {"1": {"running": {"id": 4}}, "2": {"running": None}, "3": {"running": {"id": 9}}}}
    monkeypatch.setattr(client, "send", lambda cmd, **kw: data)
    c = selfcheck.check_daemon()
    assert c.status == OK and "2 job(s) running" in c.detail


# ---------------------------------------------------------------------------
# linger, unit, service
# ---------------------------------------------------------------------------


@pytest.fixture
def no_linger_file(tmp_path, monkeypatch):
    monkeypatch.setattr(selfcheck, "LINGER_DIR", tmp_path / "linger")
    monkeypatch.setattr(selfcheck, "_user_name", lambda: "bob")


def test_linger_file_means_enabled_without_a_subprocess(tmp_path, monkeypatch, no_linger_file):
    (tmp_path / "linger").mkdir()
    (tmp_path / "linger" / "bob").write_text("")
    monkeypatch.setattr(selfcheck, "_run", lambda argv: pytest.fail("loginctl should not run"))
    assert selfcheck.check_linger().status == OK


@pytest.mark.parametrize(
    ("reply", "status"),
    [((0, "Linger=yes"), OK), ((0, "Linger=no"), WARN), ((1, ""), WARN), (None, WARN)],
)
def test_linger_via_loginctl(monkeypatch, no_linger_file, reply, status):
    calls = []
    monkeypatch.setattr(selfcheck, "_run", lambda argv: calls.append(argv) or reply)
    c = selfcheck.check_linger()
    assert c.status == status
    assert calls == [["loginctl", "show-user", str(os.getuid()), "-p", "Linger"]]
    if status == WARN:
        assert "enable-linger" in c.hint


@pytest.fixture
def home(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    return tmp_path / "home"


def _unit_file(home):
    unit = home / ".config" / "systemd" / "user" / "queuerd.service"
    unit.parent.mkdir(parents=True)
    unit.write_text("[Service]\n")
    return unit


def test_unit_missing_and_active_service(monkeypatch, home):
    calls = []
    monkeypatch.setattr(selfcheck, "_run", lambda argv: calls.append(argv) or (0, "active"))
    names = _by_name(selfcheck.check_unit())
    assert names["unit file"].status == WARN and "install.sh" in names["unit file"].hint
    assert names["service"].status == OK
    assert calls == [["systemctl", "--user", "is-active", "queuerd"]]


def test_unit_present_and_service_inactive(monkeypatch, home):
    _unit_file(home)
    monkeypatch.setattr(selfcheck, "_run", lambda argv: (3, "inactive"))
    names = _by_name(selfcheck.check_unit())
    assert names["unit file"].status == OK
    assert names["service"].status == WARN and "inactive" in names["service"].detail
    assert "qur daemon start" in names["service"].hint


@pytest.mark.parametrize("reply", [None, (1, "")])
def test_service_state_unknown_is_a_warning(monkeypatch, home, reply):
    monkeypatch.setattr(selfcheck, "_run", lambda argv: reply)
    assert _by_name(selfcheck.check_unit())["service"].status == WARN


# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------


def test_telegram_not_configured_makes_no_call(monkeypatch):
    monkeypatch.setattr(notify, "check_token", lambda t: pytest.fail("no getMe expected"))
    (c,) = selfcheck.check_telegram()
    assert c.status == OK and "not configured" in c.detail


def test_telegram_malformed_config_warns():
    path = notify.default_path()
    path.parent.mkdir(parents=True)
    path.write_text("{not json")
    (c,) = selfcheck.check_telegram()
    assert c.status == WARN and "qur connect" in c.hint


def test_telegram_valid_token(monkeypatch):
    notify.save(_bot_cfg())
    monkeypatch.setattr(notify, "check_token", lambda t: "my_bot")
    (c,) = selfcheck.check_telegram()
    assert c.status == OK and "@my_bot" in c.detail


def test_telegram_offline_is_a_warning_not_a_failure(monkeypatch):
    notify.save(_bot_cfg())

    def offline(token):
        raise notify.NotifyError("network error (URLError)")

    monkeypatch.setattr(notify, "check_token", offline)
    (c,) = selfcheck.check_telegram()
    assert c.status == WARN and "offline" in c.detail


@pytest.mark.parametrize("code", [401, 404])
def test_telegram_rejected_token_fails_and_never_prints_it(monkeypatch, code):
    notify.save(_bot_cfg())

    def rejected(token):
        raise notify.NotifyError("Unauthorized", code)

    monkeypatch.setattr(notify, "check_token", rejected)
    (c,) = selfcheck.check_telegram()
    assert c.status == FAIL
    assert TOKEN not in c.detail and TOKEN not in c.hint and TOKEN not in c.name


def test_telegram_other_api_error_is_a_warning(monkeypatch):
    notify.save(_bot_cfg())

    def busy(token):
        raise notify.NotifyError("Too Many Requests", 429)

    monkeypatch.setattr(notify, "check_token", busy)
    assert selfcheck.check_telegram()[0].status == WARN


def test_telegram_checks_every_bot(monkeypatch):
    cfg = _bot_cfg()
    cfg["bots"].append({"token": "987654321:BBH-other_token-0123456789abcdefghijk", "chat_ids": [1], "username": "second_bot"})
    notify.save(cfg)
    monkeypatch.setattr(notify, "check_token", lambda t: "x")
    checks = selfcheck.check_telegram()
    assert [c.name for c in checks] == ["telegram bot 1", "telegram second_bot"]


# ---------------------------------------------------------------------------
# subprocess wrapper, run_checks, summary
# ---------------------------------------------------------------------------


def test_run_returns_code_and_stripped_stdout(monkeypatch):
    seen = {}

    def fake(argv, **kw):
        seen.update(argv=argv, **kw)
        return subprocess.CompletedProcess(argv, 3, stdout="inactive\n", stderr="")

    monkeypatch.setattr(selfcheck.subprocess, "run", fake)
    assert selfcheck._run(["systemctl", "--user", "is-active", "queuerd"]) == (3, "inactive")
    assert seen["argv"] == ["systemctl", "--user", "is-active", "queuerd"]
    assert seen.get("shell") is None and seen["timeout"] == selfcheck.SUBPROCESS_TIMEOUT_SECS


@pytest.mark.parametrize("exc", [FileNotFoundError("systemctl"), subprocess.TimeoutExpired("x", 1)])
def test_run_returns_none_when_the_program_is_missing_or_hangs(monkeypatch, exc):
    def fake(argv, **kw):
        raise exc

    monkeypatch.setattr(selfcheck.subprocess, "run", fake)
    assert selfcheck._run(["systemctl"]) is None


def _quiet_world(monkeypatch, daemon_up=False):
    def send(cmd, **kw):
        if daemon_up:
            return {"channels": {}}
        raise client.DaemonNotRunning("queuerd is not running")

    monkeypatch.setattr(client, "send", send)
    monkeypatch.setattr(selfcheck, "_run", lambda argv: None)
    monkeypatch.setattr(selfcheck, "_free_bytes", lambda p: 10**12)


def test_run_checks_covers_everything_and_fails_when_the_daemon_is_down(monkeypatch):
    _quiet_world(monkeypatch)
    checks = selfcheck.run_checks()
    names = [c.name for c in checks]
    for expected in ("daemon", "socket", "config dir", "notify.json", "db file", "schema", "log dir", "log writable", "disk space", "linger", "unit file", "service", "telegram"):
        assert expected in names
    assert selfcheck.exit_code(checks) == 1


def test_warnings_alone_do_not_fail():
    assert selfcheck.exit_code([Check("a", OK, ""), Check("b", WARN, "")]) == 0
    assert selfcheck.exit_code([Check("a", OK, ""), Check("b", FAIL, "")]) == 1
    assert selfcheck.summary([Check("a", OK, ""), Check("b", WARN, ""), Check("c", WARN, "")]) == {OK: 1, WARN: 2, FAIL: 0}


def test_fix_changes_permissions_only(tmp_path, monkeypatch):
    """--fix tightens modes and leaves every file's content alone."""
    _quiet_world(monkeypatch, daemon_up=True)
    notify.save(_bot_cfg())
    monkeypatch.setattr(notify, "check_token", lambda t: "bot")
    path = notify.default_path()
    path.chmod(0o644)
    conn = _db_at(selfcheck._default_db_path())
    conn.close()
    selfcheck._default_db_path().chmod(0o666)
    before = {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    checks = selfcheck.run_checks(fix=True)
    after = {p for p in tmp_path.rglob("*") if p.is_file()}
    for p, content in before.items():
        assert p.read_bytes() == content
    # the read-only schema check may leave sqlite's own -wal/-shm bookkeeping files; nothing else new
    assert all(p.name.endswith(("-wal", "-shm")) for p in after - set(before))
    assert _mode(path) == 0o600 and _mode(selfcheck._default_db_path()) == 0o600
    assert any(c.fixed for c in checks)
