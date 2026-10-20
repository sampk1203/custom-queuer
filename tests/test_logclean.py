"""F14: log cleaning by age and total size (queuer.logclean), `qur clean-logs --max-mb`, and the
daemon's automatic timer."""

from __future__ import annotations

import asyncio
import contextlib
import os
import time
from pathlib import Path

import pytest
from typer.testing import CliRunner

from queuer import db, logclean
from queuer.cli import app
from queuer.daemon import (
    LOG_CLEAN_MIN_SECS,
    Daemon,
    _default_log_clean_secs,
    _env_positive_int,
)

DAY = 86400


@pytest.fixture(autouse=True)
def _no_log_clean_env(monkeypatch):
    for name in ("QUEUER_LOG_MAX_AGE_DAYS", "QUEUER_LOG_MAX_MB", "QUEUER_LOG_CLEAN_HOURS"):
        monkeypatch.delenv(name, raising=False)


async def _stop(task: asyncio.Task) -> None:
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


def _make_db(tmp_path: Path):
    conn = db.connect(tmp_path / "state" / "queuer.db")
    db.init_db(conn)
    return conn


def _finished(conn, log_dir: Path, age_days: float, size: int = 10, status: str = "done") -> int:
    """A finished job whose log is `size` bytes and whose finish time is `age_days` ago."""
    job_id = db.enqueue(conn, raw_cmd=["x"], resolved_cmd=["/bin/x"], cwd="/tmp", log_path="pending")
    db.mark_running(conn, job_id, pid=1, pgid=1)
    db.mark_finished(conn, job_id, status=status, exit_code=0 if status == "done" else 1, keep_backlog=db.DEFAULT_KEEP)
    when = time.time() - age_days * DAY
    iso = time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime(when))
    conn.execute("UPDATE jobs SET finished_at = ? WHERE id = ?", (iso, job_id))
    conn.commit()
    (log_dir / f"{job_id}.log").write_bytes(b"x" * size)
    return job_id


def _names(planned) -> list[str]:
    return [f.name for f, _ in planned]


def test_plan_by_age_only_takes_old_finished_logs(tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    conn = _make_db(tmp_path)
    old = _finished(conn, log_dir, 40)
    new = _finished(conn, log_dir, 1)
    got = logclean.plan(conn, log_dir, max_age_days=30)
    assert _names(got) == [f"{old}.log"] and got[0][1] == 10
    assert (log_dir / f"{new}.log").exists()  # plan() never deletes


def test_plan_without_limits_selects_nothing(tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    conn = _make_db(tmp_path)
    _finished(conn, log_dir, 400)
    assert logclean.plan(conn, log_dir) == []


def test_plan_never_selects_unfinished_jobs_or_foreign_files(tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    conn = _make_db(tmp_path)
    queued = db.enqueue(conn, raw_cmd=["x"], resolved_cmd=["/bin/x"], cwd="/tmp", log_path="pending")
    running = db.enqueue(conn, raw_cmd=["x"], resolved_cmd=["/bin/x"], cwd="/tmp", log_path="pending")
    db.mark_running(conn, running, pid=1, pgid=1)
    past = time.time() - 400 * DAY
    for name in (f"{queued}.log", f"{running}.log", "notes.log", "readme.txt"):
        f = log_dir / name
        f.write_bytes(b"x" * 100)
        os.utime(f, (past, past))
    assert logclean.plan(conn, log_dir, max_age_days=1, max_bytes=1) == []


def test_plan_orphans_go_by_file_time(tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    conn = _make_db(tmp_path)
    fresh, stale = log_dir / "901.log", log_dir / "902.log"
    fresh.write_text("f")
    stale.write_text("s")
    past = time.time() - 40 * DAY
    os.utime(stale, (past, past))
    assert _names(logclean.plan(conn, log_dir, max_age_days=30)) == ["902.log"]


def test_plan_by_size_deletes_the_oldest_first_until_it_fits(tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    conn = _make_db(tmp_path)
    a = _finished(conn, log_dir, 30, size=100)
    b = _finished(conn, log_dir, 20, size=100)
    c = _finished(conn, log_dir, 10, size=100)
    d = _finished(conn, log_dir, 1, size=100)
    assert _names(logclean.plan(conn, log_dir, max_bytes=250)) == [f"{a}.log", f"{b}.log"]
    assert _names(logclean.plan(conn, log_dir, max_bytes=400)) == []  # exactly fits
    assert _names(logclean.plan(conn, log_dir, max_bytes=0)) == [f"{x}.log" for x in (a, b, c, d)]


def test_plan_size_counts_unfinished_logs_but_never_deletes_them(tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    conn = _make_db(tmp_path)
    running = db.enqueue(conn, raw_cmd=["x"], resolved_cmd=["/bin/x"], cwd="/tmp", log_path="pending")
    db.mark_running(conn, running, pid=1, pgid=1)
    (log_dir / f"{running}.log").write_bytes(b"x" * 300)
    done = _finished(conn, log_dir, 5, size=100)
    # 400 bytes in total, limit 350: the finished one must go, the running one stays
    assert _names(logclean.plan(conn, log_dir, max_bytes=350)) == [f"{done}.log"]
    # limit below the running log alone: still only the finished one is selected
    assert _names(logclean.plan(conn, log_dir, max_bytes=10)) == [f"{done}.log"]


def test_plan_age_and_size_combine_without_double_counting(tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    conn = _make_db(tmp_path)
    ancient = _finished(conn, log_dir, 90, size=100)
    older = _finished(conn, log_dir, 20, size=100)
    newer = _finished(conn, log_dir, 2, size=100)
    # age removes `ancient` (300 -> 200 bytes); the size limit then removes `older` too
    got = logclean.plan(conn, log_dir, max_age_days=30, max_bytes=150)
    assert _names(got) == [f"{ancient}.log", f"{older}.log"]
    assert (log_dir / f"{newer}.log").exists()


def test_plan_covers_every_terminal_status(tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    conn = _make_db(tmp_path)
    ids = [_finished(conn, log_dir, 50, status=s) for s in ("done", "failed", "interrupted")]
    assert sorted(_names(logclean.plan(conn, log_dir, max_age_days=30))) == sorted(f"{i}.log" for i in ids)


def test_delete_removes_files_and_tolerates_a_vanished_one(tmp_path):
    a, b = tmp_path / "1.log", tmp_path / "2.log"
    a.write_bytes(b"x" * 5)
    b.write_bytes(b"y" * 7)
    gone = tmp_path / "3.log"
    count, freed = logclean.delete([(a, 5), (gone, 9), (b, 7)])
    assert (count, freed) == (2, 12) and not a.exists() and not b.exists()


def test_delete_reports_the_first_real_error_after_trying_the_rest(tmp_path):
    keep_going = tmp_path / "2.log"
    keep_going.write_text("x")
    not_a_file = tmp_path / "dir.log"
    not_a_file.mkdir()  # unlink() on a directory raises an OSError that is not FileNotFoundError
    with pytest.raises(OSError):
        logclean.delete([(not_a_file, 0), (keep_going, 1)])
    assert not keep_going.exists()


# ---------------------------------------------------------------------------
# qur clean-logs --max-mb
# ---------------------------------------------------------------------------

def _cli_env(tmp_path, monkeypatch, log_dir):
    monkeypatch.setenv("QUEUER_LOG_DIR", str(log_dir))
    monkeypatch.setenv("QUEUER_DB_PATH", str(tmp_path / "state" / "queuer.db"))


def test_clean_logs_max_mb_deletes_the_oldest_until_it_fits(tmp_path, monkeypatch):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    conn = _make_db(tmp_path)
    mb = 1024 * 1024
    a = _finished(conn, log_dir, 3, size=mb)
    b = _finished(conn, log_dir, 2, size=mb)
    c = _finished(conn, log_dir, 1, size=mb)
    conn.close()
    _cli_env(tmp_path, monkeypatch, log_dir)
    res = CliRunner().invoke(app, ["clean-logs", "--max-mb", "2", "--dry-run"])
    assert res.exit_code == 0 and "Would delete 1 log(s), 1024 KB" in res.output, res.output
    assert (log_dir / f"{a}.log").exists()  # dry run
    res = CliRunner().invoke(app, ["clean-logs", "--max-mb", "2"])
    assert res.exit_code == 0 and "Deleting 1 log(s)" in res.output, res.output
    assert not (log_dir / f"{a}.log").exists()
    assert (log_dir / f"{b}.log").exists() and (log_dir / f"{c}.log").exists()


def test_clean_logs_max_mb_must_be_positive(tmp_path, monkeypatch):
    _cli_env(tmp_path, monkeypatch, tmp_path / "logs")
    res = CliRunner().invoke(app, ["clean-logs", "--max-mb", "0"])
    assert res.exit_code != 0


def test_clean_logs_without_max_mb_ignores_total_size(tmp_path, monkeypatch):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    conn = _make_db(tmp_path)
    ids = [_finished(conn, log_dir, 1, size=2 * 1024 * 1024) for _ in range(3)]
    conn.close()
    _cli_env(tmp_path, monkeypatch, log_dir)
    res = CliRunner().invoke(app, ["clean-logs", "--days", "30"])
    assert res.exit_code == 0 and "nothing to clean" in res.output
    assert all((log_dir / f"{i}.log").exists() for i in ids)


# ---------------------------------------------------------------------------
# daemon settings
# ---------------------------------------------------------------------------

def test_log_clean_is_off_by_default(monkeypatch):
    for name in ("QUEUER_LOG_MAX_AGE_DAYS", "QUEUER_LOG_MAX_MB", "QUEUER_LOG_CLEAN_HOURS"):
        monkeypatch.delenv(name, raising=False)
    d = Daemon()
    assert d.log_max_age_days is None and d.log_max_mb is None and d.log_clean_secs == 6 * 3600


def test_log_clean_settings_come_from_the_environment(monkeypatch):
    monkeypatch.setenv("QUEUER_LOG_MAX_AGE_DAYS", "14")
    monkeypatch.setenv("QUEUER_LOG_MAX_MB", "2048")
    monkeypatch.setenv("QUEUER_LOG_CLEAN_HOURS", "0.5")
    d = Daemon()
    assert (d.log_max_age_days, d.log_max_mb, d.log_clean_secs) == (14, 2048, 1800)


def test_constructor_arguments_win_over_the_environment(monkeypatch):
    monkeypatch.setenv("QUEUER_LOG_MAX_AGE_DAYS", "14")
    d = Daemon(log_max_age_days=3, log_max_mb=5, log_clean_secs=0.01)
    assert (d.log_max_age_days, d.log_max_mb, d.log_clean_secs) == (3, 5, 0.01)


@pytest.mark.parametrize("bad", ["abc", "0", "-3", "1.5"])
def test_bad_integer_limits_are_ignored_with_a_warning(monkeypatch, capsys, bad):
    monkeypatch.setenv("QUEUER_LOG_MAX_MB", bad)
    assert _env_positive_int("QUEUER_LOG_MAX_MB") is None
    assert "ignoring QUEUER_LOG_MAX_MB" in capsys.readouterr().err


def test_unset_or_blank_limit_is_silently_none(monkeypatch, capsys):
    monkeypatch.delenv("QUEUER_LOG_MAX_MB", raising=False)
    assert _env_positive_int("QUEUER_LOG_MAX_MB") is None
    monkeypatch.setenv("QUEUER_LOG_MAX_MB", "  ")
    assert _env_positive_int("QUEUER_LOG_MAX_MB") is None
    assert capsys.readouterr().err == ""


@pytest.mark.parametrize("bad", ["abc", "0", "-1", "nan", "inf"])
def test_bad_clean_interval_falls_back_to_the_default(monkeypatch, capsys, bad):
    monkeypatch.setenv("QUEUER_LOG_CLEAN_HOURS", bad)
    assert _default_log_clean_secs() == 6 * 3600
    assert "ignoring QUEUER_LOG_CLEAN_HOURS" in capsys.readouterr().err


def test_clean_interval_is_never_shorter_than_a_minute(monkeypatch):
    monkeypatch.setenv("QUEUER_LOG_CLEAN_HOURS", "0.0001")
    assert _default_log_clean_secs() == LOG_CLEAN_MIN_SECS


# ---------------------------------------------------------------------------
# the daemon's timer
# ---------------------------------------------------------------------------

def _daemon(tmp_path, **kw) -> Daemon:
    d = Daemon(db_path=tmp_path / "queuer.db", socket_path=tmp_path / "queuer.sock", log_dir=tmp_path / "logs", **kw)
    d.log_dir.mkdir(parents=True, exist_ok=True)
    d._connect()
    return d


def _add_finished(d: Daemon, age_days: float, size: int = 10) -> int:
    assert d.conn is not None
    return _finished(d.conn, d.log_dir, age_days, size=size)


def test_clean_logs_once_applies_the_age_and_size_limits(tmp_path):
    d = _daemon(tmp_path, log_max_age_days=30, log_max_mb=1)
    old = _add_finished(d, 60)
    big_old = _add_finished(d, 10, size=1024 * 1024)
    new = _add_finished(d, 1, size=100)
    count, freed = d._clean_logs_once()
    assert count == 2 and freed == 10 + 1024 * 1024
    assert not (d.log_dir / f"{old}.log").exists() and not (d.log_dir / f"{big_old}.log").exists()
    assert (d.log_dir / f"{new}.log").exists()
    assert d._clean_logs_once() == (0, 0)


def test_clean_logs_once_with_only_an_age_limit_ignores_size(tmp_path):
    d = _daemon(tmp_path, log_max_age_days=30)
    big_new = _add_finished(d, 1, size=3 * 1024 * 1024)
    assert d._clean_logs_once() == (0, 0)
    assert (d.log_dir / f"{big_new}.log").exists()


@pytest.mark.asyncio
async def test_timer_cleans_repeatedly_and_reports_to_stderr(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr("queuer.daemon.LOG_CLEAN_FIRST_SECS", 0.01)
    d = _daemon(tmp_path, log_max_age_days=30, log_clean_secs=0.05)
    first = _add_finished(d, 60)
    task = asyncio.create_task(d._log_cleaner())
    try:
        for _ in range(100):
            if not (d.log_dir / f"{first}.log").exists():
                break
            await asyncio.sleep(0.02)
        assert not (d.log_dir / f"{first}.log").exists()
        second = _add_finished(d, 90)  # appears later: the next round takes it
        for _ in range(100):
            if not (d.log_dir / f"{second}.log").exists():
                break
            await asyncio.sleep(0.02)
        assert not (d.log_dir / f"{second}.log").exists()
    finally:
        await _stop(task)
    assert "cleaned 1 log(s)" in capsys.readouterr().err


@pytest.mark.asyncio
async def test_timer_does_nothing_without_limits(tmp_path, monkeypatch):
    monkeypatch.setattr("queuer.daemon.LOG_CLEAN_FIRST_SECS", 0.01)
    d = _daemon(tmp_path, log_clean_secs=0.01)
    old = _add_finished(d, 500)
    task = asyncio.create_task(d._log_cleaner())  # keeps looping now (event pruning), but cleans no logs
    try:
        await asyncio.sleep(0.2)
    finally:
        await _stop(task)
    assert (d.log_dir / f"{old}.log").exists()


@pytest.mark.asyncio
async def test_timer_survives_a_failing_clean_and_warns_once(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr("queuer.daemon.LOG_CLEAN_FIRST_SECS", 0.01)
    d = _daemon(tmp_path, log_max_age_days=30, log_clean_secs=0.02)
    calls = []

    def boom() -> tuple[int, int]:
        calls.append(1)
        raise OSError("disk on fire")

    monkeypatch.setattr(d, "_clean_logs_once", boom)
    task = asyncio.create_task(d._log_cleaner())
    try:
        for _ in range(100):
            if len(calls) >= 3:
                break
            await asyncio.sleep(0.02)
    finally:
        await _stop(task)
    assert len(calls) >= 3
    err = capsys.readouterr().err
    assert err.count("automatic log clean failed: OSError: disk on fire") == 1


@pytest.mark.asyncio
async def test_serve_runs_the_timer_and_stops_cleanly(tmp_path, monkeypatch):
    monkeypatch.setattr("queuer.daemon.LOG_CLEAN_FIRST_SECS", 0.05)
    d = Daemon(
        db_path=tmp_path / "queuer.db", socket_path=tmp_path / "queuer.sock", log_dir=tmp_path / "logs",
        log_max_age_days=30, log_clean_secs=0.1,
    )
    serving = asyncio.create_task(d.serve())
    for _ in range(100):
        if d.socket_path.exists() and d.conn is not None:
            break
        await asyncio.sleep(0.02)
    old = _finished(d.conn, d.log_dir, 90)
    for _ in range(150):
        if not (d.log_dir / f"{old}.log").exists():
            break
        await asyncio.sleep(0.02)
    assert not (d.log_dir / f"{old}.log").exists()
    d._stopping.set()
    await asyncio.wait_for(serving, timeout=15)
