import pytest

from queuer import db


@pytest.fixture
def conn(tmp_path):
    c = db.connect(tmp_path / "test.db")
    db.init_db(c)
    yield c
    c.close()


def _mk(conn, name="job", **kw):
    defaults = dict(
        raw_cmd=[name],
        resolved_cmd=[f"/usr/bin/{name}"],
        cwd="/tmp",
        log_path=f"/tmp/{name}.log",
    )
    defaults.update(kw)
    return db.enqueue(conn, **defaults)


def test_enqueue_append_order(conn):
    a, b, c = _mk(conn, "a"), _mk(conn, "b"), _mk(conn, "c")
    queue = db.get_queue(conn)
    assert [j["id"] for j in queue] == [a, b, c]
    assert [j["position"] for j in queue] == [0, 1, 2]


def test_enqueue_before(conn):
    a = _mk(conn, "a")
    b = _mk(conn, "b")
    c = _mk(conn, "c", before=b)
    assert [j["id"] for j in db.get_queue(conn)] == [a, c, b]


def test_enqueue_after(conn):
    a = _mk(conn, "a")
    b = _mk(conn, "b")
    c = _mk(conn, "c", after=a)
    assert [j["id"] for j in db.get_queue(conn)] == [a, c, b]


def test_enqueue_before_nonqueued_raises(conn):
    _mk(conn, "a")
    with pytest.raises(ValueError):
        _mk(conn, "b", before=999)


def test_ids_stable_after_removal(conn):
    a, b, c = _mk(conn, "a"), _mk(conn, "b"), _mk(conn, "c")
    db.remove_from_queue(conn, b)
    queue = db.get_queue(conn)
    assert [j["id"] for j in queue] == [a, c]
    assert [j["position"] for j in queue] == [0, 1]


def test_remove_running_job_raises(conn):
    a = _mk(conn, "a")
    db.mark_running(conn, a, pid=123, pgid=123)
    with pytest.raises(ValueError):
        db.remove_from_queue(conn, a)


def test_mark_running_removes_from_queue(conn):
    a = _mk(conn, "a")
    b = _mk(conn, "b")
    db.mark_running(conn, a, pid=1, pgid=1)
    assert db.get_running(conn)["id"] == a
    queue = db.get_queue(conn)
    assert [j["id"] for j in queue] == [b]
    assert queue[0]["position"] == 0


def test_reorder_before(conn):
    a, b, c = _mk(conn, "a"), _mk(conn, "b"), _mk(conn, "c")
    db.reorder_before(conn, c, a)
    assert [j["id"] for j in db.get_queue(conn)] == [c, a, b]


def test_reorder_after(conn):
    a, b, c = _mk(conn, "a"), _mk(conn, "b"), _mk(conn, "c")
    db.reorder_after(conn, a, c)
    assert [j["id"] for j in db.get_queue(conn)] == [b, c, a]


def test_backlog_eviction_caps_at_ten(conn):
    ids = [_mk(conn, f"job{i}") for i in range(12)]
    for job_id in ids:
        db.mark_running(conn, job_id, pid=1, pgid=1)
        db.mark_finished(conn, job_id, status="done", exit_code=0)
    backlog = db.get_backlog(conn, limit=100)
    assert len(backlog) == 10
    remaining_ids = {j["id"] for j in backlog}
    assert ids[0] not in remaining_ids
    assert ids[1] not in remaining_ids
    assert ids[-1] in remaining_ids


def test_requeue_copies_original_and_appends(conn):
    a = _mk(conn, "a", timeout_secs=30)
    db.mark_running(conn, a, pid=1, pgid=1)
    db.mark_finished(conn, a, status="failed", exit_code=1, note="boom")

    new_id = db.requeue(conn, a, log_path="/tmp/a-retry.log")
    new_job = db.get_job(conn, new_id)
    old_job = db.get_job(conn, a)

    assert new_job["status"] == "queued"
    assert new_job["raw_cmd"] == old_job["raw_cmd"]
    assert new_job["resolved_cmd"] == old_job["resolved_cmd"]
    assert new_job["cwd"] == old_job["cwd"]
    assert new_job["timeout_secs"] == 30
    assert old_job["status"] == "failed"  # original untouched


def test_requeue_running_job_raises(conn):
    a = _mk(conn, "a")
    db.mark_running(conn, a, pid=1, pgid=1)
    with pytest.raises(ValueError):
        db.requeue(conn, a, log_path="/tmp/x.log")


def test_pause_state_defaults_false_and_persists(conn):
    assert db.get_paused(conn) is False
    db.set_paused(conn, True)
    assert db.get_paused(conn) is True
    db.set_paused(conn, False)
    assert db.get_paused(conn) is False
