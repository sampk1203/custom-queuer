import pytest

from queuer import db


@pytest.fixture
def conn(tmp_path):
    c = db.connect(tmp_path / "test.db")
    db.init_db(c)
    yield c
    c.close()


def _mk(conn, name="job", channel=1, **kw):
    defaults = dict(
        raw_cmd=[name],
        resolved_cmd=[f"/usr/bin/{name}"],
        cwd="/tmp",
        log_path=f"/tmp/{name}.log",
        channel=channel,
    )
    defaults.update(kw)
    return db.enqueue(conn, **defaults)


# ---------------------------------------------------------------------------
# original suite (channel defaults to 1, behavior unchanged)
# ---------------------------------------------------------------------------

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
    assert old_job["status"] == "failed"


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


# ---------------------------------------------------------------------------
# original edge cases
# ---------------------------------------------------------------------------

def test_enqueue_before_and_after_mutually_exclusive(conn):
    a = _mk(conn, "a")
    with pytest.raises(ValueError):
        _mk(conn, "b", before=a, after=a)


def test_reorder_job_relative_to_itself_raises(conn):
    a = _mk(conn, "a")
    with pytest.raises(ValueError):
        db.reorder_before(conn, a, a)
    with pytest.raises(ValueError):
        db.reorder_after(conn, a, a)


def test_reorder_nonqueued_job_raises(conn):
    a = _mk(conn, "a")
    db.mark_running(conn, a, pid=1, pgid=1)
    b = _mk(conn, "b")
    with pytest.raises(ValueError):
        db.reorder_before(conn, a, b)  # a is running, not queued


def test_reorder_target_nonqueued_raises(conn):
    a = _mk(conn, "a")
    b = _mk(conn, "b")
    db.mark_running(conn, b, pid=1, pgid=1)
    with pytest.raises(ValueError):
        db.reorder_before(conn, a, b)  # b is running, not queued


def test_get_job_nonexistent_returns_none(conn):
    assert db.get_job(conn, 99999) is None


def test_requeue_nonexistent_job_raises(conn):
    with pytest.raises(ValueError):
        db.requeue(conn, 99999, log_path="/tmp/x.log")


def test_requeue_still_queued_job_raises(conn):
    a = _mk(conn, "a")  # never marked running/finished -- still queued
    with pytest.raises(ValueError):
        db.requeue(conn, a, log_path="/tmp/x.log")


def test_mark_running_nonqueued_job_raises(conn):
    a = _mk(conn, "a")
    db.mark_running(conn, a, pid=1, pgid=1)
    with pytest.raises(ValueError):
        db.mark_running(conn, a, pid=2, pgid=2)  # already running, not queued


def test_mark_finished_invalid_status_raises(conn):
    a = _mk(conn, "a")
    db.mark_running(conn, a, pid=1, pgid=1)
    with pytest.raises(ValueError):
        db.mark_finished(conn, a, status="queued")  # not a terminal status


def test_remove_nonexistent_job_raises(conn):
    with pytest.raises(ValueError):
        db.remove_from_queue(conn, 99999)


def test_env_extra_preserved_through_requeue(conn):
    a = _mk(conn, "a", env_extra={"CUDA_VISIBLE_DEVICES": "0"})
    db.mark_running(conn, a, pid=1, pgid=1)
    db.mark_finished(conn, a, status="done", exit_code=0)

    new_id = db.requeue(conn, a, log_path="/tmp/retry.log")
    new_job = db.get_job(conn, new_id)
    import json
    assert json.loads(new_job["env_extra"]) == {"CUDA_VISIBLE_DEVICES": "0"}


def test_backlog_ordered_newest_first(conn):
    a = _mk(conn, "a")
    db.mark_running(conn, a, pid=1, pgid=1)
    db.mark_finished(conn, a, status="done", exit_code=0)

    b = _mk(conn, "b")
    db.mark_running(conn, b, pid=1, pgid=1)
    db.mark_finished(conn, b, status="done", exit_code=0)

    backlog = db.get_backlog(conn, limit=10)
    assert [j["id"] for j in backlog] == [b, a]  # most recently finished first


def test_multiple_reorders_produce_consistent_final_order(conn):
    a, b, c, d, e = (_mk(conn, n) for n in "abcde")
    db.reorder_after(conn, a, e)   # b c d e a
    db.reorder_before(conn, d, b)  # d b c e a
    db.reorder_after(conn, c, a)   # d b e a c

    queue = db.get_queue(conn)
    ids = [j["id"] for j in queue]
    assert ids == [d, b, e, a, c]
    # positions must be contiguous 0..n-1 regardless of how much reordering happened
    assert [j["position"] for j in queue] == list(range(5))


def test_positions_stay_contiguous_after_interleaved_removals_and_adds(conn):
    a, b, c = _mk(conn, "a"), _mk(conn, "b"), _mk(conn, "c")
    db.remove_from_queue(conn, b)
    d = _mk(conn, "d")
    db.remove_from_queue(conn, a)
    e = _mk(conn, "e", before=d)

    queue = db.get_queue(conn)
    assert [j["id"] for j in queue] == [c, e, d]
    assert [j["position"] for j in queue] == [0, 1, 2]


# ---------------------------------------------------------------------------
# channels: registry
# ---------------------------------------------------------------------------

def test_list_channels_empty_before_any_job(conn):
    assert db.list_channels(conn) == []


def test_channel_appears_only_after_first_job_lands_on_it(conn):
    _mk(conn, "a", channel=1)
    assert db.list_channels(conn) == [1]
    _mk(conn, "b", channel=2)
    assert db.list_channels(conn) == [1, 2]


def test_list_channels_no_duplicate_on_repeated_use(conn):
    _mk(conn, "a", channel=1)
    _mk(conn, "b", channel=1)
    _mk(conn, "c", channel=1)
    assert db.list_channels(conn) == [1]


def test_list_channels_sorted_regardless_of_creation_order(conn):
    _mk(conn, "a", channel=3)
    _mk(conn, "b", channel=1)
    _mk(conn, "c", channel=2)
    assert db.list_channels(conn) == [1, 2, 3]


def test_register_channel_idempotent(conn):
    db.register_channel(conn, 5)
    db.register_channel(conn, 5)
    assert db.list_channels(conn) == [5]


def test_default_channel_is_one(conn):
    a = _mk(conn, "a")  # no channel kwarg override at the enqueue() level
    job = db.get_job(conn, a)
    assert job["channel"] == 1


# ---------------------------------------------------------------------------
# channels: isolation of queue / running / backlog / position
# ---------------------------------------------------------------------------

def test_queues_isolated_between_channels(conn):
    a1 = _mk(conn, "a1", channel=1)
    b1 = _mk(conn, "b1", channel=1)
    a2 = _mk(conn, "a2", channel=2)

    assert [j["id"] for j in db.get_queue(conn, channel=1)] == [a1, b1]
    assert [j["id"] for j in db.get_queue(conn, channel=2)] == [a2]


def test_positions_independent_per_channel(conn):
    _mk(conn, "a1", channel=1)
    _mk(conn, "b1", channel=1)
    only2 = _mk(conn, "a2", channel=2)

    ch2_queue = db.get_queue(conn, channel=2)
    assert ch2_queue[0]["id"] == only2
    assert ch2_queue[0]["position"] == 0  # not offset by channel 1's jobs


def test_running_isolated_between_channels(conn):
    a1 = _mk(conn, "a1", channel=1)
    a2 = _mk(conn, "a2", channel=2)
    db.mark_running(conn, a1, pid=1, pgid=1)

    assert db.get_running(conn, channel=1)["id"] == a1
    assert db.get_running(conn, channel=2) is None


def test_mark_running_only_reindexes_its_own_channel(conn):
    a1, b1 = _mk(conn, "a1", channel=1), _mk(conn, "b1", channel=1)
    a2 = _mk(conn, "a2", channel=2)

    db.mark_running(conn, a1, pid=1, pgid=1)

    ch1_queue = db.get_queue(conn, channel=1)
    assert [j["id"] for j in ch1_queue] == [b1]
    assert ch1_queue[0]["position"] == 0

    ch2_queue = db.get_queue(conn, channel=2)
    assert [j["id"] for j in ch2_queue] == [a2]
    assert ch2_queue[0]["position"] == 0  # untouched by channel 1's reindex


def test_backlog_isolated_between_channels(conn):
    a1 = _mk(conn, "a1", channel=1)
    db.mark_running(conn, a1, pid=1, pgid=1)
    db.mark_finished(conn, a1, status="done", exit_code=0)

    a2 = _mk(conn, "a2", channel=2)
    db.mark_running(conn, a2, pid=1, pgid=1)
    db.mark_finished(conn, a2, status="done", exit_code=0)

    assert [j["id"] for j in db.get_backlog(conn, channel=1)] == [a1]
    assert [j["id"] for j in db.get_backlog(conn, channel=2)] == [a2]


def test_backlog_eviction_caps_at_ten_per_channel_independently(conn):
    ch1_ids = [_mk(conn, f"c1-{i}", channel=1) for i in range(12)]
    for job_id in ch1_ids:
        db.mark_running(conn, job_id, pid=1, pgid=1)
        db.mark_finished(conn, job_id, status="done", exit_code=0)

    ch2_ids = [_mk(conn, f"c2-{i}", channel=2) for i in range(3)]
    for job_id in ch2_ids:
        db.mark_running(conn, job_id, pid=1, pgid=1)
        db.mark_finished(conn, job_id, status="done", exit_code=0)

    ch1_backlog = db.get_backlog(conn, channel=1, limit=100)
    ch2_backlog = db.get_backlog(conn, channel=2, limit=100)
    assert len(ch1_backlog) == 10  # capped
    assert len(ch2_backlog) == 3   # unaffected by channel 1's eviction


def test_remove_from_queue_only_reindexes_its_own_channel(conn):
    a1, b1 = _mk(conn, "a1", channel=1), _mk(conn, "b1", channel=1)
    a2, b2 = _mk(conn, "a2", channel=2), _mk(conn, "b2", channel=2)

    db.remove_from_queue(conn, a1)

    assert [j["id"] for j in db.get_queue(conn, channel=1)] == [b1]
    ch2_queue = db.get_queue(conn, channel=2)
    assert [j["id"] for j in ch2_queue] == [a2, b2]
    assert [j["position"] for j in ch2_queue] == [0, 1]  # untouched


# ---------------------------------------------------------------------------
# channels: before/after and reorder do not cross channels
# ---------------------------------------------------------------------------

def test_before_id_from_other_channel_raises(conn):
    a2 = _mk(conn, "a2", channel=2)
    with pytest.raises(ValueError):
        _mk(conn, "x1", channel=1, before=a2)


def test_after_id_from_other_channel_raises(conn):
    a2 = _mk(conn, "a2", channel=2)
    with pytest.raises(ValueError):
        _mk(conn, "x1", channel=1, after=a2)


def test_reorder_target_from_other_channel_raises(conn):
    a1 = _mk(conn, "a1", channel=1)
    a2 = _mk(conn, "a2", channel=2)
    with pytest.raises(ValueError):
        db.reorder_before(conn, a1, a2)
    with pytest.raises(ValueError):
        db.reorder_after(conn, a1, a2)


def test_reorder_within_one_channel_does_not_disturb_another(conn):
    a1, b1, c1 = _mk(conn, "a1", channel=1), _mk(conn, "b1", channel=1), _mk(conn, "c1", channel=1)
    a2, b2 = _mk(conn, "a2", channel=2), _mk(conn, "b2", channel=2)

    db.reorder_before(conn, c1, a1)

    assert [j["id"] for j in db.get_queue(conn, channel=1)] == [c1, a1, b1]
    assert [j["id"] for j in db.get_queue(conn, channel=2)] == [a2, b2]


# ---------------------------------------------------------------------------
# channels: requeue preserves channel
# ---------------------------------------------------------------------------

def test_requeue_preserves_original_channel(conn):
    a2 = _mk(conn, "a2", channel=2)
    db.mark_running(conn, a2, pid=1, pgid=1)
    db.mark_finished(conn, a2, status="failed", exit_code=1)

    new_id = db.requeue(conn, a2, log_path="/tmp/retry.log")
    new_job = db.get_job(conn, new_id)
    assert new_job["channel"] == 2
    assert [j["id"] for j in db.get_queue(conn, channel=2)] == [new_id]
    assert db.get_queue(conn, channel=1) == []


# ---------------------------------------------------------------------------
# channels: job ids are globally unique, never reused per-channel
# ---------------------------------------------------------------------------

def test_job_ids_global_not_per_channel(conn):
    a1 = _mk(conn, "a1", channel=1)
    a2 = _mk(conn, "a2", channel=2)
    b1 = _mk(conn, "b1", channel=1)
    # ids strictly increase across channels -- channel 2 does not restart at 1
    assert a1 < a2 < b1


# ---------------------------------------------------------------------------
# channels: paused state
# ---------------------------------------------------------------------------

def test_paused_defaults_false_per_channel(conn):
    assert db.get_paused(conn, channel=1) is False
    assert db.get_paused(conn, channel=2) is False


def test_pause_scoped_to_one_channel(conn):
    db.set_paused(conn, True, channel=2)
    assert db.get_paused(conn, channel=1) is False
    assert db.get_paused(conn, channel=2) is True


def test_pause_toggle_independent_per_channel(conn):
    db.set_paused(conn, True, channel=1)
    db.set_paused(conn, True, channel=2)
    db.set_paused(conn, False, channel=1)
    assert db.get_paused(conn, channel=1) is False
    assert db.get_paused(conn, channel=2) is True


# ---------------------------------------------------------------------------
# priority lane (`--now`): is_priority queue partition, stopped/running toggle
# ---------------------------------------------------------------------------

def test_priority_job_does_not_appear_in_normal_queue(conn):
    a = _mk(conn, "a")
    p = _mk(conn, "p", is_priority=True)
    assert [j["id"] for j in db.get_queue(conn)] == [a]
    assert [j["id"] for j in db.get_queue(conn, is_priority=True)] == [p]


def test_priority_lane_has_its_own_position_sequence(conn):
    p1 = _mk(conn, "p1", is_priority=True)
    p2 = _mk(conn, "p2", is_priority=True)
    queue = db.get_queue(conn, is_priority=True)
    assert [j["id"] for j in queue] == [p1, p2]
    assert [j["position"] for j in queue] == [0, 1]


def test_priority_lane_scoped_per_channel(conn):
    p1 = _mk(conn, "p1", channel=1, is_priority=True)
    p2 = _mk(conn, "p2", channel=2, is_priority=True)
    assert [j["id"] for j in db.get_queue(conn, channel=1, is_priority=True)] == [p1]
    assert [j["id"] for j in db.get_queue(conn, channel=2, is_priority=True)] == [p2]


def test_mark_stopped_then_resumed_round_trip(conn):
    a = _mk(conn, "a")
    db.mark_running(conn, a, pid=1, pgid=1)
    db.mark_stopped(conn, a)
    assert db.get_job(conn, a)["status"] == "stopped"
    assert db.get_running(conn) is None
    assert db.get_stopped(conn)["id"] == a

    db.mark_resumed(conn, a)
    assert db.get_job(conn, a)["status"] == "running"
    assert db.get_running(conn)["id"] == a
    assert db.get_stopped(conn) is None


def test_mark_stopped_requires_running(conn):
    a = _mk(conn, "a")  # still queued
    with pytest.raises(ValueError):
        db.mark_stopped(conn, a)


def test_mark_resumed_requires_stopped(conn):
    a = _mk(conn, "a")
    db.mark_running(conn, a, pid=1, pgid=1)
    with pytest.raises(ValueError):
        db.mark_resumed(conn, a)  # still running, not stopped


def test_mark_running_reindexes_only_its_own_priority_partition(conn):
    a1 = _mk(conn, "a1")
    a2 = _mk(conn, "a2")
    p1 = _mk(conn, "p1", is_priority=True)
    db.mark_running(conn, a1, pid=1, pgid=1)
    # normal queue reindexed (a1 gone), priority lane untouched
    assert [j["id"] for j in db.get_queue(conn)] == [a2]
    assert [j["position"] for j in db.get_queue(conn)] == [0]
    assert [j["id"] for j in db.get_queue(conn, is_priority=True)] == [p1]
    assert [j["position"] for j in db.get_queue(conn, is_priority=True)] == [0]


def test_stopped_status_survives_schema_migration(tmp_path):
    """A DB written by the pre-priority-lane schema (no is_priority
    column, no 'stopped' in the CHECK) upgrades cleanly and existing
    rows are preserved with is_priority=0."""
    import sqlite3

    path = tmp_path / "old.db"
    old_conn = sqlite3.connect(path)
    old_conn.executescript(
        """
        CREATE TABLE jobs (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            channel       INTEGER NOT NULL DEFAULT 1,
            status        TEXT NOT NULL CHECK (status IN
                            ('queued','running','done','failed','cancelled')),
            position      INTEGER,
            raw_cmd       TEXT NOT NULL,
            resolved_cmd  TEXT NOT NULL,
            cwd           TEXT NOT NULL,
            env_extra     TEXT,
            pid           INTEGER,
            pgid          INTEGER,
            enqueued_at   TEXT NOT NULL,
            started_at    TEXT,
            finished_at   TEXT,
            exit_code     INTEGER,
            log_path      TEXT NOT NULL,
            note          TEXT,
            timeout_secs  INTEGER
        );
        """
    )
    old_conn.execute(
        """
        INSERT INTO jobs (channel, status, position, raw_cmd, resolved_cmd, cwd,
                           enqueued_at, log_path)
        VALUES (1, 'queued', 0, '["a"]', '["/usr/bin/a"]', '/tmp', '2026-01-01', '/tmp/a.log')
        """
    )
    old_conn.commit()
    old_conn.close()

    conn = db.connect(path)
    db.init_db(conn)
    job = db.get_job(conn, 1)
    assert job["status"] == "queued"
    assert job["is_priority"] == 0
    # new status value now accepted by the CHECK constraint
    db.mark_running(conn, 1, pid=1, pgid=1)
    db.mark_stopped(conn, 1)
    assert db.get_job(conn, 1)["status"] == "stopped"
    conn.close()


# ---------------------------------------------------------------------------
# clear_backlog
# ---------------------------------------------------------------------------


def _finish(conn, job_id, status="done"):
    db.mark_running(conn, job_id, pid=1, pgid=1)
    db.mark_finished(conn, job_id, status=status)


def test_clear_backlog_removes_finished_jobs_only(conn):
    a = _mk(conn, "a")
    b = _mk(conn, "b")
    queued = _mk(conn, "c")  # left queued -- must survive
    _finish(conn, a, status="done")
    _finish(conn, b, status="failed")

    deleted = db.clear_backlog(conn)

    assert deleted == 2
    assert db.get_backlog(conn) == []
    assert [j["id"] for j in db.get_queue(conn)] == [queued]


def test_clear_backlog_scoped_to_one_channel(conn):
    a = _mk(conn, "a", channel=1)
    b = _mk(conn, "b", channel=2)
    _finish(conn, a, status="done")
    _finish(conn, b, status="done")

    deleted = db.clear_backlog(conn, channel=1)

    assert deleted == 1
    assert db.get_backlog(conn, channel=1) == []
    assert len(db.get_backlog(conn, channel=2)) == 1


def test_clear_backlog_empty_is_a_noop(conn):
    assert db.clear_backlog(conn) == 0


def test_clear_backlog_prunes_empty_channels_except_default(conn):
    a = _mk(conn, "a", channel=1)
    b = _mk(conn, "b", channel=2)
    c = _mk(conn, "c", channel=3)
    keep = _mk(conn, "d", channel=4)  # stays queued -- channel 4 must survive
    _finish(conn, a)
    _finish(conn, b)
    _finish(conn, c)

    db.clear_backlog(conn)

    assert db.list_channels(conn) == [1, 4]
    assert [j["id"] for j in db.get_queue(conn, channel=4)] == [keep]


def test_clear_backlog_keeps_default_channel_when_empty(conn):
    a = _mk(conn, "a", channel=1)
    _finish(conn, a)

    db.clear_backlog(conn)

    assert db.list_channels(conn) == [1]


def test_channel_reappears_after_prune_when_reused(conn):
    a = _mk(conn, "a", channel=2)
    _finish(conn, a)
    db.clear_backlog(conn)
    assert db.list_channels(conn) == []

    _mk(conn, "b", channel=2)

    assert db.list_channels(conn) == [2]


# ---------------------------------------------------------------------------
# auto-retry columns: max_retries / attempt
# ---------------------------------------------------------------------------

def test_enqueue_retry_fields_default_to_zero(conn):
    a = _mk(conn, "a")
    job = db.get_job(conn, a)
    assert job["max_retries"] == 0
    assert job["attempt"] == 0


def test_enqueue_stores_max_retries(conn):
    a = _mk(conn, "a", max_retries=3)
    job = db.get_job(conn, a)
    assert job["max_retries"] == 3
    assert job["attempt"] == 0


def test_manual_requeue_keeps_max_retries_and_resets_attempt(conn):
    a = _mk(conn, "a", max_retries=3, attempt=2)
    db.mark_running(conn, a, pid=1, pgid=1)
    db.mark_finished(conn, a, status="failed", exit_code=1)
    new_id = db.requeue(conn, a, log_path="/tmp/x.log")
    new_job = db.get_job(conn, new_id)
    assert new_job["max_retries"] == 3
    assert new_job["attempt"] == 0


def test_auto_requeue_increments_attempt(conn):
    a = _mk(conn, "a", max_retries=3)
    db.mark_running(conn, a, pid=1, pgid=1)
    db.mark_finished(conn, a, status="failed", exit_code=1)
    new_id = db.requeue(conn, a, log_path="/tmp/x.log", auto=True)
    new_job = db.get_job(conn, new_id)
    assert new_job["attempt"] == 1
    assert new_job["max_retries"] == 3


def test_fresh_job_has_no_origin(conn):
    job = db.get_job(conn, _mk(conn, "a"))
    assert job["origin"] is None
    assert job["parent_id"] is None


def test_manual_requeue_records_origin(conn):
    a = _mk(conn, "a")
    db.mark_running(conn, a, pid=1, pgid=1)
    db.mark_finished(conn, a, status="failed", exit_code=1)
    job = db.get_job(conn, db.requeue(conn, a, log_path="/tmp/x.log"))
    assert (job["origin"], job["parent_id"]) == ("requeue", a)


def test_auto_requeue_records_origin(conn):
    a = _mk(conn, "a", max_retries=1)
    db.mark_running(conn, a, pid=1, pgid=1)
    db.mark_finished(conn, a, status="failed", exit_code=1)
    job = db.get_job(conn, db.requeue(conn, a, log_path="/tmp/x.log", auto=True))
    assert (job["origin"], job["parent_id"]) == ("retry", a)


def test_init_db_migrates_pre_retry_database(tmp_path):
    """A DB created before auto-retry existed gains the new columns, keeps
    its rows, and the new columns default to 0."""
    import sqlite3

    path = tmp_path / "old.db"
    raw = sqlite3.connect(path)
    raw.executescript(
        """
        CREATE TABLE jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel INTEGER NOT NULL DEFAULT 1,
            status TEXT NOT NULL CHECK (status IN
                ('queued','running','stopped','done','failed','cancelled')),
            position INTEGER, raw_cmd TEXT NOT NULL, resolved_cmd TEXT NOT NULL,
            cwd TEXT NOT NULL, env_extra TEXT, pid INTEGER, pgid INTEGER,
            enqueued_at TEXT NOT NULL, started_at TEXT, finished_at TEXT,
            exit_code INTEGER, log_path TEXT NOT NULL, note TEXT,
            timeout_secs INTEGER, is_priority INTEGER NOT NULL DEFAULT 0
        );
        INSERT INTO jobs (status, position, raw_cmd, resolved_cmd, cwd, enqueued_at, log_path)
        VALUES ('queued', 0, '["a"]', '["/usr/bin/a"]', '/tmp', '2026-01-01T00:00:00+00:00', '/tmp/1.log');
        """
    )
    raw.commit()
    raw.close()

    c = db.connect(path)
    db.init_db(c)
    db.init_db(c)  # second run is a no-op
    job = db.get_job(c, 1)
    assert job["raw_cmd"] == '["a"]'
    assert job["max_retries"] == 0
    assert job["attempt"] == 0
    assert job["origin"] is None
    assert job["parent_id"] is None
    c.close()


# ---------------------------------------------------------------------------
# cancel_queued
# ---------------------------------------------------------------------------

def test_cancel_queued_keeps_job_in_history_as_cancelled(conn):
    a, b = _mk(conn, "a"), _mk(conn, "b")
    db.cancel_queued(conn, b)
    job = db.get_job(conn, b)
    assert job["status"] == "cancelled"
    assert job["note"] == "cancelled while queued"
    assert job["finished_at"] is not None and job["position"] is None
    assert [j["id"] for j in db.get_queue(conn)] == [a]
    assert b in [j["id"] for j in db.get_backlog(conn)]


def test_cancel_queued_reindexes_remaining_queue(conn):
    a, b, c = _mk(conn, "a"), _mk(conn, "b"), _mk(conn, "c")
    db.cancel_queued(conn, a)
    assert [(j["id"], j["position"]) for j in db.get_queue(conn)] == [(b, 0), (c, 1)]


def test_cancel_queued_only_touches_its_own_lane_and_channel(conn):
    a = _mk(conn, "a", channel=1)
    b = _mk(conn, "b", channel=2)
    p1 = _mk(conn, "p1", channel=1, is_priority=True)
    p2 = _mk(conn, "p2", channel=1, is_priority=True)
    db.cancel_queued(conn, p1)
    assert [(j["id"], j["position"]) for j in db.get_queue(conn, 1, is_priority=True)] == [(p2, 0)]
    assert [j["id"] for j in db.get_queue(conn, 1)] == [a]
    assert [j["id"] for j in db.get_queue(conn, 2)] == [b]


def test_cancel_queued_rejects_missing_running_and_finished(conn):
    a = _mk(conn, "a")
    with pytest.raises(ValueError):
        db.cancel_queued(conn, 999)
    db.mark_running(conn, a, pid=1, pgid=1)
    with pytest.raises(ValueError):
        db.cancel_queued(conn, a)
    db.mark_finished(conn, a, status="done", exit_code=0)
    with pytest.raises(ValueError):
        db.cancel_queued(conn, a)


# ---------------------------------------------------------------------------
# round 3: recent_requeue (Telegram double-tap guard)
# ---------------------------------------------------------------------------

def test_recent_requeue_sees_a_fresh_manual_requeue_only(conn):
    a = _mk(conn, "a")
    db.mark_finished(conn, a, status="failed", exit_code=1)
    assert db.recent_requeue(conn, a, 30) is None
    manual = db.requeue(conn, a, log_path="/tmp/x.log")
    assert db.recent_requeue(conn, a, 30) == manual
    conn.execute("UPDATE jobs SET enqueued_at = ? WHERE id = ?", ("2000-01-01T00:00:00+00:00", manual))
    conn.commit()
    assert db.recent_requeue(conn, a, 30) is None  # old enough: a new press is a new request


def test_recent_requeue_ignores_auto_retries_and_other_jobs(conn):
    a, b = _mk(conn, "a"), _mk(conn, "b")
    db.mark_finished(conn, a, status="failed", exit_code=1)
    db.mark_finished(conn, b, status="failed", exit_code=1)
    db.requeue(conn, a, log_path="/tmp/x.log", auto=True)
    assert db.recent_requeue(conn, a, 30) is None
    db.requeue(conn, b, log_path="/tmp/y.log")
    assert db.recent_requeue(conn, a, 30) is None


# ---------------------------------------------------------------------------
# B5: interrupted status
# ---------------------------------------------------------------------------

def test_mark_finished_accepts_interrupted_and_it_shows_in_backlog(conn):
    a = _mk(conn, "a")
    db.mark_running(conn, a, pid=1, pgid=1)
    db.mark_finished(conn, a, status="interrupted", note="daemon stopped")
    job = db.get_job(conn, a)
    assert job["status"] == "interrupted" and job["note"] == "daemon stopped"
    assert [j["id"] for j in db.get_backlog(conn)] == [a]
    assert db.get_running(conn) is None


def test_interrupted_job_can_be_requeued_and_cleared(conn):
    a = _mk(conn, "a", timeout_secs=30)
    db.mark_running(conn, a, pid=1, pgid=1)
    db.mark_finished(conn, a, status="interrupted", note="daemon stopped")
    new_id = db.requeue(conn, a, log_path="/tmp/a2.log")
    assert db.get_job(conn, new_id)["status"] == "queued"
    assert db.get_job(conn, new_id)["timeout_secs"] == 30
    assert db.clear_backlog(conn) == 1  # the interrupted row is history like any other
    assert db.get_job(conn, a) is None


# ---------------------------------------------------------------------------
# F2: dependencies
# ---------------------------------------------------------------------------

def _finish(conn, job_id, status="done", exit_code=0):
    db.mark_running(conn, job_id, pid=1, pgid=1)
    db.mark_finished(conn, job_id, status=status, exit_code=exit_code)


def test_enqueue_stores_dependencies_and_dep_info_reports_them(conn):
    a, b = _mk(conn, "a"), _mk(conn, "b")
    c = _mk(conn, "c", deps={a: "success", b: "any"})
    info = db.dep_info(conn, c)
    assert info == {"depends_on": [a], "after_any": [b], "waiting_on": [a, b]}
    _finish(conn, a)
    assert db.dep_info(conn, c)["waiting_on"] == [b]  # a has ended; b has not
    assert db.dep_info(conn, a) == {"depends_on": [], "after_any": [], "waiting_on": []}


def test_enqueue_rejects_unknown_dependency_and_writes_nothing(conn):
    _mk(conn, "a")
    with pytest.raises(ValueError, match="does not exist"):
        _mk(conn, "b", deps={999: "success"})
    assert len(db.get_queue(conn)) == 1
    assert conn.execute("SELECT COUNT(*) FROM job_deps").fetchone()[0] == 0


def test_enqueue_rejects_bad_dependency_mode_and_priority_with_deps(conn):
    a = _mk(conn, "a")
    with pytest.raises(ValueError, match="mode"):
        _mk(conn, "b", deps={a: "sometimes"})
    with pytest.raises(ValueError, match="--now"):
        _mk(conn, "c", deps={a: "success"}, is_priority=True)


def test_enqueue_rejects_dependency_on_a_job_queued_behind_it(conn):
    a, b = _mk(conn, "a"), _mk(conn, "b")
    with pytest.raises(ValueError, match="behind"):
        _mk(conn, "c", before=a, deps={b: "success"})  # c would sit ahead of b, waiting for it
    assert [j["id"] for j in db.get_queue(conn)] == [a, b]  # rolled back: the queue is untouched
    assert conn.execute("SELECT COUNT(*) FROM job_deps").fetchone()[0] == 0


def test_enqueue_dependency_on_an_earlier_job_in_the_same_channel_is_fine(conn):
    a = _mk(conn, "a")
    b = _mk(conn, "b", deps={a: "success"})
    assert [j["id"] for j in db.get_queue(conn)] == [a, b]


def test_enqueue_rejects_a_cycle_through_other_channels(conn):
    a = _mk(conn, "a")                                   # ch1: a
    b = _mk(conn, "b", channel=2, deps={a: "success"})   # ch2: b waits for a
    with pytest.raises(ValueError, match="deadlock"):
        # c goes in front of a on ch1 and waits for b: c -> b -> a -> c
        _mk(conn, "c", before=a, deps={b: "any"})
    assert [j["id"] for j in db.get_queue(conn)] == [a]
    assert [j["id"] for j in db.get_queue(conn, channel=2)] == [b]


def test_reorder_that_would_deadlock_is_rejected_and_changes_nothing(conn):
    a = _mk(conn, "a")
    b = _mk(conn, "b", deps={a: "success"})
    with pytest.raises(ValueError, match="deadlock"):
        db.reorder_before(conn, b, a)  # b would sit ahead of the job it waits for
    assert [j["id"] for j in db.get_queue(conn)] == [a, b]
    assert [j["position"] for j in db.get_queue(conn)] == [0, 1]


def test_dep_verdict_ready_waiting_failed_removed(conn):
    a, b = _mk(conn, "a"), _mk(conn, "b", channel=2)
    plain = _mk(conn, "plain")
    assert db.dep_verdict(conn, plain) == ("ready", None)
    c = _mk(conn, "c", channel=3, deps={a: "success", b: "any"})
    assert db.dep_verdict(conn, c) == ("waiting", a)
    _finish(conn, a)
    assert db.dep_verdict(conn, c) == ("waiting", b)
    _finish(conn, b, status="failed", exit_code=1)
    assert db.dep_verdict(conn, c) == ("ready", None)  # `any` is satisfied by a failure

    x = _mk(conn, "x")
    y = _mk(conn, "y", channel=4, deps={x: "success"})
    _finish(conn, x, status="failed", exit_code=1)
    assert db.dep_verdict(conn, y) == ("failed", x)
    conn.execute("DELETE FROM jobs WHERE id = ?", (x,))
    assert db.dep_verdict(conn, y) == ("removed", x)


@pytest.mark.parametrize("status", ["failed", "cancelled", "skipped", "interrupted"])
def test_depends_on_needs_done_every_other_end_fails_it(conn, status):
    a = _mk(conn, "a")
    b = _mk(conn, "b", channel=2, deps={a: "success"})
    db.mark_running(conn, a, pid=1, pgid=1)
    db.mark_finished(conn, a, status=status)
    assert db.dep_verdict(conn, b) == ("failed", a)


def test_skip_dependants_skips_and_cascades(conn):
    a = _mk(conn, "a")
    b = _mk(conn, "b", deps={a: "success"})
    c = _mk(conn, "c", deps={b: "success"})
    d = _mk(conn, "d", deps={a: "any"})
    e = _mk(conn, "e")
    _finish(conn, a, status="failed", exit_code=1)
    skipped, channels = db.skip_dependants(conn, a)
    assert skipped == [b, c]
    assert channels == {1}
    assert db.get_job(conn, b)["status"] == "skipped"
    assert db.get_job(conn, b)["note"] == f"dependency {a} failed"
    assert db.get_job(conn, c)["note"] == f"dependency {b} failed"
    assert db.get_job(conn, d)["status"] == "queued"  # --after-any: any end is fine
    assert [j["id"] for j in db.get_queue(conn)] == [d, e]  # the gap is closed
    assert [j["position"] for j in db.get_queue(conn)] == [0, 1]


def test_skip_dependants_leaves_a_satisfied_dependant_alone(conn):
    a = _mk(conn, "a")
    b = _mk(conn, "b", channel=2, deps={a: "success"})
    _finish(conn, a)
    assert db.skip_dependants(conn, a) == ([], {2})
    assert db.get_job(conn, b)["status"] == "queued"


def test_remove_from_queue_refuses_a_job_others_depend_on(conn):
    a = _mk(conn, "a")
    b = _mk(conn, "b", deps={a: "success"})
    c = _mk(conn, "c", deps={a: "any"})
    with pytest.raises(ValueError) as exc:
        db.remove_from_queue(conn, a)
    assert str(b) in str(exc.value) and str(c) in str(exc.value)
    assert db.get_job(conn, a)["status"] == "queued"
    db.remove_from_queue(conn, c)  # nothing depends on c: fine, and its dependency rows go with it
    assert conn.execute("SELECT COUNT(*) FROM job_deps WHERE job_id = ?", (c,)).fetchone()[0] == 0
    db.remove_from_queue(conn, b)
    db.remove_from_queue(conn, a)


def test_queued_dependants_lists_only_queued_ones(conn):
    a = _mk(conn, "a")
    b = _mk(conn, "b", channel=2, deps={a: "success"})
    c = _mk(conn, "c", channel=3, deps={a: "any"})
    _finish(conn, c, status="done")  # c can't have started before a, but the query only looks at status
    assert db.queued_dependants(conn, a) == [b]


def test_finished_dependency_of_a_queued_job_is_never_pruned_or_cleared(conn):
    a = _mk(conn, "a")
    b = _mk(conn, "b", channel=2, deps={a: "success"})
    _finish(conn, a)
    for i in range(12):  # more history than the 10 kept per channel
        x = _mk(conn, f"x{i}")
        _finish(conn, x)
    assert db.get_job(conn, a) is not None  # pruned history would have lost a
    db.clear_backlog(conn)
    assert db.get_job(conn, a) is not None
    assert db.dep_verdict(conn, b) == ("ready", None)
    _finish(conn, b)
    db.clear_backlog(conn)
    assert db.get_job(conn, a) is None
    assert conn.execute("SELECT COUNT(*) FROM job_deps").fetchone()[0] == 0


def test_requeue_does_not_copy_dependencies(conn):
    a = _mk(conn, "a")
    b = _mk(conn, "b", channel=2, deps={a: "success"})
    _finish(conn, a)
    _finish(conn, b)
    new_id = db.requeue(conn, b, log_path="/tmp/b2.log")
    assert db.dep_info(conn, new_id) == {"depends_on": [], "after_any": [], "waiting_on": []}


def test_init_db_migrates_a_pre_interrupted_database_and_keeps_ids(tmp_path):
    """The `jobs` table of a DB from before dependencies has a CHECK without
    `interrupted`/`skipped` and no `not_before`/`on_fail`: it is rebuilt in one
    transaction, rows and the id counter survive, and a second run is a no-op."""
    import sqlite3

    path = tmp_path / "old.db"
    raw = sqlite3.connect(path)
    raw.executescript(
        """
        CREATE TABLE jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel INTEGER NOT NULL DEFAULT 1,
            status TEXT NOT NULL CHECK (status IN
                ('queued','running','stopped','done','failed','cancelled')),
            position INTEGER, raw_cmd TEXT NOT NULL, resolved_cmd TEXT NOT NULL,
            cwd TEXT NOT NULL, env_extra TEXT, pid INTEGER, pgid INTEGER,
            enqueued_at TEXT NOT NULL, started_at TEXT, finished_at TEXT,
            exit_code INTEGER, log_path TEXT NOT NULL, note TEXT,
            timeout_secs INTEGER, is_priority INTEGER NOT NULL DEFAULT 0,
            max_retries INTEGER NOT NULL DEFAULT 0, attempt INTEGER NOT NULL DEFAULT 0,
            parent_id INTEGER, origin TEXT
        );
        INSERT INTO jobs (status, position, raw_cmd, resolved_cmd, cwd, enqueued_at, log_path)
        VALUES ('queued', 0, '["a"]', '["/usr/bin/a"]', '/tmp', '2026-01-01T00:00:00+00:00', '/tmp/1.log');
        INSERT INTO jobs (status, raw_cmd, resolved_cmd, cwd, enqueued_at, log_path, finished_at)
        VALUES ('done', '["b"]', '["/usr/bin/b"]', '/tmp', '2026-01-01T00:00:00+00:00', '/tmp/2.log', '2026-01-01T00:00:01+00:00');
        DELETE FROM jobs WHERE id = 2;
        """
    )
    raw.commit()
    raw.close()

    c = db.connect(path)
    db.init_db(c)
    db.init_db(c)  # second run: nothing to do
    assert db.get_job(c, 1)["raw_cmd"] == '["a"]'
    assert db.get_job(c, 1)["on_fail"] == "continue" and db.get_job(c, 1)["not_before"] is None
    new_id = _mk(c, "n")
    assert new_id == 3  # id 2 was used before, so AUTOINCREMENT must not hand it out again
    db.mark_running(c, new_id, pid=1, pgid=1)
    db.mark_finished(c, new_id, status="interrupted", note="daemon stopped")  # the new CHECK allows it
    dep = _mk(c, "dep", deps={1: "success"})
    assert db.dep_info(c, dep)["depends_on"] == [1]
    c.close()


# ---------------------------------------------------------------------------
# F6: --on-fail
# ---------------------------------------------------------------------------

def test_on_fail_defaults_to_continue_and_is_stored(conn):
    a = _mk(conn, "a")
    b = _mk(conn, "b", on_fail="pause")
    assert db.get_job(conn, a)["on_fail"] == "continue"
    assert db.get_job(conn, b)["on_fail"] == "pause"


def test_on_fail_rejects_unknown_mode_and_writes_nothing(conn):
    with pytest.raises(ValueError, match="on_fail"):
        _mk(conn, "a", on_fail="stop")
    assert db.get_queue(conn) == []


def test_requeue_keeps_on_fail(conn):
    a = _mk(conn, "a", on_fail="pause")
    _finish(conn, a, status="failed", exit_code=1)
    new_id = db.requeue(conn, a, log_path="/tmp/a2.log")
    assert db.get_job(conn, new_id)["on_fail"] == "pause"


# ---------------------------------------------------------------------------
# F7: not_before (delayed start)
# ---------------------------------------------------------------------------

def test_enqueue_stores_not_before_and_requeue_does_not_copy_it(conn):
    a = _mk(conn, "a", not_before="2030-01-01T12:00:00+00:00")
    assert db.get_job(conn, a)["not_before"] == "2030-01-01T12:00:00+00:00"
    assert db.get_queue(conn)[0]["not_before"] == "2030-01-01T12:00:00+00:00"
    assert db.get_job(conn, _mk(conn, "b"))["not_before"] is None
    db.cancel_queued(conn, a)
    new_id = db.requeue(conn, a, log_path="/tmp/a2.log")
    assert db.get_job(conn, new_id)["not_before"] is None


def test_enqueue_rejects_not_before_on_the_priority_lane(conn):
    with pytest.raises(ValueError):
        _mk(conn, "a", is_priority=True, not_before="2030-01-01T12:00:00+00:00")
    assert db.get_queue(conn, is_priority=True) == []


# ---------------------------------------------------------------------------
# F9/U3: history that scales + filters
# ---------------------------------------------------------------------------

def _fin(conn, job_id, status="done", keep=200):
    db.mark_running(conn, job_id, pid=1, pgid=1)
    db.mark_finished(conn, job_id, status=status, exit_code=0 if status == "done" else 1, keep_backlog=keep)


def _set_finished(conn, job_id, when):
    conn.execute("UPDATE jobs SET finished_at = ? WHERE id = ?", (when, job_id))
    conn.commit()


def test_daemon_default_keep_is_200_and_bare_default_is_unchanged(conn):
    assert db.DEFAULT_KEEP == 200 and db.LIST_BACKLOG == 10
    ids = [_mk(conn, f"j{i}") for i in range(30)]
    for i in ids:
        _fin(conn, i)  # keep=200
    assert len(db.get_history(conn, limit=1000)) == 30
    assert len(db.get_backlog(conn)) == 10  # the short list view stays at 10
    assert len(db.get_backlog(conn, limit=100)) == 30


def test_keep_limit_is_per_channel_and_prunes_the_oldest(conn):
    a = [_mk(conn, f"a{i}", channel=1) for i in range(8)]
    b = [_mk(conn, f"b{i}", channel=2) for i in range(2)]
    for i in a + b:
        _fin(conn, i, keep=5)
    kept_a = {j["id"] for j in db.get_history(conn, channel=1, limit=100)}
    assert kept_a == set(a[-5:])
    assert {j["id"] for j in db.get_history(conn, channel=2, limit=100)} == set(b)


def test_keep_applies_to_cancelled_and_skipped_queued_jobs_too(conn):
    ids = [_mk(conn, f"c{i}") for i in range(6)]
    for i in ids:
        db.cancel_queued(conn, i, keep_backlog=3)
    assert {j["id"] for j in db.get_history(conn, limit=100)} == set(ids[-3:])
    a = _mk(conn, "a")
    b = _mk(conn, "b", deps={a: "success"})
    _fin(conn, a, status="failed", keep=3)
    skipped, _ = db.skip_dependants(conn, a, keep_backlog=3)
    assert skipped == [b]
    assert len(db.get_history(conn, limit=100)) == 3


def test_a_dependency_of_a_queued_job_is_kept_beyond_the_limit(conn):
    a = _mk(conn, "a")
    b = _mk(conn, "b", channel=2, deps={a: "success"})
    _fin(conn, a, keep=3)
    for i in range(8):
        _fin(conn, _mk(conn, f"x{i}"), keep=3)
    assert db.get_job(conn, a) is not None  # protected while b waits
    assert len(db.get_history(conn, channel=1, limit=100)) == 4  # 3 + the protected one
    _fin(conn, b, keep=3)
    _fin(conn, _mk(conn, "y"), keep=3)
    assert db.get_job(conn, a) is None and len(db.get_history(conn, channel=1, limit=100)) == 3


def test_history_status_filter_and_validation(conn):
    ids = {s: _mk(conn, s) for s in ("done", "failed", "interrupted")}
    for s, i in ids.items():
        _fin(conn, i, status=s)
    queued = _mk(conn, "still-queued")
    assert {j["id"] for j in db.get_history(conn)} == set(ids.values())  # queued jobs are not history
    assert [j["status"] for j in db.get_history(conn, statuses=["failed"])] == ["failed"]
    got = db.get_history(conn, statuses=["failed", "interrupted"])
    assert {j["status"] for j in got} == {"failed", "interrupted"}
    with pytest.raises(ValueError, match="unknown status"):
        db.get_history(conn, statuses=["queued"])
    with pytest.raises(ValueError, match="at least 1"):
        db.get_history(conn, limit=0)
    assert db.get_job(conn, queued)["status"] == "queued"


def test_history_channel_limit_and_newest_first(conn):
    ids = [_mk(conn, f"j{i}", channel=1 + i % 2) for i in range(6)]
    for i in ids:
        _fin(conn, i)
    assert [j["id"] for j in db.get_history(conn)] == ids[::-1]
    assert [j["id"] for j in db.get_history(conn, limit=2)] == ids[::-1][:2]
    assert [j["id"] for j in db.get_history(conn, channel=2)] == [ids[5], ids[3], ids[1]]


def test_history_since_uses_the_finish_time(conn):
    old, new = _mk(conn, "old"), _mk(conn, "new")
    _fin(conn, old)
    _fin(conn, new)
    _set_finished(conn, old, "2026-01-01T00:00:00+00:00")
    _set_finished(conn, new, "2026-06-01T12:00:00+00:00")
    assert [j["id"] for j in db.get_history(conn, since="2026-03-01T00:00:00+00:00")] == [new]
    assert [j["id"] for j in db.get_history(conn, since="2026-06-01T12:00:00+00:00")] == [new]  # inclusive
    assert {j["id"] for j in db.get_history(conn, since="2025-01-01T00:00:00+00:00")} == {old, new}
    assert db.get_history(conn, since="2027-01-01T00:00:00+00:00") == []


def test_history_grep_matches_the_raw_command_case_insensitively(conn):
    a = _mk(conn, "a", raw_cmd=["python", "Train.py", "--lr", "0.1"], resolved_cmd=["/usr/bin/python", "/w/Train.py"])
    b = _mk(conn, "b", raw_cmd=["bash", "-c", 'echo "hello world" && ./eval.sh'], resolved_cmd=["/bin/bash", "-c", "x"])
    c = _mk(conn, "c", raw_cmd=["true"], resolved_cmd=["/usr/bin/true"])
    for i in (a, b, c):
        _fin(conn, i)
    assert [j["id"] for j in db.get_history(conn, grep="train.py")] == [a]
    assert [j["id"] for j in db.get_history(conn, grep="--lr 0.1")] == [a]  # words are joined by a space
    assert [j["id"] for j in db.get_history(conn, grep='"hello world"')] == [b]  # not the JSON escaping
    assert db.get_history(conn, grep="/w/Train.py") == []  # only the raw command, not the resolved one
    assert db.get_history(conn, grep="nomatch") == []
    assert len(db.get_history(conn, grep="")) == 3  # empty text = no filter


def test_history_filters_combine(conn):
    a = _mk(conn, "a", raw_cmd=["train", "1"], resolved_cmd=["/bin/train", "1"])
    b = _mk(conn, "b", raw_cmd=["train", "2"], resolved_cmd=["/bin/train", "2"], channel=2)
    c = _mk(conn, "c", raw_cmd=["eval"], resolved_cmd=["/bin/eval"])
    _fin(conn, a, status="failed")
    _fin(conn, b, status="failed")
    _fin(conn, c, status="failed")
    got = db.get_history(conn, statuses=["failed"], channel=1, grep="train")
    assert [j["id"] for j in got] == [a]


# ---------------------------------------------------------------------------
# B14: --retry-delay / --retry-backoff
# ---------------------------------------------------------------------------

def _fail_and_retry(conn, job_id):
    """Fail `job_id` and requeue it the way the daemon does for --retries."""
    _fin(conn, job_id, status="failed")
    return db.requeue(conn, job_id, log_path="/tmp/retry.log", auto=True)


def _secs_from_now(ts):
    from datetime import datetime, timezone

    return (datetime.fromisoformat(ts) - datetime.now(timezone.utc)).total_seconds()


def test_retry_delay_defaults_to_none(conn):
    a = _mk(conn, "a", max_retries=1)
    job = db.get_job(conn, a)
    assert job["retry_delay_secs"] is None and job["retry_backoff"] == 0
    retry = _fail_and_retry(conn, a)
    assert db.get_job(conn, retry)["not_before"] is None  # today's behaviour: retry at once


def test_retry_delay_is_stored_and_delays_the_automatic_retry(conn):
    a = _mk(conn, "a", max_retries=2, retry_delay_secs=300)
    assert db.get_job(conn, a)["retry_delay_secs"] == 300
    assert db.get_job(conn, a)["not_before"] is None  # the first run itself is not delayed
    retry = db.get_job(conn, _fail_and_retry(conn, a))
    assert 295 <= _secs_from_now(retry["not_before"]) <= 300
    assert (retry["origin"], retry["parent_id"], retry["attempt"]) == ("retry", a, 1)
    assert retry["retry_delay_secs"] == 300 and retry["retry_backoff"] == 0  # copied on to the next retry
    again = db.get_job(conn, _fail_and_retry(conn, retry["id"]))
    assert 295 <= _secs_from_now(again["not_before"]) <= 300  # same wait every time without backoff


def test_retry_backoff_doubles_the_delay_per_attempt(conn):
    a = _mk(conn, "a", max_retries=3, retry_delay_secs=60, retry_backoff=True)
    assert db.get_job(conn, a)["retry_backoff"] == 1
    waits = []
    job_id = a
    for _ in range(3):
        job_id = _fail_and_retry(conn, job_id)
        waits.append(_secs_from_now(db.get_job(conn, job_id)["not_before"]))
    for got, want in zip(waits, (60, 120, 240)):
        assert want - 5 <= got <= want


def test_retry_backoff_is_capped(conn):
    a = _mk(conn, "a", max_retries=100, retry_delay_secs=db.MAX_RETRY_DELAY_SECS, retry_backoff=True)
    job_id = a
    for _ in range(3):
        job_id = _fail_and_retry(conn, job_id)
    assert _secs_from_now(db.get_job(conn, job_id)["not_before"]) <= db.MAX_RETRY_DELAY_SECS
    # a huge attempt count must not overflow either
    conn.execute("UPDATE jobs SET attempt = 100000 WHERE id = ?", (job_id,))
    conn.commit()
    job_id = _fail_and_retry(conn, job_id)
    assert _secs_from_now(db.get_job(conn, job_id)["not_before"]) <= db.MAX_RETRY_DELAY_SECS


def test_manual_requeue_is_not_delayed_but_keeps_the_setting(conn):
    a = _mk(conn, "a", max_retries=2, retry_delay_secs=300, retry_backoff=True)
    _fin(conn, a, status="failed")
    new = db.get_job(conn, db.requeue(conn, a, log_path="/tmp/x.log"))
    assert new["not_before"] is None and new["origin"] == "requeue" and new["attempt"] == 0
    assert (new["retry_delay_secs"], new["retry_backoff"], new["max_retries"]) == (300, 1, 2)


def test_retry_delay_validation_writes_nothing(conn):
    for bad in (0, -5, db.MAX_RETRY_DELAY_SECS + 1):
        with pytest.raises(ValueError, match="retry delay"):
            _mk(conn, "a", retry_delay_secs=bad)
    with pytest.raises(ValueError, match="backoff"):
        _mk(conn, "a", retry_backoff=True)
    assert conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 0


def test_requeue_is_independent_of_dependants_of_a_delayed_retry(conn):
    # a dependant of the failed job is skipped at once, delay or not (the retry is a new job)
    a = _mk(conn, "a", max_retries=1, retry_delay_secs=3600)
    b = _mk(conn, "b", deps={a: "success"})
    _fin(conn, a, status="failed")
    db.requeue(conn, a, log_path="/tmp/r.log", auto=True)
    skipped, _channels = db.skip_dependants(conn, a, 200)
    assert skipped == [b] and db.get_job(conn, b)["status"] == "skipped"


def test_init_db_adds_the_retry_delay_columns_to_a_database_from_before(tmp_path):
    """A DB that already has dependencies and `not_before` (the previous schema) but no
    retry-delay columns gets them by ADD COLUMN: rows survive, a second run is a no-op."""
    import sqlite3

    path = tmp_path / "prev.db"
    raw = sqlite3.connect(path)
    raw.executescript(
        """
        CREATE TABLE jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel INTEGER NOT NULL DEFAULT 1,
            status TEXT NOT NULL CHECK (status IN
                ('queued','running','stopped','done','failed','cancelled','interrupted','skipped')),
            position INTEGER, raw_cmd TEXT NOT NULL, resolved_cmd TEXT NOT NULL,
            cwd TEXT NOT NULL, env_extra TEXT, pid INTEGER, pgid INTEGER,
            enqueued_at TEXT NOT NULL, started_at TEXT, finished_at TEXT,
            exit_code INTEGER, log_path TEXT NOT NULL, note TEXT,
            timeout_secs INTEGER, is_priority INTEGER NOT NULL DEFAULT 0,
            max_retries INTEGER NOT NULL DEFAULT 0, attempt INTEGER NOT NULL DEFAULT 0,
            parent_id INTEGER, origin TEXT, not_before TEXT,
            on_fail TEXT NOT NULL DEFAULT 'continue' CHECK (on_fail IN ('continue','pause'))
        );
        INSERT INTO jobs (status, position, raw_cmd, resolved_cmd, cwd, enqueued_at, log_path, max_retries)
        VALUES ('queued', 0, '["a"]', '["/usr/bin/a"]', '/tmp', '2026-01-01T00:00:00+00:00', '/tmp/1.log', 2);
        """
    )
    raw.commit()
    raw.close()

    c = db.connect(path)
    db.init_db(c)
    db.init_db(c)
    job = db.get_job(c, 1)
    assert job["raw_cmd"] == '["a"]' and job["max_retries"] == 2
    assert job["retry_delay_secs"] is None and job["retry_backoff"] == 0
    new_id = _mk(c, "n", max_retries=1, retry_delay_secs=60)
    assert new_id == 2 and db.get_job(c, new_id)["retry_delay_secs"] == 60
    c.close()


def test_a_database_rebuilt_by_the_older_migration_gets_the_retry_columns_too(tmp_path):
    import sqlite3

    path = tmp_path / "old.db"
    raw = sqlite3.connect(path)
    raw.executescript(
        """
        CREATE TABLE jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel INTEGER NOT NULL DEFAULT 1,
            status TEXT NOT NULL CHECK (status IN
                ('queued','running','stopped','done','failed','cancelled')),
            position INTEGER, raw_cmd TEXT NOT NULL, resolved_cmd TEXT NOT NULL,
            cwd TEXT NOT NULL, env_extra TEXT, pid INTEGER, pgid INTEGER,
            enqueued_at TEXT NOT NULL, started_at TEXT, finished_at TEXT,
            exit_code INTEGER, log_path TEXT NOT NULL, note TEXT,
            timeout_secs INTEGER, is_priority INTEGER NOT NULL DEFAULT 0,
            max_retries INTEGER NOT NULL DEFAULT 0, attempt INTEGER NOT NULL DEFAULT 0,
            parent_id INTEGER, origin TEXT
        );
        INSERT INTO jobs (status, position, raw_cmd, resolved_cmd, cwd, enqueued_at, log_path)
        VALUES ('queued', 0, '["a"]', '["/usr/bin/a"]', '/tmp', '2026-01-01T00:00:00+00:00', '/tmp/1.log');
        """
    )
    raw.commit()
    raw.close()
    c = db.connect(path)
    db.init_db(c)
    cols = [r["name"] for r in c.execute("PRAGMA table_info(jobs)").fetchall()]
    assert cols.count("retry_delay_secs") == 1 and cols.count("retry_backoff") == 1
    assert db.get_job(c, 1)["retry_backoff"] == 0
    c.close()


# ---------------------------------------------------------------------------
# F8: edit lock + edit_job
# ---------------------------------------------------------------------------

import os  # noqa: E402
import subprocess  # noqa: E402
from datetime import datetime, timedelta, timezone  # noqa: E402


def _dead_pid() -> int:
    p = subprocess.Popen(["true"])  # noqa: S603, S607
    p.wait()
    return p.pid


def _lock_row(conn, job_id):
    return conn.execute("SELECT * FROM edit_locks WHERE job_id = ?", (job_id,)).fetchone()


def test_take_and_release_edit_lock(conn):
    a = _mk(conn, "a")
    lock = db.take_edit_lock(conn, a, os.getpid())
    assert lock["holder_pid"] == os.getpid()
    assert db.live_edit_lock(conn, a) is not None
    assert db.release_edit_lock(conn, a, os.getpid()) is True
    assert db.live_edit_lock(conn, a) is None
    assert db.release_edit_lock(conn, a, os.getpid()) is False


def test_edit_lock_of_another_live_process_is_refused_own_is_renewed(conn):
    a = _mk(conn, "a")
    db.take_edit_lock(conn, a, os.getpid())
    with pytest.raises(ValueError, match="already being edited"):
        db.take_edit_lock(conn, a, 1)  # pid 1 always exists
    first = _lock_row(conn, a)["expires_at"]
    db.take_edit_lock(conn, a, os.getpid())
    assert _lock_row(conn, a)["expires_at"] >= first


def test_edit_lock_needs_a_queued_job(conn):
    a = _mk(conn, "a")
    db.mark_running(conn, a, pid=1, pgid=1)
    with pytest.raises(ValueError, match="not queued"):
        db.take_edit_lock(conn, a, os.getpid())
    with pytest.raises(ValueError, match="does not exist"):
        db.take_edit_lock(conn, 999, os.getpid())


def test_priority_job_cannot_be_edited(conn):
    a = _mk(conn, "a", is_priority=True)
    with pytest.raises(ValueError, match="--now"):
        db.take_edit_lock(conn, a, os.getpid())


def test_edit_lock_of_a_dead_process_is_stale_and_cleared(conn):
    a = _mk(conn, "a")
    db.take_edit_lock(conn, a, os.getpid())
    conn.execute("UPDATE edit_locks SET holder_pid = ?", (_dead_pid(),))
    conn.commit()
    assert db.live_edit_lock(conn, a) is None
    assert _lock_row(conn, a) is None
    db.take_edit_lock(conn, a, 1)  # free for anyone now


def test_expired_edit_lock_is_stale_and_cleared(conn):
    a = _mk(conn, "a")
    db.take_edit_lock(conn, a, os.getpid())
    past = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
    conn.execute("UPDATE edit_locks SET expires_at = ?", (past,))
    conn.commit()
    assert db.live_edit_lock(conn, a) is None
    assert _lock_row(conn, a) is None


def test_edit_lock_lifetime_is_30_minutes(conn):
    a = _mk(conn, "a")
    lock = db.take_edit_lock(conn, a, os.getpid())
    left = (datetime.fromisoformat(lock["expires_at"]) - datetime.fromisoformat(lock["taken_at"])).total_seconds()
    assert left == db.EDIT_LOCK_SECS == 1800


def test_clear_edit_locks(conn):
    a, b = _mk(conn, "a"), _mk(conn, "b")
    db.take_edit_lock(conn, a, os.getpid())
    db.take_edit_lock(conn, b, os.getpid())
    assert db.clear_edit_locks(conn) == 2
    assert db.live_edit_lock(conn, a) is None


def test_rm_cancel_and_start_drop_the_lock(conn):
    a, b, c = _mk(conn, "a"), _mk(conn, "b"), _mk(conn, "c")
    for j in (a, b, c):
        db.take_edit_lock(conn, j, os.getpid())
    db.remove_from_queue(conn, a)
    db.cancel_queued(conn, b)
    db.mark_running(conn, c, pid=1, pgid=1)
    assert conn.execute("SELECT COUNT(*) FROM edit_locks").fetchone()[0] == 0


def test_edit_job_changes_fields_and_keeps_the_rest(conn):
    a = _mk(conn, "a", timeout_secs=5)
    new = db.edit_job(conn, a, updates={"raw_cmd": '["x"]', "timeout_secs": None, "on_fail": "pause", "max_retries": 2})
    assert new["raw_cmd"] == '["x"]' and new["timeout_secs"] is None
    assert new["on_fail"] == "pause" and new["max_retries"] == 2
    assert new["resolved_cmd"] == '["/usr/bin/a"]' and new["status"] == "queued"


def test_edit_job_rejects_unknown_columns_and_bad_values(conn):
    a = _mk(conn, "a")
    with pytest.raises(ValueError, match="cannot edit"):
        db.edit_job(conn, a, updates={"status": "done"})
    with pytest.raises(ValueError, match="on_fail"):
        db.edit_job(conn, a, updates={"on_fail": "explode"})
    assert db.get_job(conn, a)["status"] == "queued"


def test_edit_job_only_queued(conn):
    a, b = _mk(conn, "a"), _mk(conn, "b")
    db.mark_running(conn, a, pid=1, pgid=1)
    db.mark_finished(conn, a, status="done", exit_code=0)
    with pytest.raises(ValueError, match="only queued jobs can be edited"):
        db.edit_job(conn, a, updates={"cwd": "/x"})
    db.mark_running(conn, b, pid=1, pgid=1)
    with pytest.raises(ValueError, match="running, not queued"):
        db.edit_job(conn, b, updates={"cwd": "/x"})
    with pytest.raises(ValueError, match="does not exist"):
        db.edit_job(conn, 999, updates={"cwd": "/x"})


def test_edit_job_respects_the_lock(conn):
    a = _mk(conn, "a")
    db.take_edit_lock(conn, a, os.getpid())
    with pytest.raises(ValueError, match="being edited"):
        db.edit_job(conn, a, updates={"cwd": "/x"})  # someone else's lock (no holder given)
    new = db.edit_job(conn, a, updates={"cwd": "/x"}, holder_pid=os.getpid())
    assert new["cwd"] == "/x" and _lock_row(conn, a) is None  # released with the edit


def test_edit_job_with_a_lost_lock_fails(conn):
    a = _mk(conn, "a")
    with pytest.raises(ValueError, match="lock on job"):
        db.edit_job(conn, a, updates={"cwd": "/x"}, holder_pid=os.getpid())
    assert db.get_job(conn, a)["cwd"] == "/tmp"


def test_edit_job_moves_to_the_end_of_another_channel(conn):
    a, b, c = _mk(conn, "a"), _mk(conn, "b"), _mk(conn, "c")
    x = _mk(conn, "x", channel=2)
    db.edit_job(conn, a, updates={}, channel=2)
    assert [j["id"] for j in db.get_queue(conn, 1)] == [b, c]
    assert [j["position"] for j in db.get_queue(conn, 1)] == [0, 1]
    assert [j["id"] for j in db.get_queue(conn, 2)] == [x, a]
    assert db.get_job(conn, a)["channel"] == 2
    db.edit_job(conn, b, updates={}, channel=7)  # a brand-new channel is registered
    assert 7 in db.list_channels(conn)


def test_edit_job_replaces_dependencies(conn):
    a, b, c = _mk(conn, "a", channel=2), _mk(conn, "b", channel=2), _mk(conn, "c")
    db.edit_job(conn, c, updates={}, deps={a: "success", b: "any"})
    info = db.dep_info(conn, c)
    assert info["depends_on"] == [a] and info["after_any"] == [b]
    db.edit_job(conn, c, updates={}, deps={})
    assert db.dep_info(conn, c)["depends_on"] == []


def test_edit_job_dependency_errors_change_nothing(conn):
    a = _mk(conn, "a")
    b = _mk(conn, "b")
    with pytest.raises(ValueError, match="does not exist"):
        db.edit_job(conn, a, updates={"cwd": "/x"}, deps={999: "success"})
    with pytest.raises(ValueError, match="itself"):
        db.edit_job(conn, a, updates={"cwd": "/x"}, deps={a: "success"})
    with pytest.raises(ValueError, match="behind this job"):
        db.edit_job(conn, a, updates={"cwd": "/x"}, deps={b: "success"})  # b is queued behind a
    with pytest.raises(ValueError, match="mode"):
        db.edit_job(conn, a, updates={"cwd": "/x"}, deps={b: "maybe"})
    assert db.get_job(conn, a)["cwd"] == "/tmp" and db.dep_info(conn, a)["depends_on"] == []


def test_edit_job_deadlock_is_rejected_and_rolled_back(conn):
    a, b = _mk(conn, "a", channel=1), _mk(conn, "b", channel=2)
    db.edit_job(conn, b, updates={}, deps={a: "success"})
    with pytest.raises(ValueError, match="deadlock"):
        db.edit_job(conn, a, updates={"cwd": "/x"}, deps={b: "success"})
    assert db.get_job(conn, a)["cwd"] == "/tmp" and db.dep_info(conn, a)["depends_on"] == []


def test_edit_job_move_that_would_deadlock_is_rejected(conn):
    a = _mk(conn, "a")
    b = _mk(conn, "b", channel=2, deps={a: "success"})
    c = _mk(conn, "c", channel=2)
    # moving a behind b on channel 2: b waits for a, a waits (FIFO) for b
    with pytest.raises(ValueError, match="deadlock"):
        db.edit_job(conn, a, updates={}, channel=2)
    assert db.get_job(conn, a)["channel"] == 1
    assert [j["id"] for j in db.get_queue(conn, 2)] == [b, c]


# ---------------------------------------------------------------------------
# Q7: numbered migrations + schema_version, Q8: WAL / busy_timeout / 0600
# ---------------------------------------------------------------------------


def _old_jobs_db(path):
    """A DB from before `schema_version` AND before the priority lane (oldest shape), 1 queued job."""
    import sqlite3

    raw = sqlite3.connect(path)
    raw.executescript(
        """
        CREATE TABLE jobs (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            channel       INTEGER NOT NULL DEFAULT 1,
            status        TEXT NOT NULL CHECK (status IN
                            ('queued','running','done','failed','cancelled')),
            position      INTEGER, raw_cmd TEXT NOT NULL, resolved_cmd TEXT NOT NULL,
            cwd           TEXT NOT NULL, env_extra TEXT, pid INTEGER, pgid INTEGER,
            enqueued_at   TEXT NOT NULL, started_at TEXT, finished_at TEXT,
            exit_code     INTEGER, log_path TEXT NOT NULL, note TEXT, timeout_secs INTEGER
        );
        INSERT INTO jobs (channel, status, position, raw_cmd, resolved_cmd, cwd, enqueued_at, log_path)
        VALUES (1, 'queued', 0, '["a"]', '["/usr/bin/a"]', '/tmp', '2026-01-01', '/tmp/a.log');
        """
    )
    raw.commit()
    raw.close()


def test_fresh_database_is_created_at_the_latest_schema_version(tmp_path):
    c = db.connect(tmp_path / "fresh.db")
    assert db.get_schema_version(c) is None  # nothing there before init_db
    db.init_db(c)
    assert db.get_schema_version(c) == db.SCHEMA_VERSION
    assert c.execute("SELECT COUNT(*) FROM schema_version").fetchone()[0] == 1  # single row
    c.close()


def test_old_database_without_schema_version_is_brought_up_to_date_without_data_loss(tmp_path):
    path = tmp_path / "old.db"
    _old_jobs_db(path)
    c = db.connect(path)
    db.init_db(c)
    assert db.get_schema_version(c) == db.SCHEMA_VERSION
    job = db.get_job(c, 1)
    assert job["status"] == "queued" and job["raw_cmd"] == '["a"]'
    cols = {r["name"] for r in c.execute("PRAGMA table_info(jobs)").fetchall()}
    assert {"is_priority", "max_retries", "not_before", "on_fail", "retry_delay_secs", "retry_backoff"} <= cols
    c.close()


def test_current_shape_database_without_the_version_table_just_gets_the_version(tmp_path):
    """A DB made by the last release (full schema, no schema_version table yet)."""
    path = tmp_path / "cur.db"
    c = db.connect(path)
    db.init_db(c)
    c.execute("DROP TABLE schema_version")
    c.commit()
    assert db.get_schema_version(c) is None
    db.init_db(c)
    assert db.get_schema_version(c) == db.SCHEMA_VERSION
    c.close()


def test_init_db_twice_changes_nothing(tmp_path):
    path = tmp_path / "old.db"
    _old_jobs_db(path)
    c = db.connect(path)
    db.init_db(c)
    db.init_db(c)
    assert db.get_schema_version(c) == db.SCHEMA_VERSION
    assert c.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 1
    assert c.execute("SELECT COUNT(*) FROM schema_version").fetchone()[0] == 1
    c.close()


def test_migrations_are_numbered_in_order_and_each_bumps_the_version(tmp_path, monkeypatch):
    seen: list[int] = []

    def step(n):
        def run(conn):
            seen.append(n)
            conn.execute("CREATE TABLE IF NOT EXISTS t%d (x INTEGER)" % n)

        return run

    path = tmp_path / "old.db"
    _old_jobs_db(path)
    c = db.connect(path)
    db.init_db(c)  # real steps first: now at SCHEMA_VERSION
    top = db.SCHEMA_VERSION
    monkeypatch.setattr(db, "_MIGRATIONS", db._MIGRATIONS + ((top + 1, step(top + 1)), (top + 2, step(top + 2))))
    monkeypatch.setattr(db, "SCHEMA_VERSION", top + 2)
    db.init_db(c)
    assert seen == [top + 1, top + 2]
    assert db.get_schema_version(c) == top + 2
    db.init_db(c)
    assert seen == [top + 1, top + 2]  # never re-run
    c.close()


def test_a_failing_migration_rolls_back_and_keeps_the_old_version(tmp_path, monkeypatch):
    path = tmp_path / "x.db"
    c = db.connect(path)
    db.init_db(c)
    top = db.SCHEMA_VERSION

    def boom(conn):
        conn.execute("CREATE TABLE half_done (x INTEGER)")
        raise RuntimeError("boom")

    monkeypatch.setattr(db, "_MIGRATIONS", db._MIGRATIONS + ((top + 1, boom),))
    monkeypatch.setattr(db, "SCHEMA_VERSION", top + 1)
    with pytest.raises(RuntimeError, match="boom"):
        db.init_db(c)
    assert db.get_schema_version(c) == top
    assert c.execute("SELECT 1 FROM sqlite_master WHERE name = 'half_done'").fetchone() is None  # rolled back
    assert not c.in_transaction
    c.close()


def test_a_database_from_a_newer_queuer_is_refused(tmp_path):
    c = db.connect(tmp_path / "new.db")
    db.init_db(c)
    c.execute("UPDATE schema_version SET version = ?", (db.SCHEMA_VERSION + 1,))
    c.commit()
    with pytest.raises(RuntimeError, match="newer"):
        db.init_db(c)
    c.close()


def test_connect_uses_wal_and_a_busy_timeout(tmp_path):
    c = db.connect(tmp_path / "w.db")
    assert c.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    assert c.execute("PRAGMA busy_timeout").fetchone()[0] == db.BUSY_TIMEOUT_MS
    c.close()


def test_the_wal_side_files_are_0600_too(tmp_path):
    path = tmp_path / "m.db"
    c = db.connect(path)
    db.init_db(c)
    _mk(c)  # a write, so the -wal file certainly exists
    for suffix in ("", "-wal", "-shm"):
        f = tmp_path / f"m.db{suffix}"
        assert f.exists(), suffix
        assert (f.stat().st_mode & 0o777) == 0o600, suffix
    c.close()


def test_two_connections_can_read_and_write(tmp_path):
    path = tmp_path / "two.db"
    a = db.connect(path)
    db.init_db(a)
    b = db.connect(path)
    db.init_db(b)  # a second process starting up while the first is open
    first = _mk(a, "one")
    assert db.get_job(b, first) is not None  # a's write is visible to b
    # an open write transaction of `a` does not stop `b` reading (WAL), and `b` can write after it commits
    a.execute("BEGIN IMMEDIATE")
    a.execute("UPDATE jobs SET note = 'x' WHERE id = ?", (first,))
    assert db.get_job(b, first) is not None  # reader not blocked
    a.commit()
    second = _mk(b, "two")
    assert db.get_job(a, second) is not None
    a.close()
    b.close()
