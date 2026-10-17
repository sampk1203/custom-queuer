"""Tests for the Telegram notifier. Every network call goes through
`notify._api`, which is replaced here -- nothing touches Telegram."""

from __future__ import annotations

import json
import os
import stat

import pytest

from queuer import notify

TOKEN = "123456789:AAH-test_token-0123456789abcdefghijk"
CFG = {"qur_id": "a3f9", "token": TOKEN, "chat_ids": [111, 222]}


# ---------------------------------------------------------------------------
# config file
# ---------------------------------------------------------------------------

def test_default_path_honours_env(tmp_path, monkeypatch):
    monkeypatch.setenv("QUEUER_NOTIFY_PATH", str(tmp_path / "x.json"))
    assert notify.default_path() == tmp_path / "x.json"


def test_save_load_roundtrip_and_mode_0600(tmp_path):
    path = tmp_path / "sub" / "notify.json"
    notify.save(CFG, path)
    assert notify.load(path) == {"qur_id": "a3f9", "bots": [{"token": TOKEN, "chat_ids": [111, 222]}]}
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o600


def test_save_leaves_no_temp_files(tmp_path):
    path = tmp_path / "notify.json"
    notify.save(CFG, path)
    assert [p.name for p in tmp_path.iterdir()] == ["notify.json"]


def test_load_missing_returns_none(tmp_path):
    assert notify.load(tmp_path / "nope.json") is None


@pytest.mark.parametrize(
    "bad",
    [
        "not json",
        "[]",
        json.dumps({"qur_id": "a", "token": "garbage", "chat_ids": [1]}),
        json.dumps({"qur_id": "a", "token": TOKEN, "chat_ids": ["x"]}),
        json.dumps({"qur_id": "", "token": TOKEN, "chat_ids": [1]}),
        json.dumps({"token": TOKEN, "chat_ids": [1]}),
    ],
)
def test_load_rejects_bad_files(tmp_path, bad):
    path = tmp_path / "notify.json"
    path.write_text(bad)
    assert notify.load(path) is None


def test_remove(tmp_path):
    path = tmp_path / "notify.json"
    notify.save(CFG, path)
    assert notify.remove(path) is True
    assert notify.remove(path) is False


def test_new_qur_id_is_short_hex_and_varies():
    ids = {notify.new_qur_id() for _ in range(50)}
    assert all(len(i) == 4 and int(i, 16) >= 0 for i in ids)
    assert len(ids) > 1


# ---------------------------------------------------------------------------
# export / import
# ---------------------------------------------------------------------------

def test_export_import_roundtrip_excludes_qur_id():
    s = notify.export_string(CFG)
    assert s.startswith("qurtg1:")
    assert "a3f9" not in s
    assert notify.parse_export_string(s) == (TOKEN, [111, 222])


def test_parse_export_tolerates_surrounding_whitespace():
    assert notify.parse_export_string("  " + notify.export_string(CFG) + "\n")[0] == TOKEN


@pytest.mark.parametrize(
    "bad",
    [
        "",
        "hello",
        "qurtg1:",
        "qurtg1:!!!notbase64!!!",
        "qurtg1:" + "e30",  # {} -> missing keys
    ],
)
def test_parse_export_rejects_garbage(bad):
    with pytest.raises(ValueError):
        notify.parse_export_string(bad)


def test_parse_export_rejects_bad_token_and_empty_chats():
    import base64

    def enc(obj):
        return "qurtg1:" + base64.urlsafe_b64encode(json.dumps(obj).encode()).decode()

    with pytest.raises(ValueError):
        notify.parse_export_string(enc({"t": "nope", "c": [1]}))
    with pytest.raises(ValueError):
        notify.parse_export_string(enc({"t": TOKEN, "c": []}))
    with pytest.raises(ValueError):
        notify.parse_export_string(enc({"t": TOKEN, "c": ["a"]}))


# ---------------------------------------------------------------------------
# send
# ---------------------------------------------------------------------------

def test_send_posts_to_every_chat(monkeypatch):
    calls = []
    monkeypatch.setattr(notify, "_api", lambda token, method, params=None, timeout=0: calls.append((token, method, params)))
    assert notify.send(CFG, "hi") == []
    assert [(c[0], c[1], c[2]["chat_id"], c[2]["text"]) for c in calls] == [
        (TOKEN, "sendMessage", 111, "hi"),
        (TOKEN, "sendMessage", 222, "hi"),
    ]


def test_send_continues_after_one_chat_fails(monkeypatch):
    sent = []

    def fake(token, method, params=None, timeout=0):
        if params["chat_id"] == 111:
            raise notify.NotifyError("blocked by user", 403)
        sent.append(params["chat_id"])

    monkeypatch.setattr(notify, "_api", fake)
    errors = notify.send(CFG, "hi")
    assert sent == [222]
    assert len(errors) == 1 and "111" in errors[0] and "blocked" in errors[0]


def test_api_errors_never_contain_the_token(monkeypatch):
    import urllib.error

    def boom(req, timeout=0):
        raise urllib.error.URLError(f"cannot reach {req.full_url}")

    monkeypatch.setattr(notify, "_open", boom)
    with pytest.raises(notify.NotifyError) as ei:
        notify._api(TOKEN, "getMe")
    assert TOKEN not in str(ei.value)


# ---------------------------------------------------------------------------
# pairing
# ---------------------------------------------------------------------------

class FakeTelegram:
    """Scripted getMe/getUpdates. `batches` is a list of update lists, one
    per getUpdates call; the `{S}` placeholder in a text is replaced by the
    secret taken from the link pair() hands to on_link."""

    def __init__(self, batches):
        self.batches = list(batches)
        self.secret = None
        self.offsets = []

    def api(self, token, method, params=None, timeout=0):
        if method == "getMe":
            return {"username": "qur_test_bot"}
        assert method == "getUpdates"
        self.offsets.append((params or {}).get("offset"))
        if not self.batches:
            return []
        batch = self.batches.pop(0)
        if isinstance(batch, Exception):
            raise batch
        return json.loads(json.dumps(batch).replace("{S}", self.secret or "?"))


def _msg(update_id, text, chat_id=555, chat_type="private"):
    return {"update_id": update_id, "message": {"text": text, "chat": {"id": chat_id, "type": chat_type}}}


def _run_pair(monkeypatch, batches, timeout=5):
    fake = FakeTelegram(batches)
    monkeypatch.setattr(notify, "_api", fake.api)
    links = []

    def on_link(link):
        links.append(link)
        fake.secret = link.split("start=")[1]

    chat = notify.pair(TOKEN, on_link, timeout=timeout)
    return chat, links, fake


def test_pair_returns_chat_id_and_builds_deep_link(monkeypatch):
    chat, links, _ = _run_pair(monkeypatch, [[_msg(1, "/start {S}", chat_id=777)]])
    assert chat == 777
    assert links[0].startswith("https://t.me/qur_test_bot?start=")
    assert len(links[0].split("start=")[1]) >= 12


def test_pair_ignores_wrong_secret_and_group_chats(monkeypatch):
    chat, _, _ = _run_pair(
        monkeypatch,
        [
            [_msg(1, "/start WRONG", chat_id=1), _msg(2, "hello", chat_id=2)],
            [_msg(3, "/start {S}", chat_id=3, chat_type="group")],
            [_msg(4, "/start {S}", chat_id=4)],
        ],
    )
    assert chat == 4


def test_pair_acknowledges_updates_with_offset(monkeypatch):
    _, _, fake = _run_pair(monkeypatch, [[_msg(10, "noise")], [_msg(11, "/start {S}")]])
    assert fake.offsets == [None, 11]


def test_pair_conflict_on_409(monkeypatch):
    fake = FakeTelegram([notify.NotifyError("Conflict: terminated by other getUpdates request", 409)])
    monkeypatch.setattr(notify, "_api", fake.api)
    with pytest.raises(notify.PairingConflict) as ei:
        notify.pair(TOKEN, lambda link: None, timeout=5)
    assert "another qur" in str(ei.value)


def test_pair_other_errors_propagate(monkeypatch):
    fake = FakeTelegram([notify.NotifyError("Unauthorized", 401)])
    monkeypatch.setattr(notify, "_api", fake.api)
    with pytest.raises(notify.NotifyError) as ei:
        notify.pair(TOKEN, lambda link: None, timeout=5)
    assert not isinstance(ei.value, notify.PairingConflict)


def test_pair_times_out(monkeypatch):
    monkeypatch.setattr(notify, "POLL_SECS", 0)
    fake = FakeTelegram([])
    monkeypatch.setattr(notify, "_api", fake.api)
    with pytest.raises(TimeoutError):
        notify.pair(TOKEN, lambda link: None, timeout=0.05)


# ---------------------------------------------------------------------------
# message text
# ---------------------------------------------------------------------------

def _job(**kw):
    base = {
        "id": 12, "channel": 1, "status": "done", "exit_code": 0, "note": None,
        "started_at": "2026-10-14T10:00:00+00:00", "finished_at": "2026-10-14T12:13:05+00:00",
        "max_retries": 0, "attempt": 0, "cwd": "/secret/dir", "raw_cmd": '["python","secret.py"]',
        "resolved_cmd": '["/secret/bin/python","/secret/secret.py"]',
    }
    base.update(kw)
    return base


def test_format_done(monkeypatch):
    monkeypatch.setattr(notify.socket, "gethostname", lambda: "bigbox")
    text = notify.format_job(_job(), CFG)
    assert text == "✅ done · job 12 · 2h13m\n$ python secret.py\nbigbox [a3f9] · ch 1 · exit 0"


def test_format_failed_timeout_and_cancelled(monkeypatch):
    monkeypatch.setattr(notify.socket, "gethostname", lambda: "bigbox")
    t = notify.format_job(_job(status="failed", exit_code=-15, note="timed out", finished_at="2026-10-14T10:00:30+00:00"), CFG)
    assert t.startswith("❌ failed · job 12 · 30s")
    assert "exit -15" in t and "timed out" in t
    c = notify.format_job(_job(status="cancelled", exit_code=None, started_at=None, finished_at=None, note="cancelled while queued"), CFG)
    assert c.startswith("🚫 cancelled · job 12\n")
    assert "exit" not in c and "cancelled while queued" in c


def test_format_shows_attempt_when_retries_enabled():
    t = notify.format_job(_job(status="failed", exit_code=1, max_retries=2, attempt=2), CFG)
    assert "attempt 3/3" in t


def test_format_never_leaks_cwd_resolved_paths_or_raw_notes():
    t = notify.format_job(
        _job(status="failed", exit_code=None, note="failed to start: [Errno 2] No such file: '/secret/bin/x'"), CFG
    )
    assert "failed to start" in t
    assert "/secret" not in t and "Errno" not in t  # cwd, resolved cmd, raw note
    t2 = notify.format_job(_job(note="some unknown note with /secret/path"), CFG)
    assert "/secret" not in t2


def test_format_shows_raw_command_quoted_one_line_and_capped():
    t = notify.format_job(_job(raw_cmd=json.dumps(["echo", "a b", "x\ny"])), CFG)
    assert "$ echo \"a b\" 'x y'\n" in t
    assert "z" * 500 in notify.format_job(_job(raw_cmd=json.dumps(["echo", "z" * 500])), CFG)  # not cut any more
    long = notify.format_job(_job(raw_cmd=json.dumps(["echo", "z" * 2000])), CFG)
    cmd_line = long.splitlines()[1]
    assert cmd_line.endswith("…") and len(cmd_line) == 2 + notify.CMD_MAX_CHARS


def test_format_survives_missing_or_bad_raw_cmd():
    for kw in ({"raw_cmd": "not json"}, {"raw_cmd": None}, {"raw_cmd": "[]"}):
        assert "$" not in notify.format_job(_job(**kw), CFG)


# ---------------------------------------------------------------------------
# id / listen / requeue button helpers
# ---------------------------------------------------------------------------

def test_valid_qur_id():
    assert notify.valid_qur_id("bigbox-1_x")
    for bad in ("", "a b", "x" * 17, "a:b", "é"):
        assert not notify.valid_qur_id(bad)


def test_listening_defaults_on_for_old_configs():
    assert notify.listening(CFG) is True
    assert notify.listening({**CFG, "listen": False}) is False


def test_callback_roundtrip_and_rejects_garbage():
    row = notify.job_markup(CFG, 42)["inline_keyboard"][0]
    assert [b["text"].split()[-1] for b in row] == ["Requeue", "Details"]
    parsed = [notify.parse_callback(b["callback_data"]) for b in row]
    assert parsed == [("rq", "a3f9", 42), ("sh", "a3f9", 42)]
    assert all(len(b["callback_data"].encode()) <= 64 for b in row)
    assert notify.details_markup(CFG, 5)["inline_keyboard"][0][0]["callback_data"] == "sh:a3f9:5"
    for bad in (None, 5, "", "rq:a3f9", "rq:a3f9:x", "zz:a3f9:1", "rq:a:b:1", "rq:a3f9:-1"):
        assert notify.parse_callback(bad) is None


def test_send_attaches_markup_and_parse_mode_only_when_given(monkeypatch):
    calls = []
    monkeypatch.setattr(notify, "_api", lambda token, method, params=None, timeout=0: calls.append(params))
    notify.send({**CFG, "chat_ids": [1]}, "hi")
    assert "reply_markup" not in calls[0] and "parse_mode" not in calls[0]
    notify.send({**CFG, "chat_ids": [1]}, "hi", notify.job_markup(CFG, 3), "HTML")
    assert calls[-1]["reply_markup"]["inline_keyboard"][0][0]["text"].endswith("Requeue")
    assert calls[-1]["parse_mode"] == "HTML"


def test_answer_callback_omits_empty_text(monkeypatch):
    calls = []
    monkeypatch.setattr(notify, "_api", lambda token, method, params=None, timeout=0: calls.append(params))
    notify.answer_callback(CFG, "id1", "")
    notify.answer_callback(CFG, "id2", "x" * 500)
    assert "text" not in calls[0] and len(calls[1]["text"]) == 200


def test_format_details_has_everything_qur_show_has_in_html(monkeypatch):
    monkeypatch.setattr(notify.socket, "gethostname", lambda: "bigbox")
    t = notify.format_details(
        _job(
            status="failed", exit_code=2, note="failed to start: [Errno 2] /secret/bin/x", pid=77, timeout_secs=300,
            enqueued_at="2026-10-14T09:59:00+00:00", log_path="/logs/12.log", origin="retry", parent_id=9,
            max_retries=2, attempt=1, raw_cmd=json.dumps(["sh", "-c", "a < b && echo <hi>"]),
        ),
        CFG,
    )
    assert "<b>Job #12</b> · failed" in t and "bigbox [a3f9]" in t
    for label in ("channel", "raw command", "resolved command", "cwd", "enqueued", "started", "finished",
                  "duration", "exit code", "timeout", "origin", "auto-retries", "pid", "log", "note"):
        assert f"<b>{label}</b>" in t, label
    assert "<code>/secret/dir</code>" in t and "<code>/logs/12.log</code>" in t
    assert "/secret/bin/x" in t and "300s" in t and "retry of #9 (1/2)" in t and "1 of 2 used" in t
    assert "&lt;hi&gt;" in t and "<hi>" not in t  # user text is escaped; only our tags remain


def test_format_details_handles_unstarted_job_and_stays_under_telegram_limit():
    t = notify.format_details(
        _job(status="cancelled", started_at=None, finished_at=None, exit_code=None, note=None, pid=None,
             timeout_secs=None, raw_cmd=json.dumps(["x" * 9000]), resolved_cmd=json.dumps(["y" * 9000]),
             cwd="/c" * 5000, log_path="/l" * 5000),
        CFG,
    )
    assert "<b>duration</b> -" in t and "<b>note</b>" not in t and "<b>auto-retries</b>" not in t
    assert len(t) < 4096


def test_get_updates_asks_for_callbacks_and_messages_and_handles_none(monkeypatch):
    seen = []

    def fake(token, method, params=None, timeout=0):
        seen.append((method, params))

    monkeypatch.setattr(notify, "_api", fake)
    assert notify.get_updates(CFG, 7) == []
    assert seen[0][0] == "getUpdates"
    assert seen[0][1]["allowed_updates"] == ["callback_query", "message"] and seen[0][1]["offset"] == 7


def test_pairing_lock_marks_and_clears(tmp_path):
    path = tmp_path / "notify.json"
    assert not notify.pairing_active(path)
    with notify.pairing_lock(path):
        assert notify.pairing_active(path)
    assert not notify.pairing_active(path)


def test_pairing_lock_cleared_on_error_and_stale_expires(tmp_path):
    path = tmp_path / "notify.json"
    with pytest.raises(KeyboardInterrupt):
        with notify.pairing_lock(path):
            raise KeyboardInterrupt
    assert not notify.pairing_active(path)
    marker = notify._pairing_marker(path)
    marker.touch()
    old = marker.stat().st_mtime - notify.PAIR_TIMEOUT_SECS - 60
    os.utime(marker, (old, old))
    assert not notify.pairing_active(path)


def test_hello_text_has_host_and_id(monkeypatch):
    monkeypatch.setattr(notify.socket, "gethostname", lambda: "bigbox")
    assert notify.hello_text(CFG) == "🔗 connected\nbigbox [a3f9]"


# ---------------------------------------------------------------------------
# several bots in one config
# ---------------------------------------------------------------------------

TOKEN2 = "987654321:BBH-other_token-0123456789abcdefghijk"


def _two():
    return {"qur_id": "a3f9", "bots": [
        {"token": TOKEN, "chat_ids": [111], "username": "first_bot"},
        {"token": TOKEN2, "chat_ids": [111, 222], "listen": False},
    ]}


def test_old_single_bot_file_still_loads_and_is_upgraded_on_save(tmp_path):
    path = tmp_path / "notify.json"
    path.write_text(json.dumps({"qur_id": "a3f9", "token": TOKEN, "chat_ids": [1], "listen": False}))
    cfg = notify.load(path)
    assert cfg == {"qur_id": "a3f9", "bots": [{"token": TOKEN, "chat_ids": [1], "listen": False}]}
    notify.save(cfg, path)
    assert json.loads(path.read_text()) == cfg  # now in the new layout, same content


def test_save_load_roundtrip_with_two_bots(tmp_path):
    path = tmp_path / "notify.json"
    notify.save(_two(), path)
    assert notify.load(path) == _two()


@pytest.mark.parametrize("bad", [
    {"qur_id": "a", "bots": []},
    {"qur_id": "a", "bots": "x"},
    {"qur_id": "a", "bots": [{"token": TOKEN, "chat_ids": [1]}, {"token": "bad", "chat_ids": [1]}]},
    {"qur_id": "a", "bots": [{"token": TOKEN, "chat_ids": [True]}]},
    {"bots": [{"token": TOKEN, "chat_ids": [1]}]},
])
def test_load_rejects_bad_multi_bot_files(tmp_path, bad):
    path = tmp_path / "notify.json"
    path.write_text(json.dumps(bad))
    assert notify.load(path) is None


def test_duplicate_token_is_collapsed():
    cfg = notify.normalize({"qur_id": "a", "bots": [{"token": TOKEN, "chat_ids": [1]}, {"token": TOKEN, "chat_ids": [2]}]})
    assert cfg["bots"] == [{"token": TOKEN, "chat_ids": [1]}]


def test_bot_cfgs_are_flat_views_with_the_qur_id():
    flat = notify.bot_cfgs(_two())
    assert [(b["qur_id"], b["token"], b["chat_ids"]) for b in flat] == [
        ("a3f9", TOKEN, [111]), ("a3f9", TOKEN2, [111, 222])]
    assert notify.listening(flat[0]) and not notify.listening(flat[1])


def test_find_bots_by_id_or_username_ignores_case_and_at():
    cfg = _two()
    assert [b["token"] for b in notify.find_bots(cfg, "123456789")] == [TOKEN]
    assert [b["token"] for b in notify.find_bots(cfg, "@First_Bot")] == [TOKEN]
    assert notify.find_bots(cfg, "987654321")[0]["token"] == TOKEN2
    assert notify.find_bots(cfg, "nope") == [] and notify.find_bots(cfg, "") == []


def test_bot_label_never_contains_the_secret():
    assert notify.bot_label({"token": TOKEN, "chat_ids": [], "username": "first_bot"}) == "@first_bot (123456789)"
    assert notify.bot_label({"token": TOKEN, "chat_ids": []}) == "123456789"


def test_with_bot_adds_replaces_and_creates():
    created = notify.with_bot(None, {"token": TOKEN, "chat_ids": [1]})
    assert len(created["qur_id"]) == 4 and len(created["bots"]) == 1
    added = notify.with_bot(created, {"token": TOKEN2, "chat_ids": [2]})
    assert [b["token"] for b in added["bots"]] == [TOKEN, TOKEN2] and added["qur_id"] == created["qur_id"]
    replaced = notify.with_bot(added, {"token": TOKEN, "chat_ids": [9]}, qur_id="new")
    assert replaced["bots"][0]["chat_ids"] == [9] and len(replaced["bots"]) == 2 and replaced["qur_id"] == "new"
    assert added["bots"][0]["chat_ids"] == [1]  # inputs untouched


def test_without_bot_returns_none_when_last_goes():
    cfg = _two()
    assert [b["token"] for b in notify.without_bot(cfg, TOKEN)["bots"]] == [TOKEN2]
    assert notify.without_bot(notify.without_bot(cfg, TOKEN), TOKEN2) is None


def test_send_all_goes_through_every_bot_and_one_failure_does_not_stop_the_rest(monkeypatch):
    calls = []

    def fake(token, method, params=None, timeout=0):
        if token == TOKEN:
            raise notify.NotifyError("bot blocked", 403)
        calls.append((token, params["chat_id"], params.get("reply_markup")))

    monkeypatch.setattr(notify, "_api", fake)
    errors = notify.send_all(_two(), "hi", lambda b: {"k": notify.bot_id(b["token"])})
    assert calls == [(TOKEN2, 111, {"k": "987654321"}), (TOKEN2, 222, {"k": "987654321"})]
    assert len(errors) == 1 and errors[0].startswith("bot 123456789:") and TOKEN.split(":")[1] not in errors[0]


def test_send_all_skips_bots_without_chats(monkeypatch):
    calls = []
    monkeypatch.setattr(notify, "_api", lambda token, method, params=None, timeout=0: calls.append(token))
    cfg = {"qur_id": "a", "bots": [{"token": TOKEN, "chat_ids": []}, {"token": TOKEN2, "chat_ids": [5]}]}
    assert notify.send_all(cfg, "hi") == [] and calls == [TOKEN2]


def test_export_of_two_bots_roundtrips_and_one_bot_stays_the_old_format():
    s = notify.export_string(_two())
    assert s.startswith("qurtg2:") and "a3f9" not in s
    assert notify.parse_export_bots(s) == [(TOKEN, [111]), (TOKEN2, [111, 222])]
    one = notify.export_string(_two(), [_two()["bots"][1]])
    assert one.startswith("qurtg1:")  # older qurs can still import a one-bot string
    assert notify.parse_export_bots(one) == [(TOKEN2, [111, 222])]
    assert notify.parse_export_bots(notify.export_string(CFG)) == [(TOKEN, [111, 222])]


def test_export_skips_bots_with_no_chats_and_refuses_when_nothing_left():
    cfg = {"qur_id": "a", "bots": [{"token": TOKEN, "chat_ids": []}, {"token": TOKEN2, "chat_ids": [5]}]}
    assert notify.parse_export_bots(notify.export_string(cfg)) == [(TOKEN2, [5])]
    with pytest.raises(ValueError):
        notify.export_string({"qur_id": "a", "bots": [{"token": TOKEN, "chat_ids": []}]})


def test_parse_export_string_refuses_multi_but_parse_export_bots_rejects_garbage():
    import base64

    def enc(obj):
        return "qurtg2:" + base64.urlsafe_b64encode(json.dumps(obj).encode()).decode()

    with pytest.raises(ValueError, match="several"):
        notify.parse_export_string(notify.export_string(_two()))
    for bad in (enc({"b": []}), enc({"b": [{"t": TOKEN, "c": []}]}), enc({"b": [{"t": "x", "c": [1]}]}),
                enc({"b": [{"t": TOKEN, "c": [1]}, {"t": TOKEN, "c": [2]}]}), enc({"x": 1}), "qurtg2:!!!"):
        with pytest.raises(ValueError):
            notify.parse_export_bots(bad)


def test_new_buttons_and_menu():
    rows = notify.job_markup(CFG, 42)["inline_keyboard"]
    assert [b["callback_data"] for r in rows for b in r] == ["rq:a3f9:42", "sh:a3f9:42", "lg:a3f9:42"]
    assert [b["callback_data"] for b in notify.details_markup(CFG, 5)["inline_keyboard"][0]] == ["sh:a3f9:5", "lg:a3f9:5"]
    assert notify.parse_callback("lg:a3f9:7") == ("lg", "a3f9", 7)
    picker = notify.list_picker_markup(CFG, [1, 2, 3, 4, 5])["inline_keyboard"]
    assert [len(r) for r in picker] == [4, 1] and picker[0][1]["callback_data"] == "lc:a3f9:2"
    assert notify.parse_callback("cn:a3f9:47") == ("cn", "a3f9", 47)
    chan = {"frozen": {"id": 5}, "running": {"id": 6}, "priority_queue": [{"id": 7}], "queue": [{"id": 8}], "backlog": [{"id": 1}, {"id": 2}]}
    rows = notify.list_markup(CFG, chan)["inline_keyboard"]
    assert [[b["callback_data"] for b in r] for r in rows] == [
        ["sh:a3f9:5", "lg:a3f9:5", "cn:a3f9:5"], ["sh:a3f9:6", "lg:a3f9:6", "cn:a3f9:6"],
        ["sh:a3f9:7", "cn:a3f9:7"], ["sh:a3f9:8", "cn:a3f9:8"],
        ["sh:a3f9:1", "lg:a3f9:1", "rl:a3f9:1"], ["sh:a3f9:2", "lg:a3f9:2", "rl:a3f9:2"]]
    assert notify.list_markup(CFG, {"queue": [], "backlog": []}) is None
    many = {"queue": [{"id": i} for i in range(500)], "backlog": [{"id": 900 + i} for i in range(10)]}
    n_buttons = sum(len(r) for r in notify.list_markup(CFG, many)["inline_keyboard"])
    assert n_buttons == notify.LIST_MAX_QUEUE_BUTTONS * 2 + 10 * 3 <= 100  # Telegram's cap
    assert notify.without_button(notify.list_markup(CFG, chan), "rl:a3f9:1")["inline_keyboard"][4] == [
        {"text": "ℹ️ #1", "callback_data": "sh:a3f9:1"}, {"text": "📄 #1", "callback_data": "lg:a3f9:1"}]
    assert notify.without_button({"inline_keyboard": [[{"text": "x", "callback_data": "a"}]]}, "a") == {"inline_keyboard": []}
    kb = notify.menu_markup()
    assert kb["is_persistent"] and [k["text"] for k in kb["keyboard"][0]] == [notify.MENU_STATUS, notify.MENU_LIST]
    assert notify.parse_menu(notify.MENU_LIST) == "list" and notify.parse_menu("/status") == "status"
    assert notify.parse_menu("hello") is None and notify.parse_menu(None) is None


def test_format_log_escapes_and_fits(tmp_path):
    job = {"id": 3, "status": "failed"}
    assert "<pre>a &lt;b&gt;</pre>" in notify.format_log(job, "a <b>\n", CFG)
    assert "(no log file)" in notify.format_log(job, None, CFG)
    assert "(log is empty)" in notify.format_log(job, "  \n", CFG)
    big = notify.format_log(job, "\n".join("<" * 100 for _ in range(30)), CFG)  # escaping x4 would overflow
    assert len(big) <= notify.TG_MAX_CHARS and "earlier output cut" in big
    assert "<script>" not in notify.format_log(job, "<script>alert(1)</script>", CFG)
    assert notify.format_log({"id": 3, "status": "<b>"}, "x", CFG).count("<b>") == 1  # only our own tag
    # one giant line still fits
    assert len(notify.format_log(job, "<" * 9000, CFG)) <= notify.TG_MAX_CHARS


def test_read_log_tail_lines_and_cleaning(tmp_path):
    log = tmp_path / "x.log"
    log.write_text("".join(f"line {i}\n" for i in range(100)))
    tail = notify.read_log_tail(log).split("\n")
    assert len(tail) == notify.LOG_TAIL_LINES >= 10 and tail[-1] == "line 99"
    assert notify.read_log_tail(log, lines=3) == "line 97\nline 98\nline 99"
    log.write_text("short\n")
    assert notify.read_log_tail(log) == "short"
    log.write_bytes(b"\x1b[31mred\x1b[0m\x00 bell\x07\rprogress 50%\rprogress 100%\n")
    assert notify.read_log_tail(log) == "red bell\nprogress 50%\nprogress 100%"
    log.write_bytes(b"x" * (notify.LOG_READ_BYTES + 10) + b"\nlast\n")  # cut-off first line is dropped
    assert notify.read_log_tail(log) == "last"
    assert notify.read_log_tail(tmp_path / "none") is None and notify.read_log_tail(None) is None


def test_format_status_and_list():
    job = {"id": 1, "raw_cmd": json.dumps(["echo", "<hi>"]), "started_at": "2026-01-01T00:00:00+00:00",
           "enqueued_at": "2026-01-01T00:00:00+00:00", "status": "done", "exit_code": 0, "finished_at": "2026-01-01T00:01:00+00:00"}
    data = {"channels": {1: {"running": job, "frozen": None, "paused": True, "queue": [job], "priority_queue": [], "backlog": [job] * 500}}}
    st = notify.format_status(data, CFG)
    assert "paused" in st and "&lt;hi&gt;" in st and "#1" in st
    ls = notify.format_list(data, CFG, 1)
    assert len(ls) <= notify.TG_MAX_CHARS and "more lines" in ls and "channel 1" in ls


def _list_job(i, cmd):
    return {"id": i, "raw_cmd": json.dumps(cmd), "status": "done", "exit_code": 0, "started_at": None, "finished_at": None,
            "enqueued_at": "2026-01-01T00:00:00+00:00"}


def test_list_shows_long_commands_in_full_and_shortens_only_when_crowded():
    word = "w" * 250
    few = {"channels": {1: {"queue": [], "backlog": [_list_job(i, ["run", word]) for i in range(5)]}}}
    assert notify.format_list(few, CFG, 1).count(word) == 5  # all five in full
    many = {"channels": {1: {"queue": [], "backlog": [_list_job(i, ["run", word]) for i in range(30)]}}}
    text = notify.format_list(many, CFG, 1)
    assert len(text) <= notify.TG_MAX_CHARS and "…" in text and "more lines" not in text  # shortened, all 30 kept
    huge = {"channels": {1: {"queue": [], "backlog": [_list_job(1, ["run", "q" * 5000])]}}}
    assert len(notify.format_list(huge, CFG, 1)) < 600  # one job: capped at LIST_CMD_MAX_CHARS


def test_details_shows_a_1000_char_command_in_full_and_still_fits():
    job = _job(raw_cmd=json.dumps(["echo", "a" * 1000]), resolved_cmd=json.dumps(["/bin/echo", "b" * 1000]))
    t = notify.format_details(job, CFG)
    assert "a" * 1000 in t and "b" * 1000 in t and len(t) <= notify.TG_MAX_CHARS
    nasty = _job(raw_cmd=json.dumps(["&" * 3000]), resolved_cmd=json.dumps(["<" * 3000]), cwd="&" * 900, log_path=">" * 900, note="&" * 900)
    assert len(notify.format_details(nasty, CFG)) <= notify.TG_MAX_CHARS  # escaping grows text; the cap shrinks to fit


def test_commands_have_no_quote_soup_and_match_the_cli_display():
    from queuer import cli

    argvs = [
        ["python", "-c", "print('a'); print('b')"],
        ["bash", "-c", "g16 < in.gjf > out.log"],
        ["echo", "it's", "$HOME", "", "plain", 'say "hi"'],
    ]
    for argv in argvs:
        assert notify._display_join(argv) == cli._display_join(argv)
        job = _job(raw_cmd=json.dumps(argv), resolved_cmd=json.dumps(argv))
        for text in (notify.format_job(job, CFG), notify.format_details(job, CFG), notify._short_cmd(job)):
            assert "'\"'\"'" not in text and "&#x27;\"&#x27;" not in text
    assert notify._display_join(argvs[0]) == "python -c \"print('a'); print('b')\""


# ---------------------------------------------------------------------------
# config permissions (the file holds bot tokens)
# ---------------------------------------------------------------------------

def _cfg_file(tmp_path, mode):
    d = tmp_path / "cfgdir"
    d.mkdir(mode=0o700)
    p = d / "notify.json"
    p.write_text("{}")
    os.chmod(p, mode)
    return p


def test_check_permissions_ok_and_missing(tmp_path):
    assert notify.check_permissions(_cfg_file(tmp_path, 0o600)) is None
    assert notify.check_permissions(tmp_path / "nope.json") is None


def test_check_permissions_reports_loose_file_without_changing_it(tmp_path):
    p = _cfg_file(tmp_path, 0o644)
    assert "644" in notify.check_permissions(p)
    assert stat.S_IMODE(p.stat().st_mode) == 0o644


def test_check_permissions_fix_tightens_file_and_folder(tmp_path):
    p = _cfg_file(tmp_path, 0o664)
    os.chmod(p.parent, 0o755)
    msg = notify.check_permissions(p, fix=True)
    assert "tightened" in msg
    assert stat.S_IMODE(p.stat().st_mode) == 0o600
    assert stat.S_IMODE(p.parent.stat().st_mode) == 0o700


def test_check_permissions_flags_symlink(tmp_path):
    real = _cfg_file(tmp_path, 0o600)
    link = tmp_path / "link.json"
    link.symlink_to(real)
    assert "symlink" in notify.check_permissions(link, fix=True)


def test_pairing_marker_dir_is_private(tmp_path):
    p = tmp_path / "new" / "notify.json"
    with notify.pairing_lock(p):
        assert stat.S_IMODE(p.parent.stat().st_mode) == 0o700


# ---------------------------------------------------------------------------
# round 3: no environment proxy for the bot token (S11), getUpdates wait
# ---------------------------------------------------------------------------

class _Resp:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def read(self):
        return b'{"ok": true, "result": {"username": "bot"}}'


def test_default_opener_has_an_empty_proxy_table():
    import urllib.request
    proxies = [h for h in notify._NO_PROXY_OPENER.handlers if isinstance(h, urllib.request.ProxyHandler)]
    assert len(proxies) == 1 and proxies[0].proxies == {}


def test_api_ignores_environment_proxies_by_default(monkeypatch):
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.invalid:3128")
    monkeypatch.setenv("ALL_PROXY", "http://proxy.invalid:3128")
    monkeypatch.delenv("QUEUER_USE_PROXY", raising=False)
    seen = []

    class Opener:
        def open(self, req, timeout=None):
            seen.append(req.full_url)
            return _Resp()

    monkeypatch.setattr(notify, "_NO_PROXY_OPENER", Opener())
    monkeypatch.setattr(notify.urllib.request, "urlopen", lambda *a, **k: pytest.fail("the proxy-honouring path was used"))
    assert notify._api(TOKEN, "getMe") == {"username": "bot"}
    assert len(seen) == 1


@pytest.mark.parametrize("value", ["", "0"])
def test_proxy_stays_off_for_empty_or_zero(monkeypatch, value):
    monkeypatch.setenv("QUEUER_USE_PROXY", value)
    seen = []

    class Opener:
        def open(self, req, timeout=None):
            seen.append(1)
            return _Resp()

    monkeypatch.setattr(notify, "_NO_PROXY_OPENER", Opener())
    monkeypatch.setattr(notify.urllib.request, "urlopen", lambda *a, **k: pytest.fail("proxy path used"))
    notify._api(TOKEN, "getMe")
    assert seen == [1]


def test_proxy_is_used_only_when_asked_for(monkeypatch):
    monkeypatch.setenv("QUEUER_USE_PROXY", "1")
    seen = []

    class Opener:
        def open(self, req, timeout=None):
            pytest.fail("the no-proxy opener was used")

    monkeypatch.setattr(notify, "_NO_PROXY_OPENER", Opener())
    monkeypatch.setattr(notify.urllib.request, "urlopen", lambda req, timeout=None: seen.append(timeout) or _Resp())
    assert notify._api(TOKEN, "getMe", timeout=7) == {"username": "bot"}
    assert seen == [7]


def test_get_updates_wait_defaults_to_a_long_poll_and_can_be_zero(monkeypatch):
    seen = []
    monkeypatch.setattr(notify, "_api", lambda token, method, params=None, timeout=0: seen.append((params, timeout)) or [])
    notify.get_updates(CFG, 5)
    notify.get_updates(CFG, None, 0)
    assert seen[0][0]["timeout"] == notify.POLL_SECS and seen[0][0]["offset"] == 5
    assert seen[1][0]["timeout"] == 0 and "offset" not in seen[1][0] and seen[1][1] == 10


# ---------------------------------------------------------------------------
# B5: interrupted jobs
# ---------------------------------------------------------------------------

def test_format_interrupted_says_daemon_stopped_not_failed(monkeypatch):
    monkeypatch.setattr(notify.socket, "gethostname", lambda: "bigbox")
    t = notify.format_job(_job(status="interrupted", exit_code=-15, note="daemon stopped"), CFG)
    assert t.startswith("⏹️ interrupted (daemon stopped) · job 12 · 2h13m\n")
    assert "failed" not in t
    assert t.count("daemon stopped") == 1  # in the head only, not repeated in the detail line


def test_interrupted_message_has_requeue_button():
    keys = [b["callback_data"] for row in notify.job_markup(CFG, 12)["inline_keyboard"] for b in row]
    assert any(k.startswith("rq:") for k in keys)


def test_legacy_restart_note_is_still_a_known_reason():
    t = notify.format_job(_job(status="failed", note="interrupted by daemon restart"), CFG)
    assert "interrupted by daemon restart" in t  # rows written before the interrupted status existed


# ---------------------------------------------------------------------------
# F2: dependencies in messages
# ---------------------------------------------------------------------------

def test_skipped_message_shows_the_dependency_reason():
    msg = notify.format_job(_job(status="skipped", exit_code=None, started_at=None, finished_at=None,
                                 note="dependency 7 failed"), CFG)
    assert "skipped" in msg and "dependency 7 failed" in msg


def test_list_and_details_show_what_a_queued_job_waits_on():
    job = _list_job(5, ["run"])
    job.update(status="queued", depends_on=[3], after_any=[4], waiting_on=[3, 4])
    data = {"channels": {1: {"queue": [job], "backlog": []}}}
    assert "waiting on 3, 4" in notify.format_list(data, CFG, 1)
    text = notify.format_details(_job(depends_on=[3], after_any=[4], waiting_on=[3, 4]), CFG)
    assert "depends on" in text and "after any" in text and "waiting on" in text


def test_format_paused_names_the_channel_job_and_resume_command():
    text = notify.format_paused(_job(id=7, channel=3), CFG)
    assert "channel 3 paused after job 7 failed" in text and "qur resume --channel 3" in text and "[a3f9]" in text


def test_details_show_on_fail_pause_only():
    assert "on fail" in notify.format_details(_job(on_fail="pause"), CFG)
    assert "on fail" not in notify.format_details(_job(on_fail="continue"), CFG)


def test_format_details_and_list_show_a_start_time():
    from datetime import datetime, timedelta, timezone

    future = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    past = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    base = {"id": 7, "raw_cmd": json.dumps(["echo"]), "resolved_cmd": json.dumps(["/bin/echo"]), "cwd": "/tmp",
            "channel": 1, "status": "queued", "enqueued_at": "2026-01-01T00:00:00+00:00"}
    assert "<b>not before</b>" in notify.format_details({**base, "not_before": future}, CFG)
    assert "<b>not before</b>" not in notify.format_details(base, CFG)
    data = {"channels": {1: {"running": None, "frozen": None, "paused": False, "priority_queue": [],
                             "backlog": [], "queue": [{**base, "not_before": future}]}}}
    assert "starts " in notify.format_list(data, CFG, 1)
    data["channels"][1]["queue"] = [{**base, "not_before": past}]
    assert "starts " not in notify.format_list(data, CFG, 1)


def test_format_details_lists_environment_masked_and_escaped():
    extra = {
        "OMP_NUM_THREADS": "8", "MY_SETTING": "<b>", "API_TOKEN": "topsecret",
        "QUR_ENV_PASSTHROUGH": "OMP_NUM_THREADS",
    }
    t = notify.format_details(_job(env_extra=json.dumps(extra)), CFG)
    assert "<b>environment</b>" in t
    assert "OMP_NUM_THREADS" in t and "OMP_NUM_THREADS=8" not in t  # passthrough: name only
    assert "MY_SETTING=&lt;b&gt;" in t and "API_TOKEN=***" in t
    assert "topsecret" not in t and "QUR_ENV_PASSTHROUGH" not in t
    assert "<b>environment</b>" not in notify.format_details(_job(), CFG)
    assert "<b>environment</b>" not in notify.format_details(_job(env_extra=None), CFG)


def test_telegram_list_stays_capped_however_long_the_history_is():
    backlog = [_job(id=1000 - i, status="done", raw_cmd=json.dumps(["echo", f"job-{i}"])) for i in range(200)]
    chan = {"running": None, "frozen": None, "paused": False, "priority_queue": [], "queue": [], "backlog": backlog}
    text = notify.format_list({"channels": {1: chan}}, CFG, 1)
    assert len(text) <= notify.TG_MAX_CHARS and "more lines" in text
    assert text.count("job-") <= notify.LIST_TEXT_MAX_BACKLOG == 100 and "job-100<" not in text
    markup = notify.list_markup(CFG, chan)["inline_keyboard"]
    assert sum(len(r) for r in markup) == notify.LIST_MAX_BACKLOG * 3 <= 100


def test_format_details_shows_the_retry_delay_only_when_set():
    base = dict(status="failed", max_retries=2, attempt=0)
    t = notify.format_details(_job(**base, retry_delay_secs=300, retry_backoff=1), CFG)
    assert "<b>retry delay</b>" in t and "5m0s, doubling every attempt" in t
    t = notify.format_details(_job(**base, retry_delay_secs=90, retry_backoff=0), CFG)
    assert "1m30s" in t and "doubling" not in t
    assert "retry delay" not in notify.format_details(_job(**base), CFG)


# ---------------------------------------------------------------------------
# secret redaction (F31/S4/S5): only Telegram text is masked
# ---------------------------------------------------------------------------

def test_finished_message_masks_secrets_in_the_command(monkeypatch):
    monkeypatch.setattr(notify.socket, "gethostname", lambda: "bigbox")
    argv = ["run", "--password", "hunter2", "--api-key=abc", "DB_TOKEN=xyz", "https://u:pw@h/x"]
    t = notify.format_job(_job(raw_cmd=json.dumps(argv)), CFG)
    assert "$ run --password *** --api-key=*** DB_TOKEN=*** https://u:***@h/x" in t
    for secret in ("hunter2", "abc", "xyz", ":pw@"):
        assert secret not in t


def test_normal_commands_are_not_changed_by_redaction(monkeypatch):
    monkeypatch.setattr(notify.socket, "gethostname", lambda: "bigbox")
    argv = ["python", "train.py", "--tokenizer=gpt2", "--max_tokens", "100", "--seed=3"]
    assert "$ python train.py --tokenizer=gpt2 --max_tokens 100 --seed=3\n" in notify.format_job(_job(raw_cmd=json.dumps(argv)), CFG)


def test_details_mask_both_commands_env_and_note():
    argv = ["run", "--token", "tok-123", "API_KEY=key-456"]
    extra = {"GH": "ghp_" + "a" * 36, "MY_PW": "pw-789"}  # a secret by shape, and a plain name holding one
    job = _job(raw_cmd=json.dumps(argv), resolved_cmd=json.dumps(["/bin/run", *argv[1:]]), env_extra=json.dumps(extra),
               status="failed", note="failed to start: DB_PASSWORD=note-000", cwd="/work")
    t = notify.format_details(job, CFG)
    for secret in ("tok-123", "key-456", "ghp_", "note-000"):
        assert secret not in t
    assert t.count("--token ***") == 2 and t.count("API_KEY=***") == 2
    assert "GH=***" in t and "DB_PASSWORD=***" in t
    assert "MY_PW=pw-789" in t  # a name that does not look secret and a value with no secret shape: shown as before
    assert "<code>/work</code>" in t


def test_details_environment_keeps_its_separators():
    t = notify.format_details(_job(env_extra=json.dumps({"API_TOKEN": "s", "B": "2"})), CFG)
    assert "<code>API_TOKEN=***, B=2</code>" in t


def test_log_tail_is_masked_and_normal_lines_are_kept():
    job = {"id": 3, "status": "done"}
    tail = "start\nstep 10 loss=0.5 tokens/s=1200\nTOKEN=abc123\nAuthorization: Bearer xyz\nghp_" + "a" * 36 + "\nok"
    t = notify.format_log(job, tail, CFG)
    for secret in ("abc123", "xyz", "ghp_"):
        assert secret not in t
    assert "TOKEN=***" in t and "Authorization: Bearer ***" in t
    assert "start" in t and "step 10 loss=0.5 tokens/s=1200" in t and t.rstrip().endswith("ok</pre>")


def test_log_tail_masking_cannot_overflow_the_message():
    job = {"id": 3, "status": "done"}
    assert len(notify.format_log(job, "\n".join("API_KEY=" + "x" * 200 for _ in range(100)), CFG)) <= notify.TG_MAX_CHARS


def test_list_and_status_mask_secrets():
    secret = "s3cr3t-value"
    queued = _list_job(1, ["train", "--token", secret])
    running = _list_job(2, ["x", f"API_KEY={secret}"])
    done = _list_job(3, ["curl", f"https://u:{secret}@h/x"])
    data = {"channels": {1: {"running": running, "frozen": None, "paused": False, "priority_queue": [],
                             "queue": [queued], "backlog": [done]}}}
    text = notify.format_list(data, CFG, 1) + notify.format_status(data, CFG) + notify._short_cmd(queued)
    assert secret not in text
    assert "train --token ***" in text and "API_KEY=***" in text and "https://u:***@h/x" in text


def test_user_patterns_apply_to_telegram_text(tmp_path, monkeypatch):
    p = tmp_path / "my-redact.json"
    p.write_text(json.dumps([r"project-x\d"]))
    os.chmod(p, 0o600)
    monkeypatch.setenv("QUEUER_REDACT_PATH", str(p))
    t = notify.format_job(_job(raw_cmd=json.dumps(["run", "project-x1"])), CFG)
    assert "$ run ***" in t and "project-x1" not in t
    assert "project-x1" not in notify.format_log({"id": 1, "status": "done"}, "using project-x1 now", CFG)


def test_a_broken_user_patterns_file_does_not_break_messages(tmp_path, monkeypatch, capsys):
    from queuer import redact

    monkeypatch.setattr(redact, "_warned", set())  # the warning is printed once per process
    monkeypatch.setattr(redact, "_extra_cache", None)
    p = tmp_path / "my-redact.json"
    p.write_text('["(broken"]')
    os.chmod(p, 0o600)
    monkeypatch.setenv("QUEUER_REDACT_PATH", str(p))
    t = notify.format_job(_job(raw_cmd=json.dumps(["run", "API_KEY=abc"])), CFG)
    assert "API_KEY=***" in t and "abc" not in t  # built-ins still apply
    assert "[queuerd] redact:" in capsys.readouterr().err


def test_malformed_command_json_still_gives_a_message():
    assert notify.format_job(_job(raw_cmd="not json"), CFG).startswith("✅ done")
    assert notify._short_cmd(_job(raw_cmd="5")) == "-"  # a number is not an argv


# ---------------------------------------------------------------------------
# Q5: failures that used to be silent are reported once on stderr
# ---------------------------------------------------------------------------

@pytest.fixture
def fresh_warnings(monkeypatch):
    monkeypatch.setattr(notify, "_WARNED", set())


def test_warn_once_prints_a_message_only_the_first_time(fresh_warnings, capsys):
    notify.warn_once("something broke")
    notify.warn_once("something broke")
    notify.warn_once("something else")
    err = capsys.readouterr().err
    assert err.count("something broke") == 1 and err.count("something else") == 1


def test_load_reports_an_unusable_file_without_leaking_its_content(fresh_warnings, capsys, tmp_path):
    path = tmp_path / "notify.json"
    path.write_text(json.dumps({"qur_id": "a", "token": "garbage-" + TOKEN, "chat_ids": [1]}))
    assert notify.load(path) is None
    assert notify.load(path) is None
    err = capsys.readouterr().err
    assert str(path) in err and "not configured" in err and TOKEN not in err
    assert err.count(str(path)) == 1  # once, not on every poll
    path.write_text("not json")
    assert notify.load(path) is None
    assert "ValueError" in capsys.readouterr().err


def test_load_of_a_missing_file_stays_quiet(fresh_warnings, capsys, tmp_path):
    assert notify.load(tmp_path / "nope.json") is None
    assert capsys.readouterr().err == ""


def test_read_log_tail_reports_unreadable_logs_but_not_missing_ones(fresh_warnings, capsys, tmp_path):
    assert notify.read_log_tail(tmp_path / "gone.log") is None
    assert capsys.readouterr().err == ""
    assert notify.read_log_tail(tmp_path) is None  # a directory: open() raises OSError
    assert "cannot read log" in capsys.readouterr().err


def test_answer_callback_and_edit_markup_report_failures_once(fresh_warnings, capsys, monkeypatch):
    def boom(*a, **k):
        raise notify.NotifyError("telegram said no")

    monkeypatch.setattr(notify, "_api", boom)
    cfg = {"token": TOKEN, "chat_ids": [1]}
    notify.answer_callback(cfg, "cb1", "hi")
    notify.answer_callback(cfg, "cb2", "hi")
    notify.edit_markup(cfg, 1, 2, {})
    err = capsys.readouterr().err
    assert err.count("could not answer a button press") == 1 and "could not update a message" in err
    assert TOKEN not in err


# ---------------------------------------------------------------------------
# F10: resource accounting in Telegram text
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "kb, text",
    [(0, "0 KB"), (1023, "1023 KB"), (1024, "1 MB"), (20 * 1024, "20 MB"), (1024 * 1024, "1.0 GB"), (32_505_856, "31.0 GB")],
)
def test_fmt_mem_kb(kb, text):
    assert notify._fmt_mem_kb(kb) == text


def test_finished_message_shows_peak_rss_and_cpu_time(monkeypatch):
    monkeypatch.setattr(notify.socket, "gethostname", lambda: "bigbox")
    text = notify.format_job(_job(max_rss_kb=32_505_856, cpu_user_secs=7000.0, cpu_sys_secs=100.0), CFG)
    assert text == (
        "✅ done · job 12 · 2h13m\n$ python secret.py\n"
        "bigbox [a3f9] · ch 1 · exit 0 · used 31.0 GB · 1h58m cpu"
    )


def test_finished_message_shows_only_the_usage_that_was_captured(monkeypatch):
    monkeypatch.setattr(notify.socket, "gethostname", lambda: "bigbox")
    only_mem = notify.format_job(_job(max_rss_kb=20 * 1024), CFG)
    assert only_mem.endswith("exit 0 · used 20 MB") and "cpu" not in only_mem
    only_cpu = notify.format_job(_job(cpu_user_secs=30.0, cpu_sys_secs=None), CFG)
    assert only_cpu.endswith("exit 0 · 30s cpu") and "used" not in only_cpu


def test_finished_message_has_no_usage_part_for_an_unsampled_job(monkeypatch):
    monkeypatch.setattr(notify.socket, "gethostname", lambda: "bigbox")
    text = notify.format_job(_job(max_rss_kb=None, cpu_user_secs=None, cpu_sys_secs=None), CFG)
    assert text == "✅ done · job 12 · 2h13m\n$ python secret.py\nbigbox [a3f9] · ch 1 · exit 0"


def test_failed_message_puts_usage_last(monkeypatch):
    monkeypatch.setattr(notify.socket, "gethostname", lambda: "bigbox")
    t = notify.format_job(
        _job(status="failed", exit_code=-15, note="timed out", finished_at="2026-10-14T10:00:30+00:00",
             max_rss_kb=20 * 1024, cpu_user_secs=20.0, cpu_sys_secs=1.0),
        CFG,
    )
    last = t.splitlines()[-1]
    assert last.startswith("bigbox [a3f9] · ch 1 · exit -15") and "timed out" in last
    assert last.endswith("used 20 MB · 21s cpu")


def test_format_details_shows_peak_rss_and_cpu_time_only_when_captured():
    base = dict(status="done")
    t = notify.format_details(_job(**base, max_rss_kb=32_505_856, cpu_user_secs=134.0, cpu_sys_secs=3.0), CFG)
    assert "<b>peak RSS</b> 31.0 GB" in t
    assert "<b>cpu time</b> 2m14s user + 3s sys" in t
    t = notify.format_details(_job(**base, max_rss_kb=20 * 1024), CFG)
    assert "<b>peak RSS</b> 20 MB" in t and "cpu time" not in t
    t = notify.format_details(_job(**base, cpu_user_secs=1.0, cpu_sys_secs=None), CFG)
    assert "<b>cpu time</b> 1s user + 0s sys" in t and "peak RSS" not in t
    t = notify.format_details(_job(**base), CFG)
    assert "peak RSS" not in t and "cpu time" not in t


def test_details_show_the_hooks_only_when_set():
    text = notify.format_details(_job(on_finish_cmd="rsync -a out/ host:", on_error_cmd="echo bad"), CFG)
    assert "on finish" in text and "rsync -a out/ host:" in text and "on error" in text and "echo bad" in text
    plain = notify.format_details(_job(), CFG)
    assert "on finish" not in plain and "on error" not in plain
