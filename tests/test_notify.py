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

    monkeypatch.setattr(notify.urllib.request, "urlopen", boom)
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
    assert "$ echo 'a b' 'x y'\n" in t
    long = notify.format_job(_job(raw_cmd=json.dumps(["echo", "z" * 500])), CFG)
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
    lm = notify.list_markup(CFG, [{"id": i} for i in range(4)])
    assert [len(r) for r in lm["inline_keyboard"]] == [3, 1] and notify.list_markup(CFG, []) is None
    assert notify.without_button(lm, "rl:a3f9:1")["inline_keyboard"][0][1]["callback_data"] == "rl:a3f9:2"
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
    big = notify.format_log(job, "<" * 3000, CFG)
    assert len(big) <= notify.TG_MAX_CHARS and "earlier output cut" in big
    log = tmp_path / "x.log"
    log.write_bytes(b"x" * 5000 + b"END")
    tail = notify.read_log_tail(str(log))
    assert len(tail) == notify.LOG_TAIL_BYTES and tail.endswith("END")
    assert notify.read_log_tail(str(tmp_path / "none")) is None


def test_format_status_and_list():
    job = {"id": 1, "raw_cmd": json.dumps(["echo", "<hi>"]), "started_at": "2026-01-01T00:00:00+00:00",
           "enqueued_at": "2026-01-01T00:00:00+00:00", "status": "done", "exit_code": 0, "finished_at": "2026-01-01T00:01:00+00:00"}
    data = {"channels": {1: {"running": job, "frozen": None, "paused": True, "queue": [job], "priority_queue": [], "backlog": [job] * 500}}}
    st = notify.format_status(data, CFG)
    assert "paused" in st and "&lt;hi&gt;" in st and "#1" in st
    ls = notify.format_list(data, CFG, 1)
    assert len(ls) <= notify.TG_MAX_CHARS and "more lines" in ls and "channel 1" in ls
