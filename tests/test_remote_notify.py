"""Phase 5: Telegram per machine. No network and no ssh: remote.ssh_run / ssh_json and the Telegram calls are faked."""

import asyncio
import json
from types import SimpleNamespace

import pytest

from queuer import hostcfg, notify, remote, remote_notify

LOCAL = "a3f9"


def job(i, status="done", **kw):
    return {"id": i, "status": status, "channel": 1, "raw_cmd": json.dumps(["lmp", "--token=hunter2", "-in", "x"]),
            "resolved_cmd": json.dumps(["/bin/lmp"]), "exit_code": 0, "started_at": "2026-10-20T10:00:00+00:00",
            "finished_at": "2026-10-20T11:00:00+00:00", "enqueued_at": "2026-10-20T09:00:00+00:00", **kw}


OVERVIEW = {"channels": {"1": {"paused": False, "running": job(5, "running"), "frozen": None,
                               "queue": [job(6, "queued"), job(7, "queued")], "priority_queue": [], "backlog": [job(4)]}}}


class Env:
    def __init__(self, monkeypatch, tmp_path, workers=("vm3",)):
        monkeypatch.setenv("QUEUER_HOSTS_PATH", str(tmp_path / "hosts" / "hosts.json"))
        cfg = hostcfg.load()
        for w in workers:
            cfg["hosts"][w] = {"ssh": w, "id": w}
        hostcfg.save(cfg)
        self.sent, self.toasts, self.edits, self.calls = [], [], [], []
        self.fail: Exception | None = None
        self.answers: dict[str, tuple[int, bytes, str]] = {}
        self.cfg = {"token": "1:x", "chat_ids": [7], "qur_id": LOCAL}
        self.to_chat = {**self.cfg, "chat_ids": [7]}
        monkeypatch.setattr(notify, "send", lambda cfg, text, markup=None, mode=None: self.sent.append((text, markup, mode)) or [])
        monkeypatch.setattr(notify, "edit_markup", lambda cfg, chat, mid, markup: self.edits.append(markup))
        monkeypatch.setattr(remote, "ssh_run", self.ssh_run)
        monkeypatch.setattr(remote, "ssh_json", self.ssh_json)
        remote_notify._requeued.clear()

        async def cmd(_args):
            return {"channels": {1: {"running": None, "frozen": None, "paused": False, "queue": [], "backlog": [job(1)]}}}

        self.daemon = SimpleNamespace(_cmd_status=cmd, _cmd_list=cmd)

    def ssh_run(self, host, argv, **kw):
        self.calls.append(list(argv))
        if self.fail:
            raise self.fail
        return self.answers.get(argv[0], (0, b"", ""))

    def ssh_json(self, host, argv, **kw):
        self.calls.append(list(argv))
        if self.fail:
            raise self.fail
        code, out, err = self.answers.get(argv[0], (0, json.dumps(OVERVIEW).encode(), ""))
        if code:
            raise remote.RemoteError(code, err)
        return json.loads(out)

    async def press(self, action, machine, job_id, markup=None):
        msg = {"chat": {"id": 7}, "message_id": 9, "reply_markup": markup}
        cq = {"id": "c1", "data": f"{action}:{machine}:{job_id}"}

        async def reply(text):
            self.toasts.append(text)

        return await remote_notify.handle_callback(self.daemon, self.cfg, self.to_chat, cq, msg, action, machine, job_id, reply)

    def menu(self, action, arg=None):
        return asyncio.run(remote_notify.menu(self.daemon, self.cfg, self.to_chat, action, arg))


@pytest.fixture
def env(monkeypatch, tmp_path):
    return Env(monkeypatch, tmp_path)


def test_old_and_machine_callbacks_parse():
    assert notify.parse_callback("rq:vm3:42") == ("rq", "vm3", 42)
    assert notify.parse_callback("rq:a3f9:42") == ("rq", "a3f9", 42)  # an old local button
    assert notify.parse_callback("ps:vm3:0") == ("ps", "vm3", 0) and notify.parse_callback("pl:a3f9:0") == ("pl", "a3f9", 0)
    assert notify.parse_callback("zz:vm3:1") is None
    assert all(len(b["callback_data"].encode()) <= 64 for row in remote_notify.job_buttons("x" * 16, 10**15)["inline_keyboard"] for b in row)


def test_menu_command_with_machine():
    assert notify.parse_menu("/status vm3") == "status" and notify.menu_machine("/status vm3") == "vm3"
    assert notify.parse_menu("/list  vm3 ") == "list" and notify.menu_machine("/list vm3") == "vm3"
    assert notify.menu_machine("/status") is None and notify.menu_machine(notify.MENU_STATUS) is None
    assert notify.parse_menu("/menu vm3") is None


def test_picker_only_when_workers_exist(monkeypatch, tmp_path):
    none = Env(monkeypatch, tmp_path, workers=())
    assert none.menu("status") is False and none.sent == []  # behaves as before
    env = Env(monkeypatch, tmp_path / "b")
    assert env.menu("status") is True
    text, markup, _ = env.sent[-1]
    data = [b["callback_data"] for row in markup["inline_keyboard"] for b in row]
    assert data == [f"ps:{LOCAL}:0", "ps:vm3:0"] and "pick a machine" in text
    env.menu("list")
    assert [b["callback_data"] for row in env.sent[-1][1]["inline_keyboard"] for b in row] == [f"pl:{LOCAL}:0", "pl:vm3:0"]


def test_host_status_has_a_section_per_worker(env):
    assert env.menu("status", LOCAL)
    text = env.sent[-1][0]
    assert "<b>vm3</b> · ▶️ 1 running · ⏳ 2 queued" in text
    env.fail = remote.HostUnreachable("down")
    env.menu("status", "here")
    assert "<b>vm3</b> · ⚠ unreachable" in env.sent[-1][0] and "Traceback" not in env.sent[-1][0]


def test_worker_status_and_list_flow(env):
    env.menu("status", "vm3")
    text = env.sent[-1][0]
    assert text.startswith("<b>Status</b> · vm3") and "#5" in text
    assert "hunter2" not in text  # redacted like local text
    env.menu("list", "vm3")
    picker = env.sent[-1][1]["inline_keyboard"][0][0]["callback_data"]
    assert picker == "lc:vm3:1"
    assert asyncio.run(env.press("lc", "vm3", 1)) is True
    assert env.toasts[0] == "fetching…"  # answered before the ssh result
    text, markup, mode = env.sent[-1]
    assert "List · channel 1</b> · vm3" in text and mode == "HTML" and "hunter2" not in text
    assert "cn:vm3:6" in json.dumps(markup) and "rl:vm3:4" in json.dumps(markup)
    assert env.menu("status", "nope") and "unknown machine" in env.sent[-1][0]


def test_details_and_logs(env):
    env.answers["show"] = (0, json.dumps(job(5, "failed", cwd="/w", log_path="/l")).encode(), "")
    env.answers["logs"] = (0, b"line1\n\x1b[31mred\x1b[0m token=abc123\n", "")
    asyncio.run(env.press("sh", "vm3", 5))
    assert "Job #5" in env.sent[-1][0] and "vm3" in env.sent[-1][0] and "hunter2" not in env.sent[-1][0]
    asyncio.run(env.press("lg", "vm3", 5))
    text = env.sent[-1][0]
    assert "red" in text and "\x1b" not in text and ["logs", "5", "-n", str(notify.LOG_TAIL_LINES)] in env.calls
    env.answers["show"] = (2, b"", "Error: no such job")
    asyncio.run(env.press("sh", "vm3", 99))
    assert "does not exist on vm3" in env.sent[-1][0]


def test_cancel_and_requeue_use_fixed_argv_and_follow_button_rules(env):
    markup = {"inline_keyboard": [[{"text": "x", "callback_data": "cn:vm3:6"}]]}
    asyncio.run(env.press("cn", "vm3", 6, markup))
    assert ["cancel", "6"] in env.calls and env.toasts[-1] == "cancelled job 6" and env.edits[-1] == {"inline_keyboard": []}
    env.answers["cancel"] = (1, b"", "Error: job 6 already finished")
    asyncio.run(env.press("cn", "vm3", 6, markup))
    assert env.toasts[-1] == "Error: job 6 already finished"
    env.answers["requeue"] = (0, b"Requeued job 4 as new job 12\n", "")
    asyncio.run(env.press("rq", "vm3", 4))
    assert ["requeue", "4"] in env.calls and env.toasts[-1] == "requeued as job 12"
    assert env.edits[-1] == notify.details_markup({"qur_id": "vm3"}, 4) and "job 4 requeued as job 12" in env.sent[-1][0]
    asyncio.run(env.press("rq", "vm3", 4))  # a second tap on the old keyboard
    assert env.toasts[-1] == "already requeued as job 12" and env.calls.count(["requeue", "4"]) == 1
    env.answers["requeue"] = (1, b"", "Error: job 9 has not finished")
    asyncio.run(env.press("rq", "vm3", 9))
    assert env.toasts[-1] == "Error: job 9 has not finished"


def test_unreachable_worker_is_a_plain_line(env):
    env.fail = remote.HostUnreachable("no route")
    asyncio.run(env.press("lc", "vm3", 1))
    assert env.sent[-1][0] == "⚠ vm3 unreachable"
    asyncio.run(env.press("rq", "vm3", 4))
    assert env.toasts[-1] == "⚠ vm3 unreachable"
    env.fail = remote.AuthFailed("denied")
    env.menu("status", "vm3")
    assert env.sent[-1][0] == "⚠ vm3 unreachable"


def test_unknown_machine_and_bad_answers(env):
    assert asyncio.run(env.press("rq", "ghost", 1)) is False  # the daemon says "another machine or an old id"
    assert asyncio.run(env.press("rq", "vm3", 2**70)) is True and env.toasts[-1] == "not a valid job id"
    env.answers["list"] = (0, b'{"channels": 5}', "")
    asyncio.run(env.press("lc", "vm3", 1))
    assert env.sent[-1][0].startswith("⚠ vm3")
    cleaned = remote_notify.clean_overview({"channels": {"1": {"running": {"id": "1:x", "status": "running"}, "queue": [{"id": 3, "status": "queued"}, {"id": None}]}}})
    assert cleaned["channels"][1]["running"] is None and [j["id"] for j in cleaned["channels"][1]["queue"]] == [3]


def test_watcher_message_carries_worker_buttons(monkeypatch, tmp_path):
    from queuer.remote_watch import RemoteWatcher

    cfg = {"qur_id": LOCAL, "bots": [{"token": "1:x", "chat_ids": [7]}]}
    seen = []
    monkeypatch.setattr(notify, "load", lambda path: cfg)
    monkeypatch.setattr(notify, "send", lambda c, text, markup=None, mode=None: seen.append(markup) or [])
    watcher = RemoteWatcher(SimpleNamespace(notify_path=tmp_path / "n.json"))
    assert asyncio.run(watcher.send("done", 42, "vm3")) is True
    assert "rq:vm3:42" in json.dumps(seen[-1])
    asyncio.run(watcher.send("note"))
    assert seen[-1] is None
