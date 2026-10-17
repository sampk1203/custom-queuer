import shlex
import shutil
from pathlib import Path

import click
import pytest

import json
from datetime import datetime, timedelta, timezone

from typer.testing import CliRunner

from queuer import client
from queuer.cli import (
    _build_job_argv,
    _fmt_elapsed,
    _fmt_origin,
    _fmt_secs,
    _needs_shell,
    _resolve_interpreter,
    app,
)


# ---------------------------------------------------------------------------
# _needs_shell
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "script",
    [
        "az login && ./deploy.sh",
        "g16 < input.gjf > output.log",
        "echo $HOME",
        "cat a.txt | wc -l",
        "cmd1; cmd2",
        "rm *.log",
        "echo `date`",
    ],
)
def test_needs_shell_true_for_shell_syntax(script):
    assert _needs_shell(script) is True


@pytest.mark.parametrize(
    "script",
    [
        "sleep 5",
        "python train.py --epochs 5",
        # parens/braces/brackets are ordinary code, not shell syntax --
        # must not force a bash wrapper.
        'python -c "print(1)"',
        'curl -d \'{"key": "value"}\' https://example.com/api',
    ],
)
def test_needs_shell_false_for_plain_commands(script):
    assert _needs_shell(script) is False


# ---------------------------------------------------------------------------
# _build_job_argv -- the core bug: bash should only wrap real shell syntax
# ---------------------------------------------------------------------------

def test_multiword_command_not_wrapped_in_bash():
    # This is the exact shape of the reported failure: `lmp -in in.lmp`
    # typed unquoted. Multiple words already tokenized by the invoking
    # shell -- no bash needed, no wrapper at all.
    argv = _build_job_argv(["lmp", "-in", "in.lmp"])
    assert argv == ["lmp", "-in", "in.lmp"]


def test_inline_eval_with_quotes_not_wrapped_in_bash():
    # This is the other reported failure: `python -c '...'` where the
    # inline script itself contains a quoted string literal. Previously
    # this got shlex.join'd into a bash -c script and then shlex.join'd
    # AGAIN on display, producing '"'"' quote soup. Now it's exec'd
    # directly -- no wrapping, no re-quoting, no mangled quotes.
    cmd = ["python", "-c", "from lammps import lammps; lammps().file('in_nvt')"]
    argv = _build_job_argv(cmd)
    assert argv == cmd
    # Display quoting (shlex.join, done once by the CLI's table rendering)
    # must match ordinary single-level shell quoting -- exactly what
    # shlex.join(cmd) itself produces, not that re-quoted a second time.
    # A double-escaped version would have this pattern appear 4 times
    # instead of 2 (one open + one close quote-escape per embedded ').
    displayed = shlex.join(argv)
    assert displayed == shlex.join(cmd)
    assert displayed.count('\'"\'"\'') == 2
    assert shlex.split(displayed) == cmd


def test_single_word_no_shell_syntax_is_split_not_wrapped():
    # `queuer add -- 'sleep 5'` -- quoted as one word but has no shell
    # syntax in it, so it should be split into a plain argv, not wrapped.
    argv = _build_job_argv(["sleep 5"])
    assert argv == ["sleep", "5"]


def test_single_word_with_shell_syntax_is_wrapped():
    argv = _build_job_argv(["az login && ./deploy.sh"])
    assert argv == ["bash", "-c", "az login && ./deploy.sh"]


def test_redirect_is_wrapped():
    argv = _build_job_argv(["g16 < input.gjf > output.log"])
    assert argv == ["bash", "-c", "g16 < input.gjf > output.log"]


def test_explicit_bash_c_passed_through_unwrapped():
    cmd = ["bash", "-c", "sleep 1 && sleep 2 && echo done"]
    assert _build_job_argv(cmd) == cmd


def test_multiword_json_arg_untouched():
    # A quoted multi-word argument (JSON payload) survives as one token
    # from the outer shell -- must be passed straight through, no bash.
    cmd = ["curl", "-d", '{"key": "value"}', "https://example.com/api"]
    assert _build_job_argv(cmd) == cmd


def test_empty_command_raises():
    with pytest.raises(ValueError):
        _build_job_argv([])
    with pytest.raises(ValueError):
        _build_job_argv([""])


def test_unbalanced_quotes_falls_back_to_bash():
    # can't be split safely with shlex -- hand it to a real shell rather
    # than raising or guessing.
    script = "echo 'unterminated"
    argv = _build_job_argv([script])
    assert argv == ["bash", "-c", script]


# ---------------------------------------------------------------------------
# _resolve_interpreter -- now runs on the *built* argv, after wrapping
# ---------------------------------------------------------------------------

def test_resolve_interpreter_resolves_bare_direct_exec_argv0():
    which_python = shutil.which("python3")
    assert which_python is not None
    argv = _resolve_interpreter(["python3", "-c", "print(1)"])
    assert argv[0] == which_python
    assert argv[1:] == ["-c", "print(1)"]


def test_resolve_interpreter_leaves_bash_c_shape_untouched():
    argv = ["bash", "-c", "az login && ./deploy.sh"]
    assert _resolve_interpreter(argv) == argv


def test_resolve_interpreter_leaves_unknown_command_untouched():
    argv = _resolve_interpreter(["definitely-not-a-real-command-xyz", "-in", "in.lmp"])
    assert argv == ["definitely-not-a-real-command-xyz", "-in", "in.lmp"]


# ---------------------------------------------------------------------------
# _display_join / _display_quote -- human-facing quoting for show/list.
# Must always round-trip exactly via shlex.split, and must not produce the
# repeated '"'"' escape-soup for a word with more than one embedded '.
# ---------------------------------------------------------------------------

from queuer.cli import _display_join, _display_quote  # noqa: E402


@pytest.mark.parametrize(
    "argv",
    [
        ["python", "-c", "from json import dumps; print(dumps({'k': 'in_nvt'}))"],
        ["python", "-c", "from lammps import lammps; lammps().file('in_nvt')"],
        ["lmp", "-in", "in.lmp"],
        ["curl", "-d", '{"key": "value"}', "https://example.com/api"],
        ["bash", "-c", "echo start && echo end"],
        ["bash", "-c", "g16 < input.gjf > output.log"],
        ["echo", "plain args here"],
        ["echo", "back`tick"],
        ["echo", "dollar$sign"],
    ],
)
def test_display_join_round_trips_exactly(argv):
    assert shlex.split(_display_join(argv)) == argv


def test_display_quote_avoids_quote_soup_for_embedded_single_quotes():
    word = "from json import dumps; print(dumps({'k': 'in_nvt'}))"
    quoted = _display_quote(word)
    # the classic POSIX single-quote-escape dance must not appear at all --
    # this word has two embedded ' and nothing that needs double-quote
    # escaping, so it should come back double-quoted, once, cleanly.
    assert '\'"\'"\'' not in quoted
    assert quoted == f'"{word}"'


def test_display_quote_falls_back_to_shlex_for_double_quotes_and_dollar():
    # a word with a literal " or $ can't be safely double-quoted as-is,
    # so this must fall back to standard shlex.quote (single-quote wrap).
    assert _display_quote('has "quotes"') == shlex.quote('has "quotes"')
    assert _display_quote("has $vars") == shlex.quote("has $vars")


def test_display_quote_leaves_plain_words_unquoted():
    assert _display_quote("train.py") == "train.py"
    assert _display_quote("--epochs") == "--epochs"


# ---------------------------------------------------------------------------
# elapsed / duration formatting
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "secs, expected",
    [(0, "0s"), (59, "59s"), (60, "1m0s"), (125, "2m5s"), (3600, "1h0m"), (7260, "2h1m")],
)
def test_fmt_secs(secs, expected):
    assert _fmt_secs(secs) == expected


def test_fmt_elapsed_counts_from_timestamp():
    ts = (datetime.now(timezone.utc) - timedelta(minutes=3, seconds=10)).isoformat()
    assert _fmt_elapsed(ts) == "3m10s"


def test_fmt_elapsed_none():
    assert _fmt_elapsed(None) == "-"


# ---------------------------------------------------------------------------
# list --channel / show with several IDs (daemon calls stubbed out)
# ---------------------------------------------------------------------------

runner = CliRunner()


def _job(job_id, channel=1, status="queued", **kw):
    base = {
        "id": job_id, "channel": channel, "status": status, "position": 0,
        "raw_cmd": json.dumps(["echo", str(job_id)]),
        "resolved_cmd": json.dumps(["/bin/echo", str(job_id)]),
        "cwd": "/tmp", "env_extra": None, "pid": None, "pgid": None,
        "enqueued_at": (datetime.now(timezone.utc) - timedelta(seconds=90)).isoformat(),
        "started_at": None, "finished_at": None, "exit_code": None,
        "log_path": f"/tmp/{job_id}.log", "note": None, "timeout_secs": None,
        "is_priority": 0, "max_retries": 0, "attempt": 0,
        "parent_id": None, "origin": None,
    }
    base.update(kw)
    return base


def _chan(queue=()):
    return {"running": None, "queue": list(queue), "priority_queue": [],
            "frozen": None, "backlog": [], "paused": False}


@pytest.fixture
def fake_daemon(monkeypatch):
    jobs = {1: _job(1), 2: _job(2, channel=2), 3: _job(3, status="done")}
    listing = {"channels": {"1": _chan([jobs[1]]), "2": _chan([jobs[2]])}}

    def send(cmd, **kw):
        if cmd == "list":
            return listing
        if cmd == "show":
            if kw["id"] not in jobs:
                raise client.DaemonError(f"job {kw['id']} does not exist")
            return jobs[kw["id"]]
        raise AssertionError(cmd)

    monkeypatch.setattr(client, "send", send)


def test_list_shows_queued_time_and_elapsed(fake_daemon):
    out = runner.invoke(app, ["list"]).output
    assert "Queued" in out and "Elapsed" in out
    assert "1m30s" in out or "1m31s" in out


def test_list_all_channels_by_default(fake_daemon):
    out = runner.invoke(app, ["list"]).output
    assert "channel 1" in out and "channel 2" in out


def test_list_channel_filter(fake_daemon):
    out = runner.invoke(app, ["list", "--channel", "2"]).output
    assert "channel 2" in out
    assert "channel 1" not in out


def test_list_channel_filter_json(fake_daemon):
    res = runner.invoke(app, ["list", "--channel", "2", "--json"])
    assert list(json.loads(res.output)["channels"]) == ["2"]


def test_list_unknown_channel_errors(fake_daemon):
    res = runner.invoke(app, ["list", "--channel", "9"])
    assert res.exit_code == 1


def test_show_multiple_ids(fake_daemon):
    res = runner.invoke(app, ["show", "1", "2", "3"])
    assert res.exit_code == 0
    assert "Job #1" in res.output and "Job #2" in res.output and "Job #3" in res.output


def test_show_one_bad_id_still_shows_others(fake_daemon):
    res = runner.invoke(app, ["show", "1", "99", "2"])
    assert res.exit_code == 1
    assert "Job #1" in res.output and "Job #2" in res.output
    assert "99" in res.output  # error message (stderr is mixed into output here)


def test_show_json_single_id_is_object(fake_daemon):
    data = json.loads(runner.invoke(app, ["show", "1", "--json"]).output)
    assert isinstance(data, dict) and data["id"] == 1


def test_show_json_multiple_ids_is_list(fake_daemon):
    data = json.loads(runner.invoke(app, ["show", "1", "2", "--json"]).output)
    assert [j["id"] for j in data] == [1, 2]


def test_show_displays_retry_count(monkeypatch):
    job = _job(5, status="failed", max_retries=3, attempt=1)
    monkeypatch.setattr(client, "send", lambda cmd, **kw: job)
    assert "1 of 3 used" in runner.invoke(app, ["show", "5"]).output


# ---------------------------------------------------------------------------
# origin: new / retry of #N / requeue of #N
# ---------------------------------------------------------------------------

def test_fmt_origin_new():
    assert _fmt_origin(_job(1)) == "new"


def test_fmt_origin_retry():
    job = _job(2, origin="retry", parent_id=1, attempt=1, max_retries=3)
    assert _fmt_origin(job) == "retry of #1 (1/3)"


def test_fmt_origin_requeue():
    assert _fmt_origin(_job(2, origin="requeue", parent_id=1)) == "requeue of #1"


def test_list_shows_origin(monkeypatch):
    retry = _job(7, origin="retry", parent_id=6, attempt=1, max_retries=2)
    requeued = _job(8, origin="requeue", parent_id=3)
    listing = {"channels": {"1": _chan([_job(5), retry, requeued])}}
    monkeypatch.setattr(client, "send", lambda cmd, **kw: listing)
    out = runner.invoke(app, ["list"], env={"COLUMNS": "200"}).output
    assert "new" in out
    assert "retry of #6 (1/2)" in out
    assert "requeue of #3" in out


def test_help_all_lists_every_command():
    res = runner.invoke(app, ["--help-all"])
    assert res.exit_code == 0
    for name in ["add", "list", "status", "show", "cancel", "clear", "rm",
                 "requeue", "pause", "resume", "logs", "clean-logs", "connect"]:
        assert f" {name} " in res.output


# ---------------------------------------------------------------------------
# cancel [JOB_ID]
# ---------------------------------------------------------------------------

def _capture_send(monkeypatch, reply):
    calls = []

    def fake(cmd, **kw):
        calls.append((cmd, kw))
        return reply

    monkeypatch.setattr(client, "send", fake)
    return calls


def test_cancel_without_args_sends_no_id_and_no_channel(monkeypatch):
    calls = _capture_send(monkeypatch, {"cancelled": 4, "channel": 1})
    res = runner.invoke(app, ["cancel"])
    assert res.exit_code == 0
    assert calls == [("cancel", {})]  # daemon applies its own default channel
    assert "Cancelled job 4 on channel 1" in res.output


def test_cancel_with_channel_only_is_unchanged(monkeypatch):
    calls = _capture_send(monkeypatch, {"cancelled": 9, "channel": 2})
    res = runner.invoke(app, ["cancel", "--channel", "2"])
    assert res.exit_code == 0
    assert calls == [("cancel", {"channel": 2})]


def test_cancel_with_job_id(monkeypatch):
    calls = _capture_send(monkeypatch, {"cancelled": 12, "channel": 3})
    res = runner.invoke(app, ["cancel", "12"])
    assert res.exit_code == 0
    assert calls == [("cancel", {"id": 12})]
    assert "Cancelled job 12 on channel 3" in res.output


def test_cancel_with_job_id_and_channel(monkeypatch):
    calls = _capture_send(monkeypatch, {"cancelled": 12, "channel": 3})
    runner.invoke(app, ["cancel", "12", "--channel", "3"])
    assert calls == [("cancel", {"id": 12, "channel": 3})]


def test_cancel_daemon_error_exits_nonzero(monkeypatch):
    def fake(cmd, **kw):
        raise client.DaemonError("job 12 is already done")

    monkeypatch.setattr(client, "send", fake)
    res = runner.invoke(app, ["cancel", "12"])
    assert res.exit_code == 1 and "already done" in res.output


# ---------------------------------------------------------------------------
# connect
# ---------------------------------------------------------------------------

from queuer import notify  # noqa: E402

TG_TOKEN = "123456789:AAH-test_token-0123456789abcdefghijk"


@pytest.fixture
def tg(monkeypatch):
    """Telegram replaced: records messages, getMe succeeds, pairing returns chat 555."""
    sent = []

    def fake_api(token, method, params=None, timeout=0):
        if method == "getMe":
            return {"username": "qur_test_bot"}
        if method == "sendMessage":
            sent.append((token, params["chat_id"], params["text"]))
            return {}
        raise AssertionError(method)

    monkeypatch.setattr(notify, "_api", fake_api)
    monkeypatch.setattr(notify, "pair", lambda token, on_link, timeout=0: (on_link("https://t.me/qur_test_bot?start=xyz"), 555)[1])
    return sent


def _one() -> dict:
    """The saved config of a one-bot machine in the flat shape
    {qur_id, token, chat_ids, listen?, username?}."""
    cfg = notify.load()
    assert cfg is not None and len(cfg["bots"]) == 1
    return {"qur_id": cfg["qur_id"], **cfg["bots"][0]}


def test_connect_first_run_prompts_token_shows_qr_and_saves(tg):
    res = runner.invoke(app, ["connect"], input=TG_TOKEN + "\n")
    assert res.exit_code == 0, res.output
    assert "https://t.me/qur_test_bot?start=xyz" in res.output
    cfg = _one()
    assert cfg["token"] == TG_TOKEN and cfg["chat_ids"] == [555] and len(cfg["qur_id"]) == 4
    assert cfg["username"] == "qur_test_bot"
    assert cfg["qur_id"] in res.output
    assert len(tg) == 1 and tg[0][1] == 555 and cfg["qur_id"] in tg[0][2]  # hello message


def test_connect_rejects_malformed_token(tg):
    res = runner.invoke(app, ["connect"], input="not-a-token\n")
    assert res.exit_code == 1 and "bot token" in res.output
    assert notify.load() is None


def test_connect_again_adds_a_second_chat_and_keeps_qur_id(tg, monkeypatch):
    runner.invoke(app, ["connect"], input=TG_TOKEN + "\n")
    first = _one()
    monkeypatch.setattr(notify, "pair", lambda token, on_link, timeout=0: 777)
    res = runner.invoke(app, ["connect"])  # token already saved: no prompt
    assert res.exit_code == 0, res.output
    cfg = _one()
    assert cfg["chat_ids"] == [555, 777] and cfg["qur_id"] == first["qur_id"]


def test_connect_same_chat_twice_is_not_duplicated(tg):
    runner.invoke(app, ["connect"], input=TG_TOKEN + "\n")
    runner.invoke(app, ["connect"])
    assert _one()["chat_ids"] == [555]


def test_connect_pairing_conflict_is_a_clean_error(tg, monkeypatch):
    def conflict(token, on_link, timeout=0):
        raise notify.PairingConflict("Conflict -- another qur may be pairing")

    monkeypatch.setattr(notify, "pair", conflict)
    res = runner.invoke(app, ["connect"], input=TG_TOKEN + "\n")
    assert res.exit_code == 1 and "another qur" in res.output
    assert notify.load() is None


def test_connect_timeout_is_a_clean_error(tg, monkeypatch):
    def slow(token, on_link, timeout=0):
        raise TimeoutError("nobody pressed Start in time")

    monkeypatch.setattr(notify, "pair", slow)
    res = runner.invoke(app, ["connect"], input=TG_TOKEN + "\n")
    assert res.exit_code == 1 and "nobody pressed Start" in res.output


def test_export_then_import_gives_each_machine_its_own_id(tg, tmp_path, monkeypatch):
    runner.invoke(app, ["connect"], input=TG_TOKEN + "\n")
    a = _one()
    exported = runner.invoke(app, ["connect", "--export"]).stdout.strip().splitlines()[0]
    assert exported.startswith("qurtg1:")

    # "second machine": a different config path
    monkeypatch.setenv("QUEUER_NOTIFY_PATH", str(tmp_path / "other" / "notify.json"))
    res = runner.invoke(app, ["connect", "--import", exported])
    assert res.exit_code == 0, res.output
    b = _one()
    assert b["token"] == a["token"] and b["chat_ids"] == a["chat_ids"]
    assert b["qur_id"] != a["qur_id"]  # own id per machine
    assert any(b["qur_id"] in m[2] for m in tg)  # hello message names the new id


def test_import_keeps_existing_qur_id(tg):
    runner.invoke(app, ["connect"], input=TG_TOKEN + "\n")
    cfg = _one()
    s = notify.export_string({**cfg, "chat_ids": [555, 888]})
    res = runner.invoke(app, ["connect", "--import", s])
    assert res.exit_code == 0
    after = _one()
    assert after["qur_id"] == cfg["qur_id"] and after["chat_ids"] == [555, 888]  # a chat we lacked is added


def test_import_garbage_fails_and_saves_nothing(tg):
    res = runner.invoke(app, ["connect", "--import", "qurtg1:!!!"])
    assert res.exit_code == 1 and "corrupt" in res.output
    assert notify.load() is None


def test_import_rejected_token_saves_nothing(tg, monkeypatch):
    def reject(token, method, params=None, timeout=0):
        raise notify.NotifyError("Unauthorized", 401)

    monkeypatch.setattr(notify, "_api", reject)
    s = notify.export_string({"token": TG_TOKEN, "chat_ids": [1]})
    res = runner.invoke(app, ["connect", "--import", s])
    assert res.exit_code == 1 and "rejected" in res.output
    assert notify.load() is None


def test_export_without_setup_errors(tg):
    res = runner.invoke(app, ["connect", "--export"])
    assert res.exit_code == 1 and "run `qur connect` first" in res.output


def test_connect_off_removes_config(tg):
    runner.invoke(app, ["connect"], input=TG_TOKEN + "\n")
    res = runner.invoke(app, ["connect", "--off"])
    assert res.exit_code == 0 and notify.load() is None
    assert runner.invoke(app, ["connect", "--off"]).exit_code == 0  # idempotent


def test_connect_options_are_mutually_exclusive(tg):
    res = runner.invoke(app, ["connect", "--export", "--off"])
    assert res.exit_code == 1 and "only one" in res.output


# ---------------------------------------------------------------------------
# connect: --id, --listen
# ---------------------------------------------------------------------------

def test_connect_id_renames_without_pairing(tg, monkeypatch):
    runner.invoke(app, ["connect"], input=TG_TOKEN + "\n")
    before = _one()
    monkeypatch.setattr(notify, "pair", lambda *a, **k: pytest.fail("must not pair"))
    res = runner.invoke(app, ["connect", "--id", "bigbox"])
    assert res.exit_code == 0 and "bigbox" in res.output
    after = _one()
    assert after["qur_id"] == "bigbox"
    assert after["token"] == before["token"] and after["chat_ids"] == before["chat_ids"]


@pytest.mark.parametrize("bad", ["", "has space", "x" * 17, "a:b"])
def test_connect_id_rejects_bad_ids(tg, bad):
    runner.invoke(app, ["connect"], input=TG_TOKEN + "\n")
    old = notify.load()["qur_id"]
    res = runner.invoke(app, ["connect", "--id", bad])
    assert res.exit_code == 1 and "id must be" in res.output
    assert notify.load()["qur_id"] == old


def test_connect_id_on_first_setup_is_used(tg):
    res = runner.invoke(app, ["connect", "--id", "lab-1"], input=TG_TOKEN + "\n")
    assert res.exit_code == 0
    assert notify.load()["qur_id"] == "lab-1"
    assert tg and "[lab-1]" in tg[0][2]  # hello message carries the new id


def test_connect_listen_toggle_keeps_everything_else(tg):
    runner.invoke(app, ["connect"], input=TG_TOKEN + "\n")
    before = _one()
    assert notify.listening(before)  # default on for a first machine
    assert runner.invoke(app, ["connect", "--no-listen"]).exit_code == 0
    off = _one()
    assert off["listen"] is False and off["qur_id"] == before["qur_id"]
    assert runner.invoke(app, ["connect", "--listen"]).exit_code == 0
    assert _one()["listen"] is True


def test_connect_import_is_send_only_unless_listen_given(tg, monkeypatch, tmp_path):
    runner.invoke(app, ["connect"], input=TG_TOKEN + "\n")
    exported = runner.invoke(app, ["connect", "--export"]).stdout.strip().splitlines()[0]
    monkeypatch.setenv("QUEUER_NOTIFY_PATH", str(tmp_path / "other" / "notify.json"))
    assert runner.invoke(app, ["connect", "--import", exported, "--id", "second"]).exit_code == 0
    cfg = _one()
    assert cfg["listen"] is False and cfg["qur_id"] == "second"
    assert runner.invoke(app, ["connect", "--import", exported, "--listen"]).exit_code == 0
    cfg = _one()
    assert cfg["listen"] is True and cfg["qur_id"] == "second"  # id kept on re-import


def test_connect_id_and_listen_conflict_with_export_and_off(tg):
    runner.invoke(app, ["connect"], input=TG_TOKEN + "\n")
    for flags in (["--export", "--id", "x"], ["--off", "--no-listen"]):
        res = runner.invoke(app, ["connect", *flags])
        assert res.exit_code == 1 and "cannot be combined" in res.output


def test_connect_pairing_holds_lock_and_releases_it(tg, monkeypatch):
    seen = []

    def pair(token, on_link, timeout=0):
        seen.append(notify.pairing_active())
        return 555

    monkeypatch.setattr(notify, "pair", pair)
    assert runner.invoke(app, ["connect"], input=TG_TOKEN + "\n").exit_code == 0
    assert seen == [True] and not notify.pairing_active()


# ---------------------------------------------------------------------------
# connect: --list, --remove-chat, --remove-bot
# ---------------------------------------------------------------------------

def test_connect_list_shows_this_machine_and_hides_token_secret(tg, monkeypatch):
    monkeypatch.setattr("socket.gethostname", lambda: "bigbox")
    assert "not set up" in runner.invoke(app, ["connect", "--list"]).output
    runner.invoke(app, ["connect", "--id", "lab-1"], input=TG_TOKEN + "\n")
    out = runner.invoke(app, ["connect", "--list"]).output
    assert "lab-1" in out and "bigbox" in out and "555" in out and "on" in out
    assert TG_TOKEN.split(":")[0] in out and TG_TOKEN.split(":")[1] not in out
    runner.invoke(app, ["connect", "--no-listen"])
    assert "send-only" in runner.invoke(app, ["connect", "--list"]).output


def test_connect_remove_chat_keeps_token_and_other_chats(tg, monkeypatch):
    runner.invoke(app, ["connect"], input=TG_TOKEN + "\n")
    monkeypatch.setattr(notify, "pair", lambda token, on_link, timeout=0: 777)
    runner.invoke(app, ["connect"])
    assert _one()["chat_ids"] == [555, 777]
    res = runner.invoke(app, ["connect", "--remove-chat", "555"])
    assert res.exit_code == 0 and "1 chat(s) left" in res.output
    cfg = _one()
    assert cfg["chat_ids"] == [777] and cfg["token"] == TG_TOKEN


def test_connect_remove_last_chat_warns(tg):
    runner.invoke(app, ["connect"], input=TG_TOKEN + "\n")
    res = runner.invoke(app, ["connect", "--remove-chat", "555"])
    assert res.exit_code == 0 and "no messages will be sent" in res.output
    assert _one()["chat_ids"] == []


def test_connect_remove_chat_unknown_or_unset_fails(tg):
    assert runner.invoke(app, ["connect", "--remove-chat", "1"]).exit_code == 1  # not set up
    runner.invoke(app, ["connect"], input=TG_TOKEN + "\n")
    res = runner.invoke(app, ["connect", "--remove-chat", "999"])
    assert res.exit_code == 1 and "not paired" in res.output
    assert _one()["chat_ids"] == [555]


def test_connect_remove_bot_is_off(tg):
    runner.invoke(app, ["connect"], input=TG_TOKEN + "\n")
    res = runner.invoke(app, ["connect", "--remove-bot"])
    assert res.exit_code == 0 and "removed" in res.output and notify.load() is None


def test_connect_new_modes_are_mutually_exclusive(tg):
    runner.invoke(app, ["connect"], input=TG_TOKEN + "\n")
    for flags in (["--list", "--off"], ["--remove-chat", "555", "--export"], ["--list", "--id", "x"],
                  ["--remove-chat", "555", "--no-listen"]):
        res = runner.invoke(app, ["connect", *flags])
        assert res.exit_code == 1, flags
    assert _one()["chat_ids"] == [555]


# ---------------------------------------------------------------------------
# connect --from (ssh pull)
# ---------------------------------------------------------------------------

def _fake_ssh(monkeypatch, *, stdout="", code=0, calls=None, raises=None):
    import subprocess

    def run(cmd, **kw):
        if calls is not None:
            calls.append(cmd)
        if raises:
            raise raises
        return subprocess.CompletedProcess(cmd, code, stdout=stdout)

    monkeypatch.setattr("queuer.cli.subprocess.run", run)


def test_connect_from_pulls_export_over_ssh_and_imports(tg, monkeypatch):
    s = notify.export_string({"token": TG_TOKEN, "chat_ids": [555]})
    calls = []
    _fake_ssh(monkeypatch, stdout=f"motd noise\n{s}\n", calls=calls)
    res = runner.invoke(app, ["connect", "--from", "sampk@laptop", "--id", "desktop"])
    assert res.exit_code == 0, res.output
    assert calls[0][:3] == ["ssh", "--", "sampk@laptop"] and "qur connect --export" in calls[0][3]
    cfg = _one()
    assert cfg["token"] == TG_TOKEN and cfg["chat_ids"] == [555]
    assert cfg["qur_id"] == "desktop" and cfg["listen"] is False  # send-only, the laptop keeps the buttons
    assert tg and "[desktop]" in tg[0][2]


@pytest.mark.parametrize("kw", [
    {"code": 255},                       # ssh failed / qur missing remotely
    {"stdout": "no string here\n"},      # remote not connected
    {"raises": FileNotFoundError()},     # no ssh binary
])
def test_connect_from_failures_save_nothing(tg, monkeypatch, kw):
    _fake_ssh(monkeypatch, **kw)
    res = runner.invoke(app, ["connect", "--from", "laptop"])
    assert res.exit_code == 1 and notify.load() is None


def test_connect_from_rejects_option_like_target_and_conflicts(tg, monkeypatch):
    calls = []
    _fake_ssh(monkeypatch, calls=calls)
    assert runner.invoke(app, ["connect", "--from", "-oProxyCommand=x"]).exit_code == 1
    assert runner.invoke(app, ["connect", "--from", "h", "--off"]).exit_code == 1
    assert runner.invoke(app, ["connect", "--from", "h", "--import", "qurtg1:x"]).exit_code == 1
    assert calls == [] and notify.load() is None


# ---------------------------------------------------------------------------
# connect: several bots
# ---------------------------------------------------------------------------

TG_TOKEN2 = "987654321:BBH-other_token-0123456789abcdefghijk"


@pytest.fixture
def tg2(monkeypatch):
    """Like `tg`, but each token has its own @username and pairing returns a
    chat id that the test can change via `chat["id"]`."""
    sent = []
    chat = {"id": 555}
    names = {TG_TOKEN: "first_bot", TG_TOKEN2: "second_bot"}

    def fake_api(token, method, params=None, timeout=0):
        if method == "getMe":
            if token not in names:
                raise notify.NotifyError("Unauthorized", 401)
            return {"username": names[token]}
        if method == "sendMessage":
            sent.append((token, params["chat_id"], params["text"]))
            return {}
        raise AssertionError(method)

    monkeypatch.setattr(notify, "_api", fake_api)
    paired = []
    monkeypatch.setattr(notify, "pair", lambda token, on_link, timeout=0: (paired.append(token), on_link("https://t.me/x?start=y"), chat["id"])[2])
    return type("T", (), {"sent": sent, "chat": chat, "paired": paired})


def _setup_two(tg2):
    assert runner.invoke(app, ["connect"], input=TG_TOKEN + "\n").exit_code == 0
    res = runner.invoke(app, ["connect", "--add-bot"], input=TG_TOKEN2 + "\n")
    assert res.exit_code == 0, res.output
    return notify.load()


def _bot(cfg, token):
    return next(b for b in cfg["bots"] if b["token"] == token)


def test_add_bot_connects_a_second_bot_and_keeps_the_first_untouched(tg2):
    runner.invoke(app, ["connect", "--id", "bigbox"], input=TG_TOKEN + "\n")
    before = notify.load()
    tg2.sent.clear()
    tg2.chat["id"] = 777
    res = runner.invoke(app, ["connect", "--add-bot"], input=TG_TOKEN2 + "\n")
    assert res.exit_code == 0, res.output
    assert "https://t.me/x?start=y" in res.output  # QR/link shown for the new bot as well
    cfg = notify.load()
    assert cfg["qur_id"] == "bigbox" and [b["token"] for b in cfg["bots"]] == [TG_TOKEN, TOKEN2 := TG_TOKEN2]
    assert cfg["bots"][0] == before["bots"][0]  # first bot unchanged
    assert cfg["bots"][1]["chat_ids"] == [777] and cfg["bots"][1]["username"] == "second_bot"
    assert tg2.paired[-1] == TG_TOKEN2
    assert [(t, c) for t, c, _ in tg2.sent] == [(TG_TOKEN2, 777)]  # hello through the new bot only
    assert "2 bots" in res.output and "@second_bot" in res.output and "@first_bot" in res.output


def test_add_bot_with_an_already_connected_token_just_pairs_another_phone(tg2):
    runner.invoke(app, ["connect"], input=TG_TOKEN + "\n")
    tg2.chat["id"] = 777
    assert runner.invoke(app, ["connect", "--add-bot"], input=TG_TOKEN + "\n").exit_code == 0
    cfg = notify.load()
    assert len(cfg["bots"]) == 1 and cfg["bots"][0]["chat_ids"] == [555, 777]


def test_add_bot_rejects_bad_or_unknown_tokens_and_saves_nothing(tg2):
    runner.invoke(app, ["connect"], input=TG_TOKEN + "\n")
    before = notify.load()
    res = runner.invoke(app, ["connect", "--add-bot"], input="nope\n")
    assert res.exit_code == 1 and "bot token" in res.output
    res = runner.invoke(app, ["connect", "--add-bot"], input="111111111:AAH-unknown\n")
    assert res.exit_code == 1 and "rejected" in res.output
    assert notify.load() == before


def test_add_bot_failed_pairing_leaves_config_alone(tg2, monkeypatch):
    runner.invoke(app, ["connect"], input=TG_TOKEN + "\n")
    before = notify.load()
    monkeypatch.setattr(notify, "pair", lambda *a, **k: (_ for _ in ()).throw(TimeoutError("nobody pressed Start in time")))
    res = runner.invoke(app, ["connect", "--add-bot"], input=TG_TOKEN2 + "\n")
    assert res.exit_code == 1 and notify.load() == before


def test_pairing_another_phone_with_two_bots_needs_bot(tg2):
    _setup_two(tg2)
    tg2.paired.clear()
    res = runner.invoke(app, ["connect"])
    assert res.exit_code == 1 and "--bot" in res.output and "--add-bot" in res.output and not tg2.paired
    tg2.chat["id"] = 888
    res = runner.invoke(app, ["connect", "--bot", "@second_bot"])
    assert res.exit_code == 0, res.output
    cfg = notify.load()
    assert tg2.paired == [TG_TOKEN2]
    assert _bot(cfg, TG_TOKEN2)["chat_ids"] == [555, 888] and _bot(cfg, TG_TOKEN)["chat_ids"] == [555]
    res = runner.invoke(app, ["connect", "--bot", "123456789"])  # by numeric id
    assert res.exit_code == 0 and tg2.paired[-1] == TG_TOKEN


def test_unknown_bot_is_a_clean_error(tg2):
    _setup_two(tg2)
    for flags in (["--bot", "nobody"], ["--bot", "nobody", "--no-listen"], ["--bot", "nobody", "--remove-chat", "555"],
                  ["--bot", "nobody", "--remove-bot"], ["--bot", "nobody", "--export"]):
        res = runner.invoke(app, ["connect", *flags])
        assert res.exit_code == 1 and "no connected bot matches" in res.output, flags


def test_list_shows_every_bot_and_never_a_secret(tg2):
    _setup_two(tg2)
    runner.invoke(app, ["connect", "--bot", "second_bot", "--no-listen"])
    out = runner.invoke(app, ["connect", "--list"]).output
    assert "@first_bot (123456789)" in out and "@second_bot (987654321)" in out
    assert out.count("Requeue/Details buttons") == 2 and "send-only" in out
    assert TG_TOKEN.split(":")[1] not in out and TG_TOKEN2.split(":")[1] not in out


def test_listen_applies_to_one_bot_with_bot_and_to_all_without(tg2):
    _setup_two(tg2)
    assert runner.invoke(app, ["connect", "--bot", "@second_bot", "--no-listen"]).exit_code == 0
    cfg = notify.load()
    assert notify.listening(_bot(cfg, TG_TOKEN)) and not notify.listening(_bot(cfg, TG_TOKEN2))
    assert runner.invoke(app, ["connect", "--no-listen"]).exit_code == 0
    cfg = notify.load()
    assert not notify.listening(_bot(cfg, TG_TOKEN)) and not notify.listening(_bot(cfg, TG_TOKEN2))
    assert runner.invoke(app, ["connect", "--listen"]).exit_code == 0
    assert all(notify.listening(b) for b in notify.load()["bots"])


def test_id_rename_is_machine_wide_and_keeps_every_bot(tg2):
    before = _setup_two(tg2)
    res = runner.invoke(app, ["connect", "--id", "desk"])
    assert res.exit_code == 0
    after = notify.load()
    assert after["qur_id"] == "desk" and after["bots"] == before["bots"]


def test_remove_bot_with_several_needs_bot_or_all(tg2):
    _setup_two(tg2)
    res = runner.invoke(app, ["connect", "--remove-bot"])
    assert res.exit_code == 1 and "--bot" in res.output and "--all" in res.output
    assert len(notify.load()["bots"]) == 2


def test_remove_one_bot_keeps_the_other_and_the_qur_id(tg2):
    before = _setup_two(tg2)
    res = runner.invoke(app, ["connect", "--remove-bot", "--bot", "@first_bot"])
    assert res.exit_code == 0 and "1 bot(s) left" in res.output, res.output
    cfg = notify.load()
    assert [b["token"] for b in cfg["bots"]] == [TG_TOKEN2] and cfg["qur_id"] == before["qur_id"]
    res = runner.invoke(app, ["connect", "--off", "--bot", "987654321"])  # the last one: whole setup goes
    assert res.exit_code == 0 and notify.load() is None


def test_remove_all_bots(tg2):
    _setup_two(tg2)
    res = runner.invoke(app, ["connect", "--remove-bot", "--all"])
    assert res.exit_code == 0 and "all bots" in res.output and notify.load() is None
    assert runner.invoke(app, ["connect", "--remove-bot", "--all"]).exit_code == 0  # idempotent


def test_remove_chat_hits_every_bot_that_has_it_or_just_the_named_one(tg2):
    _setup_two(tg2)
    tg2.chat["id"] = 777
    runner.invoke(app, ["connect", "--bot", "second_bot"])  # second bot: [555, 777]
    res = runner.invoke(app, ["connect", "--remove-chat", "555"])
    assert res.exit_code == 0 and "2 bot(s)" in res.output and "no chats left" in res.output.replace("\n", " ")
    cfg = notify.load()
    assert _bot(cfg, TG_TOKEN)["chat_ids"] == [] and _bot(cfg, TG_TOKEN2)["chat_ids"] == [777]
    runner.invoke(app, ["connect", "--bot", "first_bot"])  # first bot gets 777 too
    res = runner.invoke(app, ["connect", "--remove-chat", "777", "--bot", "first_bot"])
    assert res.exit_code == 0
    cfg = notify.load()
    assert _bot(cfg, TG_TOKEN)["chat_ids"] == [] and _bot(cfg, TG_TOKEN2)["chat_ids"] == [777]
    res = runner.invoke(app, ["connect", "--remove-chat", "777", "--bot", "first_bot"])
    assert res.exit_code == 1 and "not paired" in res.output


def test_export_all_bots_then_import_on_another_machine(tg2, monkeypatch, tmp_path):
    a = _setup_two(tg2)
    out = runner.invoke(app, ["connect", "--export"])
    exported = out.stdout.strip().splitlines()[0]
    assert exported.startswith("qurtg2:") and "tokens" in out.output
    monkeypatch.setenv("QUEUER_NOTIFY_PATH", str(tmp_path / "other" / "notify.json"))
    tg2.sent.clear()
    res = runner.invoke(app, ["connect", "--import", exported, "--id", "second"])
    assert res.exit_code == 0, res.output
    b = notify.load()
    assert b["qur_id"] == "second" and [x["token"] for x in b["bots"]] == [TG_TOKEN, TG_TOKEN2]
    assert all(x["listen"] is False and x["chat_ids"] == [555] for x in b["bots"])  # send-only
    assert {t for t, _c, _m in tg2.sent} == {TG_TOKEN, TG_TOKEN2}  # hello through each
    assert a["qur_id"] != b["qur_id"]


def test_export_one_bot_uses_the_old_single_bot_format(tg2):
    _setup_two(tg2)
    out = runner.invoke(app, ["connect", "--export", "--bot", "second_bot"])
    s = out.stdout.strip().splitlines()[0]
    assert s.startswith("qurtg1:") and notify.parse_export_bots(s) == [(TG_TOKEN2, [555])]


def test_export_skips_a_bot_with_no_chats_and_fails_if_none_has_any(tg2):
    _setup_two(tg2)
    runner.invoke(app, ["connect", "--remove-chat", "555", "--bot", "first_bot"])
    s = runner.invoke(app, ["connect", "--export"]).stdout.strip().splitlines()[0]
    assert notify.parse_export_bots(s) == [(TG_TOKEN2, [555])]
    res = runner.invoke(app, ["connect", "--export", "--bot", "first_bot"])
    assert res.exit_code == 1 and "nothing to export" in res.output


def test_import_adds_new_bots_send_only_and_keeps_existing_bot_settings(tg2):
    runner.invoke(app, ["connect", "--id", "mine"], input=TG_TOKEN + "\n")  # owns bot 1, listening
    s = notify.export_string({"bots": [{"token": TG_TOKEN, "chat_ids": [555, 999]}, {"token": TG_TOKEN2, "chat_ids": [4]}]})
    res = runner.invoke(app, ["connect", "--import", s])
    assert res.exit_code == 0, res.output
    cfg = notify.load()
    assert cfg["qur_id"] == "mine"
    one, two = _bot(cfg, TG_TOKEN), _bot(cfg, TG_TOKEN2)
    assert notify.listening(one) and one["chat_ids"] == [555, 999]  # kept its button, gained a chat
    assert two["listen"] is False and two["chat_ids"] == [4] and two["username"] == "second_bot"
    res = runner.invoke(app, ["connect", "--import", s, "--listen"])
    assert res.exit_code == 0 and all(notify.listening(b) for b in notify.load()["bots"])


def test_import_with_one_rejected_token_saves_nothing(tg2):
    s = notify.export_string({"bots": [{"token": TG_TOKEN, "chat_ids": [1]}, {"token": "111111111:AAH-unknown", "chat_ids": [2]}]})
    res = runner.invoke(app, ["connect", "--import", s])
    assert res.exit_code == 1 and "rejected" in res.output and notify.load() is None


def test_from_ssh_imports_every_bot(tg2, monkeypatch):
    s = notify.export_string({"bots": [{"token": TG_TOKEN, "chat_ids": [555]}, {"token": TG_TOKEN2, "chat_ids": [555]}]})
    _fake_ssh(monkeypatch, stdout=f"noise\n{s}\n")
    res = runner.invoke(app, ["connect", "--from", "sampk@laptop", "--id", "desktop"])
    assert res.exit_code == 0, res.output
    cfg = notify.load()
    assert cfg["qur_id"] == "desktop" and len(cfg["bots"]) == 2 and not any(b["listen"] for b in cfg["bots"])


@pytest.mark.parametrize("flags", [
    ["--bot", "x", "--import", "qurtg1:x"], ["--bot", "x", "--from", "h"], ["--bot", "x", "--list"],
    ["--bot", "x", "--add-bot"], ["--bot", "x", "--id", "y"], ["--all"], ["--all", "--export"],
    ["--remove-bot", "--all", "--bot", "x"], ["--add-bot", "--off"], ["--add-bot", "--list"], ["--add-bot", "--import", "qurtg1:x"],
])
def test_bad_flag_combinations_are_rejected_before_anything_happens(tg2, flags):
    runner.invoke(app, ["connect"], input=TG_TOKEN + "\n")
    before = notify.load()
    res = runner.invoke(app, ["connect", *flags])
    assert res.exit_code == 1, flags
    assert notify.load() == before


def test_old_single_bot_config_file_keeps_working(tg2, tmp_path):
    path = notify.default_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"qur_id": "a3f9", "token": TG_TOKEN, "chat_ids": [555]}))
    out = runner.invoke(app, ["connect", "--list"]).output
    assert "a3f9" in out and "123456789" in out and "555" in out
    tg2.chat["id"] = 777
    assert runner.invoke(app, ["connect"]).exit_code == 0  # pairs another phone, no token prompt
    cfg = notify.load()
    assert cfg["qur_id"] == "a3f9" and cfg["bots"][0]["chat_ids"] == [555, 777]
    assert "bots" in json.loads(path.read_text())  # rewritten in the new layout
    assert runner.invoke(app, ["connect", "--add-bot"], input=TG_TOKEN2 + "\n").exit_code == 0
    assert len(notify.load()["bots"]) == 2


def test_import_dash_reads_string_from_stdin_without_warning(tg, tmp_path, monkeypatch):
    runner.invoke(app, ["connect"], input=TG_TOKEN + "\n")
    a = _one()
    exported = runner.invoke(app, ["connect", "--export"]).stdout.strip().splitlines()[0]
    monkeypatch.setenv("QUEUER_NOTIFY_PATH", str(tmp_path / "other" / "notify.json"))
    res = runner.invoke(app, ["connect", "--import", "-"], input=exported + "\n")
    assert res.exit_code == 0, res.output
    assert _one()["token"] == a["token"]
    assert "visible in `ps`" not in res.output


def test_import_dash_empty_stdin_fails(tg):
    res = runner.invoke(app, ["connect", "--import", "-"], input="")
    assert res.exit_code == 1 and "nothing on stdin" in res.output


def test_import_on_argv_warns(tg):
    runner.invoke(app, ["connect"], input=TG_TOKEN + "\n")
    s = notify.export_string(_one())
    res = runner.invoke(app, ["connect", "--import", s])
    assert res.exit_code == 0 and "visible in `ps`" in res.output


# ---------------------------------------------------------------------------
# audit round: logs print verbatim, clean-logs never deletes on a missing DB
# ---------------------------------------------------------------------------

def test_logs_prints_brackets_verbatim(tmp_path, monkeypatch):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    (log_dir / "3.log").write_text("epoch [1/10] [red]x[/red] [/foo]\n")
    monkeypatch.setenv("QUEUER_LOG_DIR", str(log_dir))
    res = CliRunner().invoke(app, ["logs", "3"])
    assert res.exit_code == 0, res.output
    assert res.output == "epoch [1/10] [red]x[/red] [/foo]\n"


def test_clean_logs_refuses_when_db_missing(tmp_path, monkeypatch):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    (log_dir / "1.log").write_text("precious")
    monkeypatch.setenv("QUEUER_LOG_DIR", str(log_dir))
    monkeypatch.setenv("QUEUER_DB_PATH", str(tmp_path / "state" / "queuer.db"))
    res = CliRunner().invoke(app, ["clean-logs", "--days", "0"])
    assert res.exit_code != 0
    assert (log_dir / "1.log").read_text() == "precious"
    assert not (tmp_path / "state" / "queuer.db").exists()  # and it did not create a DB


def test_clean_logs_keeps_recent_orphans_and_deletes_old_ones(tmp_path, monkeypatch):
    import os, time
    from queuer import db

    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    dbp = tmp_path / "state" / "queuer.db"
    conn = db.connect(dbp)
    db.init_db(conn)
    conn.close()
    new, old = log_dir / "5.log", log_dir / "6.log"
    new.write_text("n")
    old.write_text("o")
    past = time.time() - 40 * 86400
    os.utime(old, (past, past))
    monkeypatch.setenv("QUEUER_LOG_DIR", str(log_dir))
    monkeypatch.setenv("QUEUER_DB_PATH", str(dbp))
    res = CliRunner().invoke(app, ["clean-logs", "--days", "30"])
    assert res.exit_code == 0, res.output
    assert new.exists() and not old.exists()


# ---------------------------------------------------------------------------
# round 3: qur logs streams and has -n; --before/--after help is honest
# ---------------------------------------------------------------------------

def _log(tmp_path, monkeypatch, text: str, job_id: int = 3):
    log_dir = tmp_path / "logs"
    log_dir.mkdir(exist_ok=True)
    (log_dir / f"{job_id}.log").write_bytes(text.encode())
    monkeypatch.setenv("QUEUER_LOG_DIR", str(log_dir))


def test_logs_never_reads_the_whole_file_into_memory(tmp_path, monkeypatch):
    _log(tmp_path, monkeypatch, "one\ntwo\n")

    def boom(*a, **k):
        raise AssertionError("read_text loads the whole log")

    monkeypatch.setattr(Path, "read_text", boom)
    res = CliRunner().invoke(app, ["logs", "3"])
    assert res.exit_code == 0, res.output
    assert res.output == "one\ntwo\n"


def test_logs_streams_a_log_bigger_than_one_chunk_intact(tmp_path, monkeypatch):
    body = "".join(f"line {i} [{i}/9]\n" for i in range(20000))  # ~300 KB, several 64 KB chunks
    _log(tmp_path, monkeypatch, body)
    res = CliRunner().invoke(app, ["logs", "3"])
    assert res.exit_code == 0 and res.output == body


@pytest.mark.parametrize("text,n,expected", [
    ("a\nb\nc\n", 2, "b\nc\n"),
    ("a\nb\nc", 2, "b\nc"),            # last line has no newline
    ("a\nb\nc\n", 1, "c\n"),
    ("a\nb\nc\n", 99, "a\nb\nc\n"),   # fewer lines than asked: all of them
    ("only", 3, "only"),
    ("a\n\n", 1, "\n"),                 # the last line is empty
])
def test_logs_lines_prints_the_last_n_lines(tmp_path, monkeypatch, text, n, expected):
    _log(tmp_path, monkeypatch, text)
    res = CliRunner().invoke(app, ["logs", "3", "-n", str(n)])
    assert res.exit_code == 0, res.output
    assert res.output == expected


def test_logs_lines_across_many_read_blocks(tmp_path, monkeypatch):
    lines = [f"row {i}\n" for i in range(5000)]  # far more than one 8 KB block
    _log(tmp_path, monkeypatch, "".join(lines))
    res = CliRunner().invoke(app, ["logs", "3", "--lines", "5"])
    assert res.output == "".join(lines[-5:])
    res = CliRunner().invoke(app, ["logs", "3", "--lines", "3000"])
    assert res.output == "".join(lines[-3000:])


def test_logs_lines_keeps_brackets_and_rejects_zero(tmp_path, monkeypatch):
    _log(tmp_path, monkeypatch, "x\n[red]y[/red] [/foo]\n")
    res = CliRunner().invoke(app, ["logs", "3", "-n", "1"])
    assert res.output == "[red]y[/red] [/foo]\n"
    assert CliRunner().invoke(app, ["logs", "3", "-n", "0"]).exit_code != 0


def test_before_after_help_says_position_only():
    import typer.main
    add = typer.main.get_command(app).commands["add"]
    helps = {p.name: (p.help or "").lower() for p in add.params}
    for name in ("before", "after"):
        assert "position only" in helps[name] and "does not" in helps[name]
    assert "keeps its memory" in helps["now"]


# ---------------------------------------------------------------------------
# B5: interrupted status in clean-logs
# ---------------------------------------------------------------------------

def test_clean_logs_deletes_old_interrupted_logs_but_not_running_ones(tmp_path, monkeypatch):
    import os, time
    from queuer import db

    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    dbp = tmp_path / "state" / "queuer.db"
    conn = db.connect(dbp)
    db.init_db(conn)
    ids = {}
    for name in ("interrupted", "running"):
        job_id = db.enqueue(
            conn, raw_cmd=["x"], resolved_cmd=["/bin/x"], cwd="/tmp", log_path=str(log_dir / f"{name}.log")
        )
        db.mark_running(conn, job_id, pid=1, pgid=1)
        if name == "interrupted":
            db.mark_finished(conn, job_id, status="interrupted", note="daemon stopped")
            conn.execute("UPDATE jobs SET finished_at = '2020-01-01T00:00:00+00:00' WHERE id = ?", (job_id,))
            conn.commit()
        ids[name] = job_id
    conn.close()
    past = time.time() - 40 * 86400
    for name, job_id in ids.items():
        f = log_dir / f"{job_id}.log"
        f.write_text(name)
        os.utime(f, (past, past))
    monkeypatch.setenv("QUEUER_LOG_DIR", str(log_dir))
    monkeypatch.setenv("QUEUER_DB_PATH", str(dbp))
    res = CliRunner().invoke(app, ["clean-logs", "--days", "30"])
    assert res.exit_code == 0, res.output
    assert not (log_dir / f"{ids['interrupted']}.log").exists()
    assert (log_dir / f"{ids['running']}.log").exists()


# ---------------------------------------------------------------------------
# F2: --depends-on / --after-any
# ---------------------------------------------------------------------------

from queuer.cli import _fmt_ids, _parse_job_ids  # noqa: E402


def test_parse_job_ids_lists_ranges_and_repeats():
    assert _parse_job_ids(["12"], "--depends-on") == [12]
    assert _parse_job_ids(["31-33"], "--depends-on") == [31, 32, 33]
    assert _parse_job_ids(["5,31-33", "40", "32"], "--depends-on") == [5, 31, 32, 33, 40]
    assert _parse_job_ids(None, "--depends-on") == []


@pytest.mark.parametrize("bad", ["abc", "1,,2", "", "0", "5-3", "1-", "-3", "1-5-9", "1-1001"])
def test_parse_job_ids_rejects_junk_with_a_clear_message(bad, capsys):
    import typer

    with pytest.raises(typer.Exit):
        _parse_job_ids([bad], "--depends-on")
    assert "--depends-on" in capsys.readouterr().err


def test_fmt_ids_collapses_runs():
    assert _fmt_ids([31, 32, 33, 40]) == "31-33, 40"
    assert _fmt_ids([5]) == "5" and _fmt_ids([5, 6]) == "5, 6" and _fmt_ids([]) == ""


def test_add_sends_dependencies_and_reports_them(monkeypatch):
    calls = _capture_send(monkeypatch, {"id": 9, "channel": 1})
    res = runner.invoke(app, ["add", "--depends-on", "31-33", "--depends-on", "40", "--after-any", "35,36", "--", "true"])
    assert res.exit_code == 0, res.output
    kw = calls[0][1]
    assert kw["depends_on"] == [31, 32, 33, 40] and kw["after_any"] == [35, 36]
    assert "31-33, 40" in res.output and "35, 36" in res.output


def test_add_without_dependencies_sends_empty_lists(monkeypatch):
    calls = _capture_send(monkeypatch, {"id": 9, "channel": 1})
    assert runner.invoke(app, ["add", "--", "true"]).exit_code == 0
    assert calls[0][1]["depends_on"] == [] and calls[0][1]["after_any"] == []


def test_add_reports_an_immediate_skip(monkeypatch):
    _capture_send(monkeypatch, {"id": 9, "channel": 1, "skipped": "dependency 4 failed"})
    res = runner.invoke(app, ["add", "--depends-on", "4", "--", "true"])
    assert res.exit_code == 0 and "dependency 4 failed" in res.output


def test_add_rejects_bad_ids_and_now_with_dependencies_without_calling_the_daemon(monkeypatch):
    calls = _capture_send(monkeypatch, {"id": 9, "channel": 1})
    assert runner.invoke(app, ["add", "--depends-on", "x", "--", "true"]).exit_code == 1
    res = runner.invoke(app, ["add", "--now", "--after-any", "3", "--", "true"])
    assert res.exit_code == 1 and "--now" in res.output
    assert calls == []


def test_list_shows_waits_on_column_only_when_needed(monkeypatch):
    waiting = _job(2, depends_on=[1], after_any=[], waiting_on=[1])
    plain = _job(3, depends_on=[], after_any=[], waiting_on=[])
    listing = {"channels": {"1": _chan([waiting, plain])}}
    monkeypatch.setattr(client, "send", lambda cmd, **kw: listing)
    out = runner.invoke(app, ["list"], env={"COLUMNS": "200"}).output
    assert "Waits on" in out
    listing = {"channels": {"1": _chan([plain])}}
    assert "Waits on" not in runner.invoke(app, ["list"], env={"COLUMNS": "200"}).output


def test_show_displays_dependencies(monkeypatch):
    job = _job(5, depends_on=[1, 2], after_any=[3], waiting_on=[2])
    monkeypatch.setattr(client, "send", lambda cmd, **kw: job)
    out = runner.invoke(app, ["show", "5"], env={"COLUMNS": "200"}).output
    assert "depends on" in out and "after any" in out and "waiting on" in out


# ---------------------------------------------------------------------------
# F3: --each / --array sweeps
# ---------------------------------------------------------------------------

from queuer.cli import _expand_each  # noqa: E402


def test_expand_each_splits_values_up_to_the_next_flag_or_double_dash():
    assert _expand_each(["--each", "a", "b", "c", "--", "run", "{}"]) == [
        "--each", "a", "--each", "b", "--each", "c", "--", "run", "{}",
    ]
    assert _expand_each(["--each", "1", "--channel", "2", "--each", "x", "--", "r", "{}"]) == [
        "--each", "1", "--channel", "2", "--each", "x", "--", "r", "{}",
    ]
    # the command after `--` is never touched, even if it says --each
    assert _expand_each(["--", "tool", "--each", "x"]) == ["--", "tool", "--each", "x"]
    with pytest.raises(click.UsageError):
        _expand_each(["--each", "--", "c"])


def test_add_each_makes_one_job_per_value_in_order(monkeypatch):
    calls = _capture_send(monkeypatch, {"id": 10, "channel": 1, "ids": [10, 11, 12]})
    res = runner.invoke(app, ["add", "--each", "a", "b", "c", "--", "echo", "x-{}", "--n={}"])
    assert res.exit_code == 0, res.output
    jobs = calls[0][1]["jobs"]
    assert [j["raw_argv"] for j in jobs] == [["echo", "x-a", "--n=a"], ["echo", "x-b", "--n=b"], ["echo", "x-c", "--n=c"]]
    assert [j["index"] for j in jobs] == [1, 2, 3]
    assert "argv" not in calls[0][1]
    assert "added jobs 10-12" in res.output


def test_add_each_quotes_values_only_where_a_shell_will_read_them(monkeypatch):
    calls = _capture_send(monkeypatch, {"id": 1, "channel": 1, "ids": [1, 2]})
    res = runner.invoke(app, ["add", "--each", "a b", "x;y", "--", "echo {} | wc -c"])
    assert res.exit_code == 0, res.output
    assert [j["raw_argv"] for j in calls[0][1]["jobs"]] == [["echo 'a b' | wc -c"], ["echo 'x;y' | wc -c"]]
    assert calls[0][1]["jobs"][0]["argv"][:2] == ["bash", "-c"]
    calls.clear()
    runner.invoke(app, ["add", "--each", "a b", "--", "echo", "{}"])
    assert calls[0][1]["jobs"][0]["raw_argv"] == ["echo", "a b"]  # separate words: no quoting needed


def test_add_each_without_placeholder_or_values_is_an_error(monkeypatch):
    calls = _capture_send(monkeypatch, {"id": 1, "channel": 1})
    res = runner.invoke(app, ["add", "--each", "a", "b", "--", "echo", "hi"])
    assert res.exit_code == 1 and "{}" in res.output
    assert runner.invoke(app, ["add", "--each", "--", "echo", "{}"]).exit_code != 0
    assert calls == []


def test_add_array_sets_index_and_keeps_the_command(monkeypatch):
    calls = _capture_send(monkeypatch, {"id": 5, "channel": 2, "ids": [5, 6, 7]})
    res = runner.invoke(app, ["add", "--array", "3-5", "--channel", "2", "--depends-on", "1", "--", "echo", "{}"])
    assert res.exit_code == 0, res.output
    kw = calls[0][1]
    assert [j["index"] for j in kw["jobs"]] == [3, 4, 5]
    assert all(j["raw_argv"] == ["echo", "{}"] for j in kw["jobs"])  # {} is only for --each
    assert kw["channel"] == 2 and kw["depends_on"] == [1]
    assert "added jobs 5-7" in res.output


@pytest.mark.parametrize(
    "flags",
    [["--array", "5"], ["--array", "5-3"], ["--array", "a-b"], ["--array", "1-1001"],
     ["--array", "1-2", "--each", "x"], ["--each"] + [str(i) for i in range(1001)]],
)
def test_add_sweep_errors_are_clear_and_nothing_is_sent(monkeypatch, flags):
    calls = _capture_send(monkeypatch, {"id": 1, "channel": 1})
    res = runner.invoke(app, ["add", *flags, "--", "echo", "{}"])
    assert res.exit_code == 1 and "Error" in res.output
    assert calls == []


def test_add_sweep_of_one_prints_a_plain_job_id(monkeypatch):
    _capture_send(monkeypatch, {"id": 8, "channel": 1, "ids": [8]})
    res = runner.invoke(app, ["add", "--array", "4-4", "--", "echo"])
    assert res.exit_code == 0 and "added jobs 8" in res.output


# ---------------------------------------------------------------------------
# F6: --on-fail
# ---------------------------------------------------------------------------

def test_add_sends_on_fail_default_and_pause(monkeypatch):
    calls = _capture_send(monkeypatch, {"id": 9, "channel": 1})
    assert runner.invoke(app, ["add", "--", "true"]).exit_code == 0
    assert calls[0][1]["on_fail"] == "continue"
    res = runner.invoke(app, ["add", "--on-fail", "pause", "--", "true"])
    assert res.exit_code == 0, res.output
    assert calls[1][1]["on_fail"] == "pause" and "paused" in res.output


def test_add_rejects_an_unknown_on_fail_without_calling_the_daemon(monkeypatch):
    calls = _capture_send(monkeypatch, {"id": 9, "channel": 1})
    res = runner.invoke(app, ["add", "--on-fail", "stop", "--", "true"])
    assert res.exit_code == 1 and "pause" in res.output and "continue" in res.output
    assert calls == []


def test_show_displays_on_fail_only_when_it_is_pause(monkeypatch):
    monkeypatch.setattr(client, "send", lambda cmd, **kw: _job(5, on_fail="pause"))
    assert "on fail" in runner.invoke(app, ["show", "5"], env={"COLUMNS": "200"}).output
    monkeypatch.setattr(client, "send", lambda cmd, **kw: _job(5, on_fail="continue"))
    assert "on fail" not in runner.invoke(app, ["show", "5"], env={"COLUMNS": "200"}).output


# ---------------------------------------------------------------------------
# F7: --delay / --at
# ---------------------------------------------------------------------------

from queuer.cli import parse_at, parse_delay  # noqa: E402


@pytest.mark.parametrize(
    "text, secs",
    [("30s", 30), ("30m", 1800), ("2h", 7200), ("1d", 86400), ("1h30m", 5400), (" 90S ", 90), ("1d2h3m4s", 93784)],
)
def test_parse_delay_accepts_units_and_chains(text, secs):
    assert parse_delay(text) == timedelta(seconds=secs)


@pytest.mark.parametrize("text", ["", "30", "m", "1.5h", "-5m", "5 min", "1w", "30m5", "0s", "0m0s"])
def test_parse_delay_rejects_junk_and_zero(text):
    with pytest.raises(ValueError):
        parse_delay(text)


def test_parse_delay_rejects_an_absurdly_long_delay():
    with pytest.raises(ValueError):
        parse_delay("99999999999999d")


def _local_now(*parts):
    return datetime(*parts).astimezone()


def test_parse_at_clock_time_today_when_still_ahead():
    now = _local_now(2026, 10, 16, 21, 0)
    assert parse_at("22:00", now) == _local_now(2026, 10, 16, 22, 0)
    assert parse_at("9:05", _local_now(2026, 10, 16, 8, 0)) == _local_now(2026, 10, 16, 9, 5)


def test_parse_at_clock_time_tomorrow_when_passed_or_equal():
    now = _local_now(2026, 10, 16, 21, 0)
    assert parse_at("06:30", now) == _local_now(2026, 10, 17, 6, 30)
    assert parse_at("21:00", now) == _local_now(2026, 10, 17, 21, 0)  # "now" is not the future
    assert parse_at("08:00", _local_now(2026, 12, 31, 23, 0)) == _local_now(2027, 1, 1, 8, 0)  # year roll


def test_parse_at_full_date_time():
    now = _local_now(2026, 10, 16, 21, 0)
    assert parse_at("2026-12-01 06:30", now) == _local_now(2026, 12, 1, 6, 30)
    assert parse_at("2026-12-01T06:30", now) == _local_now(2026, 12, 1, 6, 30)


@pytest.mark.parametrize("text", ["", "25:00", "12:60", "noon", "22", "22:00:00 pm", "2026-13-01 10:00", "2026-02-30 10:00", "2026-12-01"])
def test_parse_at_rejects_junk(text):
    with pytest.raises(ValueError):
        parse_at(text, _local_now(2026, 10, 16, 21, 0))


def test_parse_at_rejects_a_date_in_the_past():
    with pytest.raises(ValueError, match="past"):
        parse_at("2026-10-16 20:59", _local_now(2026, 10, 16, 21, 0))


def test_add_delay_sends_an_absolute_utc_not_before(monkeypatch):
    calls = _capture_send(monkeypatch, {"id": 3, "channel": 1})
    before = datetime.now(timezone.utc)
    res = runner.invoke(app, ["add", "--delay", "30m", "--", "true"])
    after = datetime.now(timezone.utc)
    assert res.exit_code == 0, res.output
    sent = datetime.fromisoformat(calls[0][1]["not_before"])
    assert sent.utcoffset() == timedelta(0)
    assert before + timedelta(minutes=30) <= sent <= after + timedelta(minutes=30)
    assert "starts no earlier than" in res.output


def test_add_at_sends_not_before_and_none_without_the_flags(monkeypatch):
    calls = _capture_send(monkeypatch, {"id": 3, "channel": 1})
    assert runner.invoke(app, ["add", "--", "true"]).exit_code == 0
    assert calls[0][1]["not_before"] is None and "starts no earlier" not in runner.invoke(app, ["add", "--", "true"]).output
    res = runner.invoke(app, ["add", "--at", "22:00", "--", "true"])
    assert res.exit_code == 0, res.output
    when = datetime.fromisoformat(calls[-1][1]["not_before"]).astimezone()
    assert (when.hour, when.minute) == (22, 0) and when > datetime.now(timezone.utc)
    assert when - datetime.now(timezone.utc) <= timedelta(hours=24, minutes=1)


@pytest.mark.parametrize(
    "extra",
    [
        ["--delay", "5m", "--at", "22:00"],
        ["--delay", "soon"],
        ["--delay", "0m"],
        ["--at", "25:99"],
        ["--at", "2001-01-01 10:00"],
        ["--now", "--delay", "5m"],
        ["--now", "--at", "22:00"],
    ],
)
def test_add_rejects_bad_delay_flags_without_calling_the_daemon(monkeypatch, extra):
    calls = _capture_send(monkeypatch, {"id": 3, "channel": 1})
    res = runner.invoke(app, ["add", *extra, "--", "true"])
    assert res.exit_code == 1 and calls == []


def test_add_delay_applies_to_every_job_of_a_sweep(monkeypatch):
    calls = _capture_send(monkeypatch, {"id": 3, "channel": 1, "ids": [3, 4]})
    res = runner.invoke(app, ["add", "--delay", "1h", "--array", "1-2", "--", "echo"])
    assert res.exit_code == 0, res.output
    assert calls[0][1]["not_before"] and len(calls[0][1]["jobs"]) == 2


def test_list_shows_a_starts_at_column_only_for_future_start_times(monkeypatch):
    future = (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat()
    past = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    chan = {"running": None, "frozen": None, "paused": False, "priority_queue": [], "backlog": [],
            "queue": [_job(1, not_before=future), _job(2)]}
    monkeypatch.setattr(client, "send", lambda cmd, **kw: {"channels": {"1": chan}})
    out = runner.invoke(app, ["list"], env={"COLUMNS": "200"}).output
    assert "Starts at" in out
    chan["queue"] = [_job(1, not_before=past), _job(2)]
    out = runner.invoke(app, ["list"], env={"COLUMNS": "200"}).output
    assert "Starts at" not in out


def test_show_displays_not_before(monkeypatch):
    future = (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat()
    monkeypatch.setattr(client, "send", lambda cmd, **kw: _job(5, not_before=future))
    out = runner.invoke(app, ["show", "5"], env={"COLUMNS": "200"}).output
    assert "not before" in out and "still waiting" in out
    monkeypatch.setattr(client, "send", lambda cmd, **kw: _job(5, status="done", not_before=future))
    assert "still waiting" not in runner.invoke(app, ["show", "5"], env={"COLUMNS": "200"}).output
    monkeypatch.setattr(client, "send", lambda cmd, **kw: _job(5))
    assert "not before" not in runner.invoke(app, ["show", "5"], env={"COLUMNS": "200"}).output


# ---------------------------------------------------------------------------
# F1/B13: --env and environment passthrough
# ---------------------------------------------------------------------------

@pytest.fixture
def clean_env(monkeypatch):
    """A caller environment with a known set of variables."""
    import os

    for name in list(os.environ):
        if name.startswith(("LC_", "QUEUER_ENV")) or name in (
            "CUDA_VISIBLE_DEVICES", "OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS",
            "LD_LIBRARY_PATH", "LANG", "TERM", "TZ",
        ):
            monkeypatch.delenv(name)
    return monkeypatch


def test_add_sends_explicit_env_and_copies_bare_keys(monkeypatch, clean_env):
    clean_env.setenv("FROM_SHELL", "shellval")
    calls = _capture_send(monkeypatch, {"id": 9, "channel": 1})
    res = runner.invoke(app, ["add", "--env", "A=1", "--env", "FROM_SHELL", "--env", "B=x=y", "--", "true"])
    assert res.exit_code == 0, res.output
    assert calls[0][1]["env"] == {"A": "1", "FROM_SHELL": "shellval", "B": "x=y"}
    assert "environment: A, FROM_SHELL, B" in res.output and "shellval" not in res.output


@pytest.mark.parametrize("flag", ["1BAD=x", "A-B=x", "=x", "NOT_SET_ANYWHERE_XYZ"])
def test_add_rejects_bad_env_without_calling_the_daemon(monkeypatch, clean_env, flag):
    calls = _capture_send(monkeypatch, {"id": 9, "channel": 1})
    res = runner.invoke(app, ["add", "--env", flag, "--", "true"])
    assert res.exit_code == 1 and calls == []


def test_add_sends_the_default_passthrough_only(monkeypatch, clean_env):
    clean_env.setenv("CUDA_VISIBLE_DEVICES", "1")
    clean_env.setenv("OMP_NUM_THREADS", "8")
    clean_env.setenv("LC_ALL", "C.UTF-8")
    clean_env.setenv("MY_API_TOKEN", "do-not-leak")
    calls = _capture_send(monkeypatch, {"id": 9, "channel": 1})
    assert runner.invoke(app, ["add", "--", "true"]).exit_code == 0
    kw = calls[0][1]
    assert kw["env"] == {}
    assert kw["env_pass"] == {"CUDA_VISIBLE_DEVICES": "1", "OMP_NUM_THREADS": "8", "LC_ALL": "C.UTF-8"}


def test_add_passthrough_list_can_be_overridden_or_emptied(monkeypatch, clean_env):
    clean_env.setenv("CUDA_VISIBLE_DEVICES", "1")
    clean_env.setenv("MPI_RANK", "2")
    clean_env.setenv("QUEUER_ENV_PASSTHROUGH", "MPI_*")
    calls = _capture_send(monkeypatch, {"id": 9, "channel": 1})
    assert runner.invoke(app, ["add", "--", "true"]).exit_code == 0
    assert calls[0][1]["env_pass"] == {"MPI_RANK": "2"}
    clean_env.setenv("QUEUER_ENV_PASSTHROUGH", "")
    assert runner.invoke(app, ["add", "--", "true"]).exit_code == 0
    assert calls[1][1]["env_pass"] == {}


def test_explicit_env_wins_over_passthrough_of_the_same_name(monkeypatch, clean_env):
    clean_env.setenv("OMP_NUM_THREADS", "8")
    calls = _capture_send(monkeypatch, {"id": 9, "channel": 1})
    assert runner.invoke(app, ["add", "--env", "OMP_NUM_THREADS=2", "--", "true"]).exit_code == 0
    kw = calls[0][1]
    assert kw["env"] == {"OMP_NUM_THREADS": "2"} and "OMP_NUM_THREADS" not in kw["env_pass"]


def test_add_env_flag_is_not_swallowed_by_each(monkeypatch, clean_env):
    calls = _capture_send(monkeypatch, {"id": 9, "ids": [9, 10], "channel": 1})
    res = runner.invoke(app, ["add", "--each", "a", "b", "--env", "X=1", "--", "echo", "{}"])
    assert res.exit_code == 0, res.output
    assert calls[0][1]["env"] == {"X": "1"} and len(calls[0][1]["jobs"]) == 2


def test_show_displays_environment_masked(monkeypatch):
    extra = {
        "OMP_NUM_THREADS": "8", "MY_SETTING": "hello", "API_TOKEN": "topsecret", "PATH": "/x:/y",
        "QUR_ENV_PASSTHROUGH": "OMP_NUM_THREADS",
    }
    monkeypatch.setattr(client, "send", lambda cmd, **kw: _job(5, env_extra=json.dumps(extra)))
    out = runner.invoke(app, ["show", "5"], env={"COLUMNS": "200"}).output
    assert "environment" in out and "MY_SETTING=hello" in out and "API_TOKEN=***" in out
    assert "OMP_NUM_THREADS" in out and "OMP_NUM_THREADS=8" not in out
    assert "topsecret" not in out and "/x:/y" not in out and "QUR_ENV_PASSTHROUGH" not in out
    monkeypatch.setattr(client, "send", lambda cmd, **kw: _job(5))
    assert "environment" not in runner.invoke(app, ["show", "5"], env={"COLUMNS": "200"}).output


# ---------------------------------------------------------------------------
# F9/U3: qur history, show ranges, list title
# ---------------------------------------------------------------------------

from queuer.cli import _collect_job_ids, parse_since  # noqa: E402


def test_parse_since_accepts_durations_and_dates():
    now = datetime(2026, 10, 16, 12, 0).astimezone()
    assert parse_since("2d", now) == now - timedelta(days=2)
    assert parse_since("1h30m", now) == now - timedelta(minutes=90)
    assert parse_since(" 6H ", now) == now - timedelta(hours=6)
    assert parse_since("2026-10-01", now) == datetime(2026, 10, 1).astimezone()
    assert parse_since("2026-10-01 14:30", now) == datetime(2026, 10, 1, 14, 30).astimezone()
    assert parse_since("2026-10-01T09:05", now) == datetime(2026, 10, 1, 9, 5).astimezone()


@pytest.mark.parametrize("bad", ["", "5", "yesterday", "0d", "2026-13-01", "2026-02-30", "10-01", "2d3"])
def test_parse_since_rejects_junk(bad):
    now = datetime(2026, 10, 16, 12, 0).astimezone()
    with pytest.raises(ValueError):
        parse_since(bad, now)


def test_collect_job_ids_keeps_order_and_expands_ranges():
    assert _collect_job_ids(["14", "12"], "X") == [14, 12]
    assert _collect_job_ids(["31-33", "40,12", "32"], "X") == [31, 32, 33, 40, 12]
    assert _collect_job_ids(None, "X") == []


def test_show_accepts_ranges_and_keeps_json_shape(fake_daemon):
    res = runner.invoke(app, ["show", "1-3"])
    assert res.exit_code == 0, res.output
    assert all(f"Job #{i}" in res.output for i in (1, 2, 3))
    assert [j["id"] for j in json.loads(runner.invoke(app, ["show", "2-3", "--json"]).output)] == [2, 3]
    assert json.loads(runner.invoke(app, ["show", "2-2", "--json"]).output)["id"] == 2  # one id -> an object
    res = runner.invoke(app, ["show", "3", "1"])
    assert res.output.index("Job #3") < res.output.index("Job #1")  # order as typed


def test_show_range_with_unknown_ids_reports_them_and_exits_1(fake_daemon):
    res = runner.invoke(app, ["show", "2-5"])
    assert res.exit_code == 1 and "Job #2" in res.output and "Job #3" in res.output
    assert "4" in res.output and "5" in res.output


def test_show_rejects_junk_ids(fake_daemon):
    assert runner.invoke(app, ["show", "abc"]).exit_code == 1
    assert runner.invoke(app, ["show", "5-2"]).exit_code == 1


def _history_rows():
    return [
        _job(7, channel=2, status="failed", exit_code=1, finished_at="2026-10-15T10:00:00+00:00",
             started_at="2026-10-15T09:00:00+00:00", raw_cmd=json.dumps(["python", "train.py"])),
        _job(5, status="done", exit_code=0, finished_at="2026-10-14T10:00:00+00:00"),
    ]


def test_history_sends_filters_and_prints_a_table(monkeypatch):
    calls = _capture_send(monkeypatch, {"jobs": _history_rows()})
    res = runner.invoke(app, ["history", "--status", "failed", "--status", "interrupted", "--channel", "2",
                              "--grep", "train", "--limit", "20"], env={"COLUMNS": "200"})
    assert res.exit_code == 0, res.output
    cmd, kw = calls[0]
    assert cmd == "history"
    assert kw == {"statuses": ["failed", "interrupted"], "channel": 2, "since": None, "grep": "train", "limit": 20}
    assert "History (2)" in res.output and "python train.py" in res.output and "failed" in res.output


def test_history_defaults_send_no_filters(monkeypatch):
    calls = _capture_send(monkeypatch, {"jobs": []})
    res = runner.invoke(app, ["history"])
    assert res.exit_code == 0 and "no matching jobs" in res.output
    assert calls[0][1] == {"statuses": None, "channel": None, "since": None, "grep": None, "limit": 50}


def test_history_since_is_sent_as_an_absolute_utc_timestamp(monkeypatch):
    calls = _capture_send(monkeypatch, {"jobs": []})
    before = datetime.now(timezone.utc)
    assert runner.invoke(app, ["history", "--since", "2d"]).exit_code == 0
    sent = datetime.fromisoformat(calls[0][1]["since"])
    assert sent.utcoffset() == timedelta(0)
    assert timedelta(days=2) - timedelta(seconds=5) <= before - sent <= timedelta(days=2) + timedelta(seconds=5)
    assert runner.invoke(app, ["history", "--since", "2026-10-01"]).exit_code == 0
    assert datetime.fromisoformat(calls[1][1]["since"]) == datetime(2026, 10, 1).astimezone()


@pytest.mark.parametrize("flags", [
    ["--status", "queued"], ["--status", "bogus"], ["--since", "yesterday"], ["--limit", "0"],
    ["--limit", "100000"], ["--channel", "0"],
])
def test_history_rejects_bad_options_without_calling_the_daemon(monkeypatch, flags):
    calls = _capture_send(monkeypatch, {"jobs": []})
    res = runner.invoke(app, ["history", *flags])
    assert res.exit_code != 0 and calls == []


def test_history_json_is_a_plain_list(monkeypatch):
    _capture_send(monkeypatch, {"jobs": _history_rows()})
    data = json.loads(runner.invoke(app, ["history", "--json"]).output)
    assert [j["id"] for j in data] == [7, 5] and data[0]["status"] == "failed"
    _capture_send(monkeypatch, {"jobs": []})
    assert json.loads(runner.invoke(app, ["history", "--json"]).output) == []


def test_history_notes_when_the_limit_was_reached(monkeypatch):
    _capture_send(monkeypatch, {"jobs": _history_rows()})
    assert "limit 2 reached" in runner.invoke(app, ["history", "--limit", "2"]).output
    assert "limit" not in runner.invoke(app, ["history", "--limit", "3"]).output.replace("--limit", "")


def test_history_full_shows_resolved_commands(monkeypatch):
    _capture_send(monkeypatch, {"jobs": [_job(5, status="done", finished_at="2026-10-14T10:00:00+00:00")]})
    wide = {"COLUMNS": "200"}
    assert "/bin/echo" in runner.invoke(app, ["history", "--full"], env=wide).output
    assert "/bin/echo" not in runner.invoke(app, ["history"], env=wide).output


def test_list_title_reflects_the_real_number_of_recent_jobs(monkeypatch):
    def listing(n):
        chan = _chan()
        chan["backlog"] = [_job(i, status="done", finished_at="2026-10-14T10:00:00+00:00") for i in range(1, n + 1)]
        return {"channels": {"1": chan}}

    monkeypatch.setattr(client, "send", lambda cmd, **kw: listing(3))
    out = runner.invoke(app, ["list"], env={"COLUMNS": "200"}).output
    assert "Recent (last 3)" in out and "qur history" not in out
    monkeypatch.setattr(client, "send", lambda cmd, **kw: listing(10))
    out = runner.invoke(app, ["list"], env={"COLUMNS": "200"}).output
    assert "Recent (last 10)" in out and "qur history" in out
    monkeypatch.setattr(client, "send", lambda cmd, **kw: listing(0))
    out = runner.invoke(app, ["list"], env={"COLUMNS": "200"}).output
    assert "Recent" in out and "last" not in out


def test_clean_logs_works_with_a_long_history(tmp_path, monkeypatch):
    """30 finished rows are kept (more than the old 10): old ones lose their log, fresh ones keep it,
    and a log whose row is gone is an orphan only when it is also old."""
    import os
    import time

    from queuer import db

    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    dbp = tmp_path / "state" / "queuer.db"
    conn = db.connect(dbp)
    db.init_db(conn)
    old_ids, new_ids = [], []
    for n in range(30):
        job_id = db.enqueue(conn, raw_cmd=["x"], resolved_cmd=["/bin/x"], cwd="/tmp", log_path=str(log_dir / f"{n}.log"))
        db.mark_running(conn, job_id, pid=1, pgid=1)
        db.mark_finished(conn, job_id, status="done", exit_code=0, keep_backlog=db.DEFAULT_KEEP)
        if n < 20:
            conn.execute("UPDATE jobs SET finished_at = '2020-01-01T00:00:00+00:00' WHERE id = ?", (job_id,))
            old_ids.append(job_id)
        else:
            new_ids.append(job_id)
    conn.commit()
    assert len(db.get_history(conn, limit=100)) == 30
    conn.close()
    past = time.time() - 40 * 86400
    for job_id in old_ids + new_ids:
        f = log_dir / f"{job_id}.log"
        f.write_text("x")
        if job_id in old_ids:
            os.utime(f, (past, past))
    (log_dir / "9999.log").write_text("orphan, fresh")  # no row, but young: stays
    monkeypatch.setenv("QUEUER_LOG_DIR", str(log_dir))
    monkeypatch.setenv("QUEUER_DB_PATH", str(dbp))
    res = CliRunner().invoke(app, ["clean-logs", "--days", "30"])
    assert res.exit_code == 0, res.output
    assert not any((log_dir / f"{i}.log").exists() for i in old_ids)
    assert all((log_dir / f"{i}.log").exists() for i in new_ids)
    assert (log_dir / "9999.log").exists()


# ---------------------------------------------------------------------------
# qur wait / --json on show and status (daemon calls stubbed out)
# ---------------------------------------------------------------------------

import types  # noqa: E402

import queuer.cli as cli_mod  # noqa: E402


def _ended(job_id, status="done", exit_code=0, **kw):
    return _job(job_id, status=status, exit_code=exit_code, started_at="2026-01-01T00:00:00+00:00",
                finished_at="2026-01-01T00:01:00+00:00", **kw)


class _WaitEnv:
    """Scripted daemon for `qur wait`: `jobs[id]` is a list of replies to successive `show`
    calls (the last one repeats; None = the job does not exist (any more)); `lists` are
    successive `list` replies. Time is fake: each sleep advances the clock."""

    def __init__(self, monkeypatch, jobs=None, lists=None):
        self.jobs = {k: list(v) for k, v in (jobs or {}).items()}
        self.lists = list(lists or [])
        self.sleeps = []
        self.shows = []
        self.now = 1000.0
        self.down = False
        monkeypatch.setattr(client, "send", self._send)
        monkeypatch.setattr(cli_mod, "time", types.SimpleNamespace(sleep=self._sleep, monotonic=lambda: self.now))

    def _sleep(self, secs):
        self.sleeps.append(secs)
        self.now += secs

    def _send(self, cmd, **kw):
        if self.down:
            raise client.DaemonNotRunning("queuerd is not running")
        if cmd == "show":
            self.shows.append(kw["id"])
            seq = self.jobs.get(kw["id"])
            if seq is None:
                raise client.DaemonError(f"job {kw['id']} does not exist")
            item = seq.pop(0) if len(seq) > 1 else seq[0]
            if item is None:
                raise client.DaemonError(f"job {kw['id']} does not exist")
            return item
        if cmd == "list":
            return self.lists.pop(0) if len(self.lists) > 1 else self.lists[0]
        raise AssertionError(cmd)


def _idle_chan(**kw):
    return {**_chan(), **kw}


def test_wait_done_exits_0_and_says_so(monkeypatch):
    env = _WaitEnv(monkeypatch, {5: [_job(5, status="running"), _ended(5)]})
    res = runner.invoke(app, ["wait", "5"])
    assert res.exit_code == 0 and "job 5: done (exit 0)" in res.output
    assert env.sleeps == [cli_mod.WAIT_POLL_SECS]  # one poll interval between the two replies


@pytest.mark.parametrize("status,code", [
    ("done", 0), ("failed", 1), ("cancelled", 2), ("skipped", 2), ("interrupted", 2),
])
def test_wait_exit_code_mirrors_the_final_status(monkeypatch, status, code):
    _WaitEnv(monkeypatch, {7: [_ended(7, status=status, exit_code=None if status != "failed" else 3)]})
    res = runner.invoke(app, ["wait", "7"])
    assert res.exit_code == code and status in res.output


def test_wait_several_jobs_exits_with_the_worst_code(monkeypatch):
    jobs = {1: [_ended(1)], 2: [_ended(2, status="cancelled", exit_code=None)], 3: [_ended(3, status="failed", exit_code=1)]}
    _WaitEnv(monkeypatch, jobs)
    assert runner.invoke(app, ["wait", "1", "2"]).exit_code == 2
    assert runner.invoke(app, ["wait", "1", "3"]).exit_code == 1
    assert runner.invoke(app, ["wait", "1", "2", "3"]).exit_code == 1  # a failure beats cancelled
    assert runner.invoke(app, ["wait", "1"]).exit_code == 0


def test_wait_takes_ranges_and_waits_for_the_slowest(monkeypatch):
    env = _WaitEnv(monkeypatch, {
        31: [_ended(31)],
        32: [_job(32, status="queued"), _job(32, status="running"), _ended(32)],
        33: [_job(33, status="queued"), _job(33, status="queued"), _job(33, status="queued"), _ended(33)],
    })
    res = runner.invoke(app, ["wait", "31-33"])
    assert res.exit_code == 0
    assert len(env.sleeps) == 3
    assert env.shows.count(31) == 1  # a finished job is not asked about again
    assert [ln for ln in res.output.splitlines() if ln.startswith("job ")][0].startswith("job 31")


def test_wait_unknown_id_fails_at_once_with_exit_4(monkeypatch):
    env = _WaitEnv(monkeypatch, {1: [_job(1, status="running")]})
    res = runner.invoke(app, ["wait", "1", "99"])
    assert res.exit_code == 4 and "job 99 does not exist" in res.output
    assert env.sleeps == []  # reported immediately, nothing was waited for


def test_wait_job_removed_meanwhile_counts_as_not_done(monkeypatch):
    _WaitEnv(monkeypatch, {4: [_job(4), None]})
    res = runner.invoke(app, ["wait", "4"])
    assert res.exit_code == 2 and "removed" in res.output


def test_wait_daemon_down_exits_3(monkeypatch):
    env = _WaitEnv(monkeypatch, {1: [_job(1)]})
    env.down = True
    res = runner.invoke(app, ["wait", "1"])
    assert res.exit_code == 3 and "not running" in res.output
    res = runner.invoke(app, ["wait", "--channel", "1"])
    assert res.exit_code == 3


def test_wait_daemon_dying_mid_wait_exits_3(monkeypatch):
    env = _WaitEnv(monkeypatch, {1: [_job(1, status="running")]})
    real = env._sleep

    def sleep_then_die(secs):
        real(secs)
        env.down = True

    monkeypatch.setattr(cli_mod.time, "sleep", sleep_then_die)
    assert runner.invoke(app, ["wait", "1"]).exit_code == 3


def test_wait_ctrl_c_exits_130(monkeypatch):
    env = _WaitEnv(monkeypatch, {1: [_job(1, status="running")]})

    def interrupt(secs):
        raise KeyboardInterrupt

    monkeypatch.setattr(cli_mod.time, "sleep", interrupt)
    res = runner.invoke(app, ["wait", "1"])
    assert res.exit_code == 130 and env.shows == [1]


def test_wait_timeout_exits_124_and_names_what_is_left(monkeypatch):
    env = _WaitEnv(monkeypatch, {1: [_ended(1)], 2: [_job(2, status="running")]})
    res = runner.invoke(app, ["wait", "1", "2", "--timeout", "5s"])
    assert res.exit_code == 124 and "still waiting for 2" in res.output
    assert sum(env.sleeps) == pytest.approx(5.0)  # the last sleep is cut to the deadline
    assert max(env.sleeps) <= cli_mod.WAIT_POLL_SECS


def test_wait_finishing_before_the_timeout_is_not_a_timeout(monkeypatch):
    _WaitEnv(monkeypatch, {1: [_job(1, status="running"), _ended(1)]})
    assert runner.invoke(app, ["wait", "1", "--timeout", "1m"]).exit_code == 0


@pytest.mark.parametrize("args", [
    [], ["--timeout", "5m"], ["1", "--channel", "1"], ["1", "--timeout", "soon"], ["1", "--timeout", "5"],
])
def test_wait_bad_arguments_exit_4_without_calling_the_daemon(monkeypatch, args):
    env = _WaitEnv(monkeypatch, {1: [_ended(1)]})
    res = runner.invoke(app, ["wait", *args])
    assert res.exit_code == 4 and env.shows == [] and "Error:" in res.output


@pytest.mark.parametrize("ids", [["abc"], ["3-1"], ["0"]])
def test_wait_rejects_junk_ids_like_show_does(monkeypatch, ids):
    env = _WaitEnv(monkeypatch, {1: [_ended(1)]})
    res = runner.invoke(app, ["wait", *ids])
    assert res.exit_code == 1 and env.shows == []  # the shared id parser exits 1


def test_wait_quiet_prints_nothing_but_keeps_the_exit_code(monkeypatch):
    _WaitEnv(monkeypatch, {1: [_ended(1, status="failed", exit_code=2)]})
    res = runner.invoke(app, ["wait", "1", "--quiet"])
    assert res.exit_code == 1 and res.output == ""


def test_wait_json_lists_final_states(monkeypatch):
    _WaitEnv(monkeypatch, {1: [_ended(1)], 2: [_ended(2, status="failed", exit_code=4, note="boom")]})
    res = runner.invoke(app, ["wait", "1-2", "--json"])
    assert res.exit_code == 1
    data = json.loads(res.stdout)
    assert data["exit_code"] == 1 and data["timed_out"] is False
    assert [j["id"] for j in data["jobs"]] == [1, 2]
    assert set(data["jobs"][1]) == {"id", "status", "exit_code", "channel", "ran_as", "note", "code", "finished"}
    assert data["jobs"][1]["status"] == "failed" and data["jobs"][1]["exit_code"] == 4 and data["jobs"][1]["note"] == "boom"
    assert "job 1: done" not in res.stdout  # nothing but the JSON on stdout


def test_wait_json_on_timeout_marks_unfinished_jobs(monkeypatch):
    _WaitEnv(monkeypatch, {1: [_job(1, status="running")]})
    res = runner.invoke(app, ["wait", "1", "--json", "--timeout", "2s"])
    data = json.loads(res.stdout)
    assert res.exit_code == 124 and data["timed_out"] is True and data["exit_code"] == 124
    assert data["jobs"][0]["status"] == "running" and data["jobs"][0]["finished"] is False


def test_wait_follows_an_auto_retry_to_the_last_attempt(monkeypatch):
    failed = _ended(5, status="failed", exit_code=1, max_retries=2, attempt=0,
                    note="auto-retry 1/2 queued as job 9")
    env = _WaitEnv(monkeypatch, {5: [failed], 9: [_job(9, status="running"), _ended(9, origin="retry", parent_id=5)]})
    res = runner.invoke(app, ["wait", "5", "--json"])
    data = json.loads(res.stdout)
    assert res.exit_code == 0 and data["jobs"][0]["status"] == "done" and data["jobs"][0]["ran_as"] == 9
    assert data["jobs"][0]["id"] == 5 and 9 in env.shows


def test_wait_reports_a_failure_when_the_retries_are_used_up(monkeypatch):
    last = _ended(9, status="failed", exit_code=1, max_retries=1, attempt=1, note="final")
    _WaitEnv(monkeypatch, {5: [_ended(5, status="failed", exit_code=1, max_retries=1, attempt=0,
                                      note="auto-retry 1/1 queued as job 9")], 9: [last]})
    res = runner.invoke(app, ["wait", "5"])
    assert res.exit_code == 1 and "last attempt was job 9" in res.output


def test_wait_channel_until_idle(monkeypatch):
    busy = {"channels": {"2": _idle_chan(running=_job(1, channel=2, status="running"))}}
    queued = {"channels": {"2": _idle_chan(queue=[_job(2, channel=2)])}}
    idle = {"channels": {"2": _idle_chan()}}
    env = _WaitEnv(monkeypatch, lists=[busy, queued, idle])
    res = runner.invoke(app, ["wait", "--channel", "2"])
    assert res.exit_code == 0 and "channel 2: idle" in res.output and len(env.sleeps) == 2


def test_wait_channel_other_channels_do_not_matter_and_unknown_channel_is_idle(monkeypatch):
    data = {"channels": {"1": _idle_chan(running=_job(1, status="running")), "2": _idle_chan()}}
    env = _WaitEnv(monkeypatch, lists=[data])
    assert runner.invoke(app, ["wait", "--channel", "2"]).exit_code == 0
    assert runner.invoke(app, ["wait", "--channel", "7"]).exit_code == 0
    assert env.sleeps == []


def test_wait_channel_timeout_and_json(monkeypatch):
    busy = {"channels": {"1": _idle_chan(running=_job(1, status="running"))}}
    _WaitEnv(monkeypatch, lists=[busy])
    res = runner.invoke(app, ["wait", "--channel", "1", "--timeout", "3s", "--json"])
    assert res.exit_code == 124
    assert json.loads(res.stdout) == {"channel": 1, "idle": False, "timed_out": True, "exit_code": 124}
    idle = {"channels": {"1": _idle_chan()}}
    _WaitEnv(monkeypatch, lists=[idle])
    res = runner.invoke(app, ["wait", "--channel", "1", "--json"])
    assert res.exit_code == 0 and json.loads(res.stdout)["idle"] is True


def test_wait_channel_paused_with_queued_jobs_hints_once(monkeypatch):
    paused = {"channels": {"1": _idle_chan(queue=[_job(1)], paused=True)}}
    _WaitEnv(monkeypatch, lists=[paused])
    res = runner.invoke(app, ["wait", "--channel", "1", "--timeout", "5s"])
    assert res.exit_code == 124 and res.stderr.count("is paused") == 1
    res = runner.invoke(app, ["wait", "--channel", "1", "--timeout", "5s", "--quiet"])
    assert "is paused" not in res.stderr


def test_wait_exit_code_helpers():
    assert cli_mod._wait_worst([]) == 0
    assert cli_mod._wait_worst([0, 2]) == 2 and cli_mod._wait_worst([2, 1, 0]) == 1


def test_status_json_is_plain_parseable_json(monkeypatch):
    data = {"channels": {"1": {"running": _job(1, status="running"), "frozen": None, "paused": False}}}
    monkeypatch.setattr(client, "send", lambda cmd, **kw: data)
    res = runner.invoke(app, ["status", "--json"])
    assert res.exit_code == 0 and "\x1b" not in res.stdout
    parsed = json.loads(res.stdout)
    assert set(parsed["channels"]["1"]) == {"running", "frozen", "paused"}
    assert parsed["channels"]["1"]["running"]["id"] == 1


def test_status_json_with_no_channels(monkeypatch):
    monkeypatch.setattr(client, "send", lambda cmd, **kw: {"channels": {}})
    assert json.loads(runner.invoke(app, ["status", "--json"]).stdout) == {"channels": {}}


def test_show_json_has_no_markup_even_for_bracketed_commands(monkeypatch):
    job = _job(1, raw_cmd=json.dumps(["echo", "[red]x[/red]"]))
    monkeypatch.setattr(client, "send", lambda cmd, **kw: job)
    res = runner.invoke(app, ["show", "1", "--json"])
    assert res.exit_code == 0 and json.loads(res.stdout)["raw_cmd"] == job["raw_cmd"]


# ---------------------------------------------------------------------------
# B14: --retry-delay / --retry-backoff
# ---------------------------------------------------------------------------

def test_add_sends_the_retry_delay_in_seconds_and_backoff(monkeypatch):
    calls = _capture_send(monkeypatch, {"id": 4, "channel": 1})
    res = runner.invoke(app, ["add", "--retries", "3", "--retry-delay", "1h30m", "--", "true"])
    assert res.exit_code == 0, res.output
    assert calls[0][1]["retry_delay_secs"] == 5400 and calls[0][1]["retry_backoff"] is False
    assert "each retry waits 1h30m" in res.output and "doubling" not in res.output
    res = runner.invoke(app, ["add", "--retries", "3", "--retry-delay", "5m", "--retry-backoff", "--", "true"])
    assert res.exit_code == 0, res.output
    assert calls[1][1]["retry_delay_secs"] == 300 and calls[1][1]["retry_backoff"] is True
    assert "doubling every attempt" in res.output


def test_add_without_retry_delay_sends_none(monkeypatch):
    calls = _capture_send(monkeypatch, {"id": 4, "channel": 1})
    res = runner.invoke(app, ["add", "--retries", "2", "--", "true"])
    assert res.exit_code == 0, res.output
    assert calls[0][1]["retry_delay_secs"] is None and calls[0][1]["retry_backoff"] is False
    assert "each retry waits" not in res.output


def test_add_rejects_bad_retry_delay_combinations_without_calling_the_daemon(monkeypatch):
    calls = _capture_send(monkeypatch, {"id": 4, "channel": 1})
    for args, text in (
        (["--retry-delay", "5m"], "--retries"),                         # no --retries
        (["--retries", "2", "--retry-backoff"], "--retry-delay"),       # backoff without a delay
        (["--retries", "2", "--retry-delay", "5"], "duration"),         # no unit
        (["--retries", "2", "--retry-delay", "0s"], "longer than zero"),
        (["--retries", "2", "--retry-delay", "31d"], "too long"),
    ):
        res = runner.invoke(app, ["add", *args, "--", "true"])
        assert res.exit_code == 1 and text in res.output, (args, res.output)
    assert calls == []


def test_add_sweep_sends_the_retry_delay_too(monkeypatch):
    calls = _capture_send(monkeypatch, {"id": 8, "channel": 1, "ids": [8, 9]})
    res = runner.invoke(app, ["add", "--retries", "1", "--retry-delay", "10s", "--each", "a", "b", "--", "echo", "{}"])
    assert res.exit_code == 0, res.output
    assert calls[0][1]["retry_delay_secs"] == 10 and len(calls[0][1]["jobs"]) == 2


def test_show_displays_the_retry_delay_only_when_set(monkeypatch):
    monkeypatch.setattr(client, "send", lambda cmd, **kw: _job(5, max_retries=3, retry_delay_secs=300, retry_backoff=1))
    out = runner.invoke(app, ["show", "5"], env={"COLUMNS": "200"}).output
    assert "retry delay" in out and "5m0s, doubling every attempt" in out
    monkeypatch.setattr(client, "send", lambda cmd, **kw: _job(5, max_retries=3, retry_delay_secs=90, retry_backoff=0))
    out = runner.invoke(app, ["show", "5"], env={"COLUMNS": "200"}).output
    assert "1m30s" in out and "doubling" not in out
    monkeypatch.setattr(client, "send", lambda cmd, **kw: _job(5, max_retries=3))
    assert "retry delay" not in runner.invoke(app, ["show", "5"], env={"COLUMNS": "200"}).output


# ---------------------------------------------------------------------------
# F8: qur edit
# ---------------------------------------------------------------------------

import os  # noqa: E402


def _edit_env(monkeypatch, job=None, edit_reply=None, fail_edit=None):
    """Fake daemon for `qur edit`: records every call, serves show/edit_lock/edit/edit_unlock."""
    job = job or _job(5, raw_cmd=json.dumps(["echo", "old"]), timeout_secs=60)
    calls: list[tuple[str, dict]] = []

    def fake(cmd, **kw):
        calls.append((cmd, kw))
        if cmd in ("show", "edit_lock"):
            return job
        if cmd == "edit":
            if fail_edit:
                raise client.DaemonError(fail_edit)
            return edit_reply or {"id": 5, "changed": ["command"]}
        if cmd == "edit_unlock":
            return {"released": True}
        raise AssertionError(cmd)

    monkeypatch.setattr(client, "send", fake)
    return calls


def _requests(calls, name="edit"):
    return [kw for cmd, kw in calls if cmd == name]


def test_edit_flags_send_only_what_was_asked(monkeypatch):
    calls = _edit_env(monkeypatch)
    res = runner.invoke(app, [
        "edit", "5", "--timeout", "0", "--channel", "2", "--retries", "3", "--on-fail", "pause",
        "--depends-on", "1,3-4", "--after-any", "9", "--env", "A=b",
    ])
    assert res.exit_code == 0 and "Edited job 5" in res.output
    assert [c for c, _ in calls] == ["edit"]
    assert _requests(calls)[0] == {
        "id": 5, "timeout_secs": None, "channel": 2, "max_retries": 3, "on_fail": "pause",
        "depends_on": [1, 3, 4], "after_any": [9], "env": {"A": "b"},
    }


def test_edit_delay_at_and_clear_delay(monkeypatch):
    calls = _edit_env(monkeypatch)
    assert runner.invoke(app, ["edit", "5", "--delay", "30m"]).exit_code == 0
    when = datetime.fromisoformat(_requests(calls)[0]["not_before"])
    assert when.tzinfo is not None and timedelta(minutes=29) < when - datetime.now(timezone.utc) < timedelta(minutes=31)
    assert runner.invoke(app, ["edit", "5", "--clear-delay"]).exit_code == 0
    assert _requests(calls)[1] == {"id": 5, "not_before": None}
    assert runner.invoke(app, ["edit", "5", "--at", "23:59"]).exit_code == 0
    assert "not_before" in _requests(calls)[2]


def test_edit_clear_deps(monkeypatch):
    calls = _edit_env(monkeypatch)
    assert runner.invoke(app, ["edit", "5", "--clear-deps"]).exit_code == 0
    assert _requests(calls)[0] == {"id": 5, "clear_deps": True}


def test_edit_new_command_after_double_dash(monkeypatch):
    calls = _edit_env(monkeypatch)
    res = runner.invoke(app, ["edit", "5", "--", "echo", "new", "--flag"])
    assert res.exit_code == 0
    req = _requests(calls)[0]
    assert req["raw_argv"] == ["echo", "new", "--flag"] and req["argv"][1:] == ["new", "--flag"]
    assert os.path.isabs(req["argv"][0]) and req["path"] == os.environ.get("PATH", "")


def test_edit_cmd_option_runs_shell_syntax_through_bash(monkeypatch):
    calls = _edit_env(monkeypatch)
    assert runner.invoke(app, ["edit", "5", "--cmd", "echo a && echo b"]).exit_code == 0
    req = _requests(calls)[0]
    assert req["raw_argv"] == ["echo a && echo b"] and req["argv"][1:] == ["-c", "echo a && echo b"]


def test_edit_cmd_and_words_together_is_an_error(monkeypatch):
    calls = _edit_env(monkeypatch)
    res = runner.invoke(app, ["edit", "5", "--cmd", "echo a", "--", "echo", "b"])
    assert res.exit_code == 1 and "not both" in res.output and calls == []


def test_edit_cwd_alone_re_resolves_the_stored_command(monkeypatch, tmp_path):
    calls = _edit_env(monkeypatch)
    assert runner.invoke(app, ["edit", "5", "--cwd", str(tmp_path)]).exit_code == 0
    assert [c for c, _ in calls] == ["show", "edit"]
    req = _requests(calls)[0]
    assert req["cwd"] == str(tmp_path) and req["raw_argv"] == ["echo", "old"] and req["argv"][1:] == ["old"]


def test_edit_relative_cwd_is_made_absolute(monkeypatch, tmp_path):
    calls = _edit_env(monkeypatch)
    monkeypatch.chdir(tmp_path)
    assert runner.invoke(app, ["edit", "5", "--cwd", "."]).exit_code == 0
    assert _requests(calls)[0]["cwd"] == str(tmp_path)


@pytest.mark.parametrize("args,word", [
    (["--on-fail", "explode"], "--on-fail"),
    (["--delay", "30m", "--clear-delay"], "--clear-delay"),
    (["--delay", "soon"], "--delay"),
    (["--clear-deps", "--depends-on", "3"], "--clear-deps"),
    (["--depends-on", "x"], "--depends-on"),
    (["--env", "1X=v"], "invalid environment"),
    (["--env", "NOT_SET_ANYWHERE_XYZ"], "not set"),
    (["--cmd", "  "], "--cmd is empty"),
])
def test_edit_bad_flags_fail_before_calling_the_daemon(monkeypatch, args, word):
    calls = _edit_env(monkeypatch)
    res = runner.invoke(app, ["edit", "5", *args])
    assert res.exit_code == 1 and word in res.output and calls == []


def test_edit_daemon_error_is_reported_with_exit_1(monkeypatch):
    _edit_env(monkeypatch, fail_edit="job 5 is running, not queued: only queued jobs can be edited")
    res = runner.invoke(app, ["edit", "5", "--timeout", "5"])
    assert res.exit_code == 1 and "only queued jobs can be edited" in res.output


def _fake_editor(monkeypatch, rewrite):
    seen = {}

    def run(path):
        text = open(path).read()
        seen["text"] = text
        if rewrite is None:
            raise RuntimeError("the editor exited with status 1")
        with open(path, "w") as f:
            f.write(rewrite(text))
        seen["path"] = path

    monkeypatch.setattr(cli_mod, "_run_editor", run)
    return seen


def test_edit_interactive_applies_the_changed_fields_and_does_not_unlock(monkeypatch):
    calls = _edit_env(monkeypatch, edit_reply={"id": 5, "changed": ["command", "timeout"]})
    seen = _fake_editor(monkeypatch, lambda t: t.replace("command: echo old", "command: echo new").replace("timeout: 60", "timeout: 2h"))
    res = runner.invoke(app, ["edit", "5"])
    assert res.exit_code == 0 and "changed command, timeout" in res.output
    assert "command: echo old" in seen["text"] and "cwd: /tmp" in seen["text"] and "timeout: 60" in seen["text"]
    assert [c for c, _ in calls] == ["edit_lock", "edit"]  # applying released the lock: no unlock
    lock, req = _requests(calls, "edit_lock")[0], _requests(calls)[0]
    assert lock == {"id": 5, "pid": os.getpid()} and req["pid"] == os.getpid()
    assert req["raw_argv"] == ["echo new"] and req["argv"][1:] == ["new"]
    assert req["timeout_secs"] == 7200 and "cwd" not in req
    assert not os.path.exists(seen["path"])  # temp file removed


def test_edit_interactive_cwd_change_re_resolves_the_command(monkeypatch, tmp_path):
    calls = _edit_env(monkeypatch)
    _fake_editor(monkeypatch, lambda t: t.replace("cwd: /tmp", f"cwd: {tmp_path}"))
    assert runner.invoke(app, ["edit", "5"]).exit_code == 0
    req = _requests(calls)[0]
    assert req["cwd"] == str(tmp_path) and req["raw_argv"] == ["echo", "old"]  # the stored words, untouched


def test_edit_interactive_timeout_none_removes_it(monkeypatch):
    calls = _edit_env(monkeypatch)
    _fake_editor(monkeypatch, lambda t: t.replace("timeout: 60", "timeout: none"))
    assert runner.invoke(app, ["edit", "5"]).exit_code == 0
    assert _requests(calls)[0]["timeout_secs"] is None


def test_edit_interactive_no_change_unlocks_and_sends_nothing(monkeypatch):
    calls = _edit_env(monkeypatch)
    _fake_editor(monkeypatch, lambda t: t)
    res = runner.invoke(app, ["edit", "5"])
    assert res.exit_code == 0 and "No changes" in res.output
    assert [c for c, _ in calls] == ["edit_lock", "edit_unlock"]
    assert _requests(calls, "edit_unlock")[0] == {"id": 5, "pid": os.getpid()}


def test_edit_interactive_comment_only_change_is_no_change(monkeypatch):
    calls = _edit_env(monkeypatch)
    _fake_editor(monkeypatch, lambda t: t + "# a note to myself\n")
    assert runner.invoke(app, ["edit", "5"]).exit_code == 0
    assert [c for c, _ in calls] == ["edit_lock", "edit_unlock"]


def test_edit_interactive_editor_failure_unlocks(monkeypatch):
    calls = _edit_env(monkeypatch)
    _fake_editor(monkeypatch, None)
    res = runner.invoke(app, ["edit", "5"])
    assert res.exit_code == 1 and "left unchanged" in res.output
    assert [c for c, _ in calls] == ["edit_lock", "edit_unlock"]


@pytest.mark.parametrize("rewrite,word", [
    (lambda t: t.replace("command: echo old", "command: "), "command"),
    (lambda t: t + "colour: red\n", "cannot read"),
    (lambda t: t.replace("timeout: 60", "timeout: soon"), "duration"),
])
def test_edit_interactive_bad_file_unlocks_and_changes_nothing(monkeypatch, rewrite, word):
    calls = _edit_env(monkeypatch)
    _fake_editor(monkeypatch, rewrite)
    res = runner.invoke(app, ["edit", "5"])
    assert res.exit_code == 1 and word in res.output
    assert [c for c, _ in calls] == ["edit_lock", "edit_unlock"]


def test_edit_interactive_daemon_rejects_the_edit_and_lock_is_released(monkeypatch):
    calls = _edit_env(monkeypatch, fail_edit="executable not found or not executable: /x")
    _fake_editor(monkeypatch, lambda t: t.replace("echo old", "nonexistent-tool"))
    res = runner.invoke(app, ["edit", "5"])
    assert res.exit_code == 1 and "executable not found" in res.output
    assert [c for c, _ in calls] == ["edit_lock", "edit", "edit_unlock"]


def test_edit_interactive_refused_when_the_job_is_not_queued(monkeypatch):
    def fake(cmd, **kw):
        raise client.DaemonError("job 5 is running, not queued: only queued jobs can be edited")

    monkeypatch.setattr(client, "send", fake)
    monkeypatch.setattr(cli_mod, "_run_editor", lambda path: pytest.fail("editor must not open"))
    res = runner.invoke(app, ["edit", "5"])
    assert res.exit_code == 1 and "only queued jobs can be edited" in res.output


def test_editor_argv_prefers_visual_then_editor_then_nano_or_vi(monkeypatch):
    monkeypatch.setenv("VISUAL", "code --wait")
    monkeypatch.setenv("EDITOR", "vim")
    assert cli_mod._editor_argv() == ["code", "--wait"]
    monkeypatch.delenv("VISUAL")
    assert cli_mod._editor_argv() == ["vim"]
    monkeypatch.delenv("EDITOR")
    monkeypatch.setattr(shutil, "which", lambda n: "/usr/bin/vi" if n == "vi" else None)
    assert cli_mod._editor_argv() == ["/usr/bin/vi"]
    monkeypatch.setattr(shutil, "which", lambda n: None)
    with pytest.raises(cli_mod.typer.Exit):
        cli_mod._editor_argv()


def test_run_editor_runs_the_editor_on_the_file_and_reports_failure(monkeypatch, tmp_path):
    f = tmp_path / "x.txt"
    f.write_text("a")
    monkeypatch.delenv("VISUAL", raising=False)
    monkeypatch.setenv("EDITOR", "true")
    cli_mod._run_editor(str(f))
    monkeypatch.setenv("EDITOR", "false")
    with pytest.raises(RuntimeError, match="status 1"):
        cli_mod._run_editor(str(f))
    monkeypatch.setenv("EDITOR", "/no/such/editor")
    with pytest.raises(RuntimeError, match="could not start"):
        cli_mod._run_editor(str(f))


def test_list_shows_editing_in_the_waits_on_column(monkeypatch):
    locked = _job(2, depends_on=[], after_any=[], waiting_on=[], editing=True)
    plain = _job(3, depends_on=[], after_any=[], waiting_on=[])
    listing = {"channels": {"1": _chan([locked, plain])}}
    monkeypatch.setattr(client, "send", lambda cmd, **kw: listing)
    out = runner.invoke(app, ["list"], env={"COLUMNS": "200"}).output
    assert "Waits on" in out and "editing" in out
    assert cli_mod._fmt_waits(_job(4, waiting_on=[1, 2], editing=True)) == "1, 2, editing"
    assert cli_mod._fmt_waits(_job(4, waiting_on=[], editing=False)) == "-"


def test_show_displays_the_edit_lock(monkeypatch):
    job = _job(5, editing=True, edit_pid=4242)
    monkeypatch.setattr(client, "send", lambda cmd, **kw: job)
    out = runner.invoke(app, ["show", "5"], env={"COLUMNS": "200"}).output
    assert "editing" in out and "4242" in out


# ---------------------------------------------------------------------------
# qur doctor / qur daemon (F32/U8, U9)
# ---------------------------------------------------------------------------

from queuer import selfcheck  # noqa: E402


def _checks(*pairs):
    return [selfcheck.Check(name, status, detail, hint) for name, status, detail, hint in pairs]


def test_doctor_prints_status_detail_and_fix_hint(monkeypatch):
    monkeypatch.setattr(selfcheck, "run_checks", lambda fix=False: _checks(
        ("daemon", "OK", "answers on /run/q.sock", ""),
        ("linger", "WARN", "not enabled", "loginctl enable-linger $USER"),
    ))
    res = runner.invoke(app, ["doctor"], env={"COLUMNS": "200"})
    assert res.exit_code == 0  # warnings do not fail
    assert "OK" in res.output and "WARN" in res.output
    assert "answers on /run/q.sock" in res.output
    assert "fix: loginctl enable-linger $USER" in res.output
    assert "1 ok, 1 warning(s), 0 failure(s)" in res.output


def test_doctor_exits_1_on_a_failure(monkeypatch):
    monkeypatch.setattr(selfcheck, "run_checks", lambda fix=False: _checks(
        ("daemon", "FAIL", "queuerd is not running", "qur daemon start"),
    ))
    res = runner.invoke(app, ["doctor"], env={"COLUMNS": "200"})
    assert res.exit_code == 1
    assert "FAIL" in res.output and "qur daemon start" in res.output


def test_doctor_does_not_treat_brackets_in_paths_as_markup(monkeypatch):
    monkeypatch.setattr(selfcheck, "run_checks", lambda fix=False: _checks(
        ("db file", "OK", "/data/[red]x[/red]/queuer.db mode 600", ""),
    ))
    res = runner.invoke(app, ["doctor"], env={"COLUMNS": "200"})
    assert "[red]x[/red]" in res.output


def test_doctor_json(monkeypatch):
    monkeypatch.setattr(selfcheck, "run_checks", lambda fix=False: _checks(
        ("daemon", "FAIL", "down", "qur daemon start"),
        ("linger", "OK", "enabled", ""),
    ))
    res = runner.invoke(app, ["doctor", "--json"])
    assert res.exit_code == 1  # still reflects the failure
    data = json.loads(res.output)
    assert data["ok"] is False
    assert data["summary"] == {"OK": 1, "WARN": 0, "FAIL": 1}
    assert data["checks"][0] == {"name": "daemon", "status": "FAIL", "detail": "down", "hint": "qur daemon start", "fixed": False}


def test_doctor_json_ok_when_nothing_fails(monkeypatch):
    monkeypatch.setattr(selfcheck, "run_checks", lambda fix=False: _checks(("a", "OK", "fine", "")))
    res = runner.invoke(app, ["doctor", "--json"])
    assert res.exit_code == 0 and json.loads(res.output)["ok"] is True


def test_doctor_fix_flag_is_passed_on(monkeypatch):
    seen = []
    monkeypatch.setattr(selfcheck, "run_checks", lambda fix=False: seen.append(fix) or _checks(("a", "OK", "x", "")))
    runner.invoke(app, ["doctor"])
    runner.invoke(app, ["doctor", "--fix"])
    assert seen == [False, True]


class _Svc:
    """Records the argv `qur daemon ...` would run, returns a canned exit code, and fakes the
    daemon's `status` reply (running jobs) for the stop/restart warning."""

    def __init__(self, monkeypatch, code=0, running=(), daemon_up=True):
        self.argvs = []
        self.code = code
        self.running = running
        self.daemon_up = daemon_up
        monkeypatch.setattr(cli_mod, "_run_service_tool", self._run)
        monkeypatch.setattr(client, "send", self._send)

    def _run(self, argv):
        self.argvs.append(argv)
        return self.code

    def _send(self, cmd, **kw):
        assert cmd == "status"
        if not self.daemon_up:
            raise client.DaemonNotRunning("queuerd is not running")
        chans = {str(i): {"running": {"id": jid}, "frozen": None} for i, jid in enumerate(self.running, start=1)}
        return {"channels": chans}


def test_daemon_start_runs_systemctl_start(monkeypatch):
    svc = _Svc(monkeypatch)
    res = runner.invoke(app, ["daemon", "start"])
    assert res.exit_code == 0 and "started" in res.output
    assert svc.argvs == [["systemctl", "--user", "start", "queuerd"]]


def test_daemon_start_failure_passes_the_exit_code_on(monkeypatch):
    svc = _Svc(monkeypatch, code=5)
    res = runner.invoke(app, ["daemon", "start"])
    assert res.exit_code == 5
    assert "qur daemon logs" in res.output
    assert len(svc.argvs) == 1


def test_daemon_stop_without_running_jobs_does_not_ask(monkeypatch):
    svc = _Svc(monkeypatch)
    res = runner.invoke(app, ["daemon", "stop"])
    assert res.exit_code == 0 and "stopped" in res.output
    assert svc.argvs == [["systemctl", "--user", "stop", "queuerd"]]


def test_daemon_stop_with_unreachable_daemon_does_not_ask(monkeypatch):
    svc = _Svc(monkeypatch, daemon_up=False)
    res = runner.invoke(app, ["daemon", "stop"])
    assert res.exit_code == 0
    assert svc.argvs == [["systemctl", "--user", "stop", "queuerd"]]


@pytest.mark.parametrize("verb", ["stop", "restart"])
def test_daemon_stop_and_restart_warn_and_abort_on_no(monkeypatch, verb):
    svc = _Svc(monkeypatch, running=(7, 9))
    res = runner.invoke(app, ["daemon", verb], input="n\n", env={"COLUMNS": "200"})
    assert res.exit_code == 1
    assert "#7" in res.output and "#9" in res.output and "interrupted" in res.output
    assert svc.argvs == []


@pytest.mark.parametrize("verb", ["stop", "restart"])
def test_daemon_stop_and_restart_proceed_on_yes_answer(monkeypatch, verb):
    svc = _Svc(monkeypatch, running=(7,))
    res = runner.invoke(app, ["daemon", verb], input="y\n")
    assert res.exit_code == 0
    assert svc.argvs == [["systemctl", "--user", verb, "queuerd"]]


@pytest.mark.parametrize("flag", ["--yes", "-y"])
def test_daemon_yes_skips_the_question(monkeypatch, flag):
    svc = _Svc(monkeypatch, running=(7,))
    res = runner.invoke(app, ["daemon", "restart", flag])
    assert res.exit_code == 0 and "interrupted" not in res.output
    assert svc.argvs == [["systemctl", "--user", "restart", "queuerd"]]


def test_daemon_stop_counts_a_frozen_job_too(monkeypatch):
    svc = _Svc(monkeypatch)

    def send(cmd, **kw):
        return {"channels": {"1": {"running": None, "frozen": {"id": 12}}}}

    monkeypatch.setattr(client, "send", send)
    res = runner.invoke(app, ["daemon", "stop"], input="n\n")
    assert res.exit_code == 1 and "#12" in res.output
    assert svc.argvs == []


def test_daemon_status_mirrors_systemctls_exit_code(monkeypatch):
    svc = _Svc(monkeypatch, code=3)
    res = runner.invoke(app, ["daemon", "status"])
    assert res.exit_code == 3
    assert svc.argvs == [["systemctl", "--user", "status", "queuerd", "--no-pager"]]


def test_daemon_logs_argv(monkeypatch):
    svc = _Svc(monkeypatch)
    assert runner.invoke(app, ["daemon", "logs"]).exit_code == 0
    assert runner.invoke(app, ["daemon", "logs", "-n", "20", "--follow"]).exit_code == 0
    assert svc.argvs == [
        ["journalctl", "--user", "-u", "queuerd", "-n", "50", "--no-pager"],
        ["journalctl", "--user", "-u", "queuerd", "-n", "20", "--no-pager", "-f"],
    ]


def test_daemon_logs_rejects_a_bad_line_count(monkeypatch):
    svc = _Svc(monkeypatch)
    res = runner.invoke(app, ["daemon", "logs", "-n", "0"])
    assert res.exit_code != 0 and svc.argvs == []


def test_daemon_without_a_subcommand_shows_help(monkeypatch):
    _Svc(monkeypatch)
    res = runner.invoke(app, ["daemon"])
    for word in ("start", "stop", "restart", "status", "logs"):
        assert word in res.output


def test_help_all_includes_daemon_and_doctor():
    res = runner.invoke(app, ["--help-all"], env={"COLUMNS": "200"})
    assert res.exit_code == 0
    assert "doctor" in res.output and "daemon" in res.output


def test_run_service_tool_runs_the_fixed_argv_without_a_shell(monkeypatch):
    seen = {}

    def fake(argv, **kw):
        seen.update(argv=argv, kw=kw)
        return types.SimpleNamespace(returncode=4)

    monkeypatch.setattr(cli_mod.subprocess, "run", fake)
    assert cli_mod._run_service_tool(["systemctl", "--user", "start", "queuerd"]) == 4
    assert seen["argv"] == ["systemctl", "--user", "start", "queuerd"]
    assert not seen["kw"].get("shell")


def test_run_service_tool_missing_program_is_a_clean_error(monkeypatch):
    def fake(argv, **kw):
        raise FileNotFoundError(argv[0])

    monkeypatch.setattr(cli_mod.subprocess, "run", fake)
    res = runner.invoke(app, ["daemon", "start"])
    assert res.exit_code == 1
    assert "systemctl not found" in res.output


def test_run_service_tool_ctrl_c_while_following_exits_130(monkeypatch):
    def fake(argv, **kw):
        raise KeyboardInterrupt

    monkeypatch.setattr(cli_mod.subprocess, "run", fake)
    assert cli_mod._run_service_tool(["journalctl", "--user", "-f"]) == 130
