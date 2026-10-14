import shlex
import shutil

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
