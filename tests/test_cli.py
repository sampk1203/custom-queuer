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
                 "requeue", "pause", "resume", "logs", "clean-logs"]:
        assert f" {name} " in res.output
