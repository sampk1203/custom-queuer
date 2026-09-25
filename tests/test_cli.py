import shlex
import shutil

import pytest

from queuer.cli import _build_job_argv, _needs_shell, _resolve_interpreter


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
