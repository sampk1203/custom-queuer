"""Tests for secret redaction (F31/S4/S5): every built-in pattern, no false positives on
normal commands and log lines, and the user's redact.json."""

from __future__ import annotations

import json
import os
import stat

import pytest

from queuer import redact

M = redact.MASK
TG_TOKEN = "123456789:AAH-test_token-0123456789abcdefghijk"


@pytest.fixture(autouse=True)
def _fresh_state(monkeypatch):
    monkeypatch.setattr(redact, "_warned", set())
    monkeypatch.setattr(redact, "_extra_cache", None)


def _write(tmp_path, content, mode=0o600):
    p = tmp_path / "redact.json"
    p.write_text(content)
    os.chmod(p, mode)
    return p


# ---------------------------------------------------------------------------
# built-in patterns
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("text, expected", [
    # NAME=VALUE
    ("export API_KEY=abc123 && run", f"export API_KEY={M} && run"),
    ("OPENAI_API_KEY=sk-abc", f"OPENAI_API_KEY={M}"),
    ("AWS_SECRET_ACCESS_KEY=wJalr/XYZ+k", f"AWS_SECRET_ACCESS_KEY={M}"),
    ("GITHUB_TOKEN=abc DB_PASSWORD=hunter2", f"GITHUB_TOKEN={M} DB_PASSWORD={M}"),
    ("PASSWD=y", f"PASSWD={M}"),
    ("SECRET_KEY_BASE=abc", f"SECRET_KEY_BASE={M}"),
    ("db.password=abc", f"db.password={M}"),
    ('api_key = "a b c"', f"api_key = {M}"),
    ("api_key='x y' next", f"api_key={M} next"),
    ('curl "https://x/y?access_token=abc&z=1"', f'curl "https://x/y?access_token={M}&z=1"'),
    # flags
    ("run --password=hunter2 --lr 1", f"run --password={M} --lr 1"),
    ("run --password hunter2 --lr 1", f"run --password {M} --lr 1"),
    ('run --api-key "a b" x', f"run --api-key {M} x"),
    ("--access-token=abc", f"--access-token={M}"),
    ("--aws-secret-access-key abc", f"--aws-secret-access-key {M}"),
    # Authorization / Bearer
    ('curl -H "Authorization: Bearer abc.def-123" x', f'curl -H "Authorization: Bearer {M}" x'),
    ("Authorization: Basic dXNlcjpwYXNz", f"Authorization: Basic {M}"),
    ("authorization: abc", f"authorization: {M}"),
    ("got Bearer abcdefghijklmnop1234 ok", f"got Bearer {M} ok"),
    # key shapes
    ("id AKIAIOSFODNN7EXAMPLE end", f"id {M} end"),
    ("id ASIAIOSFODNN7EXAMPLE end", f"id {M} end"),
    ("tok ghp_" + "a" * 36 + " end", f"tok {M} end"),
    ("tok gho_" + "b" * 30 + " end", f"tok {M} end"),
    ("tok github_pat_" + "A1b2" * 10 + " end", f"tok {M} end"),
    (f"bot {TG_TOKEN} end", f"bot {M} end"),
    (f"https://api.telegram.org/bot{TG_TOKEN}/getMe", f"https://api.telegram.org/bot{M}/getMe"),
    # URL with user:pass@
    ("git clone https://me:s3cr3t@host/repo.git", f"git clone https://me:{M}@host/repo.git"),
    ("postgres://u:p%40ss@db:5432/x", f"postgres://u:{M}@db:5432/x"),
])
def test_builtin_patterns(text, expected):
    assert redact.redact_text(text) == expected


@pytest.mark.parametrize("text", [
    "python train.py --epochs 10 --lr 1e-3 --tokenizer=gpt2 --max_tokens=100",
    "ssh git@github.com ls",
    "git clone ssh://git@github.com/x/y.git",
    "http://localhost:8080/health",
    "loss=0.12 token_count=100 keyboard=us KEYBOARD=us",
    "step 10 loss=0.123 lr=1e-3 tokens/s=1200",
    "sort --key-file=k.pem --token-count 5",
    "mpirun -np 4 lmp -in in.lammps -var seed=12345",
    "2026-10-17 12:30:45 INFO step 100 done",
    "export PATH=/usr/bin:$PATH",
    "pip install keyring",
    "echo password",
    "make -j8 && ./run.sh --verbose --token --verbose",
    "AKIAshort ghp_short 12345:short",
    "Authorization header was missing",
    "",
])
def test_no_false_positives_on_normal_text(text):
    assert redact.redact_text(text) == text


def test_redaction_is_idempotent():
    samples = [
        "run --password hunter2 --api-key=abc API_KEY=x https://u:p@h/x Authorization: Bearer abc",
        f"{TG_TOKEN} AKIAIOSFODNN7EXAMPLE ghp_{'a' * 36}",
        'api_key = "a b c"',
    ]
    for s in samples:
        once = redact.redact_text(s)
        assert redact.redact_text(once) == once


def test_redaction_works_across_lines_without_eating_the_next_line():
    text = "start\nAPI_KEY=abc\nnext line\nname=\"unterminated\nmore"
    assert redact.redact_text(text) == f"start\nAPI_KEY={M}\nnext line\nname=\"unterminated\nmore"
    assert redact.redact_text("TOKEN=\nabc") == "TOKEN=\nabc"  # empty value: nothing to hide, next line untouched


# ---------------------------------------------------------------------------
# argv (word by word)
# ---------------------------------------------------------------------------

def test_argv_masks_the_word_after_a_bare_secret_flag():
    assert redact.redact_argv(["curl", "--password", "hunter2", "--user", "bob"]) == ["curl", "--password", M, "--user", "bob"]


def test_argv_hides_values_with_spaces_completely():
    assert redact.redact_argv(["x", "--password=a b"]) == ["x", f"--password={M}"]
    assert redact.redact_argv(["env", "MY_TOKEN=a b", "FOO=1"]) == ["env", f"MY_TOKEN={M}", "FOO=1"]


def test_argv_does_not_swallow_a_following_flag():
    assert redact.redact_argv(["run", "--token", "--verbose", "x"]) == ["run", "--token", "--verbose", "x"]


def test_argv_text_patterns_still_apply_inside_a_word():
    assert redact.redact_argv(["curl", "-H", "Authorization: Bearer abc"]) == ["curl", "-H", f"Authorization: Bearer {M}"]
    assert redact.redact_argv(["bash", "-c", "export API_KEY=abc; run"]) == ["bash", "-c", f"export API_KEY={M}; run"]


def test_argv_edge_cases():
    assert redact.redact_argv(["a", 5]) == ["a", 5]  # non-strings pass through
    assert redact.redact_argv(["--token="]) == ["--token="]  # empty value: nothing to hide
    assert redact.redact_argv([]) == []
    assert redact.redact_argv(["--password"]) == ["--password"]  # flag last: nothing follows


# ---------------------------------------------------------------------------
# redact.json
# ---------------------------------------------------------------------------

def test_default_path_follows_the_notify_config(tmp_path, monkeypatch):
    monkeypatch.delenv("QUEUER_REDACT_PATH", raising=False)
    monkeypatch.setenv("QUEUER_NOTIFY_PATH", str(tmp_path / "cfg" / "notify.json"))
    assert redact.default_path() == tmp_path / "cfg" / "redact.json"
    monkeypatch.setenv("QUEUER_REDACT_PATH", str(tmp_path / "mine.json"))
    assert redact.default_path() == tmp_path / "mine.json"
    monkeypatch.delenv("QUEUER_REDACT_PATH")
    monkeypatch.delenv("QUEUER_NOTIFY_PATH")
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    assert redact.default_path() == tmp_path / "xdg" / "queuer" / "redact.json"


def test_no_file_means_no_extra_patterns(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("QUEUER_REDACT_PATH", str(tmp_path / "missing.json"))
    assert redact.extra_patterns() == []
    assert redact.redact_text("hello hunter2") == "hello hunter2"
    assert capsys.readouterr().err == ""


def test_extra_patterns_are_applied(tmp_path, monkeypatch):
    p = _write(tmp_path, json.dumps([r"hunter\d", r"project-(\w+)-secret"]))
    monkeypatch.setenv("QUEUER_REDACT_PATH", str(p))
    assert redact.redact_text("pw hunter2 ok") == f"pw {M} ok"
    assert redact.redact_text("see project-apollo-secret now") == f"see project-{M}-secret now"  # group only
    assert redact.redact_argv(["run", "hunter9"]) == ["run", M]


def test_extra_patterns_reload_when_the_file_changes(tmp_path, monkeypatch):
    p = _write(tmp_path, json.dumps([r"alpha\d"]))
    monkeypatch.setenv("QUEUER_REDACT_PATH", str(p))
    assert redact.redact_text("alpha1 beta2") == f"{M} beta2"
    p.write_text(json.dumps([r"beta\d", r"gamma\d"]))
    assert redact.redact_text("alpha1 beta2") == f"alpha1 {M}"


def test_group_that_did_not_take_part_masks_the_whole_match(tmp_path, monkeypatch):
    monkeypatch.setenv("QUEUER_REDACT_PATH", str(_write(tmp_path, json.dumps(["x(y)?z"]))))
    assert redact.redact_text("a xz b") == f"a {M} b"
    assert redact.redact_text("a xyz b") == f"a x{M}z b"


def test_bad_patterns_are_skipped_and_logged_once_without_the_pattern(tmp_path, monkeypatch, capsys):
    p = _write(tmp_path, json.dumps(["(unclosed", r"ok\d", 5, "x*", ""]))
    monkeypatch.setenv("QUEUER_REDACT_PATH", str(p))
    assert redact.redact_text("ok1 (unclosed xxx") == f"{M} (unclosed xxx"  # the good one still works
    redact.redact_text("again")
    err = capsys.readouterr().err
    assert err.count("not a valid regex") == 1  # once, not once per call
    assert "matches the empty string" in err and "not a non-empty string" in err
    assert "unclosed" not in err  # the pattern itself may be the secret
    assert "[queuerd] redact:" in err


@pytest.mark.parametrize("content", ["not json at all", '{"a": 1}', '"hunter2"'])
def test_unusable_file_is_ignored_with_one_log_line(tmp_path, monkeypatch, capsys, content):
    monkeypatch.setenv("QUEUER_REDACT_PATH", str(_write(tmp_path, content)))
    assert redact.redact_text("hunter2 API_KEY=x") == f"hunter2 API_KEY={M}"  # built-ins still work
    redact.redact_text("hunter2")
    assert capsys.readouterr().err.count("[queuerd] redact:") == 1


def test_file_writable_by_others_is_ignored(tmp_path, monkeypatch, capsys):
    p = _write(tmp_path, json.dumps(["hunter2"]), mode=0o666)
    monkeypatch.setenv("QUEUER_REDACT_PATH", str(p))
    assert redact.extra_patterns() == []
    assert "writable by others" in capsys.readouterr().err


def test_file_readable_by_others_is_tightened_to_0600(tmp_path, monkeypatch, capsys):
    p = _write(tmp_path, json.dumps(["hunter2"]), mode=0o644)
    monkeypatch.setenv("QUEUER_REDACT_PATH", str(p))
    assert redact.redact_text("a hunter2 b") == f"a {M} b"
    assert stat.S_IMODE(os.stat(p).st_mode) == 0o600
    assert "tightened to 600" in capsys.readouterr().err


def test_symlinked_file_is_ignored(tmp_path, monkeypatch, capsys):
    real = _write(tmp_path, json.dumps(["hunter2"]))
    link = tmp_path / "link.json"
    link.symlink_to(real)
    monkeypatch.setenv("QUEUER_REDACT_PATH", str(link))
    assert redact.extra_patterns() == []
    assert "not a regular file" in capsys.readouterr().err


def test_only_the_first_100_patterns_are_used(tmp_path, monkeypatch, capsys):
    p = _write(tmp_path, json.dumps([f"zz{i}zz" for i in range(redact.MAX_EXTRA_PATTERNS + 5)]))
    monkeypatch.setenv("QUEUER_REDACT_PATH", str(p))
    assert len(redact.extra_patterns()) == redact.MAX_EXTRA_PATTERNS
    assert "first 100 patterns" in capsys.readouterr().err
