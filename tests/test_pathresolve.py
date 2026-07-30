import shutil

from queuer.pathresolve import resolve_command


def test_flags_untouched(tmp_path):
    argv = ["ls", "-la", "--color=auto"]
    resolved = resolve_command(argv, str(tmp_path))
    # argv[0] resolves via PATH if "ls" exists on this system; flags never move
    assert resolved[1] == "-la"
    assert resolved[2] == "--color=auto"


def test_slash_containing_path_resolved_even_if_nonexistent(tmp_path):
    argv = ["run.sh", "output/results.csv"]
    resolved = resolve_command(argv, str(tmp_path))
    assert resolved[1] == str((tmp_path / "output" / "results.csv").resolve())


def test_relative_path_with_dotdot_resolved(tmp_path):
    argv = ["run.sh", "../shared/config.yaml"]
    resolved = resolve_command(argv, str(tmp_path))
    assert resolved[1] == str((tmp_path / "../shared/config.yaml").resolve())


def test_bare_existing_file_resolved(tmp_path):
    existing = tmp_path / "data.csv"
    existing.write_text("a,b\n1,2\n")
    argv = ["process.py", "data.csv"]
    resolved = resolve_command(argv, str(tmp_path))
    assert resolved[1] == str(existing.resolve())


def test_bare_nonexistent_word_left_untouched(tmp_path):
    argv = ["echo", "hello", "world"]
    resolved = resolve_command(argv, str(tmp_path))
    assert resolved[1] == "hello"
    assert resolved[2] == "world"


def test_argv0_bare_command_resolved_via_path(tmp_path):
    # Must NOT follow symlinks here -- resolving through e.g. a venv's
    # python3 shim to the base interpreter would break venv activation
    # for the job. shutil.which() itself doesn't dereference symlinks,
    # so the expected value matches it directly (not further resolved).
    which_python = shutil.which("python3")
    argv = ["python3", "-c", "print(1)"]
    resolved = resolve_command(argv, str(tmp_path))
    assert resolved[0] == which_python
    assert resolved[1] == "-c"
    assert resolved[2] == "print(1)"  # not a path, left alone


def test_argv0_unresolvable_left_as_is(tmp_path):
    argv = ["not-a-real-command-xyz", "--flag"]
    resolved = resolve_command(argv, str(tmp_path))
    assert resolved[0] == "not-a-real-command-xyz"


def test_absolute_path_passthrough_resolved(tmp_path):
    # An already-absolute path must come back byte-for-byte identical --
    # in particular, if /usr/bin/python3 is itself a symlink (e.g. to
    # python3.10), it must NOT be rewritten to the symlink's target.
    argv = ["/usr/bin/python3", "script.py"]
    resolved = resolve_command(argv, str(tmp_path))
    assert resolved[0] == "/usr/bin/python3"


def test_empty_argv(tmp_path):
    assert resolve_command([], str(tmp_path)) == []
