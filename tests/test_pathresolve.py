import shutil

from queuer.pathresolve import resolve_command


def test_flags_untouched(tmp_path):
    argv = ["ls", "-la", "--color=auto"]
    resolved = resolve_command(argv, str(tmp_path))
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
    which_python = shutil.which("python3")
    argv = ["python3", "-c", "print(1)"]
    resolved = resolve_command(argv, str(tmp_path))
    assert resolved[0] == which_python
    assert resolved[1] == "-c"
    assert resolved[2] == "print(1)"


def test_argv0_unresolvable_left_as_is(tmp_path):
    argv = ["not-a-real-command-xyz", "--flag"]
    resolved = resolve_command(argv, str(tmp_path))
    assert resolved[0] == "not-a-real-command-xyz"


def test_absolute_path_passthrough_resolved(tmp_path):
    argv = ["/usr/bin/python3", "script.py"]
    resolved = resolve_command(argv, str(tmp_path))
    assert resolved[0] == "/usr/bin/python3"


def test_empty_argv(tmp_path):
    assert resolve_command([], str(tmp_path)) == []


# ---------------------------------------------------------------------------
# additional edge cases
# ---------------------------------------------------------------------------

def test_empty_string_token_left_alone_not_rewritten_to_cwd(tmp_path):
    # regression test: Path(cwd) / "" == Path(cwd), which always exists --
    # without an explicit guard this would silently become the cwd's path
    argv = ["echo", ""]
    resolved = resolve_command(argv, str(tmp_path))
    assert resolved[1] == ""


def test_multiple_path_tokens_in_one_command(tmp_path):
    argv = ["cp", "src/in.txt", "dst/out.txt"]
    resolved = resolve_command(argv, str(tmp_path))
    assert resolved[1] == str((tmp_path / "src" / "in.txt").resolve())
    assert resolved[2] == str((tmp_path / "dst" / "out.txt").resolve())


def test_existing_directory_token_is_resolved(tmp_path):
    subdir = tmp_path / "outputs"
    subdir.mkdir()
    argv = ["ls", "outputs"]
    resolved = resolve_command(argv, str(tmp_path))
    assert resolved[1] == str(subdir.resolve())


def test_dotfile_bare_token_resolved_if_exists(tmp_path):
    dotfile = tmp_path / ".env"
    dotfile.write_text("KEY=value\n")
    argv = ["cat", ".env"]
    resolved = resolve_command(argv, str(tmp_path))
    assert resolved[1] == str(dotfile.resolve())


def test_non_argv0_bare_command_name_not_path_looked_up(tmp_path):
    # PATH lookup is only for argv[0] (the executable). A bare word that
    # happens to match a command name elsewhere in argv must NOT be
    # rewritten -- it's just a string argument to the program being run.
    argv = ["echo", "python3"]
    resolved = resolve_command(argv, str(tmp_path))
    assert resolved[1] == "python3"


def test_absolute_flag_like_token_with_slash_still_treated_as_path(tmp_path):
    # a token containing '/' is treated as a path even if it looks like it
    # could be a flag -- this is a known, deliberate tradeoff of the "/"
    # rule, not a special case for flags
    argv = ["run.sh", "--output=results/out.txt"]
    resolved = resolve_command(argv, str(tmp_path))
    # contains '/' so it gets the path treatment applied to the whole token
    assert resolved[1] == str((tmp_path / "--output=results/out.txt").resolve())


def test_cwd_with_trailing_slash_handled(tmp_path):
    cwd_with_slash = str(tmp_path) + "/"
    argv = ["run.sh", "data/file.txt"]
    resolved = resolve_command(argv, cwd_with_slash)
    assert resolved[1] == str((tmp_path / "data" / "file.txt").resolve())


def test_argv0_absolute_path_to_nonexistent_file_left_absolute(tmp_path):
    argv = ["/opt/does/not/exist/mytool", "--flag"]
    resolved = resolve_command(argv, str(tmp_path))
    assert resolved[0] == "/opt/does/not/exist/mytool"
