import os
import shutil

import pytest

from queuer.pathresolve import resolve_command


def test_flags_untouched(tmp_path):
    argv = ["ls", "-la", "--color=auto"]
    resolved = resolve_command(argv, str(tmp_path))
    assert resolved[1] == "-la"
    assert resolved[2] == "--color=auto"


def test_slash_containing_argument_left_exactly_as_typed(tmp_path):
    # H3: only argv[0] is resolved; the job runs with cwd=<add-time cwd>, so a relative
    # argument already means the same thing at run time
    argv = ["run.sh", "output/results.csv"]
    resolved = resolve_command(argv, str(tmp_path))
    assert resolved[1] == "output/results.csv"

def test_relative_argument_with_dotdot_left_exactly_as_typed(tmp_path):
    argv = ["run.sh", "../shared/config.yaml"]
    resolved = resolve_command(argv, str(tmp_path))
    assert resolved[1] == "../shared/config.yaml"

def test_bare_existing_file_argument_left_exactly_as_typed(tmp_path):
    (tmp_path / "data.csv").write_text("a,b\n1,2\n")
    argv = ["process.py", "data.csv"]
    resolved = resolve_command(argv, str(tmp_path))
    assert resolved[1] == "data.csv"

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


def test_multiple_path_arguments_in_one_command_untouched(tmp_path):
    argv = ["cp", "src/in.txt", "dst/out.txt"]
    resolved = resolve_command(argv, str(tmp_path))
    assert resolved[1:] == ["src/in.txt", "dst/out.txt"]

def test_existing_directory_argument_untouched(tmp_path):
    (tmp_path / "outputs").mkdir()
    argv = ["ls", "outputs"]
    resolved = resolve_command(argv, str(tmp_path))
    assert resolved[1] == "outputs"

def test_existing_dotfile_argument_untouched(tmp_path):
    (tmp_path / ".env").write_text("KEY=value\n")
    argv = ["cat", ".env"]
    resolved = resolve_command(argv, str(tmp_path))
    assert resolved[1] == ".env"

def test_non_argv0_bare_command_name_not_path_looked_up(tmp_path):
    # PATH lookup is only for argv[0] (the executable). A bare word that
    # happens to match a command name elsewhere in argv must NOT be
    # rewritten -- it's just a string argument to the program being run.
    argv = ["echo", "python3"]
    resolved = resolve_command(argv, str(tmp_path))
    assert resolved[1] == "python3"


def test_flag_value_with_slash_is_not_rewritten(tmp_path):
    # H3: `--output=results/out.txt` used to become `<cwd>/--output=results/out.txt`
    argv = ["run.sh", "--output=results/out.txt", "--config=conf/a.yaml"]
    resolved = resolve_command(argv, str(tmp_path))
    assert resolved[1:] == ["--output=results/out.txt", "--config=conf/a.yaml"]

def test_cwd_with_trailing_slash_handled(tmp_path):
    cwd_with_slash = str(tmp_path) + "/"
    argv = ["bin/tool", "data/file.txt"]
    resolved = resolve_command(argv, cwd_with_slash)
    assert resolved[0] == str(tmp_path / "bin" / "tool")
    assert resolved[1] == "data/file.txt"

def test_argv0_absolute_path_to_nonexistent_file_left_absolute(tmp_path):
    argv = ["/opt/does/not/exist/mytool", "--flag"]
    resolved = resolve_command(argv, str(tmp_path))
    assert resolved[0] == "/opt/does/not/exist/mytool"


# ---------------------------------------------------------------------------
# H3: only argv[0] is touched
# ---------------------------------------------------------------------------

def test_relative_argv0_with_slash_made_absolute(tmp_path):
    resolved = resolve_command(["./run.sh", "x"], str(tmp_path))
    assert resolved == [str(tmp_path / "run.sh"), "x"]


def test_relative_argv0_with_dotdot_normalised_not_symlink_followed(tmp_path):
    real = tmp_path / "real"
    real.mkdir()
    (tmp_path / "bin").mkdir()
    (tmp_path / "bin" / "py").symlink_to(shutil.which("python3") or "/usr/bin/python3")
    resolved = resolve_command(["../bin/py"], str(real))
    assert resolved == [str(tmp_path / "bin" / "py")]  # abspath, the symlink itself stays


def test_bare_argv0_existing_in_cwd_made_absolute(tmp_path):
    (tmp_path / "run.sh").write_text("#!/bin/sh\n")
    assert resolve_command(["run.sh"], str(tmp_path)) == [str(tmp_path / "run.sh")]


def test_argv0_uses_the_given_path_env_not_the_daemons(tmp_path):
    tool = tmp_path / "bin" / "mytool-h3"
    tool.parent.mkdir()
    tool.write_text("#!/bin/sh\n")
    tool.chmod(0o755)
    resolved = resolve_command(["mytool-h3", "--x"], str(tmp_path), path_env=str(tool.parent))
    assert resolved == [str(tool), "--x"]


@pytest.mark.parametrize("arg", [
    "--model=facebook/opt-125m", "facebook/opt-125m", "https://host/path", "user@host:/data",
    "-I../include", "s/a/b/", "org/name", "1/2", "out=results/run1", "-var",
])
def test_realistic_arguments_survive_byte_for_byte(tmp_path, arg):
    assert resolve_command(["prog", arg], str(tmp_path))[1] == arg


def test_argument_naming_an_existing_folder_is_not_rewritten(tmp_path):
    # `python main.py train` with a folder called `train` next to it
    (tmp_path / "train").mkdir()
    (tmp_path / "main.py").write_text("")
    resolved = resolve_command(["python3", "main.py", "train"], str(tmp_path))
    assert resolved[1:] == ["main.py", "train"]


def test_shell_c_script_and_extra_words_untouched(tmp_path):
    (tmp_path / "a.txt").write_text("")
    argv = ["bash", "-c", "curl https://example.com/x > out/a.txt", "name", "a.txt", "dir/b"]
    resolved = resolve_command(argv, str(tmp_path))
    assert resolved[1:] == argv[1:]
    assert os.path.isabs(resolved[0])


def test_inline_code_of_other_interpreters_untouched(tmp_path):
    argv = ["python3", "-c", "print(open('a/b').read()); import os"]
    assert resolve_command(argv, str(tmp_path))[1:] == argv[1:]
