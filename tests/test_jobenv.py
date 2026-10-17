"""F1/B13: job environment helpers (parsing, passthrough list, display masking)."""

import json

import pytest

from queuer import jobenv


def test_parse_sets_and_copies_and_later_wins():
    env = {"HOME_VAR": "from-shell", "EMPTY": ""}
    out = jobenv.parse_env_options(["A=1", "HOME_VAR", "EMPTY", "A=2", "B=x=y", "C="], env)
    assert out == {"A": "2", "HOME_VAR": "from-shell", "EMPTY": "", "B": "x=y", "C": ""}
    assert jobenv.parse_env_options(None, env) == {} and jobenv.parse_env_options([], env) == {}


@pytest.mark.parametrize("bad", ["1A=x", "A-B=x", "=x", "", "A B=1", "QUR_ENV_PASSTHROUGH=x", "é=1"])
def test_parse_rejects_bad_names(bad):
    with pytest.raises(ValueError):
        jobenv.parse_env_options([bad], {})


def test_parse_rejects_nul_and_unset_bare_key():
    with pytest.raises(ValueError, match="NUL"):
        jobenv.parse_env_options(["A=x\0y"], {})
    with pytest.raises(ValueError, match="not set"):
        jobenv.parse_env_options(["NOPE"], {})


def test_check_env_validates_what_the_daemon_receives():
    assert jobenv.check_env({"A": "1"}) == {"A": "1"}
    for bad in ([], {"A": 1}, {"1A": "x"}, {"A": "x\0"}, {jobenv.PASSTHROUGH_KEY: "x"}):
        with pytest.raises(ValueError):
            jobenv.check_env(bad)


def test_default_passthrough_copies_only_listed_names_that_are_set():
    environ = {
        "CUDA_VISIBLE_DEVICES": "0,1", "OMP_NUM_THREADS": "4", "LANG": "C.UTF-8", "LC_ALL": "C", "LC_TIME": "x",
        "TERM": "xterm", "TZ": "UTC", "LD_LIBRARY_PATH": "/opt/lib", "MKL_NUM_THREADS": "2", "OPENBLAS_NUM_THREADS": "3",
        "AWS_SECRET_ACCESS_KEY": "s3cret", "GITHUB_TOKEN": "t", "SSH_AUTH_SOCK": "/s", "LCX": "no", "PATH": "/bin",
    }
    got = jobenv.collect_passthrough(environ)
    assert set(got) == {
        "CUDA_VISIBLE_DEVICES", "OMP_NUM_THREADS", "LANG", "LC_ALL", "LC_TIME", "TERM", "TZ",
        "LD_LIBRARY_PATH", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS",
    }
    assert got["CUDA_VISIBLE_DEVICES"] == "0,1"
    assert jobenv.collect_passthrough({"LANG": "C"}) == {"LANG": "C"}  # unset names are simply absent
    assert jobenv.collect_passthrough({}) == {}


def test_passthrough_list_is_overridable_by_env_var():
    environ = {"QUEUER_ENV_PASSTHROUGH": "FOO, BAR_*,MPI_X", "FOO": "1", "BAR_1": "2", "BAR_2": "3", "LANG": "C", "MPI_X": "4"}
    assert jobenv.passthrough_patterns(environ) == ["FOO", "BAR_*", "MPI_X"]
    assert jobenv.collect_passthrough(environ) == {"FOO": "1", "BAR_1": "2", "BAR_2": "3", "MPI_X": "4"}
    nothing = {"QUEUER_ENV_PASSTHROUGH": "", "LANG": "C", "CUDA_VISIBLE_DEVICES": "0"}
    assert jobenv.passthrough_patterns(nothing) == [] and jobenv.collect_passthrough(nothing) == {}
    assert jobenv.passthrough_patterns({}) == list(jobenv.DEFAULT_PASSTHROUGH)


def test_job_environment_defaults_unbuffered_and_hides_the_marker():
    assert jobenv.job_environment({}) == {"PYTHONUNBUFFERED": "1"}
    env = jobenv.job_environment({"A": "1", jobenv.PASSTHROUGH_KEY: "A"})
    assert env == {"PYTHONUNBUFFERED": "1", "A": "1"}
    assert jobenv.job_environment({"PYTHONUNBUFFERED": "0"})["PYTHONUNBUFFERED"] == "0"  # --env can override


def test_describe_shows_names_only_for_passthrough_and_hides_secret_values():
    extra = {
        "CUDA_VISIBLE_DEVICES": "0", "MY_SETTING": "hello", "API_TOKEN": "abc", "db_password": "pw",
        "SECRET_KEY": "k", "MONKEY": "m", "PATH": "/a:/b", "VIRTUAL_ENV": "/v", "PYTHONPATH": "/p", "QUR_INDEX": "3",
        jobenv.PASSTHROUGH_KEY: "CUDA_VISIBLE_DEVICES",
    }
    lines = jobenv.describe(json.dumps(extra))
    assert "CUDA_VISIBLE_DEVICES" in lines and not any(line.startswith("CUDA_VISIBLE_DEVICES=") for line in lines)
    assert "MY_SETTING=hello" in lines and "QUR_INDEX=3" in lines
    assert "API_TOKEN=***" in lines and "db_password=***" in lines and "SECRET_KEY=***" in lines and "MONKEY=***" in lines
    assert "PATH" in lines and "VIRTUAL_ENV" in lines and "PYTHONPATH" in lines
    joined = "\n".join(lines)
    for hidden in ("abc", "pw", "/a:/b", jobenv.PASSTHROUGH_KEY):
        assert hidden not in joined


def test_describe_copes_with_nothing_and_garbage():
    assert jobenv.describe(None) == [] and jobenv.describe("") == []
    assert jobenv.describe("not json") == [] and jobenv.describe("[1]") == []
    assert jobenv.describe({"A": "1"}) == ["A=1"]


def test_default_passthrough_covers_the_families_md_and_gpu_tools_read():
    """5.todo M4: CUDA_HOME, OMP_PLACES / OMP_PROC_BIND, NCCL_*, OMPI_*, UCX_*, CONDA_*, MKL_* ..."""
    wanted = {
        "CUDA_HOME": "/usr/local/cuda", "CUDA_VISIBLE_DEVICES": "0", "OMP_PLACES": "cores", "OMP_PROC_BIND": "close",
        "OMP_NUM_THREADS": "4", "MKL_DYNAMIC": "FALSE", "OPENBLAS_CORETYPE": "Haswell", "NCCL_DEBUG": "INFO",
        "OMPI_MCA_btl": "self,vader", "UCX_TLS": "sm,cuda", "CONDA_PREFIX": "/opt/conda", "CONDA_DEFAULT_ENV": "md",
    }
    unrelated = {"AWS_SECRET_ACCESS_KEY": "s", "GITHUB_TOKEN": "t", "SSH_AUTH_SOCK": "/s", "OMPX": "1", "CUDAX": "1", "MYCONDA_X": "1"}
    assert jobenv.collect_passthrough({**wanted, **unrelated}) == wanted


def test_base_environment_has_path_home_and_the_four_names_when_set():
    env = jobenv.base_environment({
        "PATH": "/bin", "HOME": "/home/a", "USER": "a", "LOGNAME": "a", "SHELL": "/bin/bash", "TMPDIR": "/t",
        "AWS_SECRET_ACCESS_KEY": "s", "LANG": "C",
    })
    assert env == {"PATH": "/bin", "HOME": "/home/a", "USER": "a", "LOGNAME": "a", "SHELL": "/bin/bash", "TMPDIR": "/t"}


def test_base_environment_never_invents_or_passes_bad_values():
    assert jobenv.base_environment({}) == {"PATH": "", "HOME": ""}  # as before: PATH and HOME are always there
    assert jobenv.base_environment({"PATH": "/bin", "HOME": "/h", "USER": "a\0b", "SHELL": ""}) == {"PATH": "/bin", "HOME": "/h", "SHELL": ""}


def test_job_environment_layers_over_the_base_so_env_can_override_user():
    env = jobenv.base_environment({"PATH": "/bin", "HOME": "/h", "USER": "daemon-user"})
    env.update(jobenv.job_environment({"USER": "bob"}))
    assert env["USER"] == "bob" and env["PYTHONUNBUFFERED"] == "1"


@pytest.mark.parametrize("name", ["MYSQL_PWD", "DB_PASS", "AWS_CREDENTIALS", "API_AUTH", "SESSION_COOKIE", "db.creds", "my-pass", "PWD", "git_credential"])
def test_whole_segment_secret_names_are_hidden(name):
    """5.todo M6"""
    assert jobenv.is_secret_name(name)
    assert jobenv.describe({name: "hunter2"}) == [f"{name}=***"]


@pytest.mark.parametrize("name", ["QUR_ENV_PASSTHROUGH", "QUEUER_ENV_PASSTHROUGH", "AUTHOR", "PASSENGER", "COMPASS", "MY_SETTING", "PWDX", "TOKENIZER"])
def test_look_alike_names_stay_readable(name):
    if name == "TOKENIZER":  # contains TOKEN: the old substring rule (unchanged) still hides it
        assert jobenv.is_secret_name(name)
    else:
        assert not jobenv.is_secret_name(name)
