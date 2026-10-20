"""Job environment helpers (F1/B13): `--env` parsing, the passthrough list, display masking.

A job's extra environment lives in the per-job `env_extra` JSON dict. Besides the real
variables it can hold one reserved key, PASSTHROUGH_KEY, listing the names that were copied
automatically from the caller's environment (as opposed to given with `--env`), so `qur show`
can print those by name only. The daemon strips that key before starting the job.
"""

from __future__ import annotations

import fnmatch
import json
import re
from collections.abc import Mapping
from typing import Any

PASSTHROUGH_KEY = "QUR_ENV_PASSTHROUGH"  # reserved: never reaches the job
PASSTHROUGH_ENV_VAR = "QUEUER_ENV_PASSTHROUGH"  # overrides DEFAULT_PASSTHROUGH
# 5.todo M4: widened from the single names (CUDA_VISIBLE_DEVICES, OMP_NUM_THREADS, ...) to the
# families MD/DFT/GPU tools read: CUDA_HOME, OMP_PLACES / OMP_PROC_BIND, MKL_*, NCCL_*, OMPI_*,
# UCX_*, CONDA_*. Anything else (LAMMPS_POTENTIALS, ...): QUEUER_ENV_PASSTHROUGH or --env.
DEFAULT_PASSTHROUGH = (
    "CUDA_*",
    "OMP_*",
    "MKL_*",
    "OPENBLAS_*",
    "NCCL_*",
    "OMPI_*",
    "UCX_*",
    "CONDA_*",
    "LD_LIBRARY_PATH",
    "LANG",
    "LC_*",
    "TERM",
    "TZ",
)
# 5.todo M4: always taken from the DAEMON's own environment (when set there) and given to every
# job: some MPI, conda and licence tools refuse to start without them. `--env` can override.
BASE_ENV_NAMES = ("USER", "LOGNAME", "SHELL", "TMPDIR")
DEFAULT_JOB_ENV = {"PYTHONUNBUFFERED": "1"}  # applied to every job; an --env can override
SECRET_WORDS = ("KEY", "TOKEN", "SECRET", "PASSWORD")  # hidden when the name CONTAINS one (MONKEY, API_KEY_2)
# 5.todo M6: hidden only as a WHOLE name segment (split on _ . -), so MYSQL_PWD, DB_PASS,
# AWS_CREDENTIALS, API_AUTH and SESSION_COOKIE are masked but QUR_ENV_PASSTHROUGH, AUTHOR and
# `--tokenizer`-style names stay readable
SECRET_SEGMENTS = frozenset({"PASS", "PWD", "CRED", "CREDS", "CREDENTIAL", "CREDENTIALS", "AUTH", "COOKIE", "COOKIES", "SESSION"})
# variables queuer itself sets from the client: shown by name only (values are long)
NAME_ONLY = frozenset({"PATH", "VIRTUAL_ENV", "PYTHONPATH"})

_NAME_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def check_name(name: str) -> None:
    if _NAME_RE.fullmatch(name) is None:
        raise ValueError(f"invalid environment variable name {name!r} (use letters, digits and _, not starting with a digit)")
    if name == PASSTHROUGH_KEY:
        raise ValueError(f"{PASSTHROUGH_KEY} is reserved")


def check_env(env: Any, what: str = "env") -> dict[str, str]:
    """Validate a name -> value dict as received by the daemon."""
    if not isinstance(env, dict):
        raise ValueError(f"{what} must be an object of strings")
    for name, value in env.items():
        if not isinstance(name, str) or not isinstance(value, str):
            raise ValueError(f"{what} must map names to strings")
        check_name(name)
        if "\0" in value:
            raise ValueError("environment values must not contain NUL bytes")
    return env


def passthrough_patterns(environ: Mapping[str, str]) -> list[str]:
    """Names/globs copied by default. `QUEUER_ENV_PASSTHROUGH` (comma or space separated,
    `*` allowed) replaces the built-in list; set but empty means "pass nothing"."""
    raw = environ.get(PASSTHROUGH_ENV_VAR)
    if raw is None:
        return list(DEFAULT_PASSTHROUGH)
    return [p for p in re.split(r"[,\s]+", raw) if p]


def collect_passthrough(environ: Mapping[str, str], patterns: list[str] | None = None) -> dict[str, str]:
    """The caller's variables whose names match the passthrough list (only those that are set)."""
    pats = passthrough_patterns(environ) if patterns is None else patterns
    out: dict[str, str] = {}
    for name, value in environ.items():
        if _NAME_RE.fullmatch(name) is None or name == PASSTHROUGH_KEY or "\0" in value:
            continue
        if any(fnmatch.fnmatchcase(name, p) for p in pats):
            out[name] = value
    return out


def parse_env_options(values: list[str] | None, environ: Mapping[str, str]) -> dict[str, str]:
    """`--env KEY` (copy from `environ`) and `--env KEY=VAL` (set). Later options win.
    Raises ValueError on a bad name, a NUL byte, or a bare KEY that is not set."""
    out: dict[str, str] = {}
    for item in values or []:
        name, sep, value = item.partition("=")
        check_name(name)
        if not sep:
            if name not in environ:
                raise ValueError(f"--env {name}: not set in the environment (use {name}=VALUE to set it)")
            value = environ[name]
        if "\0" in value:
            raise ValueError(f"--env {name}: value must not contain NUL bytes")
        out[name] = value
    return out


def is_secret_name(name: str) -> bool:
    upper = name.upper()
    if any(word in upper for word in SECRET_WORDS):
        return True
    return any(part in SECRET_SEGMENTS for part in re.split(r"[_.\-]+", upper))


def base_environment(environ: Mapping[str, str]) -> dict[str, str]:
    """What every job (and hook) starts from: PATH and HOME (always, empty if unset, as before)
    plus USER, LOGNAME, SHELL and TMPDIR when the daemon has them. Nothing else of the
    daemon's environment is passed on, and an unset name is never invented."""
    env = {"PATH": environ.get("PATH", ""), "HOME": environ.get("HOME", "")}
    for name in BASE_ENV_NAMES:
        value = environ.get(name)
        if value is not None and "\0" not in value:
            env[name] = value
    return env


def job_environment(env_extra: Mapping[str, str]) -> dict[str, str]:
    """What the daemon layers over PATH/HOME: the defaults, then the job's own env_extra
    without the reserved marker."""
    env = dict(DEFAULT_JOB_ENV)
    env.update({k: v for k, v in env_extra.items() if k != PASSTHROUGH_KEY})
    return env


def mask_env_extra(env_extra: Any) -> Any:
    """`env_extra` (JSON text or a dict) with every value replaced by `***`, names kept: what leaves the
    machine in an event payload or a stored snapshot (an explicit `--env KEY=SECRET` must not travel).
    The passthrough key only lists names, so it stays. Anything that is not a dict comes back unchanged."""
    if not env_extra:
        return env_extra
    as_text = isinstance(env_extra, str)
    if as_text:
        try:
            env = json.loads(env_extra)
        except ValueError:
            return None  # unparseable text could hold anything: drop it
    else:
        env = env_extra
    if not isinstance(env, dict):
        return env_extra
    masked = {str(k): (v if k == PASSTHROUGH_KEY else "***") for k, v in env.items()}
    return json.dumps(masked) if as_text else masked


def describe(env_extra: str | Mapping[str, str] | None) -> list[str]:
    """Display lines for `qur show` and Telegram Details: `NAME` for variables copied
    automatically (and queuer's own PATH/VIRTUAL_ENV/PYTHONPATH), `NAME=***` when the name
    looks secret, `NAME=value` otherwise."""
    if not env_extra:
        return []
    if isinstance(env_extra, str):
        try:
            env = json.loads(env_extra)
        except ValueError:
            return []
    else:
        env = dict(env_extra)
    if not isinstance(env, dict):
        return []
    passed = set(str(env.get(PASSTHROUGH_KEY) or "").split(","))
    lines: list[str] = []
    for name in sorted(env):
        if name == PASSTHROUGH_KEY:
            continue
        if name in NAME_ONLY or name in passed:
            lines.append(name)
        elif is_secret_name(name):
            lines.append(f"{name}=***")
        else:
            lines.append(f"{name}={env[name]}")
    return lines
