"""Command path resolution for queuer.

Given a raw argv as typed by the user, produce a "resolved" argv where
path-like tokens are canonicalized to absolute paths, while flags and
plain non-path words are left untouched.

Rule (per the project plan, section 2):
  A token is treated as a path and gets canonicalized if either:
    - it contains a '/' (relative or absolute path) -- resolved even if
      the target doesn't exist, since a not-yet-created output path is
      still a path; or
    - it is a bare filename that exists relative to cwd (covers args like
      `data.csv` sitting in the current directory).

  argv[0] specifically (the executable itself) additionally gets resolved
  via PATH lookup if it's a bare command name (e.g. `python3`), since
  that's what actually determines what gets exec'd.

  Anything else -- flags (`-x`, `--verbose`), and bare words that aren't
  existing files -- is left exactly as typed.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path


def resolve_command(argv: list[str], cwd: str, path_env: str | None = None) -> list[str]:
    """Return a new argv list with path-like tokens canonicalized.

    `cwd` is the directory the command was enqueued from (captured by the
    CLI at `add` time), used as the base for resolving relative paths.

    `path_env` is the PATH to use for argv[0] lookup (e.g. `python3` ->
    absolute path). Must be the *client's* PATH, not the resolving
    process's own -- the daemon is long-lived and its PATH predates any
    venv the client had active at enqueue time, so it can't be trusted to
    resolve bare commands like `python`.
    """
    if not argv:
        return []
    return [_resolve_token(token, cwd, is_argv0=(i == 0), path_env=path_env) for i, token in enumerate(argv)]


def _resolve_token(token: str, cwd: str, *, is_argv0: bool, path_env: str | None = None) -> str:
    if token == "":
        # Path(cwd) / "" == Path(cwd), which always exists -- without this
        # guard an empty argument would get silently rewritten to the cwd's
        # own absolute path by the "bare existing file" branch below.
        return token

    # Explicit path (contains a separator) -> make absolute relative to cwd,
    # regardless of whether it currently exists. Uses abspath (not resolve())
    # deliberately: resolve() follows symlinks to their real target, which
    # would silently rewrite e.g. `.venv/bin/python3` to the base interpreter
    # it points at -- breaking venv activation for the job. abspath only
    # normalizes `..`/`.` and makes the path absolute; it never dereferences
    # symlinks.
    if "/" in token:
        return os.path.abspath(os.path.join(cwd, token))

    # Bare filename that happens to exist relative to cwd.
    candidate = Path(cwd) / token
    if candidate.exists():
        return os.path.abspath(str(candidate))

    # argv[0] as a bare command name -> resolve via PATH (e.g. "python3").
    if is_argv0:
        which = shutil.which(token, path=path_env)
        if which:
            return os.path.abspath(which)

    # Everything else: flags, literal args, words that aren't files.
    return token
