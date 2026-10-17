"""Command path resolution for queuer.

Given a raw argv as typed by the user, produce a "resolved" argv. ONLY argv[0]
(the executable itself) is resolved; every other token is left exactly as typed.

Rule for argv[0]:
  - it contains a '/' (relative or absolute path): made absolute against cwd;
  - or it is a bare name that exists relative to cwd: made absolute;
  - or it is a bare command name (e.g. `python3`): looked up on PATH (the
    CLIENT's PATH, see `resolve_command`) and made absolute.

Why the arguments are not rewritten (H3): the job is started with
cwd=<the directory it was added from>, so every relative path in its arguments
already means the same thing at run time. Rewriting tokens could only break
them: `--config=conf/a.yaml`, `out=results/run1`, a Hugging Face id like
`facebook/opt-125m`, a URL (abspath collapses the `//`), `user@host:/data`,
`s/a/b/`, `python main.py train` when a folder named `train` exists. Shell
`-c` scripts, inline `python -c` code and everything else pass through
byte for byte.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

# Recognized shell interpreters. cli.py imports this to tell a hand-written
# `bash -c '...'` (left alone) from a command that needs shell wrapping.
SHELL_INTERPRETERS = {"bash", "sh", "zsh", "dash", "ksh", "ash"}


def resolve_command(argv: list[str], cwd: str, path_env: str | None = None) -> list[str]:
    """Return a new argv list with argv[0] resolved to an absolute path and every other
    token unchanged (H3).

    `cwd` is the directory the command was enqueued from (captured by the
    CLI at `add` time), used as the base for a relative argv[0].

    `path_env` is the PATH to use for argv[0] lookup (e.g. `python3` ->
    absolute path). Must be the *client's* PATH, not the resolving
    process's own -- the daemon is long-lived and its PATH predates any
    venv the client had active at enqueue time, so it can't be trusted to
    resolve bare commands like `python`.
    """
    if not argv:
        return []
    return [_resolve_program(argv[0], cwd, path_env), *argv[1:]]


def _resolve_program(token: str, cwd: str, path_env: str | None = None) -> str:
    if token == "":  # nosec B105 - an empty program name, not a password
        # Path(cwd) / "" == Path(cwd), which always exists -- without this
        # guard an empty program name would get silently rewritten to the cwd's
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

    # Bare name that happens to exist relative to cwd.
    candidate = Path(cwd) / token
    if candidate.exists():
        return os.path.abspath(str(candidate))

    # Bare command name -> resolve via PATH (e.g. "python3").
    which = shutil.which(token, path=path_env)
    if which:
        return os.path.abspath(which)

    # Not found anywhere: left as typed (the daemon refuses it as "not found").
    return token
