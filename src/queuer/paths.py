"""Default paths and small constants shared by cli, daemon and selfcheck.

Split out of daemon.py (item 4 of 5.todo) so that `qur <any command>` does not have
to import asyncio and the rest of daemon.py just to find the database, socket and
log paths, or the systemd unit name.
"""

from __future__ import annotations

import os
from pathlib import Path

from queuer import db

DEFAULT_GRACE_SECS = 5

OK = "OK"
WARN = "WARN"
FAIL = "FAIL"

UNIT_NAME = "queuerd"


def _default_db_path() -> Path:
    override = os.environ.get("QUEUER_DB_PATH")
    return Path(override) if override else db.DEFAULT_DB_PATH


def _default_socket_path() -> Path:
    override = os.environ.get("QUEUER_SOCKET_PATH")
    if override:
        return Path(override)
    runtime_dir = os.environ.get("XDG_RUNTIME_DIR")  # unset or empty -> private fallback
    return (Path(runtime_dir) if runtime_dir else _fallback_socket_dir()) / "queuer.sock"


def _fallback_socket_dir() -> Path:
    """Per-user dir used when XDG_RUNTIME_DIR is unset (cron, sudo, ...). Never
    a bare /tmp: another user could pre-create the socket path there."""
    return Path("/tmp") / f"queuer-{os.getuid()}"  # nosec B108 - uid-scoped, ownership/mode/symlink checked by the caller


def _default_log_dir() -> Path:
    override = os.environ.get("QUEUER_LOG_DIR")
    if override:
        return Path(override)
    return Path.home() / ".local" / "state" / "queuer" / "logs"
