"""Shared test setup.

Every test gets a throwaway notify config path. Without this, a Daemon built
with defaults would read the developer's real ~/.config/queuer/notify.json
and send real Telegram messages while the suite runs.

Every other QUEUER_* variable of the shell that runs the suite is removed for the
duration of each test (5.todo M7): install.sh runs this suite from the owner's shell,
and a QUEUER_POST_JOB_HOOK, QUEUER_KEEP, QUEUER_LOG_MAX_* or QUEUER_ENV_PASSTHROUGH
exported there would otherwise be read by every test Daemon.
"""

import os

import pytest

# The variables the isolation fixtures below set themselves; the scrub leaves them alone.
ISOLATED_QUEUER_VARS = ("QUEUER_NOTIFY_PATH", "QUEUER_DB_PATH", "QUEUER_LOG_DIR", "QUEUER_SOCKET_PATH")


@pytest.fixture(autouse=True)
def _no_inherited_queuer_settings(monkeypatch) -> None:
    """Tests start with none of the developer's QUEUER_* settings. A test that wants one
    sets it itself with monkeypatch.setenv (undone after the test, like this removal)."""
    for name in [n for n in os.environ if n.startswith("QUEUER_") and n not in ISOLATED_QUEUER_VARS]:
        monkeypatch.delenv(name, raising=False)


@pytest.fixture(autouse=True)
def _isolated_notify_config(tmp_path, monkeypatch):
    monkeypatch.setenv("QUEUER_NOTIFY_PATH", str(tmp_path / "notify-config" / "notify.json"))


@pytest.fixture(autouse=True)
def _isolated_state(tmp_path, monkeypatch):
    """Never touch the developer's real queuer state or live daemon socket
    (install.sh runs this suite on the machine that runs the real daemon)."""
    monkeypatch.setenv("QUEUER_DB_PATH", str(tmp_path / "iso-state" / "queuer.db"))
    monkeypatch.setenv("QUEUER_LOG_DIR", str(tmp_path / "iso-state" / "logs"))
    monkeypatch.setenv("QUEUER_SOCKET_PATH", str(tmp_path / "iso-run" / "queuer.sock"))
