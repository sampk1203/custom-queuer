"""Shared test setup.

Every test gets a throwaway notify config path. Without this, a Daemon built
with defaults would read the developer's real ~/.config/queuer/notify.json
and send real Telegram messages while the suite runs.
"""

import pytest


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
