"""Shared test setup.

Every test gets a throwaway notify config path. Without this, a Daemon built
with defaults would read the developer's real ~/.config/queuer/notify.json
and send real Telegram messages while the suite runs.
"""

import pytest


@pytest.fixture(autouse=True)
def _isolated_notify_config(tmp_path, monkeypatch):
    monkeypatch.setenv("QUEUER_NOTIFY_PATH", str(tmp_path / "notify-config" / "notify.json"))
