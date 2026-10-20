"""The shared test setup (tests/conftest.py): the developer's own QUEUER_* settings never
reach a test (5.todo M7). The end-to-end test runs a tiny suite in a child pytest with
a hostile environment, because the suite's own environment is already scrubbed."""

from __future__ import annotations

import os
import shutil
import subprocess  # nosec B404
import sys
from pathlib import Path

CONFTEST = Path(__file__).with_name("conftest.py")

ISOLATED = ["QUEUER_DB_PATH", "QUEUER_HOSTS_PATH", "QUEUER_LOG_DIR", "QUEUER_NOTIFY_PATH", "QUEUER_SOCKET_PATH"]

INNER_TEST = """
import os


def test_only_the_isolation_variables_are_left():
    left = sorted(n for n in os.environ if n.startswith("QUEUER_"))
    assert left == %r


def test_the_isolation_variables_point_into_the_test_folder(tmp_path):
    for name in %r:
        assert os.environ[name].startswith(str(tmp_path.parent))
"""


def test_every_test_starts_with_only_the_isolation_variables():
    left = sorted(n for n in os.environ if n.startswith("QUEUER_"))
    assert left == ISOLATED


def test_a_test_can_still_set_its_own_variable(monkeypatch):
    monkeypatch.setenv("QUEUER_KEEP", "7")
    assert os.environ["QUEUER_KEEP"] == "7"


def test_exported_queuer_settings_do_not_reach_the_tests(tmp_path):
    suite = tmp_path / "suite"
    suite.mkdir()
    shutil.copy(CONFTEST, suite / "conftest.py")
    (suite / "test_inner.py").write_text(INNER_TEST % (ISOLATED, ISOLATED))
    env = dict(os.environ)
    env.update(
        QUEUER_POST_JOB_HOOK="touch /tmp/must-never-run",
        QUEUER_KEEP="3",
        QUEUER_LOG_MAX_AGE_DAYS="1",
        QUEUER_LOG_MAX_MB="1",
        QUEUER_LOG_CLEAN_HOURS="1",
        QUEUER_ENV_PASSTHROUGH="NOTHING",
        QUEUER_USE_PROXY="1",
        QUEUER_REDACT_PATH="/nonexistent/redact.json",
        QUEUER_DB_PATH="/nonexistent/real.db",  # an isolation variable: the fixtures replace it
    )
    # fixed argv, no shell; -p no:cacheprovider keeps the child from writing a cache folder
    res = subprocess.run(  # nosec B603
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", str(suite)],
        capture_output=True,
        text=True,
        env=env,
        cwd=suite,
        timeout=120,
        check=False,
    )
    assert res.returncode == 0, res.stdout + res.stderr
    assert "2 passed" in res.stdout
