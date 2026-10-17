"""install.sh is never executed by the tests; these only read it (and let bash check its
syntax with -n, which runs nothing)."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parent.parent / "install.sh"


def _text() -> str:
    return SCRIPT.read_text()


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash not available")
def test_install_sh_has_valid_syntax():
    res = subprocess.run(["bash", "-n", str(SCRIPT)], capture_output=True, text=True, check=False)
    assert res.returncode == 0, res.stderr


def test_skip_tests_flag_is_parsed_and_documented():
    text = _text()
    assert "--skip-tests) SKIP_TESTS=yes" in text
    assert "SKIP_TESTS=no" in text
    assert "./install.sh install --skip-tests" in text  # in the header comment


def test_pytest_only_runs_when_tests_are_not_skipped():
    text = _text()
    guard = text.index('if [ "$SKIP_TESTS" = yes ]; then')
    run = text.index("uv run pytest -v")
    else_ = text.index("else", guard)
    assert guard < else_ < run  # the pytest call sits in the else branch
    assert text.count("uv run pytest") == 1


def test_unknown_arguments_are_still_rejected():
    assert 'Usage: $0 [install|uninstall] [--skip-tests]' in _text()
