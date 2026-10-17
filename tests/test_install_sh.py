"""install.sh is never executed by the tests; these only read it (and let bash check its
syntax with -n, which runs nothing)."""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest

from queuer import selfcheck

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


def test_yes_flag_skips_every_uninstall_prompt_and_keeps_logs_and_linger():
    text = _text()
    assert "--yes|-y) ASSUME_YES=yes" in text
    assert "./install.sh uninstall --yes" in text  # in the header comment
    guard = text.index('if [ "$ASSUME_YES" = yes ]; then')
    proceed = text.index('ask "Proceed? [y/N] "')
    assert guard < proceed  # the prompts sit in the else branch
    # the defaults in --yes mode are the safe ones: no log deletion, linger stays on
    assert text.index("DEL_LOGS=no") < guard and text.index("DEL_LINGER=no") < guard


def test_usage_line_mentions_yes():
    assert "[--skip-tests] [--yes]" in _text()


def test_bashrc_is_backed_up_before_completion_lines_are_removed():
    text = _text()
    assert "sed -i.queuer-bak" in text and ".bashrc.queuer-bak" in text
    assert "sed -i -e" not in text  # no in-place edit without a backup


def _unit_template_lines() -> set[str]:
    """Every `Key=Value` line install.sh writes into the unit (both heredocs)."""
    text = _text()
    hardening = text.split("<< 'EOT'\n", 1)[1].split("\nEOT", 1)[0]
    unit = text.split('cat > "$SERVICE_FILE" << EOF\n', 1)[1].split("\nEOF", 1)[0]
    lines = (hardening + "\n" + unit).splitlines()
    return {line.strip() for line in lines if re.match(r"^[A-Za-z]+=", line.strip())}


def test_unit_template_and_doctor_check_the_same_settings():
    """qur doctor (F36) compares the installed unit with selfcheck.UNIT_SETTINGS /
    HARDENING_SETTINGS; this fails when install.sh and those constants drift apart."""
    expected = {f"{k}={v}" for k, v in {**selfcheck.UNIT_SETTINGS, **selfcheck.HARDENING_SETTINGS}.items()}
    expected |= {
        "ExecStart=$PROJECT_DIR/.venv/bin/queuerd",
        "Description=queuer background job queue daemon",
        "After=default.target",
        "WantedBy=default.target",
    }
    assert _unit_template_lines() == expected


# --- 5.todo M3: OOMPolicy=continue -------------------------------------------------------


def test_unit_sets_oom_policy_continue_outside_the_hardening_block():
    """One OOM-killed job must not stop the whole service, whatever QUEUER_HARDEN says."""
    text = _text()
    hardening = text.split("<< 'EOT'\n", 1)[1].split("\nEOT", 1)[0]
    unit = text.split('cat > "$SERVICE_FILE" << EOF\n', 1)[1].split("\nEOF", 1)[0]
    assert "OOMPolicy=continue" in unit
    assert "OOMPolicy" not in hardening  # level `none` writes it too
    assert selfcheck.UNIT_SETTINGS["OOMPolicy"] == "continue"


def test_unit_heredoc_comments_cannot_expand():
    """The unit heredoc is unquoted: a stray $ or backtick in a comment would be expanded."""
    unit = _text().split('cat > "$SERVICE_FILE" << EOF\n', 1)[1].split("\nEOF", 1)[0]
    for line in unit.splitlines():
        if line.startswith("#"):
            assert "`" not in line and "$" not in line


# --- 5.todo M2: never kill a running job without asking ---------------------------------


def test_install_asks_before_stopping_the_daemon():
    text = _text()
    ask_at = text.index('confirm_stop_jobs "Re-installing"')
    stop_at = text.index("systemctl --user stop queuerd")
    assert ask_at < stop_at
    assert text.index("uv run pytest -v") < ask_at  # after the tests, before the service is touched


def test_uninstall_asks_before_stopping_the_daemon():
    text = _text()
    ask_at = text.index('confirm_stop_jobs "Uninstalling"')
    assert text.index('ask "Proceed? [y/N] "') < ask_at < text.index("systemctl --user disable --now queuerd")


def test_running_jobs_come_from_the_daemon_and_the_default_answer_is_no():
    text = _text()
    assert 'status --json' in text
    body = text.split("confirm_stop_jobs() {", 1)[1].split("\n}\n", 1)[0]
    assert 'ask "Kill them and continue? [y/N] "' in body
    assert 'ASSUME_YES" = "yes"' in body  # --yes skips the question
    # anything but y / yes cancels with a non-zero exit (so a script with no terminal never kills a job)
    assert "y|Y|yes|YES) ;;" in body and "exit 1" in body


def test_no_blanket_pkill_of_every_queuerd():
    """`pkill -x queuerd` also took out a dev copy; a process the service does not manage is
    now listed and stopped only after its own yes (or --yes)."""
    text = _text()
    code = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))
    assert "pkill" not in code
    body = text.split("stop_stray_queuerd() {", 1)[1].split("\n}\n", 1)[0]
    assert 'ask "Stop it? [y/N] "' in body and 'ASSUME_YES" != yes' in body


def test_usage_lines_agree_and_yes_is_documented_for_install():
    text = _text()
    assert text.count("Usage: $0 [install|uninstall] [--skip-tests] [--yes]") == 2
    assert "./install.sh install --yes" in text  # in the header comment
