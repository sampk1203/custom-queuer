#!/usr/bin/env bash
# Installs / re-installs queuerd as a systemd --user service.
# Safe to re-run any time (e.g. after moving the project or pulling updates).
#
# Order of operations, and why: bootstrap uv -> sync venv -> run full test
# suite -> only then touch the running service. If the tests fail, the
# script stops there and the previously-installed (working) daemon, if
# any, is left running untouched.
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_DIR="$HOME/.config/systemd/user"
SERVICE_FILE="$SERVICE_DIR/queuerd.service"

cd "$PROJECT_DIR"

echo "==> Project dir: $PROJECT_DIR"

# ---------------------------------------------------------------------------
# 1. Bootstrap uv if it's not already available
# ---------------------------------------------------------------------------

if ! command -v uv > /dev/null 2>&1; then
    echo "==> uv not found -- installing it"
    curl -LsSf https://astral.sh/uv/install.sh | sh
    # the official installer puts uv in ~/.local/bin; make it visible to the
    # rest of this script without requiring a new shell
    export PATH="$HOME/.local/bin:$PATH"

    if ! command -v uv > /dev/null 2>&1; then
        echo "ERROR: uv installation completed but 'uv' is still not on PATH." >&2
        echo "Open a new terminal (or 'source ~/.bashrc') and re-run this script." >&2
        exit 1
    fi
else
    echo "==> uv already available: $(command -v uv)"
fi

# ---------------------------------------------------------------------------
# 2. Sync the venv (creates .venv if it doesn't exist, installs deps
#    including dev dependencies like pytest-asyncio)
# ---------------------------------------------------------------------------

echo "==> Syncing uv environment"
uv sync

# ---------------------------------------------------------------------------
# 3. Run the full test suite -- installation stops here if anything fails
# ---------------------------------------------------------------------------

echo "==> Running test suite"
if ! uv run pytest -v; then
    echo "" >&2
    echo "ERROR: tests failed. Install aborted -- the daemon has NOT been" >&2
    echo "touched. Fix the failing tests, then re-run this script." >&2
    exit 1
fi
echo "==> All tests passed"

# ---------------------------------------------------------------------------
# 3b. Install the CLI (queuer / qur / queuerd) onto PATH as real executables
#     -- editable, so code changes take effect without reinstalling
# ---------------------------------------------------------------------------

echo "==> Installing CLI onto PATH (queuer, qur, queuerd)"
uv tool install --editable . --force
uv tool update-shell

# ---------------------------------------------------------------------------
# 4. Stop any existing queuerd (systemd-managed or a stray manual process)
# ---------------------------------------------------------------------------

echo "==> Stopping any existing queuerd (systemd-managed or stray manual process)"
systemctl --user stop queuerd 2>/dev/null || true
pkill -x queuerd 2>/dev/null || true
sleep 1
if pgrep -x queuerd > /dev/null; then
    echo "WARNING: a queuerd process is still running after attempting to stop it." >&2
    pgrep -a -x queuerd >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# 5. Write / update the systemd unit
# ---------------------------------------------------------------------------

echo "==> Writing systemd --user unit to $SERVICE_FILE"
mkdir -p "$SERVICE_DIR"
cat > "$SERVICE_FILE" << EOF
[Unit]
Description=queuer background job queue daemon
After=default.target

[Service]
Type=simple
ExecStart=$PROJECT_DIR/.venv/bin/queuerd
Restart=on-failure
RestartSec=2

[Install]
WantedBy=default.target
EOF

# ---------------------------------------------------------------------------
# 6. Enable + start, enable linger
# ---------------------------------------------------------------------------

echo "==> Reloading systemd and enabling queuerd"
systemctl --user daemon-reload
systemctl --user enable --now queuerd

echo "==> Enabling linger for $USER (so queuerd survives logout)"
loginctl enable-linger "$USER"

echo "==> Verifying exactly one queuerd process is running"
pgrep -a -x queuerd

echo "==> Status:"
systemctl --user status queuerd --no-pager

echo "==> Done. 'queuer' / 'qur' commands work from any directory now -- no uv run needed."
echo "    (open a new terminal if 'qur' isn't found yet -- uv tool update-shell just ran)"
