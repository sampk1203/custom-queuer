#!/usr/bin/env bash
# Installs / re-installs queuerd as a systemd --user service.
# Safe to re-run any time (e.g. after moving the project or pulling updates).
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_DIR="$HOME/.config/systemd/user"
SERVICE_FILE="$SERVICE_DIR/queuerd.service"

echo "==> Project dir: $PROJECT_DIR"

echo "==> Syncing uv environment"
cd "$PROJECT_DIR"
uv sync

echo "==> Stopping any existing queuerd (systemd-managed or stray manual process)"
systemctl --user stop queuerd 2>/dev/null || true
pkill -x queuerd 2>/dev/null || true
sleep 1
if pgrep -x queuerd > /dev/null; then
    echo "WARNING: a queuerd process is still running after attempting to stop it." >&2
    pgrep -a -x queuerd >&2
    exit 1
fi

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

echo "==> Reloading systemd and enabling queuerd"
systemctl --user daemon-reload
systemctl --user enable --now queuerd

echo "==> Enabling linger for $USER (so queuerd survives logout)"
loginctl enable-linger "$USER"

echo "==> Verifying exactly one queuerd process is running"
pgrep -a -x queuerd

echo "==> Status:"
systemctl --user status queuerd --no-pager

echo "==> Done. 'queuer' commands should now work from any directory via: uv run queuer --help"
