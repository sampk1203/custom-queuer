#!/usr/bin/env bash
# Installs / re-installs queuerd as a systemd --user service, or uninstalls it.
# Safe to re-run any time (e.g. after moving the project or pulling updates).
#
#   ./install.sh              asks: Enter = install, 1 = uninstall
#   ./install.sh install      skip the question
#   ./install.sh uninstall    skip the question
#
# Order of operations, and why: bootstrap uv -> sync venv -> run full test
# suite -> install CLI + shell completion -> only then touch the running service. If the tests fail, the
# script stops there and the previously-installed (working) daemon, if
# any, is left running untouched.
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_DIR="$HOME/.config/systemd/user"
SERVICE_FILE="$SERVICE_DIR/queuerd.service"

cd "$PROJECT_DIR"

# ---------------------------------------------------------------------------
# 0. Install or uninstall? Enter = install, 1 = uninstall. (No terminal, e.g.
#    piped in: install, unless told otherwise on the command line.)
# ---------------------------------------------------------------------------

ask() {  # ask "prompt" -> sets REPLY; empty when there is no terminal
    REPLY=""
    if [ -t 0 ]; then read -r -p "$1" REPLY || REPLY=""; fi
}

ACTION="${1:-}"
if [ -z "$ACTION" ]; then
    echo "What do you want to do?"
    echo "  [Enter]  install (or re-install)"
    echo "  [1]      uninstall -- remove everything this script installed"
    ask "> "
    case "$REPLY" in
        1) ACTION=uninstall ;;
        *) ACTION=install ;;
    esac
fi

if [ "$ACTION" = "uninstall" ]; then
    STATE_DIR="$HOME/.local/state/queuer"  # where the daemon really keeps it (it ignores XDG_STATE_HOME)
    CONFIG_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/queuer"
    SOCKET="${XDG_RUNTIME_DIR:-/tmp/queuer-$(id -u)}/queuer.sock"
    ZSH_COMP=("$HOME/.zfunc/_qur" "$HOME/.zfunc/_queuer")
    FISH_COMP=("$HOME/.config/fish/completions/qur.fish" "$HOME/.config/fish/completions/queuer.fish")
    BASH_COMP=("$HOME/.bash_completions/qur.sh" "$HOME/.bash_completions/queuer.sh")

    echo ""
    echo "This will remove:"
    echo "  - the queuerd service (stopped, disabled) and $SERVICE_FILE"
    echo "  - the queuer / qur / queuerd commands (uv tool) and their shell completion"
    echo "  - the project virtualenv $PROJECT_DIR/.venv"
    echo "  - the job database (queue + history) in $STATE_DIR"
    echo "  - the Telegram setup (bot token, chats, id) in $CONFIG_DIR"
    echo "  - the daemon socket $SOCKET"
    echo "It will NOT delete this project folder, uv itself, or the PATH line"
    echo "that 'uv tool update-shell' added to your shell rc."
    echo ""
    ask "Proceed? [y/N] "
    case "$REPLY" in y|Y|yes|YES) ;; *) echo "Cancelled. Nothing changed."; exit 0 ;; esac

    ask "Also delete job logs ($STATE_DIR/logs)? [y/N] "
    DEL_LOGS=no; case "$REPLY" in y|Y|yes|YES) DEL_LOGS=yes ;; esac
    echo "Linger (keeps your user services running after logout) was enabled by the"
    echo "installer. Other user services you run may rely on it."
    ask "Disable linger for $USER? [y/N] "
    DEL_LINGER=no; case "$REPLY" in y|Y|yes|YES) DEL_LINGER=yes ;; esac

    echo "==> Stopping and disabling queuerd"
    systemctl --user disable --now queuerd 2>/dev/null || true
    pkill -x queuerd 2>/dev/null || true
    sleep 1
    if pgrep -x queuerd > /dev/null; then
        echo "WARNING: a queuerd process is still running:" >&2
        pgrep -a -x queuerd >&2
        echo "Kill it (kill <pid>) and re-run to finish." >&2
        exit 1
    fi
    rm -f "$SERVICE_FILE"
    systemctl --user daemon-reload 2>/dev/null || true
    systemctl --user reset-failed queuerd 2>/dev/null || true

    if [ "$DEL_LINGER" = yes ]; then
        echo "==> Disabling linger for $USER"
        loginctl disable-linger "$USER" || echo "WARNING: could not disable linger." >&2
    fi

    echo "==> Removing the queuer / qur / queuerd commands"
    if command -v uv > /dev/null 2>&1; then
        uv tool uninstall queuer 2>/dev/null || echo "    (queuer was not installed as a uv tool)"
    else
        echo "    uv not found -- skipped; remove the commands yourself if they exist"
    fi

    echo "==> Removing shell completion"
    rm -f "${ZSH_COMP[@]}" "${FISH_COMP[@]}" "${BASH_COMP[@]}"
    if [ -f "$HOME/.bashrc" ]; then  # drop the lines typer added to .bashrc
        sed -i -e "\|^source '$HOME/.bash_completions/qur.sh'\$|d" \
               -e "\|^source '$HOME/.bash_completions/queuer.sh'\$|d" "$HOME/.bashrc"
    fi

    echo "==> Removing job database and Telegram setup"
    rm -f "$STATE_DIR"/queuer.db "$STATE_DIR"/queuer.db-wal "$STATE_DIR"/queuer.db-shm
    rm -rf "$CONFIG_DIR"
    rm -f "$SOCKET"
    rmdir "/tmp/queuer-$(id -u)" 2>/dev/null || true  # private fallback dir, only if empty
    if [ "$DEL_LOGS" = yes ]; then
        echo "==> Removing job logs"
        rm -rf "$STATE_DIR/logs"
    else
        echo "==> Keeping job logs in $STATE_DIR/logs"
    fi
    rmdir "$STATE_DIR" 2>/dev/null || true  # only if now empty

    echo "==> Removing the virtualenv"
    rm -rf "$PROJECT_DIR/.venv"

    echo "==> Uninstalled. Open a new terminal so the removed commands and completion disappear."
    echo "    To reinstall: ./install.sh"
    exit 0
fi

if [ "$ACTION" != "install" ]; then
    echo "Usage: $0 [install|uninstall]" >&2
    exit 2
fi

echo "==> Project dir: $PROJECT_DIR"

# ---------------------------------------------------------------------------
# 1. Bootstrap uv if it's not already available
# ---------------------------------------------------------------------------

if ! command -v uv > /dev/null 2>&1; then
    echo "==> uv not found -- installing it"
    # download first, run second: a cut-off download is never executed
    UV_INSTALLER="$(mktemp)"
    trap 'rm -f "$UV_INSTALLER"' EXIT
    curl --proto '=https' --tlsv1.2 -LsSf https://astral.sh/uv/install.sh -o "$UV_INSTALLER"
    sh "$UV_INSTALLER"
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
# 3c. Install shell tab-completion for `qur` and `queuer`
#     Typer detects the shell from the parent process, so run it inside the
#     user's login shell ($SHELL) -- otherwise it would see this script's own
#     bash and install bash completion for a zsh/fish user. A failure here is
#     only a warning: completion is a convenience, not worth aborting for.
# ---------------------------------------------------------------------------

echo "==> Installing shell completion for ${SHELL:-bash}"
UV_TOOL_BIN="$(uv tool dir --bin)"
for prog in qur queuer; do
    # "; exit \$?" stops the shell from exec-ing the command directly, which
    # would remove the shell from the process tree and break detection
    if ! "${SHELL:-bash}" -c "'$UV_TOOL_BIN/$prog' --install-completion; exit \$?"; then
        echo "WARNING: could not install completion for '$prog' (run '$prog --install-completion' manually)." >&2
    fi
done

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
# 4b. Tighten permissions of state created by older versions (daemon is
#     stopped, nothing is writing): owner-only db, logs, config.
# ---------------------------------------------------------------------------

STATE_DIR="$HOME/.local/state/queuer"
CONFIG_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/queuer"
echo "==> Making db, logs and config owner-only"
for dir in "$STATE_DIR" "$STATE_DIR/logs" "$CONFIG_DIR"; do
    if [ -d "$dir" ] && [ ! -L "$dir" ]; then chmod 700 "$dir"; fi
done
if [ -d "$STATE_DIR" ]; then
    find "$STATE_DIR" -maxdepth 2 -type f \( -name 'queuer.db*' -o -name '*.log' \) -exec chmod 600 {} +
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
