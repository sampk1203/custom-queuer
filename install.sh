#!/usr/bin/env bash
# Installs / re-installs queuerd as a systemd --user service, or uninstalls it.
# Safe to re-run any time (e.g. after moving the project or pulling updates).
#
#   ./install.sh                         asks: Enter = install, 1 = uninstall
#   ./install.sh install                 skip the question
#   ./install.sh uninstall               skip the question
#   ./install.sh install --skip-tests    install without running the test suite first
#   ./install.sh uninstall --yes         uninstall without any prompt (no terminal needed);
#                                        keeps the job logs and leaves linger enabled
#   ./install.sh install --yes           re-install without asking, even if jobs are running
#
# Stopping the daemon kills its running jobs (recorded "interrupted", never retried). Before
# it does that, install and uninstall list the running jobs and ask (default: no). With no
# terminal and no --yes the answer is no, so a re-run from a script never kills a job.
#
# Order of operations, and why: bootstrap uv -> sync venv -> run full test
# suite (unless --skip-tests) -> install CLI + shell completion -> only then touch the
# running service. If the tests fail, the script stops there and the previously-installed
# (working) daemon, if any, is left running untouched. The same goes for a "no" to the
# question about running jobs.
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

# M2 (5.todo): never kill a running job without asking. `qur daemon stop/restart` ask
# first; the installer used to stop the service without a word.
running_jobs() {  # one line per running or frozen job; nothing when idle or no daemon answers
    local qur_bin py
    if [ -x "$PROJECT_DIR/.venv/bin/qur" ]; then
        qur_bin="$PROJECT_DIR/.venv/bin/qur"
    else
        qur_bin="$(command -v qur || true)"
    fi
    [ -n "$qur_bin" ] || return 0  # no CLI, so no daemon of ours to ask
    py="$PROJECT_DIR/.venv/bin/python"
    [ -x "$py" ] || py="$(command -v python3 || true)"
    if [ -z "$py" ]; then
        echo "WARNING: python3 not found, so running jobs cannot be checked." >&2
        return 0
    fi
    # `qur status --json` fails when the daemon does not answer: then there is nothing to kill
    "$qur_bin" status --json 2> /dev/null | "$py" -c '
import json, sys
try:
    channels = json.load(sys.stdin).get("channels", {})
except ValueError:
    sys.exit(0)
for channel in sorted(channels, key=int):
    for key in ("running", "frozen"):
        job = channels[channel].get(key)
        if not job:
            continue
        try:
            words = " ".join(json.loads(job["raw_cmd"]))
        except (KeyError, TypeError, ValueError):
            words = "?"
        if len(words) > 70:
            words = words[:67] + "..."
        print("#%s (channel %s, %s)  %s" % (job["id"], channel, key, words))
' 2> /dev/null || true
}

confirm_stop_jobs() {  # confirm_stop_jobs WHAT -> returns when it is fine to stop the daemon, else exits 1
    local jobs
    jobs="$(running_jobs)"
    [ -n "$jobs" ] || return 0
    echo ""
    echo "queuerd is running these jobs:"
    echo "$jobs" | sed 's/^/  /'
    echo "$1 stops the daemon, which kills them. They are recorded as 'interrupted'"
    echo "(not failed, not retried); the queued jobs stay queued."
    # written "yes" in quotes on purpose: tests/test_install_sh.py finds the uninstall prompts by the
    # first unquoted `[ "$ASSUME_YES" = yes ]`
    if [ "$ASSUME_YES" = "yes" ]; then
        echo "--yes: continuing."
        return 0
    fi
    ask "Kill them and continue? [y/N] "
    case "$REPLY" in
        y|Y|yes|YES) ;;
        *)
            echo "Cancelled. The daemon and its jobs were not touched." >&2
            echo "Re-run when the jobs have finished, or use --yes to kill them." >&2
            exit 1
            ;;
    esac
}

wait_unit_stopped() {  # up to 25 s for the unit to go inactive; exits 1 if it does not
    local _
    for _ in $(seq 1 25); do systemctl --user is-active --quiet queuerd 2> /dev/null || return 0; sleep 1; done
    echo "WARNING: the queuerd service is still active after 25 s:" >&2
    systemctl --user status queuerd --no-pager >&2 || true
    exit 1
}

stop_stray_queuerd() {  # a queuerd that systemd does not manage (started by hand, a dev copy)
    local pids _
    pids="$(pgrep -x queuerd || true)"
    [ -n "$pids" ] || return 0
    echo "A queuerd process that the service does not manage is running:"
    pgrep -a -x queuerd
    if [ "$ASSUME_YES" != yes ]; then
        ask "Stop it? [y/N] "
        case "$REPLY" in
            y|Y|yes|YES) ;;
            *)
                echo "Cancelled. That process was left running." >&2
                exit 1
                ;;
        esac
    fi
    # SIGTERM only: a stopping daemon kills its jobs (up to 5 s grace) and records them as
    # "interrupted" before it exits, so give it time to do that
    # shellcheck disable=SC2086
    kill $pids 2> /dev/null || true
    for _ in $(seq 1 25); do pgrep -x queuerd > /dev/null || break; sleep 1; done
    if pgrep -x queuerd > /dev/null; then
        echo "WARNING: a queuerd process is still running:" >&2
        pgrep -a -x queuerd >&2
        echo "Kill it (kill <pid>) and re-run to finish." >&2
        exit 1
    fi
}

ACTION=""
SKIP_TESTS=no
ASSUME_YES=no
for arg in "$@"; do
    case "$arg" in
        install|uninstall) ACTION="$arg" ;;
        --skip-tests) SKIP_TESTS=yes ;;
        --yes|-y) ASSUME_YES=yes ;;
        "") ;;
        *)
            echo "Usage: $0 [install|uninstall] [--skip-tests] [--yes]" >&2
            exit 2
            ;;
    esac
done

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
    DEL_LOGS=no
    DEL_LINGER=no
    if [ "$ASSUME_YES" = yes ]; then
        echo "--yes: proceeding without questions (job logs are kept, linger stays enabled)."
    else
        ask "Proceed? [y/N] "
        case "$REPLY" in y|Y|yes|YES) ;; *) echo "Cancelled. Nothing changed."; exit 0 ;; esac

        ask "Also delete job logs ($STATE_DIR/logs)? [y/N] "
        case "$REPLY" in y|Y|yes|YES) DEL_LOGS=yes ;; esac
        echo "Linger (keeps your user services running after logout) was enabled by the"
        echo "installer. Other user services you run may rely on it."
        ask "Disable linger for $USER? [y/N] "
        case "$REPLY" in y|Y|yes|YES) DEL_LINGER=yes ;; esac
    fi

    confirm_stop_jobs "Uninstalling"
    echo "==> Stopping and disabling queuerd"
    systemctl --user disable --now queuerd 2>/dev/null || true
    wait_unit_stopped
    stop_stray_queuerd
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
        # -i.queuer-bak keeps the original next to the edited file (GNU and BSD sed)
        sed -i.queuer-bak -e "\|^source '$HOME/.bash_completions/qur.sh'\$|d" \
               -e "\|^source '$HOME/.bash_completions/queuer.sh'\$|d" "$HOME/.bashrc"
        echo "    (your old .bashrc is kept as $HOME/.bashrc.queuer-bak)"
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

    echo "NOTE: the Telegram bot token still works. If you are done with the bot, send /revoke to"
    echo "      @BotFather. Job logs (kept above if you said no) can hold secrets."
    echo "==> Uninstalled. Open a new terminal so the removed commands and completion disappear."
    echo "    To reinstall: ./install.sh"
    exit 0
fi

if [ "$ACTION" != "install" ]; then
    echo "Usage: $0 [install|uninstall] [--skip-tests] [--yes]" >&2
    exit 2
fi

echo "==> Project dir: $PROJECT_DIR"

# ---------------------------------------------------------------------------
# 1. Bootstrap uv if it's not already available
# ---------------------------------------------------------------------------

if ! command -v uv > /dev/null 2>&1; then
    # Pinned release + SHA-256 (taken from the release's own .sha256 files): a
    # tampered or cut-off download is rejected, nothing from the net is run
    # unchecked. To bump: change UV_VERSION and both hashes together.
    UV_VERSION="0.13.0"
    case "$(uname -s)-$(uname -m)" in
        Linux-x86_64)  UV_ARCH=x86_64;  UV_SHA256="1468ebd5a5541121837c5a2817b9972ba6090fa6caa3d142620850a47fb75154" ;;
        Linux-aarch64) UV_ARCH=aarch64; UV_SHA256="3ccfb6af6e242433eb552f7d9676abd5412c6595c497e990d9c8cb7b5bd4d2c3" ;;
        *)
            echo "ERROR: uv not found and no pinned download for $(uname -sm)." >&2
            echo "Install uv yourself (https://docs.astral.sh/uv/) and re-run this script." >&2
            exit 1
            ;;
    esac
    echo "==> uv not found -- installing pinned uv $UV_VERSION ($UV_ARCH)"
    UV_TARBALL="uv-${UV_ARCH}-unknown-linux-gnu.tar.gz"
    UV_TMP="$(mktemp -d)"
    trap 'rm -rf "$UV_TMP"' EXIT
    curl --proto '=https' --tlsv1.2 -LsSf \
        "https://github.com/astral-sh/uv/releases/download/${UV_VERSION}/${UV_TARBALL}" -o "$UV_TMP/$UV_TARBALL"
    if ! echo "${UV_SHA256}  $UV_TMP/$UV_TARBALL" | sha256sum -c - > /dev/null 2>&1; then
        echo "ERROR: the uv download does not match its pinned SHA-256. Not installing it." >&2
        exit 1
    fi
    mkdir -p "$UV_TMP/x" "$HOME/.local/bin"
    tar -xzf "$UV_TMP/$UV_TARBALL" -C "$UV_TMP/x"
    for bin in uv uvx; do
        found="$(find "$UV_TMP/x" -type f -name "$bin" | head -n 1)"
        if [ -n "$found" ]; then install -m 0755 "$found" "$HOME/.local/bin/$bin"; fi
    done
    # make uv visible to the rest of this script without requiring a new shell
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

if [ "$SKIP_TESTS" = yes ]; then
    echo "==> Skipping the test suite (--skip-tests)"
else
    echo "==> Running test suite"
    if ! uv run pytest -v; then
        echo "" >&2
        echo "ERROR: tests failed. Install aborted -- the daemon has NOT been" >&2
        echo "touched. Fix the failing tests, then re-run this script (or --skip-tests to install anyway)." >&2
        exit 1
    fi
    echo "==> All tests passed"
fi

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

# M2: a running job is listed and the answer asked for first (default no); only the
# service is stopped (no blanket pkill), and a process the service does not manage is
# stopped only after a separate yes.
confirm_stop_jobs "Re-installing"
echo "==> Stopping any existing queuerd (systemd-managed or stray manual process)"
systemctl --user stop queuerd 2>/dev/null || true
wait_unit_stopped
stop_stray_queuerd

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

# Hardening (S8). These settings sandbox the daemon -- and are INHERITED BY EVERY JOB
# it runs, so only options that cannot break normal compute jobs are used. Deliberately
# NOT set: ProtectHome / ProtectSystem=strict (jobs write to $HOME and their cwd) and
# PrivateTmp (the daemon's /tmp/queuer-<uid> socket fallback must stay visible).
#
#   QUEUER_HARDEN=basic  (default) UMask, NoNewPrivileges, LockPersonality, RestrictAddressFamilies
#   QUEUER_HARDEN=none   the old, unsandboxed unit
#
# (A third level with ProtectKernelTunables/Modules was removed: an unprivileged user service
# cannot drop capabilities on many systems -- status=218/CAPABILITIES -- and it added next to
# nothing, your user cannot write /proc/sys or load kernel modules anyway.)
#
# If systemd refuses to start the service with `basic`, the installer falls back to `none` and
# says so; it never leaves you without a running daemon.
HARDEN="${QUEUER_HARDEN:-basic}"
case "$HARDEN" in
    basic|none) ;;
    *) echo "QUEUER_HARDEN must be basic or none (got '$HARDEN')" >&2; exit 2 ;;
esac

hardening_lines() {  # hardening_lines LEVEL -> the [Service] lines for that level
    [ "$1" = none ] && return 0
    cat << 'EOT'
# --- hardening (S8); inherited by jobs ---
# New files (job output, logs) are private to you even under a 022 umask.
UMask=0077
# setuid/sudo inside a job will not gain privileges. Jobs that need sudo: QUEUER_HARDEN=none.
NoNewPrivileges=yes
LockPersonality=yes
# AF_NETLINK is kept on purpose: getifaddrs() (MPI, NCCL, psutil, `ip`) needs it.
RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6 AF_NETLINK
EOT
}

write_unit() {  # write_unit LEVEL
    echo "==> Writing systemd --user unit to $SERVICE_FILE (hardening: $1)"
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
# On stop the daemon SIGTERMs (5 s later SIGKILLs) running jobs, records them as "interrupted" and
# sends the Telegram message: ~20 s worst case. Do not hang a logout on them.
TimeoutStopSec=30
# One OOM-killed job (the kernel or systemd-oomd picks a job process, not the daemon) must not
# make systemd stop the whole service: the default policy can, and every other channel's job
# would then be killed and recorded as interrupted. Needs systemd 243 or newer; an older one
# ignores the line.
OOMPolicy=continue
$(hardening_lines "$1")

[Install]
WantedBy=default.target
EOF
}

start_service() {  # start at $HARDEN, stepping down until systemd accepts the unit
    local level
    for level in basic none; do
        case "$HARDEN:$level" in
            none:basic) continue ;;  # stricter than asked for
        esac
        write_unit "$level"
        systemctl --user daemon-reload
        systemctl --user enable queuerd
        systemctl --user restart queuerd || true
        sleep 2
        if systemctl --user is-active --quiet queuerd; then
            echo "==> queuerd is running (hardening: $level)"
            [ "$level" = "$HARDEN" ] || echo "    (asked for '$HARDEN'; this system refused it, so '$level' is in use)"
            return 0
        fi
        echo "!!  queuerd did not start with hardening '$level'; last log lines:" >&2
        journalctl --user -u queuerd -n 8 --no-pager >&2 || true
    done
    echo "queuerd failed to start even without hardening; see: journalctl --user -u queuerd" >&2
    return 1
}

# ---------------------------------------------------------------------------
# 6. Enable + start, enable linger
# ---------------------------------------------------------------------------

echo "==> Enabling and starting queuerd"
start_service

echo "==> Enabling linger for $USER (so queuerd survives logout)"
loginctl enable-linger "$USER"

echo "==> Verifying exactly one queuerd process is running"
pgrep -a -x queuerd

echo "==> Status:"
systemctl --user status queuerd --no-pager

echo "==> Done. 'queuer' / 'qur' commands work from any directory now -- no uv run needed."
echo "    (open a new terminal if 'qur' isn't found yet -- uv tool update-shell just ran)"
