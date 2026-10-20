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
ISOLATED_QUEUER_VARS = ("QUEUER_NOTIFY_PATH", "QUEUER_DB_PATH", "QUEUER_LOG_DIR", "QUEUER_SOCKET_PATH", "QUEUER_HOSTS_PATH")


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


@pytest.fixture(autouse=True)
def _isolated_hosts(tmp_path, monkeypatch):
    """Never read or write the developer's real hosts.json: a worker listed there would turn the
    Telegram Status / List keys into a machine picker and make `qur logs` read the file."""
    monkeypatch.setenv("QUEUER_HOSTS_PATH", str(tmp_path / "iso-hosts" / "hosts.json"))


# ---------------------------------------------------------------------------
# fake_ssh (QUEUER_REMOTE_PLAN.md, phase 2.5)
# ---------------------------------------------------------------------------

_FAKE_SSH_SHIM = r'''#!/usr/bin/env bash
# Test double for ssh: `ssh [options] -- HOST COMMAND`. Options are ignored (except -t / -o / -i / -p
# which are skipped with their value). COMMAND runs with bash -c under the worker's own state dirs.
# Per-worker switches (files in workers/HOST/, see FakeSsh.set_flag) imitate what a real login shell does:
#   banner    its text is printed to stdout before a `bash -lc ...` command runs (a noisy ~/.profile)
#   noenv     the QUEUER_* variables are NOT passed (a non-login ssh session does not read ~/.profile
#             or the systemd override that set them); only HOME is set, so the defaults apply
#   fishlike  a command starting with `VAR=value cmd` is rejected, as fish / csh do
root="$FAKE_SSH_ROOT"
args=("$@")
i=0
while [ $i -lt ${#args[@]} ]; do
  a="${args[$i]}"
  case "$a" in
    --) i=$((i+1)); break ;;
    -o|-i|-p|-l|-F) i=$((i+2)) ;;
    *) i=$((i+1)) ;;
  esac
done
host="${args[$i]}"; cmd="${args[$((i+1))]}"
printf '%s\n' "$host :: $cmd" >> "$root/ssh.log"
if [ ! -d "$root/workers/$host" ]; then
  echo "ssh: Could not resolve hostname $host: Name or service not known" >&2; exit 255
fi
mode=$(cat "$root/workers/$host/mode" 2>/dev/null || echo ok)
case "$mode" in
  unreachable) echo "ssh: connect to host $host port 22: Connection refused" >&2; exit 255 ;;
  denied) echo "$host: Permission denied (publickey)." >&2; exit 255 ;;
  hostkey) echo "Host key verification failed." >&2; exit 255 ;;
esac
wd="$root/workers/$host"
if [ -e "$wd/fishlike" ] && [[ "$cmd" =~ ^[A-Za-z_][A-Za-z0-9_]*= ]]; then
  echo "fish: Unknown command: ${cmd%%=*}" >&2; exit 127
fi
if [ -e "$wd/noenv" ]; then
  for v in $(compgen -e | grep '^QUEUER_' || true); do unset "$v"; done
  export HOME="$wd/home"
else
  set -a
  . "$wd/env"
  set +a
fi
unset QUR_HOST
export PATH="$root/bin:$PATH"
case "$cmd" in
  *"bash -lc"*) [ -f "$wd/banner" ] && cat "$wd/banner" ;;
esac
exec bash -c "$cmd"
'''


class FakeSsh:
    """A fake `ssh` first on PATH. Each worker gets its own queuer state (db, logs, socket, jobs
    dir, notify config), so a host and several workers live in one test without a network."""

    def __init__(self, root, python: str) -> None:
        self.root = root
        self._daemons: list = []
        (root / "bin").mkdir(parents=True)
        (root / "workers").mkdir()
        ssh = root / "bin" / "ssh"
        ssh.write_text(_FAKE_SSH_SHIM)
        ssh.chmod(0o755)
        qur = root / "bin" / "qur"  # the worker's `qur`: this checkout, not whatever is installed
        qur.write_text(f'#!/usr/bin/env bash\nexec "{python}" -m queuer.cli "$@"\n')
        qur.chmod(0o755)

    def add_worker(self, name: str) -> dict:
        d = self.root / "workers" / name
        state = d / "state"
        paths = {
            "QUEUER_DB_PATH": state / "queuer.db",
            "QUEUER_LOG_DIR": state / "logs",
            "QUEUER_SOCKET_PATH": d / "run" / "queuer.sock",
            "QUEUER_NOTIFY_PATH": d / "config" / "notify.json",
            "QUEUER_HOSTS_PATH": d / "config" / "hosts.json",
            "QUEUER_JOBS_DIR": d / "jobs",
            "HOME": d / "home",  # so $HOME/.local/bin cannot shadow the fake worker's qur
        }
        d.mkdir(parents=True)
        (d / "home").mkdir()
        (d / "env").write_text("".join(f"{k}='{v}'\n" for k, v in paths.items()))
        return {k: v for k, v in paths.items()}

    def start_daemon(self, name: str) -> None:
        """A real queuerd for this worker (own socket and database); stopped when the test ends."""
        import subprocess
        import sys
        import time

        d = self.root / "workers" / name
        env = dict(os.environ)
        for line in (d / "env").read_text().splitlines():
            k, _, v = line.partition("=")
            env[k] = v.strip("'")
        env.pop("QUR_HOST", None)
        proc = subprocess.Popen([sys.executable, "-m", "queuer.daemon"], env=env,
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self._daemons.append(proc)
        sock = d / "run" / "queuer.sock"
        for _ in range(100):
            if sock.exists():
                return
            time.sleep(0.1)
        raise RuntimeError(f"worker daemon {name} did not start")

    def stop_daemons(self) -> None:
        for proc in self._daemons:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except Exception:
                proc.kill()

    def set_mode(self, name: str, mode: str) -> None:
        """'ok' (default), 'unreachable', 'denied' or 'hostkey' (an unknown host key)."""
        (self.root / "workers" / name / "mode").write_text(mode)

    def set_flag(self, name: str, flag: str, text: str | None = "") -> None:
        """Switch a login-shell behaviour on ('banner' takes the text it prints; 'noenv', 'fishlike'
        take none) or, with text=None, off again. See the comment at the top of the shim."""
        path = self.root / "workers" / name / flag
        if text is None:
            path.unlink(missing_ok=True)
        else:
            path.write_text(text)

    @property
    def calls(self) -> list[str]:
        log = self.root / "ssh.log"
        return log.read_text().splitlines() if log.exists() else []


@pytest.fixture
def fake_ssh(tmp_path, monkeypatch):
    import sys

    fake = FakeSsh(tmp_path / "fake-ssh", sys.executable)
    monkeypatch.setenv("FAKE_SSH_ROOT", str(fake.root))
    monkeypatch.setenv("PATH", f"{fake.root / 'bin'}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))  # ssh control sockets
    monkeypatch.setenv("QUEUER_HOSTS_PATH", str(tmp_path / "hosts-config" / "hosts.json"))
    yield fake
    fake.stop_daemons()
