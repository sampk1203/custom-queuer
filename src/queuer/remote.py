"""SSH transport for remote workers (QUEUER_REMOTE_PLAN.md, phase 2.2).

Everything that runs `ssh` lives here, and this is the only module that looks at ssh's stderr
text (to sort failures into the classes below). Fixed argv lists, never `shell=True`; every
word that reaches the remote shell goes through `shlex.quote`.

Keys (decision 12): qur does nothing with keys. The login uses whatever ssh would use (an
alias from ~/.ssh/config, or the host's `ssh_options`, e.g. `["-i", "~/.ssh/qur_vm3"]`);
agent forwarding is always off; nothing here creates, copies, reads or prints a key.
"""

from __future__ import annotations

import fnmatch
import json
import os
import re
import shlex
import subprocess  # nosec B404 - fixed argv lists only, never a shell
import tempfile
import threading
import time
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

PROTOCOL = 1
CONNECT_TIMEOUT_SECS = 10
DEFAULT_TIMEOUT_SECS = 60
REMOTE_PATH = 'PATH="$HOME/.local/bin:$PATH"'  # where `install.sh` puts qur; ssh sessions often lack it
MAX_OUTPUT_BYTES = 16 * 1024 * 1024  # most stdout one ssh call may return before it is cut off (a runaway worker)
MAX_STDERR_BYTES = 64 * 1024  # stderr is only ever shown as text: the rest is dropped
STALL_SECS = 300  # an upload or download that moves no byte for this long is a hung worker (see `_run_capped`)
# One denylist for every direction (send, pack, pull, rerun --replace). Two tiers:
#  NEVER_*     keys: never travel, no switch overrides it (decision 12)
#  SENSITIVE_* credentials and repo data: skipped unless the user passes --include-sensitive
NEVER_DIRS = (".ssh", ".gnupg")
NEVER_PATTERNS = ("*.pem", "*.key", "*.p12", "*.pfx")
KEY_FILE_NAMES = ("id_rsa", "id_ed25519", "id_ecdsa", "id_dsa", "id_ecdsa_sk", "id_ed25519_sk")  # and their .pub
SENSITIVE_DIRS = (".aws", ".git", ".kube")
SENSITIVE_PATTERNS = (".env", ".env.*", ".netrc", ".pgpass", ".npmrc", ".pypirc", "kubeconfig")


def _looks_like_key(path: Path) -> bool:
    """Content check for a file named `id_*` that is not one of the standard key names: only an actual key
    (a PEM/OpenSSH private key or an `ssh-...` public key line) counts, so `id_map.npy` or `id_table.csv` go through."""
    try:
        if path.is_symlink() or not path.is_file():
            return False
        with open(path, "rb") as f:
            head = f.read(512)
    except OSError:
        return False
    return b"PRIVATE KEY" in head or head.startswith((b"ssh-", b"ecdsa-", b"sk-ssh", b"sk-ecdsa"))


def secret_reason(rel: str, base: str, path: Path | None = None, include_sensitive: bool = False) -> str | None:
    """Why a file or folder must not travel: "key" (never), "sensitive" (only with --include-sensitive), or None.
    `rel` is the path inside the folder (any depth is checked), `base` its last part, `path` the real file
    (needed to look inside an `id_*` file)."""
    parts = rel.split("/")
    if any(p in NEVER_DIRS for p in parts) or any(fnmatch.fnmatch(base, pat) for pat in NEVER_PATTERNS):
        return "key"
    if base in KEY_FILE_NAMES or (base.endswith(".pub") and base[:-4] in KEY_FILE_NAMES):
        return "key"
    if fnmatch.fnmatch(base, "id_*") and path is not None and _looks_like_key(path):
        return "key"
    if not include_sensitive and (
        any(p in SENSITIVE_DIRS for p in parts) or any(fnmatch.fnmatch(base, pat) for pat in SENSITIVE_PATTERNS)
    ):
        return "sensitive"
    return None


class RemoteTransportError(Exception):
    """Base of every error this module raises."""


class HostUnreachable(RemoteTransportError):
    """ssh could not connect (network, DNS, refused, timeout) or the connection dropped."""


class StalledTransfer(Exception):
    """Internal: ssh moved no byte for STALL_SECS. Callers turn it into HostUnreachable."""


class AuthFailed(RemoteTransportError):
    """ssh connected but the login was refused (no passphrase-free key, host key changed...)."""


class RemoteQurMissing(RemoteTransportError):
    """The worker has no `qur` on its PATH (nor in ~/.local/bin)."""


class ProtocolMismatch(RemoteTransportError):
    """The worker speaks another wire protocol version (Appendix A)."""


class RemoteError(RemoteTransportError):
    """The remote command ran and failed: its exit code and stderr."""

    def __init__(self, exit_code: int, stderr: str):
        super().__init__(stderr.strip() or f"remote command exited {exit_code}")
        self.exit_code = exit_code
        self.stderr = stderr


# ---------------------------------------------------------------------------
# building the command
# ---------------------------------------------------------------------------


def control_dir() -> Path:
    """Short 0700 folder for the ssh connection-reuse sockets (unix socket paths are length-limited)."""
    base = os.environ.get("XDG_CACHE_HOME") or str(Path.home() / ".cache")
    path = Path(base) / "queuer" / "ssh"
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path, 0o700)
    return path


def ssh_options(host: dict[str, Any], *, interactive: bool = False) -> list[str]:
    """The ssh options every call uses, then the host's own `ssh_options`. `interactive` (a command
    typed in the user's terminal) leaves BatchMode off so ssh may ask for a passphrase."""
    opts = [
        "-o", f"ConnectTimeout={CONNECT_TIMEOUT_SECS}",
        "-o", "ServerAliveInterval=15",
        "-o", "ServerAliveCountMax=3",
        "-o", "ForwardAgent=no",
        "-o", "ForwardX11=no",
        "-o", "ControlMaster=auto",
        "-o", f"ControlPath={control_dir()}/%C",
        "-o", "ControlPersist=60",
    ]
    if not interactive:
        opts[:0] = ["-o", "BatchMode=yes"]
    opts += [os.path.expanduser(o) if o.startswith("~") else o for o in host.get("ssh_options", [])]
    return opts


def remote_command(argv: Sequence[str], *, setup: str | None = None, raw: bool = False) -> str:
    """The one string handed to the remote shell. By default `qur <argv>` with ~/.local/bin on the
    PATH; `raw` runs argv itself (preflight checks). Whatever the account's login shell is (fish and
    csh reject `VAR=x cmd`), a command that needs more than plain words always runs in `bash -c`;
    with `setup` that is `bash -c 'SETUP && qur ...'`, so `source venv/bin/activate` works too."""
    words = " ".join(shlex.quote(a) for a in argv)
    if raw and not setup:
        return words  # already plain words, every shell reads them the same way
    body = words if raw else f"{REMOTE_PATH} qur {words}".rstrip()
    return "bash -c " + shlex.quote(f"{setup} && {body}" if setup else body)


_CONTROL_RE = re.compile("[\x00-\x08\x0b-\x1f\x7f-\x9f\u202a-\u202e\u2066-\u2069]")
_LINE_BREAK_RE = re.compile("[\t\n\r\x0b\x0c\x85\u2028\u2029]+")
_CONTROL_LINE_RE = re.compile("[\x00-\x1f\x7f-\x9f\u202a-\u202e\u2066-\u2069]")


def safe_text(value: Any, *, multiline: bool = False) -> str:
    """Worker-supplied text made safe for this machine's terminal: C0/C1 control characters (ESC, so no
    escape sequence, OSC-8 link or title change), bidi overrides removed. A single-line value also loses
    newlines and tabs. Use it on everything that came from a worker before it is printed; for rich output
    follow it with `rich.markup.escape`."""
    text = value if isinstance(value, str) else str(value)
    if not multiline:
        text = _LINE_BREAK_RE.sub(" ", text)  # "error\ntrace" must not become "errortrace"
    return (_CONTROL_RE if multiline else _CONTROL_LINE_RE).sub("", text)


def parse_json(text: str) -> Any:
    """Parse the JSON a worker command printed. A login shell may print a banner or a profile's
    echo before it (`bash -lc`), so when the whole text is not JSON, parsing is retried from each
    line that starts with `{` or `[` (our commands print one JSON document, last, one line or
    indented) and finally from the last non-empty line. Raises ValueError when nothing parses."""
    try:
        return json.loads(text)
    except ValueError:
        lines = text.splitlines()
        for i, line in enumerate(lines):
            if line.lstrip()[:1] in ("{", "["):
                try:
                    return json.loads("\n".join(lines[i:]))
                except ValueError:
                    continue
        tail = [ln for ln in lines if ln.strip()]
        if not tail:
            raise
        return json.loads(tail[-1])


def ssh_argv(
    host: dict[str, Any],
    argv: Sequence[str],
    *,
    tty: bool = False,
    raw: bool = False,
    use_setup: bool = False,
    interactive: bool = False,
) -> list[str]:
    cmd = remote_command(argv, setup=host.get("setup") if use_setup else None, raw=raw)
    return ["ssh", *(["-t"] if tty else []), *ssh_options(host, interactive=interactive), "--", host["ssh"], cmd]


# ---------------------------------------------------------------------------
# errors
# ---------------------------------------------------------------------------

_AUTH_MARKERS = ("permission denied", "host key verification failed", "authentication failed",
                 "too many authentication failures", "no supported authentication")


def classify(returncode: int, stderr: str, *, raw: bool = False) -> None:
    """Raise the right error for an ssh-level failure (exit 255, or `qur` not found); return for
    anything else, which is the remote command's own result."""
    low = stderr.lower()
    if returncode == 255:
        if any(m in low for m in _AUTH_MARKERS):
            raise AuthFailed(stderr.strip() or "ssh login refused")
        raise HostUnreachable(stderr.strip() or "ssh could not connect")
    if returncode == 127 and not raw and "qur" in low and "not found" in low:
        raise RemoteQurMissing("qur is not installed on the worker (or not in ~/.local/bin)")


# ---------------------------------------------------------------------------
# running
# ---------------------------------------------------------------------------


def _text(data: bytes | None) -> str:
    return (data or b"").decode("utf-8", "replace")


def _run_capped(
    cmd: Sequence[str],
    *,
    input: bytes | None = None,  # noqa: A002 - same name as subprocess.run
    stdin: Any = None,
    stdout: Any = subprocess.PIPE,
    timeout: float | None = None,
    max_out: int = MAX_OUTPUT_BYTES,
    stall_secs: float | None = None,
) -> tuple[int, bytes, bytes]:
    """`subprocess.run(capture_output=True)` with a ceiling: stdout beyond `max_out` bytes kills the process and
    raises RemoteError (a hostile or runaway worker must not fill this machine's memory); stderr beyond
    MAX_STDERR_BYTES is read and dropped. Raises subprocess.TimeoutExpired like subprocess.run.
    `stall_secs`: kill the process and raise `StalledTransfer` when no byte moved for that long (output
    received, a file written to `stdout`, or a file given as `stdin` read by ssh). A `stdin` file that is
    fully read no longer counts: from then on the worker is working, and only `timeout` applies."""
    if input is not None:
        stdin = subprocess.PIPE
    elif stdin is None:
        stdin = subprocess.DEVNULL
    proc = subprocess.Popen(cmd, stdin=stdin, stdout=stdout, stderr=subprocess.PIPE)  # nosec B603 - fixed argv, no shell
    out, err = bytearray(), bytearray()
    over = threading.Event()

    def pump(stream: Any, buf: bytearray, cap: int, fatal: bool) -> None:
        try:
            while True:
                chunk = stream.read1(65536)  # whatever has arrived: read() would wait for 64 KB or EOF, hiding progress
                if not chunk:
                    return
                if len(buf) + len(chunk) > cap:
                    if fatal:
                        over.set()
                        proc.kill()
                        return
                    buf += chunk[: max(0, cap - len(buf))]
                else:
                    buf += chunk
        except (OSError, ValueError):
            return

    def feed() -> None:
        try:
            assert proc.stdin is not None and input is not None
            proc.stdin.write(input)
        except (OSError, ValueError):
            pass
        finally:
            try:
                assert proc.stdin is not None
                proc.stdin.close()
            except (OSError, ValueError):
                pass

    threads = [threading.Thread(target=pump, args=(proc.stderr, err, MAX_STDERR_BYTES, False), daemon=True)]
    if proc.stdout is not None:
        threads.append(threading.Thread(target=pump, args=(proc.stdout, out, max_out, True), daemon=True))
    if input is not None:
        threads.append(threading.Thread(target=feed, daemon=True))
    for t in threads:
        t.start()
    def progress() -> tuple[int, int, int]:
        """(bytes captured, size of the `stdout` file, offset in the `stdin` file); the offset is shared with ssh"""
        written = sent = 0
        try:
            if hasattr(stdout, "fileno"):
                written = os.fstat(stdout.fileno()).st_size
        except (OSError, ValueError):
            pass
        try:
            if hasattr(stdin, "fileno"):
                sent = os.lseek(stdin.fileno(), 0, os.SEEK_CUR)
        except (OSError, ValueError):
            pass
        return len(out) + len(err), written, sent

    def sending() -> bool:
        """True while a `stdin` file still has bytes ssh has not read"""
        try:
            return hasattr(stdin, "fileno") and os.lseek(stdin.fileno(), 0, os.SEEK_CUR) < os.fstat(stdin.fileno()).st_size
        except (OSError, ValueError):
            return False

    try:
        if stall_secs is None:
            proc.wait(timeout=timeout)
        else:
            started = last_move = time.monotonic()
            last = progress()
            while True:
                try:
                    proc.wait(timeout=min(1.0, stall_secs))
                    break
                except subprocess.TimeoutExpired:
                    pass
                now = time.monotonic()
                if timeout is not None and now - started > timeout:
                    raise subprocess.TimeoutExpired(list(cmd), timeout)
                cur = progress()
                if cur != last:
                    last, last_move = cur, now
                elif now - last_move > stall_secs and (sending() or stdin is None or not hasattr(stdin, "fileno")):
                    proc.kill()
                    proc.wait()
                    raise StalledTransfer(f"no data for {stall_secs:g}s")
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        raise
    for t in threads:
        t.join(timeout=5)
    if over.is_set():
        raise RemoteError(0, f"the worker sent more than {max_out // (1024 * 1024)} MB of output; stopped")
    return proc.returncode, bytes(out), bytes(err)


def ssh_run(
    host: dict[str, Any],
    argv: Sequence[str],
    *,
    stdin: bytes | None = None,
    timeout: float | None = DEFAULT_TIMEOUT_SECS,
    tty: bool = False,
    raw: bool = False,
    use_setup: bool = False,
) -> tuple[int, bytes, str]:
    """Run `qur <argv>` (or `argv` itself with raw=True) on the worker; return (exit code, stdout,
    stderr). ssh-level failures raise (HostUnreachable, AuthFailed, RemoteQurMissing); a remote
    command that merely exits non-zero is returned."""
    cmd = ssh_argv(host, argv, tty=tty, raw=raw, use_setup=use_setup)
    try:
        code, out, err = _run_capped(cmd, input=stdin, timeout=timeout)
    except subprocess.TimeoutExpired:
        raise HostUnreachable(f"no answer from {host['ssh']} within {timeout:g}s") from None
    except FileNotFoundError:
        raise HostUnreachable("the `ssh` command is not installed on this machine") from None
    stderr = _text(err)
    classify(code, stderr, raw=raw)
    return code, out, stderr


def ssh_json(
    host: dict[str, Any],
    argv: Sequence[str],
    *,
    stdin: bytes | None = None,
    timeout: float | None = DEFAULT_TIMEOUT_SECS,
    use_setup: bool = False,
) -> Any:
    """Run a `qur ... --json` command and return the parsed output. A non-zero exit raises
    RemoteError; an object whose `protocol` is not ours raises ProtocolMismatch."""
    code, out, err = ssh_run(host, argv, stdin=stdin, timeout=timeout, use_setup=use_setup)
    if code != 0:
        raise RemoteError(code, err)
    try:
        data = parse_json(_text(out))
    except ValueError:
        raise RemoteError(0, f"the worker did not answer with JSON: {_text(out)[:200]!r}") from None
    check_protocol(data)
    return data


def check_protocol(data: Any) -> None:
    if isinstance(data, dict) and "protocol" in data and data["protocol"] != PROTOCOL:
        raise ProtocolMismatch(
            f"the worker speaks protocol {data['protocol']}, this qur speaks {PROTOCOL}: upgrade the older one"
        )


def ssh_recv_to_file(
    host: dict[str, Any],
    argv: Sequence[str],
    dest: Path,
    *,
    timeout: float | None = None,
    use_setup: bool = False,
    validate: Callable[[Path], None] | None = None,
    stall_secs: float | None = STALL_SECS,
) -> int:
    """Run a remote stream command (`qur remote pack ID`) and write its stdout to `dest.part`
    (0600), renamed to `dest` only on success. Returns the byte count; a failure removes the
    partial file and raises (RemoteError carries the worker's exit code: 2 no folder, 3 too big).
    `validate(part)` runs on the finished download before the rename; whatever it raises leaves nothing behind."""
    dest.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    # a unique temp name (0600, O_EXCL): a manual and an automatic pull of one folder never share a .part file
    fd, part_name = tempfile.mkstemp(dir=dest.parent, prefix=dest.name + ".", suffix=".part")
    part = Path(part_name)
    cmd = ssh_argv(host, argv, use_setup=use_setup)
    try:
        with os.fdopen(fd, "wb") as f:
            code, _, err = _run_capped(cmd, stdout=f, timeout=timeout, stall_secs=stall_secs)
        stderr = _text(err)
        classify(code, stderr)
        if code != 0:
            raise RemoteError(code, stderr)
        if validate is not None:
            validate(part)
        size = part.stat().st_size
        os.replace(part, dest)
        return size
    except subprocess.TimeoutExpired:
        raise HostUnreachable(f"download from {host['ssh']} timed out") from None
    except StalledTransfer as e:
        raise HostUnreachable(f"download from {host['ssh']} stalled ({e}); the worker may be hung") from None
    except FileNotFoundError:
        raise HostUnreachable("the `ssh` command is not installed on this machine") from None
    finally:
        try:
            os.unlink(part)  # still there only when something above failed
        except OSError:
            pass


def ssh_exec(host: dict[str, Any], argv: Sequence[str], *, tty: bool = False) -> int:
    """Run `qur <argv>` on the worker with this terminal's stdin/stdout/stderr (the `qur -H` passthrough)
    and return its exit code. ssh may ask for a passphrase here: this is the user's own terminal."""
    cmd = ssh_argv(host, argv, tty=tty, interactive=True)
    try:
        return subprocess.run(cmd, check=False).returncode  # nosec B603 - fixed argv, no shell
    except FileNotFoundError:
        raise HostUnreachable("the `ssh` command is not installed on this machine") from None


def ssh_send_file(
    host: dict[str, Any],
    path: Path,
    argv: Sequence[str],
    *,
    timeout: float | None = None,
    use_setup: bool = False,
    stall_secs: float | None = STALL_SECS,
) -> tuple[int, bytes, str]:
    """Run `qur <argv>` on the worker with the file `path` as its stdin (a payload built by the
    caller, streamed, never held in memory). Returns (exit code, stdout, stderr)."""
    cmd = ssh_argv(host, argv, use_setup=use_setup)
    try:
        with open(path, "rb") as f:
            code, out, err = _run_capped(cmd, stdin=f, timeout=timeout, stall_secs=stall_secs)
    except subprocess.TimeoutExpired:
        raise HostUnreachable(f"sending to {host['ssh']} timed out") from None
    except StalledTransfer as e:
        raise HostUnreachable(f"sending to {host['ssh']} stalled ({e}); the worker may be hung") from None
    except FileNotFoundError:
        raise HostUnreachable("the `ssh` command is not installed on this machine") from None
    stderr = _text(err)
    classify(code, stderr)
    return code, out, stderr
