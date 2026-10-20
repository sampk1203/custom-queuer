"""`qur doctor`: one pass over everything that has to be right for queuer to work and
to keep its secrets private (F32/U8).

Each check returns a `Check` with a status of OK, WARN or FAIL and, when something is
off, a one-line fix hint. `run_checks(fix=True)` also repairs permission problems
(loose modes on files and folders you own) and nothing else: it never starts services,
edits the unit, deletes files or touches the database contents. The installed systemd
unit is only READ and compared with the template install.sh writes (F36).

Everything that talks to the system goes through a few small wrappers (`_run`,
`_writable`, `_free_bytes`) so tests can replace them. All subprocess calls use a fixed
argv, never a shell.
"""

from __future__ import annotations

import os
import pwd
import re
import shutil
import sqlite3
import stat

# only used with fixed argv (loginctl / systemctl), never a shell
import subprocess  # nosec B404
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from queuer import client, db, notify, redact
from queuer.paths import (
    FAIL,
    OK,
    UNIT_NAME,
    WARN,
    _default_db_path,
    _default_log_dir,
    _default_socket_path,
    _fallback_socket_dir,
)

DISK_FAIL_BYTES = 100 * 1024 * 1024  # less than this free where the logs go: FAIL
DISK_WARN_BYTES = 1024 * 1024 * 1024  # less than this: WARN
SUBPROCESS_TIMEOUT_SECS = 10
LINGER_DIR = Path("/var/lib/systemd/linger")
MOUNTINFO = Path("/proc/self/mountinfo")

# 5.todo M8: SQLite WAL needs working shared memory and file locks. These filesystems are the
# network / cluster ones where that is not guaranteed (a home directory on a cluster is the usual case).
NETWORK_FS_TYPES = frozenset({"nfs", "nfs4", "cifs", "smb3", "smbfs", "afs", "9p", "ncpfs", "lustre", "gpfs", "beegfs", "ceph", "glusterfs", "orangefs", "panfs"})

# What install.sh writes into the unit (F36). Keep these in step with `write_unit` and
# `hardening_lines` there; tests/test_install_sh.py fails when they drift apart.
# OOMPolicy=continue (5.todo M3): one OOM-killed job must not make systemd stop the whole unit.
UNIT_SETTINGS = {"Type": "simple", "Restart": "on-failure", "RestartSec": "2", "TimeoutStopSec": "30", "OOMPolicy": "continue"}
HARDENING_SETTINGS = {
    "UMask": "0077",
    "NoNewPrivileges": "yes",
    "LockPersonality": "yes",
    "RestrictAddressFamilies": "AF_UNIX AF_INET AF_INET6 AF_NETLINK",
}
UNIT_REINSTALL_HINT = "./install.sh   (in the project folder; it stops the daemon, so wait for running jobs first)"


@dataclass
class Check:
    name: str
    status: str
    detail: str
    hint: str = ""
    fixed: bool = False

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


# ---------------------------------------------------------------------------
# wrappers around the outside world (replaced in tests)
# ---------------------------------------------------------------------------


def _run(argv: list[str]) -> tuple[int, str] | None:
    """Run a fixed argv, return (exit code, stripped stdout), or None if the program is
    missing or hangs."""
    try:
        # fixed argv built from constants in this module, no shell involved
        res = subprocess.run(argv, capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT_SECS, check=False)  # nosec B603
    except (OSError, subprocess.TimeoutExpired):
        return None
    return res.returncode, (res.stdout or "").strip()


def _read_mountinfo() -> str | None:
    """Text of /proc/self/mountinfo, or None when it cannot be read (not Linux, no /proc)."""
    try:
        return MOUNTINFO.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None


def _writable(path: Path) -> bool:
    return os.access(path, os.W_OK | os.X_OK)


def _free_bytes(path: Path) -> int:
    return shutil.disk_usage(path).free


def _user_name() -> str:
    try:
        return pwd.getpwuid(os.getuid()).pw_name
    except KeyError:
        return os.environ.get("USER", "")


def _nearest_existing(path: Path) -> Path:
    while not path.exists() and path != path.parent:
        path = path.parent
    return path


def _fmt_mode(mode: int) -> str:
    return f"{mode & 0o777:03o}"


# ---------------------------------------------------------------------------
# permission checks (the part --fix may repair)
# ---------------------------------------------------------------------------

_KIND_TEST = {"file": stat.S_ISREG, "dir": stat.S_ISDIR, "socket": stat.S_ISSOCK}
_KIND_WORD = {"file": "a regular file", "dir": "a folder", "socket": "a socket"}


def _check_node(
    name: str,
    path: Path,
    kind: str,
    want: int,
    fix: bool,
    chmod_ok: bool = True,
    missing: Check | None = None,
    loose_status: str = FAIL,
) -> Check | None:
    """Owner and mode of one path: must be what `kind` says, not a symlink, owned by you,
    and with no group/other access. `want` is the mode `--fix` sets (0600/0700). With
    `chmod_ok` False a loose mode is reported but never changed (a folder you pointed a
    QUEUER_* variable at may be $HOME). A symlink or a foreign owner is only reported.
    `missing` is returned when the path does not exist (None = nothing to report)."""
    try:
        st = os.lstat(path)
    except FileNotFoundError:
        return missing
    except OSError as e:
        return Check(name, FAIL, f"{path}: cannot stat ({e.strerror})")
    if stat.S_ISLNK(st.st_mode):
        return Check(name, FAIL, f"{path} is a symlink", f"replace it with a real {kind} you own (check where it points first)")
    if not _KIND_TEST[kind](st.st_mode):
        return Check(name, FAIL, f"{path} is not {_KIND_WORD[kind]}", f"move {path} away and let queuer recreate it")
    if st.st_uid != os.getuid():
        return Check(name, FAIL, f"{path} is owned by uid {st.st_uid}, not you", f"sudo chown {os.getuid()} {path}")
    if st.st_mode & 0o077 == 0:
        return Check(name, OK, f"{path} mode {_fmt_mode(st.st_mode)}")
    old = _fmt_mode(st.st_mode)
    if fix and chmod_ok:
        try:
            os.chmod(path, want)
        except OSError as e:
            return Check(name, FAIL, f"{path} mode {old}; could not change it ({e.strerror})", f"chmod {want:o} {path}")
        return Check(name, OK, f"{path} was mode {old}; set to {want:o}", fixed=True)
    cmd = f"chmod {want:o} {path}"
    hint = "qur doctor --fix" if chmod_ok else cmd
    return Check(name, loose_status, f"{path} mode {old} (other users have access)", hint)


def check_socket(fix: bool) -> list[Check]:
    path = _default_socket_path()
    out: list[Check] = []
    if path.parent == _fallback_socket_dir():  # the private /tmp/queuer-<uid> folder
        node = _check_node("socket dir", path.parent, "dir", 0o700, fix, loose_status=FAIL)
        if node is not None:
            out.append(node)
    gone = Check("socket", WARN, f"no socket at {path}", "the daemon is not running: qur daemon start")
    node = _check_node("socket", path, "socket", 0o600, fix, missing=gone)
    if node is not None:
        out.append(node)
    return out


def check_config(fix: bool) -> list[Check]:
    notify_path = notify.default_path()
    # a config folder you moved with QUEUER_NOTIFY_PATH may be shared: report, never chmod
    own_dir = "QUEUER_NOTIFY_PATH" not in os.environ
    out: list[Check] = []
    node = _check_node("config dir", notify_path.parent, "dir", 0o700, fix, chmod_ok=own_dir, missing=Check("config dir", OK, f"{notify_path.parent} does not exist (nothing configured)"), loose_status=WARN)
    if node is not None:
        out.append(node)
    node = _check_node("notify.json", notify_path, "file", 0o600, fix, missing=Check("notify.json", OK, "not configured (qur connect enables Telegram alerts)"))
    if node is not None:
        out.append(node)
    node = _check_node("redact.json", redact.default_path(), "file", 0o600, fix)  # optional file: silent if absent
    if node is not None:
        out.append(node)
    return out


def check_database(fix: bool) -> list[Check]:
    db_path = _default_db_path()
    own_dir = db_path.parent == db.DEFAULT_DB_PATH.parent  # same rule as db.connect
    out: list[Check] = []
    node = _check_node("db dir", db_path.parent, "dir", 0o700, fix, chmod_ok=own_dir, loose_status=WARN)
    if node is not None:
        out.append(node)
    node = _check_node("db file", db_path, "file", 0o600, fix, missing=Check("db file", WARN, f"no database at {db_path}", "the daemon creates it on first start"))
    if node is not None:
        out.append(node)
    side = [n for n in (_check_node(f"db{suffix}", Path(f"{db_path}{suffix}"), "file", 0o600, fix) for suffix in ("-wal", "-shm")) if n is not None]
    if side:  # one line for the WAL side files (they hold the same data as the database)
        shown = [c for c in side if c.status != OK or c.fixed] or side
        status = FAIL if any(c.status == FAIL for c in side) else OK
        hint = next((c.hint for c in side if c.hint), "")
        out.append(Check("db wal/shm", status, "; ".join(c.detail for c in shown), hint, fixed=status == OK and any(c.fixed for c in side)))
    return out


def check_logs(fix: bool) -> list[Check]:
    log_dir = _default_log_dir()
    own_dir = "QUEUER_LOG_DIR" not in os.environ
    out: list[Check] = []
    missing = Check("log dir", OK, f"{log_dir} does not exist yet (created with the first job)")
    node = _check_node("log dir", log_dir, "dir", 0o700, fix, chmod_ok=own_dir, missing=missing, loose_status=WARN)
    if node is not None:
        out.append(node)
    probe = _nearest_existing(log_dir)
    if _writable(probe):
        out.append(Check("log writable", OK, f"{probe} is writable"))
    else:
        out.append(Check("log writable", FAIL, f"{probe} is not writable by you; job output cannot be saved", f"chmod u+rwx {probe} (or fix its owner)"))
    try:
        free = _free_bytes(probe)
    except OSError as e:
        out.append(Check("disk space", WARN, f"cannot read free space of {probe} ({e.strerror})"))
        return out
    mb = free / (1024 * 1024)
    text = f"{mb / 1024:.1f} GB free where the logs go" if mb >= 1024 else f"{mb:.0f} MB free where the logs go"
    if free < DISK_FAIL_BYTES:
        out.append(Check("disk space", FAIL, text, "free some space; `qur clean-logs --days 7` deletes old job logs"))
    elif free < DISK_WARN_BYTES:
        out.append(Check("disk space", WARN, text, "`qur clean-logs --days 7` deletes old job logs"))
    else:
        out.append(Check("disk space", OK, text))
    return out


def _unescape_mount_field(field: str) -> str:
    """mountinfo writes space, tab, newline and backslash in a path as \\NNN (octal)."""
    return re.sub(r"\\([0-7]{3})", lambda m: chr(int(m.group(1), 8)), field)


def _mount_for(path: str, mountinfo: str) -> tuple[str, str] | None:
    """(mount point, filesystem type) of the mount that holds `path` (an absolute, resolved
    path): the longest mount point that is a prefix of it, the later line when mounts are
    stacked on the same point. None when no line parses."""
    best: tuple[str, str] | None = None
    for line in mountinfo.splitlines():
        parts = line.split()
        try:
            sep = parts.index("-", 6)  # fields 7+ are optional tags, then "-", fstype, source, options
            mount_point = _unescape_mount_field(parts[4])
            fs_type = parts[sep + 1]
        except (ValueError, IndexError):
            continue
        prefix = mount_point.rstrip("/")
        if path != mount_point and not path.startswith(prefix + "/"):
            continue
        if best is None or len(mount_point) >= len(best[0]):
            best = (mount_point, fs_type)
    return best


def _is_risky_fs(fs_type: str) -> bool:
    """Network / cluster filesystems and FUSE mounts (not `fuseblk`, a local block device)."""
    return fs_type in NETWORK_FS_TYPES or fs_type == "fuse" or fs_type.startswith("fuse.")


def check_db_filesystem() -> Check:
    """5.todo M8: the database runs in WAL mode, which is unsafe on NFS/CIFS and some FUSE
    mounts (cluster home directories). Read only: looks at /proc/self/mountinfo, opens nothing."""
    name = "db filesystem"
    db_dir = _default_db_path().parent
    where = _nearest_existing(db_dir).resolve()  # the folder may not exist yet; symlinks resolved
    text = _read_mountinfo()
    if text is None:
        return Check(name, WARN, f"cannot read {MOUNTINFO}, so the filesystem of {db_dir} is unknown", "run `findmnt -T <db folder>`; it should not be nfs, cifs or fuse")
    found = _mount_for(str(where), text)
    if found is None:
        return Check(name, WARN, f"cannot find the mount that holds {db_dir}", "run `findmnt -T <db folder>`; it should not be nfs, cifs or fuse")
    mount_point, fs_type = found
    if _is_risky_fs(fs_type):
        return Check(
            name,
            WARN,
            f"{db_dir} is on {fs_type} (mounted at {mount_point}); SQLite WAL needs working shared memory and file locks, which network and FUSE filesystems may not give",
            "set QUEUER_DB_PATH to a folder on a local disk, then restart the daemon (see the README, `doctor`)",
        )
    return Check(name, OK, f"{db_dir} is on {fs_type} (a local filesystem)")


# ---------------------------------------------------------------------------
# the rest
# ---------------------------------------------------------------------------


def check_daemon() -> Check:
    try:
        data = client.send("status")
    except client.DaemonNotRunning as e:
        return Check("daemon", FAIL, str(e), "qur daemon start   (then: qur daemon logs)")
    except client.DaemonError as e:
        return Check("daemon", FAIL, f"queuerd answered with an error: {e}", "qur daemon logs")
    running = 0
    channels = data.get("channels", {}) if isinstance(data, dict) else {}
    for chan in channels.values():
        if isinstance(chan, dict) and chan.get("running"):
            running += 1
    return Check("daemon", OK, f"answers on {_default_socket_path()}; {running} job(s) running")


def check_linger() -> Check:
    user = _user_name()
    hint = "loginctl enable-linger $USER   (without it the daemon stops when you log out)"
    if user and (LINGER_DIR / user).exists():
        return Check("linger", OK, f"enabled for {user}")
    res = _run(["loginctl", "show-user", str(os.getuid()), "-p", "Linger"])
    if res is None or res[0] != 0:
        return Check("linger", WARN, "cannot tell (loginctl unavailable, or you have no session)", hint)
    if res[1].strip() == "Linger=yes":
        return Check("linger", OK, f"enabled for {user or os.getuid()}")
    return Check("linger", WARN, "not enabled: the daemon stops when you log out", hint)


def _unit_path() -> Path:
    return Path.home() / ".config" / "systemd" / "user" / f"{UNIT_NAME}.service"


def _project_dir() -> Path | None:
    """The project folder this queuer runs from (editable install), or None."""
    root = Path(__file__).resolve().parents[2]
    return root if (root / "pyproject.toml").is_file() else None


def _service_values(text: str) -> dict[str, str]:
    """`Key=Value` lines of the [Service] section (comments and other sections skipped;
    a repeated key keeps its last value)."""
    values: dict[str, str] = {}
    in_service = False
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line[0] in "#;":
            continue
        if line.startswith("[") and line.endswith("]"):
            in_service = line == "[Service]"
            continue
        if in_service and "=" in line:
            key, _, value = line.partition("=")
            values[key.strip()] = value.strip()
    return values


def _exec_path(value: str) -> str:
    """The program of an ExecStart value (systemd's `-`, `@`, `+`, `!`, `:` prefixes dropped)."""
    words = value.lstrip("@-:+!").split()
    return words[0] if words else ""


def _check_unit_exec(value: str | None) -> Check:
    if not value:
        return Check("unit exec", FAIL, "the unit has no ExecStart line", UNIT_REINSTALL_HINT)
    exe = _exec_path(value)
    if not (os.path.isfile(exe) and os.access(exe, os.X_OK)):
        return Check("unit exec", FAIL, f"ExecStart points at {exe}, which is missing or not executable (project moved or deleted?)", UNIT_REINSTALL_HINT)
    project = _project_dir()
    if project is not None:
        exe_path = Path(exe)
        if exe_path.name != "queuerd" or exe_path.parent.resolve() != (project / ".venv" / "bin").resolve():
            expected = project / ".venv" / "bin" / "queuerd"
            return Check("unit exec", WARN, f"ExecStart is {exe}, but this queuer lives in {project} (expected {expected})", UNIT_REINSTALL_HINT)
    return Check("unit exec", OK, f"ExecStart {exe}")


def _check_unit_settings(values: dict[str, str]) -> Check:
    """Restart/stop settings must equal the template; the hardening lines are either all
    there (level basic), all absent (level none: QUEUER_HARDEN=none, or systemd refused
    basic and install.sh fell back) or something in between (a stale or hand-edited unit)."""
    problems = [f"{key} is {values[key]!r}, expected {want!r}" if key in values else f"{key} is missing" for key, want in UNIT_SETTINGS.items() if values.get(key) != want]
    present = [key for key in HARDENING_SETTINGS if key in values]
    wrong = [key for key in present if values[key] != HARDENING_SETTINGS[key]]
    if present and (len(present) < len(HARDENING_SETTINGS) or wrong):
        absent = [key for key in HARDENING_SETTINGS if key not in values]
        parts = [f"{key} is {values[key]!r}, expected {HARDENING_SETTINGS[key]!r}" for key in wrong]
        if absent:
            parts.append("missing " + ", ".join(absent))
        problems.append("hardening lines differ (" + "; ".join(parts) + ")")
        level = "partial"
    else:
        level = "basic" if present else "none"
    if problems:
        return Check("unit settings", WARN, "the installed unit differs from this queuer's template: " + "; ".join(problems), UNIT_REINSTALL_HINT)
    return Check("unit settings", OK, f"matches this queuer's template (hardening: {level})")


def check_unit_content(unit: Path) -> list[Check]:
    """Compare the installed unit with what install.sh would write now (F36). Read only:
    never edits the unit (`qur doctor --fix` does not touch it either)."""
    try:
        text = unit.read_text()
    except (OSError, UnicodeDecodeError) as e:
        return [Check("unit settings", WARN, f"cannot read {unit} ({type(e).__name__})")]
    values = _service_values(text)
    return [_check_unit_exec(values.get("ExecStart")), _check_unit_settings(values)]


def check_unit() -> list[Check]:
    unit = _unit_path()
    out: list[Check] = []
    if unit.is_file():
        out.append(Check("unit file", OK, str(unit)))
        out += check_unit_content(unit)
    else:
        out.append(Check("unit file", WARN, f"{unit} not found (daemon started by hand?)", "./install.sh   (in the project folder)"))
    res = _run(["systemctl", "--user", "is-active", UNIT_NAME])
    if res is None:
        out.append(Check("service", WARN, "systemctl not available; cannot check the service"))
    elif res[1] == "active":
        out.append(Check("service", OK, f"{UNIT_NAME} is active"))
    elif res[1]:
        out.append(Check("service", WARN, f"{UNIT_NAME} is {res[1]}", "qur daemon start   (or: qur daemon logs)"))
    else:
        out.append(Check("service", WARN, "cannot reach systemd --user (no user session bus?)"))
    return out


def check_telegram() -> list[Check]:
    """Only when configured: one getMe per bot. A network failure is a WARN (offline is
    not a queuer fault); a token Telegram rejects is a FAIL. The token never appears."""
    path = notify.default_path()
    cfg = notify.load(path)
    if cfg is None:
        if path.exists():
            return [Check("telegram", WARN, f"{path} is unreadable or malformed", "qur connect   (sets it up again)")]
        return [Check("telegram", OK, "not configured")]
    out: list[Check] = []
    for i, bot in enumerate(cfg["bots"], start=1):
        name = f"telegram {bot['username']}" if bot.get("username") else f"telegram bot {i}"
        try:
            username = notify.check_token(bot["token"])
        except notify.NotifyError as e:
            if e.code is None:
                out.append(Check(name, WARN, f"could not reach Telegram ({e}); offline?"))
            elif e.code in (401, 404):
                out.append(Check(name, FAIL, f"Telegram rejected the bot token ({e})", "revoked or mistyped: qur connect --add-bot, then qur connect --remove-bot --bot ID for the old one"))
            else:
                out.append(Check(name, WARN, f"Telegram answered with an error ({e})"))
        except (KeyError, TypeError):
            out.append(Check(name, WARN, "unexpected reply from Telegram"))
        else:
            out.append(Check(name, OK, f"token accepted (@{username})"))
    return out


def check_schema() -> Check:
    db_path = _default_db_path()
    if not db_path.is_file():
        return Check("schema", WARN, f"no database at {db_path}", "the daemon creates it on first start")
    try:
        # read-only: never creates or migrates anything
        conn = sqlite3.connect(f"{db_path.resolve().as_uri()}?mode=ro", uri=True, timeout=5)
        try:
            conn.row_factory = sqlite3.Row
            version = db.get_schema_version(conn)
        finally:
            conn.close()
    except sqlite3.Error as e:
        return Check("schema", FAIL, f"cannot read the database: {e}", "is another process holding it, or is the file damaged?")
    if version is None:
        return Check("schema", WARN, "old database without a schema version", "the daemon upgrades it on start: qur daemon restart")
    if version > db.SCHEMA_VERSION:
        return Check("schema", FAIL, f"database is at version {version}, this queuer knows {db.SCHEMA_VERSION}", "upgrade queuer (git pull, ./install.sh); do not use an old copy")
    if version < db.SCHEMA_VERSION:
        return Check("schema", WARN, f"database is at version {version}, this queuer expects {db.SCHEMA_VERSION}", "the daemon upgrades it on start: qur daemon restart")
    return Check("schema", OK, f"version {version} (current)")


HOSTS_TIMEOUT_SECS = 120  # `qur doctor` waits at most this long for ALL workers together (they are checked in parallel)


def _check_one_host(name: str, host: dict[str, Any]) -> list[Check]:
    """The checks for ONE worker (runs in its own thread, see `check_hosts`)."""
    from queuer import remote_cli

    hard = {"ssh reachable", "remote qur found", "protocol compatible", "jobs dir writable"}
    out: list[Check] = []
    steps = remote_cli.check_host(name, {**host, "name": name})
    for s in steps:
        status = OK if s["ok"] else (FAIL if s["step"] in hard else WARN)
        hint = "" if s["ok"] else f"qur hosts check {name}"
        out.append(Check(f"host {name}: {s['step']}", status, str(s["detail"]), hint))
    if not steps or not steps[0]["ok"]:
        return out  # nothing to ask the daemon about while ssh itself fails
    try:
        reply = client.send("remote_check", host=name)
    except (client.DaemonNotRunning, client.DaemonError) as e:
        out.append(Check(f"host {name}: daemon ssh", WARN, f"cannot ask queuerd: {e}", "qur daemon start"))
        return out
    ok = bool(reply.get("ok"))
    out.append(Check(
        f"host {name}: daemon ssh", OK if ok else FAIL, str(reply.get("detail", "")),
        "" if ok else "queuerd runs without a terminal or agent: the worker login must work without a passphrase or ssh-agent (qur hosts check --from-daemon)",
    ))
    return out


def check_hosts(timeout: float | None = None) -> list[Check]:
    """F21 Phase 7: one block per enabled worker in hosts.json: ssh login, remote qur, protocol,
    daemon, jobs dir, linger and `setup` (the `qur hosts check` steps), plus whether the DAEMON
    (not this shell) can log in (`remote_check`). Nothing is changed; no worker configured: no checks.
    A worker that cannot be reached is a FAIL (its jobs are not being watched), a soft step a WARN.
    The workers are checked in parallel (one thread each), so a few dead ones cost one timeout, not
    their sum; a worker still busy after `timeout` seconds (default HOSTS_TIMEOUT_SECS) is a FAIL."""
    from queuer import hostcfg

    problem = hostcfg.check_permissions()
    if problem:  # hosts.json names ssh options and shell snippets qur executes: it must not be writable by others
        return [Check("hosts.json permissions", FAIL, problem, f"chmod 600 {hostcfg.default_path()}")]
    hosts = {n: h for n, h in hostcfg.load()["hosts"].items() if h["enabled"]}
    results: dict[str, list[Check]] = {}

    def work(name: str, host: dict[str, Any]) -> None:
        try:
            results[name] = _check_one_host(name, host)
        except Exception as e:  # one broken worker must not hide the others
            results[name] = [Check(f"host {name}: checks", FAIL, f"the check itself failed: {type(e).__name__}: {e}", f"qur hosts check {name}")]

    threads = {name: threading.Thread(target=work, args=(name, host), daemon=True) for name, host in hosts.items()}
    for t in threads.values():
        t.start()
    deadline = time.monotonic() + (HOSTS_TIMEOUT_SECS if timeout is None else timeout)
    for t in threads.values():
        t.join(max(0.0, deadline - time.monotonic()))
    out: list[Check] = []
    for name in sorted(hosts):
        done = results.get(name)
        if done is None:
            done = [Check(f"host {name}: checks", FAIL, "no answer in time", f"qur hosts check {name}")]
        out += done
    return out


# ---------------------------------------------------------------------------
# run everything
# ---------------------------------------------------------------------------


def run_checks(fix: bool = False, hosts: bool = True) -> list[Check]:
    checks: list[Check] = [check_daemon()]
    checks += check_socket(fix)
    checks += check_config(fix)
    checks += check_database(fix)
    checks.append(check_schema())
    checks.append(check_db_filesystem())
    checks += check_logs(fix)
    checks.append(check_linger())
    checks += check_unit()
    checks += check_telegram()
    if hosts:
        checks += check_hosts()
    return checks


def exit_code(checks: list[Check]) -> int:
    """1 if any check FAILed, else 0 (warnings do not fail)."""
    return 1 if any(c.status == FAIL for c in checks) else 0


def summary(checks: list[Check]) -> dict[str, int]:
    return {s: sum(1 for c in checks if c.status == s) for s in (OK, WARN, FAIL)}
