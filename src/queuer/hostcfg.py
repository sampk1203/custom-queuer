"""Host configuration for remote workers (QUEUER_REMOTE_PLAN.md, phase 2.1).

`~/.config/queuer/hosts.json` (0600) lists the workers this machine can drive over ssh:

    {"version": 1, "hosts": {"vm3": {"ssh": "vm3", "id": "vm3", "jobs_dir": "~/qur-jobs",
      "setup": "source ~/venvs/md/bin/activate", "pull_dir": "~/qur-results/vm3",
      "pull_exclude": ["*.lammpstrj"], "poll_secs": 15, "ssh_options": ["-i", "~/.ssh/qur_vm3"],
      "enabled": true}}}

JSON, not TOML (Python 3.10 floor). Mirrors `notify.load/save/check_permissions`: a file that
cannot be used is reported once on stderr and treated as "no hosts"; unknown keys are kept.
qur never creates, copies or reads SSH keys; `ssh_options` is only passed to `ssh`.
"""

from __future__ import annotations

import json
import os
import re
import stat
import tempfile
from pathlib import Path
from typing import Any

from queuer import notify

VERSION = 1
NAME_RE = notify.ID_RE  # host names and ids: ^[A-Za-z0-9_-]{1,16}$
DEFAULT_JOBS_DIR = "~/qur-jobs"
DEFAULT_POLL_SECS = 15
MIN_POLL_SECS = 5
MAX_POLL_SECS = 3600
KNOWN_KEYS = (
    "ssh", "id", "jobs_dir", "setup", "pull_dir", "pull_exclude", "poll_secs", "ssh_options", "enabled",
)


def default_path() -> Path:
    override = os.environ.get("QUEUER_HOSTS_PATH")
    if override:
        return Path(override)
    base = os.environ.get("XDG_CONFIG_HOME") or str(Path.home() / ".config")
    return Path(base) / "queuer" / "hosts.json"


def check_name(name: Any) -> str:
    if not isinstance(name, str) or not NAME_RE.match(name):
        raise ValueError(f"host name {name!r} must be 1-16 letters, digits, '_' or '-'")
    return name


def _str_list(value: Any, what: str) -> list[str]:
    if not isinstance(value, list) or not all(isinstance(x, str) and "\0" not in x for x in value):
        raise ValueError(f"{what} must be a list of strings")
    return list(value)


def check_host(name: str, raw: Any) -> dict[str, Any]:
    """One host entry in canonical form (defaults filled in; unknown keys kept). Raises ValueError."""
    check_name(name)
    if not isinstance(raw, dict):
        raise ValueError(f"host {name}: must be an object")
    entry: dict[str, Any] = {k: v for k, v in raw.items() if k not in KNOWN_KEYS}
    target = raw.get("ssh")
    if not isinstance(target, str) or not target.strip() or target.startswith("-") or re.search(r"[\s\0]", target):
        raise ValueError(f"host {name}: 'ssh' must be a [user@]host or an ssh alias (no spaces, not starting with '-')")
    entry["ssh"] = target
    host_id = raw.get("id", name)
    if not isinstance(host_id, str) or not notify.ID_RE.match(host_id):
        raise ValueError(f"host {name}: 'id' must be 1-16 letters, digits, '_' or '-'")
    entry["id"] = host_id
    jobs_dir = raw.get("jobs_dir") or DEFAULT_JOBS_DIR
    if not isinstance(jobs_dir, str) or "\0" in jobs_dir:
        raise ValueError(f"host {name}: 'jobs_dir' must be a path")
    entry["jobs_dir"] = jobs_dir
    setup = raw.get("setup")
    if setup is not None and (not isinstance(setup, str) or "\0" in setup):
        raise ValueError(f"host {name}: 'setup' must be a shell command string")
    entry["setup"] = setup or None
    pull_dir = raw.get("pull_dir") or f"~/qur-results/{host_id}"
    if not isinstance(pull_dir, str) or "\0" in pull_dir:
        raise ValueError(f"host {name}: 'pull_dir' must be a path")
    entry["pull_dir"] = pull_dir
    entry["pull_exclude"] = _str_list(raw.get("pull_exclude", []), f"host {name}: 'pull_exclude'")
    poll = raw.get("poll_secs", DEFAULT_POLL_SECS)
    if type(poll) is not int or not MIN_POLL_SECS <= poll <= MAX_POLL_SECS:
        raise ValueError(f"host {name}: 'poll_secs' must be a whole number between {MIN_POLL_SECS} and {MAX_POLL_SECS}")
    entry["poll_secs"] = poll
    entry["ssh_options"] = _str_list(raw.get("ssh_options", []), f"host {name}: 'ssh_options'")
    enabled = raw.get("enabled", True)
    if type(enabled) is not bool:
        raise ValueError(f"host {name}: 'enabled' must be true or false")
    entry["enabled"] = enabled
    return entry


def normalize(raw: Any) -> dict[str, Any] | None:
    """Canonical config from parsed JSON, or None when the file is unusable. A single bad host
    makes the whole file unusable (a half-read host list would be worse than none)."""
    if not isinstance(raw, dict) or not isinstance(raw.get("hosts", {}), dict):
        return None
    if raw.get("version", VERSION) != VERSION:
        return None
    try:
        hosts = {name: check_host(name, entry) for name, entry in raw.get("hosts", {}).items()}
    except ValueError as e:
        notify.warn_once(f"{default_path()}: {e}")
        return None
    ids = [h["id"] for h in hosts.values()]
    if len(set(ids)) != len(ids):
        notify.warn_once(f"{default_path()}: two hosts share the same id")
        return None
    return {"version": VERSION, "hosts": hosts}


def load(path: Path | None = None, *, check: bool = True) -> dict[str, Any]:
    """The saved config; an empty one (`{"version": 1, "hosts": {}}`) when the file is missing
    or unusable (reported once on stderr, never its content). The file names ssh options (ProxyCommand)
    and `setup` shell snippets that get executed, so a file that others can write (or that is a symlink,
    or not ours) is refused like a missing one. `check=False` is for `qur hosts list/add/edit/remove`,
    which must still be able to show and repair it."""
    path = path or default_path()
    empty: dict[str, Any] = {"version": VERSION, "hosts": {}}
    if check:
        problem = check_permissions(path)
        if problem:
            notify.warn_once(f"refusing to use hosts.json: {problem}; fix it with `chmod 600 {path}`")
            return empty
    try:
        raw = json.loads(path.read_text())
    except FileNotFoundError:
        return empty
    except (OSError, ValueError) as e:
        kind = "ValueError" if isinstance(e, ValueError) else type(e).__name__
        notify.warn_once(f"cannot use {path}: {kind}; treating it as no remote hosts")
        return empty
    cfg = normalize(raw)
    if cfg is None:
        notify.warn_once(f"{path} is invalid; treating it as no remote hosts")
        return empty
    return cfg


def save(cfg: dict[str, Any], path: Path | None = None) -> None:
    """Atomic write, mode 0600 from the first byte, folder 0700 when we create it."""
    norm = normalize(cfg)
    if norm is None:
        raise ValueError("invalid hosts config")
    path = path or default_path()
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".hosts-")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(norm, f, indent=2)
            f.write("\n")
        os.chmod(tmp, 0o600)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def check_permissions(path: Path | None = None, fix: bool = False) -> str | None:
    """Problem with the file's permissions as a sentence, or None. It names ssh options and
    shell snippets, so it must be ours, a regular file, and closed to group/other."""
    path = path or default_path()
    try:
        st = os.lstat(path)
    except OSError:
        return None
    if stat.S_ISLNK(st.st_mode):
        return f"{path} is a symlink"
    if st.st_uid != os.getuid():
        return f"{path} is owned by another user"
    if st.st_mode & 0o077:
        if fix:
            os.chmod(path, 0o600)
            return f"{path} was mode {st.st_mode & 0o777:o}; tightened to 600"
        return f"{path} is mode {st.st_mode & 0o777:o} (other users could change which machines qur logs in to)"
    return None


def get(name: str, path: Path | None = None) -> dict[str, Any] | None:
    """The canonical entry of host `name` (with its "name" added), or None."""
    entry = load(path)["hosts"].get(name)
    return None if entry is None else {**entry, "name": name}


def names(path: Path | None = None) -> list[str]:
    return sorted(load(path)["hosts"])
