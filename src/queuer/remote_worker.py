"""Worker side of remote queuing (QUEUER_REMOTE_PLAN.md, phase 1).

`qur remote ...` commands a host runs over ssh: info, events, find, receive, pack, prepare,
update, cat, gc. Hidden from `--help`, documented in the README "Remote workers" section.

A worker is a normal queuer; nothing here talks to a network. Every command takes fixed
arguments only (never a shell string), validates every path (relative, no `..`, never written
through a symlink, always inside the job folder / jobs dir) and exits with the shared codes:
0 ok, 2 not found, 3 too big, 4 bad arguments, 5 required path missing, 6 folder in use.
"""

from __future__ import annotations

import fcntl
import fnmatch
import json
import os
import re
import secrets
import shutil
import socket
import sqlite3
import sys
import tarfile
import time
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO, NoReturn, Optional

import typer

from queuer import client, db, notify, remote
from queuer.paths import _default_db_path, _default_socket_path

PROTOCOL = 1
EXIT_NOT_FOUND = 2
EXIT_TOO_BIG = 3
EXIT_BAD_ARGS = 4
EXIT_MISSING = 5
EXIT_IN_USE = 6

RUN_KEY_FILE = ".qur-run-key"
RECEIVED_FILE = ".qur-received"
PREV_DIR = ".qur-prev"
RESERVED_ROOT = (RUN_KEY_FILE, RECEIVED_FILE, PREV_DIR)
NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
CAT_DEFAULT_MB = 5.0
_SUFFIX_CHARS = "abcdefghijklmnopqrstuvwxyz0123456789"


class RemoteError(Exception):
    """A refused or failed remote command; `code` is the process exit code."""

    def __init__(self, message: str, code: int = EXIT_BAD_ARGS):
        super().__init__(message)
        self.code = code


def _die(e: RemoteError) -> NoReturn:
    typer.echo(f"Error: {e}", err=True)
    raise typer.Exit(code=e.code)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def jobs_dir(override: str | None = None) -> Path:
    """Where received folders live: `--jobs-dir`, else QUEUER_JOBS_DIR, else ~/qur-jobs (absolute)."""
    raw = override or os.environ.get("QUEUER_JOBS_DIR") or str(Path.home() / "qur-jobs")
    path = Path(os.path.expanduser(raw))
    if not path.is_absolute():
        raise RemoteError("the jobs dir must be an absolute path")
    return path


def _ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    return path


def _inside(child: Path | str, parent: Path | str) -> bool:
    """True when real path `child` is `parent` or below it (both resolved)."""
    c, p = os.path.realpath(child), os.path.realpath(parent)
    return c == p or c.startswith(p.rstrip(os.sep) + os.sep)


def check_rel(name: str, what: str = "path") -> PurePosixPath:
    """A safe relative path: not empty, not absolute, no `..` or empty parts, no NUL."""
    if not name or "\0" in name:
        raise RemoteError(f"bad {what}: {name!r}")
    p = PurePosixPath(name)
    if p.is_absolute() or any(part in ("..", "") for part in name.split("/") if part != ".") or name.startswith("/"):
        raise RemoteError(f"{what} must be relative and stay inside the folder: {name!r}")
    parts = [x for x in p.parts if x != "."]
    if not parts:
        raise RemoteError(f"bad {what}: {name!r}")
    return PurePosixPath(*parts)


def _no_symlink_parents(root: Path, rel: PurePosixPath) -> None:
    """Refuse when any existing directory on the way to `root/rel` (below root) is a symlink."""
    cur = root
    for part in rel.parts[:-1]:
        cur = cur / part
        if os.path.islink(cur):
            raise RemoteError(f"refusing to write through a symlink: {rel}")


def _is_received_folder(folder: Path) -> bool:
    """A real directory (not a symlink) that `remote receive` made: it carries the `.qur-received`
    marker as a plain file. This is the safety check for a job folder, in place of "inside the jobs
    dir", because an ssh session does not see the daemon's QUEUER_JOBS_DIR and a host may use any
    jobs dir (receive and gc get theirs explicitly)."""
    marker = folder / RECEIVED_FILE
    return (
        folder.is_dir()
        and not os.path.islink(folder)
        and marker.is_file()
        and not os.path.islink(marker)
    )


def _daemon_running() -> bool:
    try:
        client.send("status")
        return True
    except (client.DaemonNotRunning, client.DaemonError):
        return False


def _open_db() -> sqlite3.Connection:
    """The worker's database for a `qur remote ...` command. Every command here only reads it, and
    the host runs one every few seconds, so when the database is already at this qur's schema it is
    opened read-only: no write transaction per poll, and an ssh-launched qur never changes a
    database under a running queuerd. Only a missing database is created, and only an older one is
    migrated, and not while queuerd runs (an older daemon would meet a schema it does not know):
    exit 4 with the way out. A newer database is refused as everywhere else."""
    path = _default_db_path()
    version: int | None = None
    if path.exists():
        try:
            ro = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True, timeout=db.BUSY_TIMEOUT_MS / 1000)
            ro.row_factory = sqlite3.Row
            ro.execute(f"PRAGMA busy_timeout = {db.BUSY_TIMEOUT_MS}")
            version = db.get_schema_version(ro)
            if version == db.SCHEMA_VERSION:
                return ro
            ro.close()
        except sqlite3.Error:
            version = None  # cannot be read read-only here (no -shm access, empty file ...): the normal way below
    if version is not None and version > db.SCHEMA_VERSION:
        raise RemoteError(f"the worker's database is at schema version {version}, newer than this qur supports ({db.SCHEMA_VERSION}): upgrade qur on the worker")
    if version is not None and version < db.SCHEMA_VERSION and _daemon_running():
        raise RemoteError(
            f"the worker's database is at schema version {version} and this qur needs {db.SCHEMA_VERSION}, "
            "but queuerd is running with the older version: restart it (`qur daemon restart`) so that it migrates first"
        )
    conn = db.connect(path)
    try:
        db.init_db(conn)
    except RuntimeError as e:  # a newer schema found while migrating
        conn.close()
        raise RemoteError(str(e)) from None
    return conn


def _job_folder(conn: sqlite3.Connection, job_id: int) -> tuple[dict[str, Any], Path]:
    """The job and its folder: its own recorded `job_dir`, which must be a folder `remote receive` made."""
    job = db.get_job(conn, job_id)
    if job is None:
        raise RemoteError(f"no such job: {job_id}", EXIT_NOT_FOUND)
    if not job.get("job_dir"):
        raise RemoteError(f"job {job_id} has no job folder", EXIT_NOT_FOUND)
    folder = Path(job["job_dir"])
    if os.path.islink(folder) or not folder.is_dir():
        raise RemoteError(f"the folder of job {job_id} does not exist: {folder}", EXIT_NOT_FOUND)
    if not _is_received_folder(folder):
        raise RemoteError(f"the folder of job {job_id} was not made by `qur run` (no {RECEIVED_FILE} marker): {folder}")
    return job, folder


def _parse_age(text: str) -> float:
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([smhd]?)\s*", text or "")
    if not m:
        raise RemoteError(f"bad duration {text!r} (use e.g. 7d, 12h, 30m)")
    return float(m.group(1)) * {"": 86400, "s": 1, "m": 60, "h": 3600, "d": 86400}[m.group(2)]


def _mb(value: float | None, what: str = "--max-mb") -> int | None:
    if value is None:
        return None
    if value <= 0:
        raise RemoteError(f"{what} must be positive")
    return int(value * 1024 * 1024)


# ---------------------------------------------------------------------------
# safe extraction (3.10 has no tarfile `filter=`)
# ---------------------------------------------------------------------------


def safe_extract(
    stream: BinaryIO,
    dest: Path,
    *,
    max_bytes: int | None = None,
    backup_dir: Path | None = None,
    reserved: tuple[str, ...] = RESERVED_ROOT,
) -> dict[str, Any]:
    """Extract a tar.gz stream into the existing folder `dest`, member by member.

    Rejects (RemoteError, exit 4): absolute paths, `..`, names that touch the reserved markers (`reserved`, default RESERVED_ROOT;
    a pulled folder may carry `.qur-prev`, so `qur pull --extract` passes only the two marker files),
    device/fifo members, symlinks or hardlinks that point outside `dest`, and any write through a
    symlinked directory. A member whose size pushes the total past `max_bytes` stops with exit 3.
    Regular files are created with O_EXCL | O_NOFOLLOW. With `backup_dir` an existing file is moved
    there (same relative path) first; without it an existing file is an error. Nothing is deleted.
    Returns {"added": [...], "replaced": [...], "files": N, "bytes": N}.
    """
    added: list[str] = []
    replaced: list[str] = []
    unchanged: list[str] = []
    total = 0
    files = 0
    root = dest.resolve()
    try:
        tf = tarfile.open(fileobj=stream, mode="r|gz")  # noqa: SIM115 - closed in finally
    except (tarfile.TarError, OSError, EOFError) as e:
        raise RemoteError(f"not a tar.gz stream: {e}") from None
    try:
        for member in tf:
            name = member.name
            if name.rstrip("/") in ("", "."):
                continue  # the "./" root entry
            rel = check_rel(name.rstrip("/"), "member name")
            if rel.parts[0] in reserved:
                raise RemoteError(f"the archive may not contain {rel.parts[0]}")
            target = root.joinpath(*rel.parts)
            _no_symlink_parents(root, rel)
            if member.isdir():
                if os.path.islink(target):
                    raise RemoteError(f"refusing to write through a symlink: {rel}")
                target.mkdir(parents=True, exist_ok=True, mode=0o700)
                continue
            if not (member.isreg() or member.issym() or member.islnk()):
                raise RemoteError(f"refusing {name!r}: devices and fifos are not allowed")
            target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            if backup_dir is not None and member.isreg() and os.path.isfile(target) and not os.path.islink(target):
                # replacing a file: write the new content aside first, so an identical re-send (a resumed
                # `qur rerun`) changes nothing and leaves no new .qur-prev entry
                total += member.size
                if max_bytes is not None and total > max_bytes:
                    raise RemoteError("the archive is too big", EXIT_TOO_BIG)
                src_file = tf.extractfile(member)
                assert src_file is not None
                tmp = target.with_name(f".qur-tmp-{secrets.token_hex(4)}")
                fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), (member.mode & 0o777) | 0o600)
                try:
                    with os.fdopen(fd, "wb") as out:
                        shutil.copyfileobj(src_file, out)
                    if _same_content(target, tmp):
                        os.unlink(tmp)
                        unchanged.append(str(rel))
                        continue
                    saved = backup_dir.joinpath(*rel.parts)
                    saved.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                    os.replace(target, saved)
                    os.replace(tmp, target)
                except BaseException:
                    if os.path.lexists(tmp):
                        os.unlink(tmp)
                    raise
                replaced.append(str(rel))
                files += 1
                continue
            if os.path.lexists(target):
                if backup_dir is None or (os.path.isdir(target) and not os.path.islink(target)):
                    raise RemoteError(f"already exists, not overwriting: {rel}")
                saved = backup_dir.joinpath(*rel.parts)
                saved.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                os.replace(target, saved)
                replaced.append(str(rel))
            else:
                added.append(str(rel))
            if member.issym():
                link = member.linkname
                if os.path.isabs(link) or not _inside(os.path.join(target.parent, link), root):
                    raise RemoteError(f"symlink points outside the folder: {name} -> {link}")
                os.symlink(link, target)
            elif member.islnk():
                src_rel = check_rel(member.linkname, "hardlink target")
                src = root.joinpath(*src_rel.parts)
                if os.path.islink(src) or not src.is_file() or not _inside(src, root):
                    raise RemoteError(f"hardlink points outside the folder: {name} -> {member.linkname}")
                total += src.stat().st_size
                if max_bytes is not None and total > max_bytes:
                    raise RemoteError("the archive is too big", EXIT_TOO_BIG)
                shutil.copyfile(src, target)
                os.chmod(target, (src.stat().st_mode & 0o777) | 0o600)
                files += 1
            else:
                total += member.size
                if max_bytes is not None and total > max_bytes:
                    raise RemoteError("the archive is too big", EXIT_TOO_BIG)
                src_file = tf.extractfile(member)
                assert src_file is not None
                fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                             (member.mode & 0o777) | 0o600)
                with os.fdopen(fd, "wb") as out:
                    shutil.copyfileobj(src_file, out)
                files += 1
    except (tarfile.TarError, EOFError, OSError) as e:
        if isinstance(e, FileExistsError):
            raise RemoteError(f"already exists, not overwriting: {e.filename}") from None
        raise RemoteError(f"bad archive: {e}") from None
    finally:
        tf.close()
    return {"added": added, "replaced": replaced, "unchanged": unchanged, "files": files, "bytes": total}


def _same_content(a: Path, b: Path) -> bool:
    if a.stat().st_size != b.stat().st_size:
        return False
    with open(a, "rb") as fa, open(b, "rb") as fb:
        while True:
            ca, cb = fa.read(1 << 20), fb.read(1 << 20)
            if ca != cb:
                return False
            if not ca:
                return True


def _rmtree_checked(path: Path, parent: Path) -> None:
    """Delete a folder that must be a real directory directly inside `parent`."""
    if os.path.islink(path) or not path.is_dir() or Path(os.path.realpath(path)).parent != Path(os.path.realpath(parent)):
        raise RemoteError(f"refusing to delete {path}")
    shutil.rmtree(path)


# ---------------------------------------------------------------------------
# info / events / find
# ---------------------------------------------------------------------------


def _machine_id() -> str:
    cfg = notify.load()
    qur_id = cfg.get("qur_id") if isinstance(cfg, dict) else None
    if isinstance(qur_id, str) and notify.ID_RE.match(qur_id):
        return qur_id
    host = re.sub(r"[^A-Za-z0-9_-]", "-", socket.gethostname().split(".")[0])[:16]
    return host or "worker"


def _version() -> str:
    try:
        from importlib.metadata import version

        return version("queuer")
    except Exception:  # not installed as a package
        return "unknown"


def info(jobs_dir_override: str | None = None) -> dict[str, Any]:
    conn = _open_db()
    try:
        daemon = True
        try:
            client.send("status")
        except (client.DaemonNotRunning, client.DaemonError):
            daemon = False
        return {
            "protocol": PROTOCOL,
            "queuer_version": _version(),
            "schema_version": db.get_schema_version(conn),
            "machine_id": _machine_id(),
            "hostname": socket.gethostname(),
            "jobs_dir": str(jobs_dir(jobs_dir_override)),
            "db_path": str(_default_db_path()),  # what THIS ssh session resolved: `hosts check` compares it with a login shell's
            "socket_path": str(_default_socket_path()),
            "epoch": db.get_epoch(conn),
            "time": datetime.now().astimezone().isoformat(timespec="seconds"),
            "daemon": daemon,
        }
    finally:
        conn.close()


def find_run_key(run_key: str) -> dict[str, Any]:
    conn = _open_db()
    try:
        job = db.find_by_run_key(conn, run_key)
        return job or {}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# receive / pack
# ---------------------------------------------------------------------------


def _existing_run_folder(root: Path, run_key: str) -> Path | None:
    if not root.is_dir():
        return None
    for entry in sorted(root.iterdir()):
        marker = entry / RUN_KEY_FILE
        try:
            if entry.is_dir() and not entry.is_symlink() and marker.is_file() and marker.read_text().strip() == run_key:
                return entry
        except OSError:
            continue
    return None


def receive(
    name: str,
    stream: BinaryIO,
    *,
    jobs_dir_override: str | None = None,
    run_key: str | None = None,
    max_mb: float | None = None,
) -> dict[str, Any]:
    if not NAME_RE.match(name):
        raise RemoteError(f"bad --name {name!r}: letters, digits, '.', '_' and '-', at most 64")
    run_key = db.check_run_key(run_key) if run_key else None
    root = _ensure_dir(jobs_dir(jobs_dir_override))
    if not run_key:
        return _receive_locked(root, name, stream, None, max_mb)
    # one upload per run key at a time: a parallel resume must not wipe a folder that is still being filled.
    # flock is released by the kernel when the holder dies, so there is no stale lock to clean up.
    lock_dir = _default_db_path().parent / "locks"  # not in the jobs dir: nothing else may appear there
    lock_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd = os.open(lock_dir / f"recv-{run_key}.lock", os.O_RDWR | os.O_CREAT, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise RemoteError(f"another upload of run {run_key} is still in progress; try again when it ends") from None
        return _receive_locked(root, name, stream, run_key, max_mb)
    finally:
        os.close(fd)


def _receive_locked(root: Path, name: str, stream: BinaryIO, run_key: str | None, max_mb: float | None) -> dict[str, Any]:
    if run_key:
        prior = _existing_run_folder(root, run_key)
        if prior is not None:
            marker = prior / RECEIVED_FILE
            if marker.is_file():
                try:
                    done = json.loads(marker.read_text())
                    return {"path": str(prior), "files": int(done["files"]), "bytes": int(done["bytes"])}
                except (OSError, ValueError, KeyError):
                    pass  # unreadable marker: treat as half-received
            _rmtree_checked(prior, root)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    while True:
        folder = root / f"{name}-{stamp}-{''.join(secrets.choice(_SUFFIX_CHARS) for _ in range(4))}"
        try:
            folder.mkdir(mode=0o700)
            break
        except FileExistsError:
            continue
    try:
        if run_key:
            fd = os.open(folder / RUN_KEY_FILE, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "w") as f:
                f.write(run_key + "\n")
        result = safe_extract(stream, folder, max_bytes=_mb(max_mb))
        summary = {"files": result["files"], "bytes": result["bytes"]}
        fd = os.open(folder / RECEIVED_FILE, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as f:
            json.dump(summary, f)
    except BaseException:
        shutil.rmtree(folder, ignore_errors=True)  # a folder we just made: never keep half of a bad archive
        raise
    return {"path": str(folder), **summary}


def _skipped(
    rel: str, base: str, extra: tuple[str, ...], *, path: Path | None = None, include_sensitive: bool = False,
    found: list[str] | None = None,
) -> bool:
    """True when `rel` must not travel: a key or (without `include_sensitive`) a credential/repo file, see
    remote.secret_reason, or a path matching `extra`. Skipped secrets are appended to `found` when given."""
    if remote.secret_reason(rel, base, path, include_sensitive):
        if found is not None:
            found.append(rel)
        return True
    return any(fnmatch.fnmatch(base, pat) or fnmatch.fnmatch(rel, pat) for pat in extra)


def _pack_entries(root: Path, exclude: tuple[str, ...], include_sensitive: bool, skipped: list[str]) -> Iterator[tuple[Path, str, int]]:
    """(path, relative name, size) of everything `pack` ships, one at a time. Symlinks that leave the folder, devices
    and FIFOs are left out; symlinked directories are yielded (stored as links) but not walked into."""
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        here = Path(dirpath)
        rel_dir = here.relative_to(root).as_posix()
        keep_dirs = []
        for d in sorted(dirnames):
            rel = d if rel_dir == "." else f"{rel_dir}/{d}"
            if _skipped(rel, d, exclude, path=here / d, include_sensitive=include_sensitive, found=skipped):
                continue
            keep_dirs.append(d)
            link = here / d
            if link.is_symlink() and (os.path.isabs(os.readlink(link)) or not _inside(link.parent / os.readlink(link), root)):
                continue
            yield link, rel, 0
        dirnames[:] = [d for d in keep_dirs if not os.path.islink(here / d)]
        for f in sorted(filenames):
            rel = f if rel_dir == "." else f"{rel_dir}/{f}"
            if (rel_dir == "." and f in (RUN_KEY_FILE, RECEIVED_FILE)) or _skipped(
                rel, f, exclude, path=here / f, include_sensitive=include_sensitive, found=skipped
            ):
                continue
            path = here / f
            if path.is_symlink():
                if os.path.isabs(os.readlink(path)) or not _inside(path.parent / os.readlink(path), root):
                    continue
                yield path, rel, 0
            elif path.is_file():
                yield path, rel, path.stat().st_size
        # anything else (FIFO, device, socket) is not shipped


def pack(
    job_id: int, out: BinaryIO, *, exclude: tuple[str, ...] = (), max_mb: float | None = None,
    include_sensitive: bool = False,
) -> dict[str, Any]:
    """Write a tar.gz of the job's folder to `out`. Skips key files, credential/repo files (.env, .netrc, .aws,
    .git ... unless `include_sensitive`) and the queuer markers; symlinks that leave the folder are left out,
    nothing is followed. The result lists what was skipped as secrets under "skipped"."""
    conn = _open_db()
    try:
        _job, folder = _job_folder(conn, job_id)
    finally:
        conn.close()
    root = folder.resolve()
    limit = _mb(max_mb)
    skipped: list[str] = []
    total = 0
    if limit is not None:  # a first pass only adds up sizes, so a folder over the limit is refused before any output
        seen = 0
        for _path, _rel, size in _pack_entries(root, exclude, include_sensitive, []):
            seen += size
            if seen > limit:
                raise RemoteError("the folder is bigger than --max-mb (uncompressed size)", EXIT_TOO_BIG)
    count = 0
    try:  # entries are streamed from the walk: a folder with millions of files is never held in memory
        with tarfile.open(fileobj=out, mode="w|gz") as tf:
            for path, rel, size in _pack_entries(root, exclude, include_sensitive, skipped):
                tf.add(path, arcname=rel, recursive=False)
                total += size
                count += 1
    except (OSError, tarfile.TarError) as e:  # a file that grew, shrank or vanished while we read it
        raise RemoteError(f"the folder changed while it was being packed ({type(e).__name__}: {e}); try again when the job is idle") from None
    return {"entries": count, "bytes": total, "skipped": skipped}


# ---------------------------------------------------------------------------
# prepare / update / cat / gc
# ---------------------------------------------------------------------------


def _split_pair(spec: str, *, last: bool, what: str) -> tuple[str, str]:
    if "=" not in spec:
        raise RemoteError(f"{what} must look like A=B (got {spec!r})")
    a, b = spec.rsplit("=", 1) if last else spec.split("=", 1)
    if not a or not b:
        raise RemoteError(f"{what} must look like A=B (got {spec!r})")
    return a, b


def _resolve_source(conn: sqlite3.Connection, src: str) -> Path:
    """An absolute worker path, `~/...`, or `job:ID/rel/path` (inside that job's folder)."""
    if src.startswith("job:"):
        head, _, rel = src[4:].partition("/")
        if not head.isdigit() or not rel:
            raise RemoteError(f"bad source {src!r}: use job:ID/relative/path")
        _job, folder = _job_folder(conn, int(head))
        path = folder.joinpath(*check_rel(rel, "source path").parts)
        if not _inside(path, folder):
            raise RemoteError(f"source leaves the job folder: {src}")
        return path
    path = Path(os.path.expanduser(src))
    if not path.is_absolute():
        raise RemoteError(f"source must be an absolute path or job:ID/path (got {src!r})")
    return path


def _target_dir(directory: str) -> Path:
    d = Path(os.path.expanduser(directory))
    if not d.is_absolute() or os.path.islink(d) or not d.is_dir():
        raise RemoteError(f"--dir must be an existing absolute folder: {directory}", EXIT_NOT_FOUND)
    if not _is_received_folder(d):
        raise RemoteError(f"--dir was not made by `qur run` (no {RECEIVED_FILE} marker): {directory}")
    return d


def prepare(
    directory: str,
    require: tuple[str, ...] = (),
    link: tuple[str, ...] = (),
    copy: tuple[str, ...] = (),
    max_mb: float | None = None,
) -> tuple[dict[str, Any], int]:
    """Check required paths, create links, copy worker-side files. Returns (result, exit code)."""
    folder = _target_dir(directory)
    limit = _mb(max_mb)
    conn = _open_db()
    try:
        required: list[str] = []
        links: list[dict[str, str]] = []
        copied: list[dict[str, Any]] = []
        missing: list[str] = []
        for raw in require:
            p = Path(os.path.expanduser(raw))
            if not p.is_absolute():
                raise RemoteError(f"--require needs an absolute or ~/ path (got {raw!r})")
            (required if p.exists() and os.access(p, os.R_OK) else missing).append(str(p))
        for spec in link:
            name, target = _split_pair(spec, last=False, what="--link")
            rel = check_rel(name, "link name")
            tpath = Path(os.path.expanduser(target))
            if not tpath.is_absolute():
                raise RemoteError(f"link target must be absolute or ~/ (got {target!r})")
            if not tpath.exists():
                missing.append(str(tpath))
                continue
            _no_symlink_parents(folder, rel)
            dest = folder.joinpath(*rel.parts)
            if os.path.lexists(dest):
                if not (os.path.islink(dest) and os.readlink(dest) == str(tpath)):
                    raise RemoteError(f"not replacing an existing file: {rel}")
            else:
                dest.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                os.symlink(tpath, dest)
            links.append({"name": str(rel), "target": str(tpath)})
        spent = 0
        for spec in copy:
            src, dest_name = _split_pair(spec, last=True, what="--copy")
            rel = check_rel(dest_name, "copy destination")
            source = _resolve_source(conn, src)
            if not source.exists():
                missing.append(src)
                continue
            _no_symlink_parents(folder, rel)
            dest = folder.joinpath(*rel.parts)
            size = _tree_size(source)
            if os.path.lexists(dest):
                if not _same_copy(source, dest, size):
                    raise RemoteError(f"not overwriting an existing file: {rel}")
                copied.append({"src": src, "dest": str(rel), "bytes": size})  # earlier run already copied it
                continue
            spent += size
            if limit is not None and spent > limit:
                raise RemoteError("the files to copy are bigger than --max-mb", EXIT_TOO_BIG)
            dest.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            if source.is_dir():
                shutil.copytree(source, dest, symlinks=True)
            else:
                shutil.copy2(source, dest)
            copied.append({"src": src, "dest": str(rel), "bytes": size})
    finally:
        conn.close()
    result = {"required": required, "links": links, "copied": copied, "missing": missing}
    return result, EXIT_MISSING if missing else 0


def _same_copy(source: Path, dest: Path, size: int) -> bool:
    """True when `dest` looks like the earlier copy2/copytree of `source` (same kind, size, mtime)."""
    if os.path.islink(dest):
        return False
    if source.is_dir():
        return dest.is_dir() and _tree_size(dest) == size
    if not dest.is_file():
        return False
    a, b = source.stat(), dest.stat()
    return a.st_size == b.st_size and abs(a.st_mtime_ns - b.st_mtime_ns) < 2_000_000_000


def _tree_size(path: Path) -> int:
    if path.is_file():
        return path.stat().st_size
    total = 0
    for dirpath, _dirs, files in os.walk(path):
        for f in files:
            try:
                total += os.lstat(os.path.join(dirpath, f)).st_size
            except OSError:
                pass
    return total


def update(job_id: int, stream: BinaryIO, *, force: bool = False, max_mb: float | None = None) -> dict[str, Any]:
    conn = _open_db()
    try:
        _job, folder = _job_folder(conn, job_id)
        if not force:
            busy = conn.execute(
                "SELECT id FROM jobs WHERE job_dir = ? AND status IN ('queued','running','stopped') ORDER BY id",
                (_job["job_dir"],),
            ).fetchall()
            if busy:
                ids = ", ".join(str(r["id"]) for r in busy)
                raise RemoteError(f"the folder is in use by job(s) {ids} (use --force to update anyway)", EXIT_IN_USE)
    finally:
        conn.close()
    backup_rel = f"{PREV_DIR}/{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    result = safe_extract(stream, folder, max_bytes=_mb(max_mb), backup_dir=folder / backup_rel)
    return {
        "added": result["added"],
        "replaced": result["replaced"],
        "unchanged": result["unchanged"],
        "backup": backup_rel if result["replaced"] else None,
    }


def cat(job_id: int, rel: str, out: BinaryIO, *, max_mb: float = CAT_DEFAULT_MB) -> int:
    conn = _open_db()
    try:
        _job, folder = _job_folder(conn, job_id)
    finally:
        conn.close()
    r = check_rel(rel, "file")
    path = folder.joinpath(*r.parts)
    _no_symlink_parents(folder, r)
    if not _inside(path, folder) or not path.exists():
        raise RemoteError(f"no such file in the job folder: {rel}", EXIT_NOT_FOUND)
    if not path.is_file():
        raise RemoteError(f"not a regular file: {rel}")
    limit = _mb(max_mb) or 0
    if path.stat().st_size > limit:
        raise RemoteError(f"{rel} is bigger than {max_mb:g} MB", EXIT_TOO_BIG)
    with open(path, "rb") as f:
        shutil.copyfileobj(f, out)
    return path.stat().st_size


def gc(
    older_than: str = "7d", delete: bool = False, jobs_dir_override: str | None = None, force_completed: bool = False,
) -> dict[str, Any]:
    """Folders under the jobs dir that carry a run-key marker, are no job's folder and are older than the limit.
    Two kinds: "orphans" never finished receiving (no `.qur-received`): leftovers of an interrupted upload, the only
    ones `delete` removes; "completed" were received fine and just lost their job row (history pruning, `qur rm`):
    they may hold results nobody pulled yet, so they are listed but kept unless `force_completed`.
    Lists only unless `delete`; never touches a folder without the marker or with a job."""
    limit = _parse_age(older_than)
    root = jobs_dir(jobs_dir_override)
    conn = _open_db()
    try:
        used = {os.path.realpath(r["job_dir"]) for r in conn.execute("SELECT job_dir FROM jobs WHERE job_dir IS NOT NULL")}
    finally:
        conn.close()
    orphans: list[dict[str, Any]] = []
    completed: list[dict[str, Any]] = []
    deleted: list[str] = []
    if root.is_dir():
        now = time.time()
        for entry in sorted(root.iterdir()):
            marker = entry / RUN_KEY_FILE
            if entry.is_symlink() or not entry.is_dir() or not marker.is_file() or marker.is_symlink():
                continue
            if os.path.realpath(entry) in used:
                continue
            age = now - marker.stat().st_mtime
            if age < limit:
                continue
            finished = (entry / RECEIVED_FILE).is_file() and not (entry / RECEIVED_FILE).is_symlink()
            (completed if finished else orphans).append({"path": str(entry), "age_days": int(age // 86400)})
            if delete and (not finished or force_completed):
                _rmtree_checked(entry, root)
                deleted.append(str(entry))
    return {"orphans": orphans, "completed": completed, "deleted": deleted}


# ---------------------------------------------------------------------------
# the `qur remote` command group
# ---------------------------------------------------------------------------

remote_app = typer.Typer(
    help="Commands a host runs on a worker over ssh (see README, Remote workers). Not for everyday use.",
    no_args_is_help=True,
)


def _json_out(obj: Any) -> None:
    typer.echo(json.dumps(obj))


@remote_app.command("info")
def info_cmd(
    as_json: bool = typer.Option(True, "--json/--no-json", help="Print JSON (always)"),
    jobs_dir_opt: Optional[str] = typer.Option(None, "--jobs-dir"),
) -> None:
    """Protocol, versions, machine id, jobs dir and database epoch of this worker."""
    try:
        _json_out(info(jobs_dir_opt))
    except RemoteError as e:
        _die(e)


@remote_app.command("events")
def events_cmd(
    after: int = typer.Option(0, "--after", min=0, help="Only events with a higher seq"),
    limit: int = typer.Option(100, "--limit", min=1, max=1000),
    as_json: bool = typer.Option(True, "--json/--no-json"),
) -> None:
    """The event feed (final outcomes, paused channels) after SEQ."""
    conn = _open_db()
    try:
        _json_out(db.get_events(conn, after, limit))
    finally:
        conn.close()


@remote_app.command("find")
def find_cmd(
    run_key: str = typer.Option(..., "--run-key"),
    as_json: bool = typer.Option(True, "--json/--no-json"),
) -> None:
    """The job row with this run key, or {}."""
    try:
        _json_out(find_run_key(db.check_run_key(run_key) or ""))
    except ValueError as e:
        _die(RemoteError(str(e)))


@remote_app.command("receive")
def receive_cmd(
    name: str = typer.Option(..., "--name", help="Folder name prefix"),
    jobs_dir_opt: Optional[str] = typer.Option(None, "--jobs-dir"),
    run_key: Optional[str] = typer.Option(None, "--run-key", help="Make a repeat of this call safe"),
    max_mb: Optional[float] = typer.Option(None, "--max-mb"),
) -> None:
    """Read a tar.gz on stdin into a new folder under the jobs dir; print {path, files, bytes}."""
    try:
        _json_out(receive(name, sys.stdin.buffer, jobs_dir_override=jobs_dir_opt, run_key=run_key, max_mb=max_mb))
    except RemoteError as e:
        _die(e)
    except ValueError as e:
        _die(RemoteError(str(e)))


@remote_app.command("pack")
def pack_cmd(
    job_id: int = typer.Argument(..., metavar="ID"),
    exclude: Optional[list[str]] = typer.Option(None, "--exclude", help="Skip names/paths matching this pattern (repeatable)"),
    max_mb: Optional[float] = typer.Option(None, "--max-mb", help="Refuse a folder whose UNCOMPRESSED size is bigger than this"),
    include_sensitive: bool = typer.Option(False, "--include-sensitive", help="Also ship .env, .netrc, .aws, .git ... (keys are never shipped)"),
) -> None:
    """Write a tar.gz of the job's folder to stdout (exit 2 no folder, 3 too big, 4 bad arguments)."""
    try:
        result = pack(job_id, sys.stdout.buffer, exclude=tuple(exclude or ()), max_mb=max_mb, include_sensitive=include_sensitive)
        sys.stdout.buffer.flush()
        if result["skipped"]:
            shown = ", ".join(result["skipped"][:20]) + (" ..." if len(result["skipped"]) > 20 else "")
            typer.echo(f"skipped as secrets: {shown}", err=True)
    except RemoteError as e:
        _die(e)


@remote_app.command("prepare")
def prepare_cmd(
    directory: str = typer.Option(..., "--dir", help="The job folder (inside the jobs dir)"),
    require: Optional[list[str]] = typer.Option(None, "--require", help="Path that must exist (repeatable)"),
    link: Optional[list[str]] = typer.Option(None, "--link", help="NAME=PATH: symlink DIR/NAME -> PATH (repeatable)"),
    copy: Optional[list[str]] = typer.Option(None, "--copy", help="SRC=DEST: copy a worker file (SRC or job:ID/rel) to DIR/DEST"),
    max_mb: Optional[float] = typer.Option(None, "--max-mb"),
    as_json: bool = typer.Option(True, "--json/--no-json"),
) -> None:
    """Check required paths and set up links/copies of files already on the worker (exit 5 if any missing)."""
    try:
        result, code = prepare(directory, tuple(require or ()), tuple(link or ()), tuple(copy or ()), max_mb)
    except RemoteError as e:
        _die(e)
    _json_out(result)
    if code:
        raise typer.Exit(code=code)


@remote_app.command("update")
def update_cmd(
    job: int = typer.Option(..., "--job"),
    force: bool = typer.Option(False, "--force", help="Update even while a job uses the folder"),
    max_mb: Optional[float] = typer.Option(None, "--max-mb"),
) -> None:
    """Add or replace files in a job's folder from a tar.gz on stdin (replaced files go to .qur-prev/)."""
    try:
        _json_out(update(job, sys.stdin.buffer, force=force, max_mb=max_mb))
    except RemoteError as e:
        _die(e)


@remote_app.command("cat")
def cat_cmd(
    job: int = typer.Option(..., "--job"),
    rel: str = typer.Argument(..., metavar="REL"),
    max_mb: float = typer.Option(CAT_DEFAULT_MB, "--max-mb"),
) -> None:
    """Write one file of the job's folder to stdout."""
    try:
        cat(job, rel, sys.stdout.buffer, max_mb=max_mb)
        sys.stdout.buffer.flush()
    except RemoteError as e:
        _die(e)


@remote_app.command("gc")
def gc_cmd(
    older_than: str = typer.Option("7d", "--older-than", help="Age limit, e.g. 7d, 12h"),
    delete: bool = typer.Option(False, "--delete", help="Delete what is listed (default: list only)"),
    as_json: bool = typer.Option(True, "--json/--no-json"),
    jobs_dir_opt: Optional[str] = typer.Option(None, "--jobs-dir", help="Where job folders live (`qur -H HOST remote gc` passes the host's jobs_dir)"),
    force_completed: bool = typer.Option(False, "--force-completed", help="With --delete: also delete finished folders whose job row is gone (they may hold unpulled results)"),
) -> None:
    """List (or delete) folders no job uses any more: interrupted uploads ("orphans", deleted) and finished folders
    without a job row ("completed", kept unless --force-completed)."""
    try:
        _json_out(gc(older_than, delete, jobs_dir_opt, force_completed))
    except RemoteError as e:
        _die(e)
