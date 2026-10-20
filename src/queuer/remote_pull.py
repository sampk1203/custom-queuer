"""Pull a worker's job folder back as a tarball (QUEUER_REMOTE_PLAN.md, phase 6).

- `qur pull HOST:ID` runs `qur remote pack ID` on the worker and streams the result to
  `<pull_dir>/<folder name>.tar.gz.part`; the file is checked (gzip CRC + a full `tarfile`
  listing) and only then renamed. An interrupted or corrupt transfer leaves nothing at the final
  name. An existing tarball is never replaced without `--force`.
- `qur pull HOST:ID --file REL` fetches one file (`qur remote cat`), 5 MB unless `--max-mb`.
- `--extract` unpacks next to the tarball with the same safe-extraction rules as `remote receive`.
- Defaults come from the host config: `pull_dir` and `pull_exclude` (`--exclude` adds to them,
  `--no-default-excludes` ignores them).
- The watcher (`remote_watch`) calls `pull_tarball` for runs recorded with `--pull`.

Nothing here touches keys: the worker's `pack` never ships `.ssh`, `id_*`, `*.pem`, `*.key`.
"""

from __future__ import annotations

import gzip
import os
import re
import shutil
import tarfile
import zlib
from pathlib import Path, PurePosixPath
from typing import Any, NamedTuple, NoReturn, Optional

import typer

from queuer import hostcfg, remote, remote_worker

EXTRACT_DEFAULT_MB = 10240  # `--extract` stops after this much unpacked data unless --max-mb says otherwise (gzip bombs)
FILE_DEFAULT_MB = remote_worker.CAT_DEFAULT_MB
TARBALL_SUFFIX = ".tar.gz"
REF_RE = re.compile(r"^([A-Za-z0-9_-]{1,16}):(\d+)$")


class PullError(Exception):
    """A pull that cannot be done or failed; the message is meant for the user."""


class PullResult(NamedTuple):
    path: Path
    size: int
    extracted: Path | None


def human_size(n: int) -> str:
    size = float(n)
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024 or unit == "GB":
            return f"{int(size)} B" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{n} B"  # unreachable


def effective_excludes(host: dict[str, Any], extra: list[str] | tuple[str, ...] = (), *, use_defaults: bool = True) -> list[str]:
    return [*(host.get("pull_exclude", []) if use_defaults else []), *extra]


def folder_name(job_dir: str | None, host_name: str, job_id: int) -> str:
    """Tarball stem: the job folder's own name when it is a plain one, else `<host>-<id>`."""
    name = PurePosixPath(job_dir).name if job_dir else ""
    return name if remote_worker.NAME_RE.match(name) else f"{host_name}-{job_id}"


def _explain(e: remote.RemoteError, host_name: str, job_id: int, *, one_file: bool = False) -> str:
    if e.exit_code == remote_worker.EXIT_NOT_FOUND:
        return f"job {job_id} does not exist on {host_name}, has no job folder, or the file is not in it ({str(e).splitlines()[0] if str(e) else 'not found'})"
    if e.exit_code == remote_worker.EXIT_TOO_BIG:
        return "too big: raise --max-mb" + ("" if one_file else " or leave big files out with --exclude")
    return remote.safe_text(str(e), multiline=True)


def lookup_job_dir(host: dict[str, Any], job_id: int) -> str | None:
    """The job's folder on the worker, from `qur show ID --json`."""
    data = remote.ssh_json(host, ["show", str(job_id), "--json"], timeout=30)
    if isinstance(data, list):
        data = data[0] if data else {}
    folder = data.get("job_dir") if isinstance(data, dict) else None
    return folder if isinstance(folder, str) and folder else None


def validate_tarball(path: Path) -> None:
    """Read the whole file once: gzip CRC to the end, every tar header and member readable."""
    try:
        with gzip.open(path, "rb") as gz:
            with tarfile.open(fileobj=gz, mode="r|") as tf:
                for _member in tf:
                    pass
            while gz.read(1 << 20):
                pass
    except (OSError, EOFError, tarfile.TarError, zlib.error) as e:
        raise PullError(f"the downloaded file is not a valid tar.gz ({type(e).__name__}: {e})") from None


def extract_tarball(tar_path: Path, max_mb: float | None = None) -> Path:
    """Unpack `X.tar.gz` into a new folder `X` next to it (safe rules; never into an existing folder)."""
    target = tar_path.with_name(tar_path.name[: -len(TARBALL_SUFFIX)] if tar_path.name.endswith(TARBALL_SUFFIX) else tar_path.name + ".d")
    try:
        target.mkdir(mode=0o700)
    except FileExistsError:
        raise PullError(f"{target} already exists; move it away or extract by hand") from None
    try:
        with open(tar_path, "rb") as f:
            remote_worker.safe_extract(
                f, target, reserved=(remote_worker.RUN_KEY_FILE, remote_worker.RECEIVED_FILE),
                max_bytes=int((max_mb or EXTRACT_DEFAULT_MB) * 1024 * 1024),
            )
    except remote_worker.RemoteError as e:
        shutil.rmtree(target, ignore_errors=True)  # a folder we just created
        raise PullError(f"cannot extract {tar_path.name}: {e}") from None
    except BaseException:
        shutil.rmtree(target, ignore_errors=True)
        raise
    return target


def pull_tarball(
    host: dict[str, Any],
    job_id: int,
    job_dir: str | None = None,
    *,
    to: str | Path | None = None,
    exclude: list[str] | tuple[str, ...] = (),
    max_mb: float | None = None,
    force: bool = False,
    unique: bool = False,
    extract: bool = False,
    include_sensitive: bool = False,
) -> PullResult:
    """Download the worker job's folder as `<dir>/<folder name>.tar.gz`. Blocking (ssh): call it from a thread.

    `job_dir` is looked up on the worker when not given. An existing tarball is an error unless
    `force`; with `unique` (auto-pull: a rerun reuses the folder, so the name repeats) the file is
    called `<folder name>-<job id>.tar.gz` instead of failing."""
    if job_dir is None:
        job_dir = lookup_job_dir(host, job_id)
    if not job_dir:
        raise PullError(f"job {job_id} on {host['name']} has no job folder (it was not started with `qur run`)")
    dest_dir = Path(os.path.expanduser(str(to or host["pull_dir"])))
    stem = folder_name(job_dir, host["name"], job_id)
    final = dest_dir / f"{stem}{TARBALL_SUFFIX}"
    if os.path.lexists(final) and not force:
        if not unique:
            raise PullError(f"{final} already exists (use --force to replace it)")
        final = dest_dir / f"{stem}-{job_id}{TARBALL_SUFFIX}"
        if os.path.lexists(final):
            raise PullError(f"{final} already exists")
    argv = ["remote", "pack", str(job_id)]
    for pattern in exclude:
        argv += ["--exclude", pattern]
    if max_mb is not None:
        argv += ["--max-mb", f"{max_mb:g}"]
    if include_sensitive:
        argv.append("--include-sensitive")
    try:
        size = remote.ssh_recv_to_file(host, argv, final, validate=validate_tarball)
    except remote.RemoteError as e:
        raise PullError(_explain(e, host["name"], job_id)) from None
    return PullResult(final, size, extract_tarball(final, max_mb) if extract else None)


def pull_file(
    host: dict[str, Any],
    job_id: int,
    rel: str,
    *,
    to: str | Path | None = None,
    max_mb: float = FILE_DEFAULT_MB,
    force: bool = False,
) -> tuple[Path, int]:
    """Download one file of the job's folder (no tarball). Default place: `<pull_dir>/<file name>`;
    `to` is a file path, or an existing folder to put the file in."""
    try:
        clean = remote_worker.check_rel(rel, "file")
    except remote_worker.RemoteError as e:
        raise PullError(str(e)) from None
    base = clean.name
    if to is None:
        dest = Path(os.path.expanduser(str(host["pull_dir"]))) / base
    else:
        dest = Path(os.path.expanduser(str(to)))
        if dest.is_dir():
            dest = dest / base
    if os.path.lexists(dest) and not force:
        raise PullError(f"{dest} already exists (use --force to replace it)")
    argv = ["remote", "cat", "--job", str(job_id), "--max-mb", f"{max_mb:g}", "--", str(clean)]
    try:
        size = remote.ssh_recv_to_file(host, argv, dest)
    except remote.RemoteError as e:
        raise PullError(_explain(e, host["name"], job_id, one_file=True)) from None
    return dest, size


def _fail(msg: str) -> NoReturn:
    typer.echo(f"Error: {msg}", err=True)
    raise typer.Exit(code=1)


def pull_cmd(
    ref: str = typer.Argument(..., metavar="HOST:ID", help="Worker host (from hosts.json) and job id, e.g. vm3:42"),
    to: Optional[str] = typer.Option(None, "--to", help="Folder for the tarball (default: the host's pull_dir); with --file, the file path"),
    exclude: Optional[list[str]] = typer.Option(None, "--exclude", help="Leave out names/paths matching this pattern (repeatable; adds to the host's pull_exclude)"),
    no_default_excludes: bool = typer.Option(False, "--no-default-excludes", help="Ignore the host's pull_exclude"),
    max_mb: Optional[float] = typer.Option(None, "--max-mb", help="Refuse a folder (or, with --file, a file) bigger than this; --file defaults to 5"),
    extract: bool = typer.Option(False, "--extract", help="Also unpack the tarball into a folder next to it"),
    file: Optional[str] = typer.Option(None, "--file", metavar="REL", help="Fetch one file of the job folder instead of the whole folder"),
    force: bool = typer.Option(False, "--force", help="Replace an existing tarball / file"),
    include_sensitive: bool = typer.Option(False, "--include-sensitive", help="Also fetch .env, .netrc, .aws, .git ... (keys never travel)"),
) -> None:
    """Fetch a worker job's folder as a .tar.gz (never auto-extracted unless --extract).

    \b
    qur pull vm3:42                          # -> ~/qur-results/vm3/<folder>.tar.gz
    qur pull vm3:42 --to ~/results --extract
    qur pull vm3:42 --exclude '*.lammpstrj' --max-mb 2000
    qur pull vm3:42 --file log.lammps --to .

    Key files (.ssh, private keys, *.pem, *.key, *.p12, *.pfx) are never included; .env, .netrc, .aws, .git and similar
    are left out unless --include-sensitive. --max-mb counts uncompressed bytes (and limits --extract). The download goes to a .part file
    that is checked before it gets its final name. Run it on the host: it is not forwarded by -H.
    """
    m = REF_RE.match(ref)
    if m is None:
        raise typer.BadParameter("expected HOST:ID, for example vm3:42", param_hint="HOST:ID")
    host = hostcfg.get(m.group(1))
    if host is None:
        raise typer.BadParameter(f"unknown host {m.group(1)!r}; configured: {', '.join(hostcfg.names()) or 'none'} (qur hosts add)", param_hint="HOST:ID")
    job_id = int(m.group(2))
    if file is not None and (extract or exclude or no_default_excludes or include_sensitive):
        raise typer.BadParameter("--file fetches one file; --extract, --exclude, --no-default-excludes and --include-sensitive only apply to the folder tarball")
    if max_mb is not None and max_mb <= 0:
        raise typer.BadParameter("--max-mb must be positive")
    if file is None:
        try:  # a running job writes into its folder: the tarball may be cut or inconsistent
            info = remote.ssh_json(host, ["show", str(job_id), "--json"], timeout=30)
            info = info[0] if isinstance(info, list) and info else info
            if isinstance(info, dict) and info.get("status") in ("running", "queued", "stopped"):
                typer.echo(f"warning: {host['name']}:{job_id} is {info['status']}; its folder may still change, so the tarball can be incomplete", err=True)
        except Exception:  # nosec B110 - only a courtesy: the pull itself reports real problems
            pass
    try:
        if file is not None:
            path, size = pull_file(host, job_id, file, to=to, max_mb=max_mb or FILE_DEFAULT_MB, force=force)
            typer.echo(f"pulled {host['name']}:{job_id}/{file} -> {path} ({human_size(size)})")
            return
        res = pull_tarball(
            host, job_id, to=to, exclude=effective_excludes(host, exclude or (), use_defaults=not no_default_excludes),
            max_mb=max_mb, force=force, extract=extract, include_sensitive=include_sensitive,
        )
    except PullError as e:
        _fail(str(e))
    except remote.RemoteTransportError as e:
        _fail(str(e))
    except KeyboardInterrupt:
        typer.echo("interrupted; nothing was kept", err=True)
        raise typer.Exit(code=130) from None
    typer.echo(f"pulled {host['name']}:{job_id} -> {res.path} ({human_size(res.size)})")
    if res.extracted is not None:
        typer.echo(f"extracted to {res.extracted}")
