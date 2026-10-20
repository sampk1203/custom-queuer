"""`qur run HOST FOLDER ... -- CMD` (QUEUER_REMOTE_PLAN.md, phase 3).

Sends a folder to a worker, runs the command there in its own new folder, and returns. Every step
is idempotent through a run key, so `qur run --resume` continues an interrupted run without
duplicating the folder or the job. States in `remote_runs`: created -> sent -> prepared -> queued.

The worker queues the job itself (its own queue, its own daemon); the host only records the run.
`qur add` runs on the worker inside the activated environment (host `setup`, `--venv`, `--setup`,
then `qur-env.sh` from the folder), so the executable and environment are captured there.
"""

from __future__ import annotations

import fnmatch
import json
import os
import re
import shlex
import sqlite3
import tarfile
import tempfile
import time
import uuid
from pathlib import Path, PurePosixPath
from typing import Any

import typer
from typer.core import TyperCommand

from queuer import db, hostcfg, remote
from queuer.paths import _default_db_path
from queuer.remote_worker import NAME_RE, RESERVED_ROOT, _inside, _machine_id, _skipped

RUN_KEY_RE = re.compile(r"^[0-9a-f]{32}$")
QURIGNORE = ".qurignore"
EXIT_SETUP_FAILED = 97
EXIT_NO_FOLDER = 98

VALUE_OPTS = ("--extra", "--name", "--setup", "--venv", "--require", "--link", "--worker-copy", "--max-mb")
WARN_BYTES = 1 << 30  # warn above ~1 GB when no --max-mb is given
FLAG_OPTS = ("--pull", "--no-pull", "--dry-run", "--wait", "--resume", "--include-sensitive")
REJECTED = {
    "--each": "--each is not available through `qur run` in v1 (the jobs would share one folder): run one `qur run` per variant",
    "--array": "--array is not available through `qur run` in v1 (the jobs would share one folder): run one `qur run` per variant",
    "--run-key": "--run-key is set by `qur run`",
    "--job-dir": "--job-dir is set by `qur run`",
    "--submitted-from": "--submitted-from is set by `qur run`",
    "--rerun-of": "--rerun-of belongs to `qur rerun`",
    "--json": "--json is used by `qur run` itself",
}


class RunCommand(TyperCommand):
    """Keeps the raw argument list: `qur run` forwards every `qur add` option verbatim, so it needs
    the tokens exactly as typed (and where the `--` was), not click's parse of them."""

    def parse_args(self, ctx: Any, args: list[str]) -> list[str]:
        ctx.meta["qur.run.args"] = list(args)
        return super().parse_args(ctx, args)


RUN_CONTEXT = {"ignore_unknown_options": True, "allow_extra_args": True}


# ---------------------------------------------------------------------------
# argument parsing
# ---------------------------------------------------------------------------


def parse_args(raw: list[str]) -> dict[str, Any]:
    """Split `qur run` arguments into its own options, the `qur add` options to forward, and CMD."""
    if "--" in raw:
        cut = raw.index("--")
        pre, cmd = raw[:cut], raw[cut + 1:]
    else:
        pre, cmd = list(raw), []
    spec: dict[str, Any] = {
        "host": None, "folder": None, "extra": [], "name": None, "setup": None, "venv": None,
        "require": [], "link": [], "worker_copy": [], "pull": False, "dry_run": False, "wait": False,
        "max_mb": None, "include_sensitive": False, "resume": False, "resume_key": None, "resume_host": None, "forward": [], "cmd": cmd,
    }
    i = 0
    positional: list[str] = []
    while i < len(pre):
        tok = pre[i]
        name, eq, inline = tok.partition("=") if tok.startswith("--") else (tok, "", "")
        if name in REJECTED:
            raise typer.BadParameter(REJECTED[name])
        if name in VALUE_OPTS:
            if eq:
                value = inline
            else:
                i += 1
                if i >= len(pre):
                    raise typer.BadParameter(f"Option '{name}' requires an argument.")
                value = pre[i]
            key = name[2:].replace("-", "_")
            if isinstance(spec[key], list):
                spec[key].append(value)
            else:
                spec[key] = value
        elif tok in FLAG_OPTS:
            if tok == "--resume":
                spec["resume"] = True
                if i + 1 < len(pre) and RUN_KEY_RE.match(pre[i + 1]):
                    i += 1
                    spec["resume_key"] = pre[i]
            elif tok == "--pull":
                spec["pull"] = True
            elif tok == "--no-pull":
                spec["pull"] = False
            else:
                spec[tok[2:].replace("-", "_")] = True
        elif not tok.startswith("-") and len(positional) < 2 and not spec["forward"]:
            positional.append(tok)  # HOST and FOLDER come first
        else:
            spec["forward"].append(tok)
        i += 1
    if spec["resume"]:
        spec["resume_host"] = positional[0] if positional else None
        return spec
    if len(positional) < 2:
        raise typer.BadParameter("usage: qur run HOST FOLDER [options] -- CMD...   (HOST and FOLDER come first)")
    spec["host"], spec["folder"] = positional
    if spec["max_mb"] is not None:
        try:
            spec["max_mb"] = float(spec["max_mb"])
        except ValueError:
            raise typer.BadParameter(f"--max-mb needs a number (got {spec['max_mb']!r})") from None
        if spec["max_mb"] <= 0:
            raise typer.BadParameter("--max-mb must be positive")
    if not cmd:
        raise typer.BadParameter("no command: write it after `--`, e.g. qur run vm3 ./job -- lmp -in in.lammps")
    return spec


# ---------------------------------------------------------------------------
# payload
# ---------------------------------------------------------------------------


def default_name(folder: Path) -> str:
    name = re.sub(r"[^A-Za-z0-9._-]", "-", folder.resolve().name).lstrip("._-")[:64]
    return name or "run"


def _read_qurignore(folder: Path) -> list[str]:
    try:
        lines = (folder / QURIGNORE).read_text().splitlines()
    except OSError:
        return []
    return [ln.strip().rstrip("/") for ln in lines if ln.strip() and not ln.lstrip().startswith("#")]


def _dest(value: str) -> str:
    p = PurePosixPath(value)
    if p.is_absolute() or ".." in p.parts or not p.parts or "\0" in value or p.parts[0] in RESERVED_ROOT:
        raise ValueError(f"bad destination inside the job folder: {value!r}")
    return str(p)


def _split_extra(spec: str) -> tuple[Path, str]:
    """`SRC` or `SRC:DEST`; a path that exists as written is taken whole (colons allowed in names)."""
    if os.path.lexists(spec) or ":" not in spec:
        src, dest = Path(spec), Path(spec).name
    else:
        s, d = spec.rsplit(":", 1)
        src, dest = Path(s), d
    if not os.path.lexists(src):
        raise ValueError(f"--extra: {src} does not exist")
    return src, _dest(dest)


def build_payload(folder: Path, extras: list[str], out: Path, include_sensitive: bool = False) -> dict[str, Any]:
    """tar.gz of `folder` plus extras into `out` (0600). Honours `.qurignore`; never includes keys (`.ssh`, private
    keys, `*.pem`, `*.key`, ...) or queuer's own markers, and leaves out `.env`, `.netrc`, `.aws`, `.git` and the like
    unless `include_sensitive`. Symlinks are stored, not followed."""
    patterns = _read_qurignore(folder)
    seen: set[str] = set()
    stats: dict[str, Any] = {"files": 0, "bytes": 0, "skipped_secrets": [], "ignored": 0, "skipped_unsafe": []}

    def add_tree(tf: tarfile.TarFile, src: Path, prefix: str, use_ignore: bool) -> None:
        root = src if src.is_dir() and not src.is_symlink() else src.parent

        def unsafe(path: Path, rel: str) -> bool:
            """Same rules as the worker's safe_extract: links leaving the folder and FIFOs/devices are refused there."""
            if path.is_symlink():
                target = os.readlink(path)
                bad = os.path.isabs(target) or not _inside(path.parent / target, root)
            else:
                bad = not (path.is_file() or path.is_dir())
            if bad:
                stats["skipped_unsafe"].append(rel)
            return bad

        def add(path: Path, rel: str) -> None:
            if unsafe(path, rel):
                return
            if rel in seen:
                raise ValueError(f"two files would land at {rel!r} in the job folder")
            seen.add(rel)
            tf.add(path, arcname=rel, recursive=False, filter=_plain)
            if path.is_file() and not path.is_symlink():
                stats["files"] += 1
                stats["bytes"] += path.stat().st_size

        def skip(rel_in: str, base: str, rel_out: str) -> bool:
            if _skipped(rel_in, base, (), path=src / rel_in if src.is_dir() else src, include_sensitive=include_sensitive):
                stats["skipped_secrets"].append(rel_out)
                return True
            if not prefix and rel_in.split("/")[0] in RESERVED_ROOT and "/" not in rel_in:
                return True
            if use_ignore and any(fnmatch.fnmatch(base, p) or fnmatch.fnmatch(rel_in, p) for p in patterns):
                stats["ignored"] += 1
                return True
            return False

        if src.is_file() or src.is_symlink():
            if not _skipped(src.name, src.name, (), path=src, include_sensitive=include_sensitive):
                add(src, prefix)
            else:
                stats["skipped_secrets"].append(prefix)
            return
        if prefix:
            add(src, prefix)
        for dirpath, dirnames, filenames in os.walk(src, followlinks=False):
            here = Path(dirpath)
            rel_dir = here.relative_to(src).as_posix()
            keep = []
            for d in sorted(dirnames):
                rel = d if rel_dir == "." else f"{rel_dir}/{d}"
                out_rel = f"{prefix}/{rel}" if prefix else rel
                if skip(rel, d, out_rel):
                    continue
                add(here / d, out_rel)
                if not os.path.islink(here / d):
                    keep.append(d)
            dirnames[:] = keep
            for f in sorted(filenames):
                rel = f if rel_dir == "." else f"{rel_dir}/{f}"
                out_rel = f"{prefix}/{rel}" if prefix else rel
                if not skip(rel, f, out_rel):
                    add(here / f, out_rel)

    fd = os.open(out, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "wb") as raw, tarfile.open(fileobj=raw, mode="w:gz") as tf:
        add_tree(tf, folder, "", True)
        for spec in extras:
            src, dest = _split_extra(spec)
            add_tree(tf, src, dest, False)
    return stats


def _plain(info: tarfile.TarInfo) -> tarfile.TarInfo:
    info.uid = info.gid = 0
    info.uname = info.gname = ""
    return info


# ---------------------------------------------------------------------------
# run records (host database + saved arguments for --resume)
# ---------------------------------------------------------------------------


def _conn() -> sqlite3.Connection:
    conn = db.connect(_default_db_path())
    db.init_db(conn)
    return conn


def _state_dir() -> Path:
    path = _default_db_path().parent / "runs"
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    return path


def _save_spec(key: str, spec: dict[str, Any]) -> None:
    target = _state_dir() / f"{key}.json"
    fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        json.dump(spec, f)


def _redact_spec(key: str) -> None:
    """After the job is queued the saved arguments are never needed again (a resume finds the job through
    `remote find` before it reads them): blank `--env` values and the setup text, which may hold secrets."""
    target = _state_dir() / f"{key}.json"
    try:
        saved = json.loads(target.read_text())
    except (OSError, ValueError):
        return
    for field in ("forward", "add_args"):
        toks = saved.get(field)
        if not isinstance(toks, list):
            continue
        out: list[Any] = []
        for i, t in enumerate(toks):
            if isinstance(t, str) and i and toks[i - 1] == "--env" and "=" in t:
                t = t.split("=", 1)[0] + "=<redacted>"
            elif isinstance(t, str) and t.startswith("--env=") and t.count("=") > 1:
                t = t.split("=", 2)[0] + "=" + t.split("=", 2)[1] + "=<redacted>"
            out.append(t)
        saved[field] = out
    if saved.get("setup"):
        saved["setup"] = None
    fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        json.dump(saved, f)


def _load_spec(key: str) -> dict[str, Any]:
    try:
        return json.loads((_state_dir() / f"{key}.json").read_text())
    except (OSError, ValueError):
        raise typer.BadParameter(f"the saved arguments of run {key} are gone; start a new `qur run`") from None


def _new_run(conn: sqlite3.Connection, key: str, host: str, folder: Path | None, pull: bool, kind: str = "run") -> None:
    now = db._now()
    conn.execute(
        "INSERT INTO remote_runs (run_key, host, kind, state, local_folder, pull, created_at, updated_at) "
        "VALUES (?, ?, ?, 'created', ?, ?, ?, ?)",
        (key, host, kind, str(folder) if folder else None, int(pull), now, now),
    )
    conn.commit()


_RUN_COLUMNS = ("state", "remote_dir", "remote_id", "parent_remote_id")


def _update_run(conn: sqlite3.Connection, key: str, **fields: Any) -> None:
    assert set(fields) <= set(_RUN_COLUMNS)
    sets = ", ".join(f"{k} = ?" for k in fields)
    conn.execute(f"UPDATE remote_runs SET {sets}, updated_at = ? WHERE run_key = ?", (*fields.values(), db._now(), key))  # nosec B608 - column names are our own constants
    conn.commit()


def _get_run(conn: sqlite3.Connection, key: str | None, kind: str = "run", host: str | None = None) -> dict[str, Any]:
    """The run to resume. Without a key: the newest unfinished one, only on `host` when given."""
    if key:
        row = conn.execute("SELECT * FROM remote_runs WHERE run_key = ? AND kind = ?", (key, kind)).fetchone()
    else:
        sql = "SELECT * FROM remote_runs WHERE kind = ? AND state != 'queued'"
        args: list[Any] = [kind]
        if host:
            sql += " AND host = ?"
            args.append(host)
        row = conn.execute(sql + " ORDER BY created_at DESC, rowid DESC LIMIT 1", args).fetchone()
    if row is None:
        where = f" on {host}" if host and not key else ""
        raise typer.BadParameter(f"no such run to resume{where}" if key else f"no unfinished run to resume{where}")
    if not key:
        typer.echo(f"resuming run {row['run_key']} on {row['host']} (state {row['state']})")
    return dict(row)


# ---------------------------------------------------------------------------
# worker-side steps
# ---------------------------------------------------------------------------


def sh_path(path: str) -> str:
    """A path for the worker's shell: `~` expands there, everything else is quoted."""
    if path == "~":
        return '"$HOME"'
    if path.startswith("~/"):
        return '"$HOME"/' + shlex.quote(path[2:])
    return shlex.quote(path)


def env_prelude(host: dict[str, Any], venv: str | None, setup: str | None) -> str:
    """Shell that applies the environment layers in order (host setup, --venv, --setup, qur-env.sh)
    inside the current shell, with its stdout sent to stderr; any failure exits 97 before `qur add`."""
    layers = []
    if host.get("setup"):
        layers.append("{ " + host["setup"] + "\n}")
    if venv:
        layers.append("{ source " + sh_path(venv.rstrip("/")) + "/bin/activate; }")
    if setup:
        layers.append("{ " + setup + "\n}")
    layers.append("{ if [ -f ./qur-env.sh ]; then . ./qur-env.sh; fi; }")
    return "{ " + " && ".join(layers) + "\n} 1>&2 || exit " + str(EXIT_SETUP_FAILED)


def add_script(host: dict[str, Any], cwd: str, venv: str | None, setup: str | None, add_argv: list[str], *, cwd_fallback: bool = False) -> str:
    cd = f"cd {sh_path(cwd)} 2>/dev/null" + (' || cd "$HOME"' if cwd_fallback else f" || exit {EXIT_NO_FOLDER}")
    return "\n".join([
        'export PATH="$HOME/.local/bin:$PATH"',
        cd,
        env_prelude(host, venv, setup),
        "exec qur " + " ".join(shlex.quote(a) for a in add_argv),
    ])


def worker_shell(host: dict[str, Any], script: str, timeout: float = 120) -> tuple[int, str, str]:
    # the script (it carries --env values) goes over stdin: ssh argv is visible in `ps` on both machines
    code, out, err = remote.ssh_run(host, ["bash", "-l", "-s"], stdin=script.encode(), raw=True, timeout=timeout)
    return code, out.decode("utf-8", "replace"), err


def _fail(msg: str) -> None:
    typer.echo(f"Error: {msg}", err=True)
    raise typer.Exit(code=1)


def _step(n: int, text: str) -> None:
    typer.echo(f"[{n}/5] {text}")


def drive(host_name: str, host: dict[str, Any], key: str, spec: dict[str, Any], run: dict[str, Any]) -> int:
    """Continue the run from its saved state to `queued`; returns the worker job id."""
    conn = _conn()
    try:
        label = f"{host_name}"
        # the job may already exist (crash between `qur add` and our record)
        found = remote.ssh_json(host, ["remote", "find", "--run-key", key, "--json"])
        if found.get("id"):
            _update_run(conn, key, state="queued", remote_id=int(found["id"]), remote_dir=found.get("job_dir") or run.get("remote_dir"))
            typer.echo(f"already queued as {label}:{found['id']} (run {key})")
            return int(found["id"])
        state, remote_dir = run["state"], run.get("remote_dir")
        name = spec["name"] or default_name(Path(spec["folder"]))
        if state == "created":
            _step(2, f"packing {spec['folder']}")
            folder = Path(spec["folder"])
            with tempfile.TemporaryDirectory(prefix="qur-run-") as tmp:
                payload = Path(tmp) / "payload.tar.gz"
                try:
                    stats = build_payload(folder, spec["extra"], payload, bool(spec.get("include_sensitive")))
                except ValueError as e:
                    _fail(str(e))
                if stats["skipped_secrets"]:
                    typer.echo(f"      skipped key and credential files (--include-sensitive sends .env, .netrc, .aws, .git; keys never): {', '.join(sorted(set(stats['skipped_secrets'])))}")
                if stats["skipped_unsafe"]:
                    typer.echo(f"      skipped (links leaving the folder, FIFOs, devices): {', '.join(sorted(set(stats['skipped_unsafe'])))}")
                if stats["ignored"]:
                    typer.echo(f"      {stats['ignored']} path(s) left out by {QURIGNORE}")
                max_mb = spec.get("max_mb")
                mb = stats["bytes"] / (1 << 20)
                _step(3, f"sending {stats['files']} file(s), {stats['bytes']} bytes ({mb:.1f} MB)")
                if max_mb is not None and mb > max_mb:
                    _fail(f"the folder is {mb:.1f} MB, bigger than --max-mb {max_mb:g}; nothing was sent (use .qurignore or raise --max-mb)")
                if max_mb is None and stats["bytes"] > WARN_BYTES:
                    typer.echo(f"      warning: {mb:.0f} MB is a lot to send; --max-mb N makes this an error, .qurignore leaves files out", err=True)
                code, out, err = remote.ssh_send_file(
                    host, payload,
                    ["remote", "receive", "--name", name, "--run-key", key, "--jobs-dir", host["jobs_dir"],
                     *(["--max-mb", f"{max_mb:g}"] if max_mb is not None else [])],
                )
            if code != 0:
                raise remote.RemoteError(code, err)
            remote_dir = remote.parse_json(out.decode("utf-8", "replace"))["path"]
            _update_run(conn, key, state="sent", remote_dir=remote_dir)
            state = "sent"
            typer.echo(f"      worker folder: {remote_dir}")
        if state == "sent":
            if spec["require"] or spec["link"] or spec["worker_copy"]:
                _step(4, "checking worker-side files")
                assert remote_dir is not None  # state "sent" always recorded the folder
                argv = ["remote", "prepare", "--dir", remote_dir, "--json"]
                for flag, values in (("--require", spec["require"]), ("--link", spec["link"]), ("--copy", spec["worker_copy"])):
                    for v in values:
                        argv += [flag, v]
                code, out, err = remote.ssh_run(host, argv, timeout=300)
                if code == 5:
                    try:
                        missing = remote.parse_json(out.decode("utf-8", "replace")).get("missing", [])
                    except ValueError:
                        missing = []
                    _fail("required path(s) missing on the worker, nothing queued: " + ", ".join(map(str, missing))
                          + f"\n(the folder {remote_dir} stays until `qur -H {host_name} remote gc`)")
                if code != 0:
                    raise remote.RemoteError(code, err)
            _update_run(conn, key, state="prepared")
            state = "prepared"
        if state == "prepared":
            _step(5, "queueing on the worker")
            assert remote_dir is not None
            argv = ["add", "--json", "--run-key", key, "--job-dir", remote_dir, "--submitted-from", _machine_id(),
                    *spec["forward"], "--", *spec["cmd"]]
            add_code, add_out, add_err = worker_shell(host, add_script(host, remote_dir, spec["venv"], spec["setup"], argv))
            if add_code == EXIT_SETUP_FAILED:
                _fail("the environment setup failed on the worker, nothing queued:\n" + (add_err or add_out).strip())
            if add_code != 0:
                _fail(f"`qur add` failed on the worker (exit {add_code}):\n" + (add_err or add_out).strip())
            try:
                data = remote.parse_json(add_out)
                ids = data["ids"]
            except (ValueError, KeyError, TypeError):
                _fail(f"unexpected answer from `qur add` on the worker: {add_out[:200]!r}")
            _update_run(conn, key, state="queued", remote_id=int(ids[0]))
            typer.echo(f"queued as {label}:{ids[0]} in {remote_dir}" + (" (already existed)" if data.get("existing") else ""))
            return int(ids[0])
        raise AssertionError(f"unknown run state {state!r}")
    finally:
        conn.close()


def wait_for(host: dict[str, Any], job_id: int) -> int:
    """Block like `qur wait` (same exit codes) by polling the worker; survives dropped links."""
    typer.echo(f"waiting for job {job_id} ...")
    try:
        while True:
            try:
                code, out, err = remote.ssh_run(host, ["wait", str(job_id), "--timeout", "55s"], timeout=90)
            except remote.HostUnreachable:
                time.sleep(15)
                continue
            except remote.RemoteTransportError as e:  # AuthFailed, RemoteQurMissing, ProtocolMismatch, ...
                typer.echo(f"Error: {remote.safe_text(e)}", err=True)
                typer.echo(f"The job {job_id} is queued on the worker; it keeps running. Fix the connection and check it with `qur -H HOST show {job_id}`.", err=True)
                return 1
            if code == 124:
                continue
            typer.echo((out.decode("utf-8", "replace") or err).rstrip())
            return code
    except KeyboardInterrupt:
        return 130


def run_cmd(ctx: typer.Context) -> None:
    """Send a folder to a worker and queue a command there, in its own new job folder.

    \b
    qur run HOST FOLDER [options] -- CMD...
    qur run --resume [RUN_KEY]

    HOST and FOLDER come first. Every `qur add` option is forwarded verbatim to the worker
    (--channel, --depends-on (worker job ids), --retries, --timeout, --env, --delay, --at, ...).
    --each and --array are not available. Key files (.ssh, id_*, *.pem, *.key) are never sent;
    `.qurignore` in FOLDER lists more paths to leave out (one glob per line).

    \b
    Options of `qur run` itself:
      --extra PATH[:DEST]       also send a file or folder (repeatable)
      --name NAME               prefix of the worker folder (default: the folder's name)
      --venv PATH               source PATH/bin/activate on the worker before `qur add`
      --setup CMD               extra shell snippet for this run, after the host's `setup`
      --require PATH            fail before queueing if this worker path is missing (repeatable)
      --link NAME=PATH          symlink NAME in the job folder to a worker path (repeatable)
      --worker-copy SRC=DEST    copy a worker file into the job folder (SRC or job:ID/rel; repeatable)
      --include-sensitive       also send .env, .netrc, .aws, .git and similar (keys are never sent)
      --pull / --no-pull        record that the finished folder should be pulled (default: no)
      --wait                    block until the job ends; exit code like `qur wait`
      --dry-run                 show what the worker would run; sends nothing, queues nothing
      --max-mb N                refuse to send a folder bigger than N MB (default: warn above ~1 GB)
      --resume [RUN_KEY]        continue an interrupted run (default: the newest unfinished one;
                                `--resume HOST` limits that to one host)

    The environment is built on the worker in this order: host `setup`, --venv, --setup, then
    `qur-env.sh` from the folder if present. A failing step aborts before anything is queued.
    PATH, VIRTUAL_ENV, LD_LIBRARY_PATH, CUDA_* and the other captured variables are those of the
    worker's shell after these steps, never of this machine (a bare `--env KEY` also reads KEY there).

    \b
    Examples:
      qur run vm3 ./lammps-run -- lmp -in in.lammps
      qur run vm3 ./job --venv ~/venvs/md --retries 2 -- python train.py
      qur run vm3 ./job --link model.pt=/data/models/llzo.pt --require /data/potentials -- lmp -in in.x
    """
    spec = parse_args(ctx.meta.get("qur.run.args", []))
    conn = _conn()
    try:
        if spec["resume"]:
            run = _get_run(conn, spec["resume_key"], host=spec["resume_host"])
            key = run["run_key"]
            saved = _load_spec(key)
            spec = {**saved, "pull": bool(run["pull"]), "wait": spec["wait"], "dry_run": False}
        else:
            run = None
            key = ""
    finally:
        conn.close()
    host_name = spec["host"]
    host = hostcfg.get(host_name)
    if host is None:
        raise typer.BadParameter(f"unknown host {host_name!r}; configured: {', '.join(hostcfg.names()) or 'none'} (qur hosts add)")
    if run is None and not spec["dry_run"]:
        folder = Path(spec["folder"])
        if not folder.is_dir():
            raise typer.BadParameter(f"{spec['folder']} is not a folder")
    if spec["name"] is not None and not NAME_RE.match(spec["name"]):
        raise typer.BadParameter("--name must be 1-64 letters, digits, '.', '_' or '-'")
    try:
        _step(1, f"checking {host_name}")
        info = remote.ssh_json(host, ["remote", "info", "--json"], timeout=30)
        if not info.get("daemon"):
            typer.echo(f"      warning: queuerd is not running on {host_name}; the job will wait until it starts", err=True)
        if spec["dry_run"]:
            argv = ["add", "--dry-run", *spec["forward"], "--", *spec["cmd"]]
            code, out, err = worker_shell(host, add_script(host, host["jobs_dir"], spec["venv"], spec["setup"], argv, cwd_fallback=True))
            if code == EXIT_SETUP_FAILED:
                _fail("the environment setup failed on the worker:\n" + (err or out).strip())
            typer.echo(f"dry run on {host_name}: would send {spec['folder']} into a new folder under {host['jobs_dir']}; nothing was sent")
            typer.echo((out or err).rstrip())
            raise typer.Exit(code=0 if code == 0 else 1)
        if run is None:
            key = uuid.uuid4().hex
            spec["folder"] = str(Path(spec["folder"]).resolve())
            conn = _conn()
            try:
                _new_run(conn, key, host_name, Path(spec["folder"]), spec["pull"])
                run = _get_run(conn, key)
            finally:
                conn.close()
            _save_spec(key, {k: v for k, v in spec.items() if k not in ("resume", "resume_key", "wait", "dry_run")})
            typer.echo(f"run key {key}")
        job_id = drive(host_name, host, key, spec, run)
        _redact_spec(key)
    except remote.RemoteTransportError as e:
        typer.echo(f"Error: {remote.safe_text(e)}", err=True)
        if key:
            typer.echo(f"The run is saved. Continue with: qur run --resume {key}", err=True)
        raise typer.Exit(code=1) from None
    except typer.Exit as e:
        if e.exit_code and key:
            typer.echo(f"The run is saved. Continue with: qur run --resume {key}", err=True)
        raise
    if spec["wait"]:
        raise typer.Exit(code=wait_for(host, job_id))
