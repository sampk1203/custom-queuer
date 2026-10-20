"""`qur rerun HOST:ID ...` (QUEUER_REMOTE_PLAN.md, phase 3B).

Runs a job again **in the same job folder** on the worker: optionally replace or edit some of the
files already there, optionally change the command, then queue a new worker job with
`--rerun-of ID`. Nothing is re-sent except the files you name, nothing is ever deleted (replaced
files are moved to `.qur-prev/<timestamp>/` by `qur remote update`), and every step is idempotent
through a run key (`remote_runs`, kind `rerun`), so `qur rerun --resume` never duplicates the job.

Environment: like `qur run`, the new job's `qur add` runs on the worker inside the activated
environment (host `setup`, `--venv`, `--setup`, then `qur-env.sh` from the folder). PATH,
LD_LIBRARY_PATH, CUDA_* and the rest are therefore captured **fresh from the worker**; the old
job's captured copies are not reused, and nothing comes from this (host) machine's environment.
"""

from __future__ import annotations

import json
import os
import re
import tarfile
import tempfile
import uuid
from pathlib import Path
from typing import Any

import typer
from typer.core import TyperCommand

from queuer import db, hostcfg, jobenv, remote
from queuer.remote_cli import REF_RE
from queuer.remote_run import (
    EXIT_SETUP_FAILED,
    REJECTED,
    RUN_KEY_RE,
    _conn,
    _dest,
    _fail,
    _get_run,
    _load_spec,
    _new_run,
    _plain,
    _redact_spec,
    _save_spec,
    _state_dir,
    _update_run,
    add_script,
    wait_for,
    worker_shell,
)
from queuer.remote_worker import EXIT_IN_USE, _machine_id

VALUE_OPTS = ("--replace", "--edit", "--setup", "--venv")
FLAG_OPTS = ("--pull", "--no-pull", "--dry-run", "--force", "--wait", "--resume")
NOT_FOR_RERUN = {
    "--extra": "--extra belongs to `qur run`; `qur rerun` takes --replace LOCAL[=REL] for files",
    "--name": "--name belongs to `qur run`: a rerun uses the job's existing folder",
    "--require": "--require belongs to `qur run`",
    "--link": "--link belongs to `qur run`",
    "--worker-copy": "--worker-copy belongs to `qur run`",
}
REJECTED_RERUN = {k: v for k, v in REJECTED.items() if k != "--rerun-of"}
REJECTED_RERUN["--rerun-of"] = "--rerun-of is set by `qur rerun`"
REJECTED_RERUN.update(NOT_FOR_RERUN)
EDIT_MAX_MB = 5
ACTIVE = db.ACTIVE_STATUSES


class RerunCommand(TyperCommand):
    """Keeps the raw argument list: every `qur add` option is forwarded verbatim (see RunCommand)."""

    def parse_args(self, ctx: Any, args: list[str]) -> list[str]:
        ctx.meta["qur.rerun.args"] = list(args)
        return super().parse_args(ctx, args)


RERUN_CONTEXT = {"ignore_unknown_options": True, "allow_extra_args": True}


# ---------------------------------------------------------------------------
# arguments
# ---------------------------------------------------------------------------


def parse_args(raw: list[str]) -> dict[str, Any]:
    """Split `qur rerun` arguments into its own options, the `qur add` options to forward, and CMD."""
    if "--" in raw:
        cut = raw.index("--")
        pre, cmd = raw[:cut], raw[cut + 1:]
    else:
        pre, cmd = list(raw), []
    spec: dict[str, Any] = {
        "ref": None, "replace": [], "edit": [], "setup": None, "venv": None, "pull": False,
        "dry_run": False, "force": False, "wait": False, "resume": False, "resume_key": None, "resume_host": None,
        "forward": [], "cmd": cmd,
    }
    positional: list[str] = []
    i = 0
    while i < len(pre):
        tok = pre[i]
        name, eq, inline = tok.partition("=") if tok.startswith("--") else (tok, "", "")
        if name in REJECTED_RERUN:
            raise typer.BadParameter(REJECTED_RERUN[name])
        if name in VALUE_OPTS:
            if eq:
                value = inline
            else:
                i += 1
                if i >= len(pre):
                    raise typer.BadParameter(f"Option '{name}' requires an argument.")
                value = pre[i]
            key = name[2:]
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
        elif not tok.startswith("-") and not positional and not spec["forward"]:
            positional.append(tok)  # HOST:ID comes first
        else:
            spec["forward"].append(tok)
        i += 1
    if spec["resume"]:
        spec["resume_host"] = positional[0].split(":", 1)[0] if positional else None
        return spec
    if not positional:
        raise typer.BadParameter("usage: qur rerun HOST:ID [options] [-- CMD...]   (HOST:ID comes first)")
    spec["ref"] = positional[0]
    return spec


def split_job_ref(ref: str) -> tuple[str, int]:
    """`vm3:42` -> ("vm3", 42). Exactly one job id; the host must be configured."""
    m = REF_RE.match(ref)
    if not m or not m.group(2).isdigit():
        raise typer.BadParameter(f"{ref!r} is not HOST:ID with one job id (example: vm3:42)")
    if hostcfg.get(m.group(1)) is None:
        raise typer.BadParameter(f"unknown host {m.group(1)!r}; configured: {', '.join(hostcfg.names()) or 'none'} (qur hosts add)")
    return m.group(1), int(m.group(2))


def _chain_ids(forward: list[str]) -> set[int]:
    """Worker job ids named by `--depends-on` / `--after-any` in the forwarded options."""
    ids: set[int] = set()
    for i, tok in enumerate(forward):
        name, eq, inline = tok.partition("=")
        if name not in ("--depends-on", "--after-any"):
            continue
        value = inline if eq else (forward[i + 1] if i + 1 < len(forward) else "")
        for part in value.split(","):
            m = re.fullmatch(r"\s*(\d+)(?:-(\d+))?\s*", part)
            if m:
                lo, hi = int(m.group(1)), int(m.group(2) or m.group(1))
                ids.update(range(lo, min(hi, lo + db.MAX_SWEEP) + 1))
    return ids


# ---------------------------------------------------------------------------
# what the new job inherits from the old one
# ---------------------------------------------------------------------------

# job column -> (qur add option, how to print the value); applied only when the option was not
# given again on the rerun command line
_CARRY: tuple[tuple[str, str, Any], ...] = (
    ("channel", "--channel", str),
    ("timeout_secs", "--timeout", str),
    ("max_retries", "--retries", str),
    ("retry_delay_secs", "--retry-delay", lambda v: f"{v}s"),
    ("on_fail", "--on-fail", str),
    ("on_finish_cmd", "--on-finish", str),
    ("on_error_cmd", "--on-error", str),
    ("stop_signal", "--stop-signal", str),
    ("stop_grace_secs", "--stop-grace", lambda v: f"{v}s"),
)


def _given(forward: list[str], flag: str) -> bool:
    return any(t == flag or t.startswith(flag + "=") for t in forward)


def carried_options(job: dict[str, Any], forward: list[str]) -> tuple[list[str], list[str]]:
    """`qur add` options copied from the old job (same channel, retries, timeout, hooks, explicit
    --env ...) unless the rerun gives them again. Returns (options, names of --env values that were
    not carried because their name looks secret). Variables queuer captured automatically
    (PATH, LD_LIBRARY_PATH, CUDA_*, ...) are never carried: they are captured again on the worker."""
    out: list[str] = []
    for column, flag, fmt in _CARRY:
        value = job.get(column)
        if value in (None, "") or _given(forward, flag):
            continue
        if column == "on_fail" and value == "continue":
            continue
        if column == "max_retries" and not value:
            continue
        out += [flag, fmt(value)]
    if job.get("retry_backoff") and job.get("retry_delay_secs") is not None and not _given(forward, "--retry-backoff"):
        out.append("--retry-backoff")
    env = job.get("env_extra") or {}
    if isinstance(env, str):
        try:
            env = json.loads(env)
        except ValueError:
            env = {}
    auto = set(str(env.get(jobenv.PASSTHROUGH_KEY) or "").split(",")) | set(jobenv.NAME_ONLY) | {jobenv.PASSTHROUGH_KEY}
    explicit_given = {t.split("=", 1)[0] for i, t in enumerate(forward) if i and forward[i - 1] == "--env"}
    explicit_given |= {t[len("--env="):].split("=", 1)[0] for t in forward if t.startswith("--env=")}
    skipped: list[str] = []
    for name in sorted(env):
        if name in auto or name in explicit_given:
            continue
        if jobenv.is_secret_name(name):
            skipped.append(name)  # keeps secrets off ssh argv and process lists: give --env again
            continue
        out += ["--env", f"{name}={env[name]}"]
    return out, skipped


# ---------------------------------------------------------------------------
# payload of replaced / edited files
# ---------------------------------------------------------------------------


def _split_replace(spec: str) -> tuple[Path, str]:
    """`LOCAL` or `LOCAL=REL`; a path that exists as written is taken whole (`=` allowed in names)."""
    if os.path.lexists(spec) or "=" not in spec:
        src, rel = Path(spec), Path(spec).name
    else:
        s, r = spec.rsplit("=", 1)
        src, rel = Path(s), r
    if not os.path.lexists(src):
        raise ValueError(f"--replace: {src} does not exist")
    if os.path.islink(src) or not src.is_file():
        raise ValueError(f"--replace: {src} must be a regular file")
    if remote.secret_reason(src.name, src.name, src, include_sensitive=True):
        raise ValueError(f"--replace: {src.name} looks like a key file and is never sent")
    return src, _dest(rel)


def build_update_payload(replace: list[str], edited: dict[str, bytes], out: Path) -> list[str]:
    """tar.gz (0600) of the files to put into the job folder; returns their relative paths."""
    seen: dict[str, Path | None] = {}
    for spec in replace:
        src, rel = _split_replace(spec)
        if rel in seen:
            raise ValueError(f"two files would land at {rel!r} in the job folder")
        seen[rel] = src
    for rel in edited:
        if rel in seen:
            raise ValueError(f"{rel!r} is given both to --replace and --edit")
        seen[rel] = None
    fd = os.open(out, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "wb") as raw, tarfile.open(fileobj=raw, mode="w:gz") as tf:
        for rel, member_src in seen.items():
            if member_src is not None:
                tf.add(member_src, arcname=rel, recursive=False, filter=_plain)
            else:
                data = edited[rel]
                info = tarfile.TarInfo(rel)
                info.size, info.mode = len(data), 0o644
                tf.addfile(_plain(info), fileobj=_BytesReader(data))
    return list(seen)


class _BytesReader:
    def __init__(self, data: bytes):
        self._data, self._pos = data, 0

    def read(self, n: int = -1) -> bytes:
        end = len(self._data) if n < 0 else self._pos + n
        chunk, self._pos = self._data[self._pos:end], min(end, len(self._data))
        return chunk


def fetch_file(host: dict[str, Any], job_id: int, rel: str) -> bytes:
    code, out, err = remote.ssh_run(host, ["remote", "cat", "--job", str(job_id), "--max-mb", str(EDIT_MAX_MB), "--", rel], timeout=120)
    if code != 0:
        raise remote.RemoteError(code, err)
    return out


def edit_files(host: dict[str, Any], job_id: int, rels: list[str]) -> dict[str, bytes]:
    """Fetch each file, open it in the user's editor, return only those that changed."""
    from queuer.cli import _run_editor  # lazy: cli imports this module

    changed: dict[str, bytes] = {}
    for rel in rels:
        rel = _dest(rel)
        original = fetch_file(host, job_id, rel)
        with tempfile.TemporaryDirectory(prefix="qur-edit-") as tmp:
            path = Path(tmp) / (Path(rel).name or "file")
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "wb") as f:
                f.write(original)
            try:
                _run_editor(str(path))
            except RuntimeError as e:
                _fail(str(e))
            edited = path.read_bytes()
        if edited == original:
            typer.echo(f"      {rel}: unchanged, not sent")
        else:
            changed[rel] = edited
    return changed


# ---------------------------------------------------------------------------
# worker lookups and the "folder in use" rule
# ---------------------------------------------------------------------------


def _show(host: dict[str, Any], job_id: int) -> dict[str, Any]:
    data = remote.ssh_json(host, ["show", str(job_id), "--json"], timeout=30)
    if not isinstance(data, dict):
        raise remote.RemoteError(0, f"unexpected answer for job {job_id}")
    return data


def folder_users(host: dict[str, Any], job: dict[str, Any]) -> list[int]:
    """Ids of queued / running / stopped jobs (the old job included) that use the job's folder."""
    busy = [int(job["id"])] if job.get("status") in ACTIVE else []
    for other in job.get("same_dir_jobs") or []:
        if _show(host, int(other)).get("status") in ACTIVE:
            busy.append(int(other))
    return sorted(busy)


def check_folder_free(busy: list[int], forward: list[str], changing_files: bool, force: bool) -> None:
    """The rule of plan 3B.6: refuse (exit 6) while a job uses the folder, unless the new job is
    chained behind all of them with --depends-on / --after-any. Replacing files under a job that
    has not finished is never done without --force, chained or not."""
    if not busy or force:
        return
    ids = ", ".join(map(str, busy))
    if not set(busy) <= _chain_ids(forward):
        typer.echo(f"Error: job(s) {ids} still use this folder. Wait for them, chain behind them with "
                   f"--depends-on {busy[0]}, or use --force.", err=True)
        raise typer.Exit(code=EXIT_IN_USE)
    if changing_files:
        typer.echo(f"Error: the new job is chained behind job(s) {ids}, but they have not finished and "
                   "would see the replaced files. Change only the command (no --replace/--edit), "
                   "or use --force.", err=True)
        raise typer.Exit(code=EXIT_IN_USE)


# ---------------------------------------------------------------------------
# steps
# ---------------------------------------------------------------------------


def _step(n: int, text: str) -> None:
    typer.echo(f"[{n}/4] {text}")


def _payload_path(key: str) -> Path:
    return _state_dir() / f"{key}.files.tar.gz"


def drive(host_name: str, host: dict[str, Any], key: str, spec: dict[str, Any], run: dict[str, Any]) -> int:
    """Continue the rerun from its saved state to `queued`; returns the new worker job id."""
    conn = _conn()
    try:
        found = remote.ssh_json(host, ["remote", "find", "--run-key", key, "--json"])
        if found.get("id"):
            _update_run(conn, key, state="queued", remote_id=int(found["id"]))
            typer.echo(f"already queued as {host_name}:{found['id']} (run {key})")
            return int(found["id"])
        state = run["state"]
        if state == "created":
            payload = _payload_path(key)
            if payload.exists():
                _step(3, f"replacing {len(spec['files'])} file(s) in {spec['remote_dir']}")
                code, out, err = remote.ssh_send_file(host, payload, ["remote", "update", "--job", str(spec["job_id"])] + (["--force"] if spec["force"] else []))
                if code == EXIT_IN_USE:
                    _fail(f"the folder is in use by another job; nothing changed ({err.strip()})")
                if code != 0:
                    raise remote.RemoteError(code, err)
                info = remote.parse_json(out.decode("utf-8", "replace"))
                if info.get("added"):
                    typer.echo(f"      added: {', '.join(info['added'])}")
                if info.get("unchanged"):
                    typer.echo(f"      already up to date: {', '.join(info['unchanged'])}")
                if info.get("replaced"):
                    typer.echo(f"      replaced: {', '.join(info['replaced'])}  (old copies in {info.get('backup')}/)")
            _update_run(conn, key, state="sent")
            state = "sent"
        if state in ("sent", "prepared"):
            _step(4, "queueing on the worker")
            argv = ["add", "--json", "--run-key", key, "--job-dir", spec["remote_dir"], "--submitted-from", _machine_id(),
                    "--rerun-of", str(spec["job_id"]), *spec["add_args"], "--", *spec["cmd"]]
            add_code, add_out, add_err = worker_shell(host, add_script(host, spec["remote_dir"], spec["venv"], spec["setup"], argv))
            if add_code == EXIT_SETUP_FAILED:
                _fail("the environment setup failed on the worker, nothing queued:\n" + (add_err or add_out).strip())
            if add_code != 0:
                _fail(f"`qur add` failed on the worker (exit {add_code}):\n" + (add_err or add_out).strip())
            try:
                data = remote.parse_json(add_out)
                new_id = int(data["ids"][0])
            except (ValueError, KeyError, IndexError, TypeError):
                _fail(f"unexpected answer from `qur add` on the worker: {add_out[:200]!r}")
            _update_run(conn, key, state="queued", remote_id=new_id)
            typer.echo(f"queued as {host_name}:{new_id} in {spec['remote_dir']} (rerun of {host_name}:{spec['job_id']})")
            return new_id
        raise AssertionError(f"unknown run state {state!r}")
    finally:
        conn.close()


def rerun_cmd(ctx: typer.Context) -> None:
    """Run a job again in the same folder on a worker: optionally replace or edit files, optionally change the command.

    \b
    qur rerun HOST:ID [options] [-- CMD...]
    qur rerun --resume [RUN_KEY]

    The folder is the one job ID already used; nothing is re-sent except the files you name.
    Without `-- CMD` the original command is run again. Options of the original job (channel,
    retries, timeout, hooks, explicit --env) are kept unless you give them again; every `qur add`
    option is forwarded verbatim. Variables captured from the shell (PATH, LD_LIBRARY_PATH, CUDA_*)
    are captured again on the worker, never from this machine.

    \b
    Options of `qur rerun` itself:
      --replace LOCAL[=REL]   send this local file into the job folder as REL (default: its name); repeatable
      --edit REL              fetch REL from the folder, open it in $EDITOR, send it back if changed; repeatable
      --venv PATH             source PATH/bin/activate on the worker before `qur add`
      --setup CMD             extra shell snippet for this rerun, after the host's `setup`
      --force                 go ahead while another job uses the folder (replaced files are still backed up)
      --pull / --no-pull      record that the finished folder should be pulled (default: no)
      --wait                  block until the new job ends; exit code like `qur wait`
      --dry-run               show files, backup location, command and queue position; changes nothing
      --resume [RUN_KEY]      continue an interrupted rerun (default: the newest unfinished one)

    Replaced files are first moved to .qur-prev/<timestamp>/ inside the folder; nothing is deleted.
    Refused (exit 6) while another queued, running or being-edited job uses the folder, unless the
    new job is chained behind it with --depends-on (and no files are replaced).

    \b
    Examples:
      qur rerun vm3:42 --replace ./in.restart -- lmp -in in.restart
      qur rerun vm3:42 --edit in.lammps
      qur rerun vm3:42 --depends-on 42 -- lmp -in in.next
    """
    spec = parse_args(ctx.meta.get("qur.rerun.args", []))
    conn = _conn()
    try:
        if spec["resume"]:
            run = _get_run(conn, spec["resume_key"], "rerun", host=spec["resume_host"])
            key = run["run_key"]
            saved = _load_spec(key)
            spec = {**saved, "pull": bool(run["pull"]), "wait": spec["wait"], "dry_run": False}
        else:
            run, key = None, ""
    finally:
        conn.close()
    host_name = spec["host"] if run is not None else split_job_ref(spec["ref"])[0]
    job_id = spec["job_id"] if run is not None else split_job_ref(spec["ref"])[1]
    host = hostcfg.get(host_name)
    if host is None:
        raise typer.BadParameter(f"unknown host {host_name!r}; configured: {', '.join(hostcfg.names()) or 'none'} (qur hosts add)")
    try:
        if run is None:
            _step(1, f"looking up {host_name}:{job_id}")
            job = _show(host, job_id)
            if not job.get("job_dir"):
                _fail(f"job {host_name}:{job_id} has no job folder: it was not started with `qur run`. Use `qur run` for a new folder.")
            changing = bool(spec["replace"] or spec["edit"])
            check_folder_free(folder_users(host, job), spec["forward"], changing, spec["force"])
            raw_cmd = job.get("raw_cmd")
            if not spec["cmd"] and not raw_cmd:
                _fail(f"job {host_name}:{job_id} has no recorded command (old or malformed worker row); give one after `--`")
            cmd = spec["cmd"]
            if not cmd:
                try:
                    cmd = json.loads(str(raw_cmd))
                except ValueError:
                    _fail(f"job {host_name}:{job_id} has an unreadable recorded command; give one after `--`")
            carried, secret = carried_options(job, spec["forward"])
            if secret:
                typer.echo(f"      not carried (secret-looking names): {', '.join(secret)}; give --env again if the job needs them", err=True)
            add_args = [*carried, *spec["forward"]]
            if spec["dry_run"]:
                _dry_run(host_name, host, job, spec, cmd, add_args)
                return
            _step(2, "preparing files")
            edited = edit_files(host, job_id, spec["edit"]) if spec["edit"] else {}
            key = uuid.uuid4().hex
            files: list[str] = []
            payload = _payload_path(key)
            if spec["replace"] or edited:
                try:
                    files = build_update_payload(spec["replace"], edited, payload)
                except ValueError as e:
                    _fail(str(e))
            elif spec["edit"]:
                _fail("no file was changed and no --replace given: nothing to rerun")
            saved = {
                "host": host_name, "job_id": job_id, "remote_dir": job["job_dir"], "files": files, "cmd": cmd,
                "add_args": add_args, "venv": spec["venv"], "setup": spec["setup"], "force": spec["force"], "pull": spec["pull"],
            }
            conn = _conn()
            try:
                _new_run(conn, key, host_name, None, spec["pull"], kind="rerun")
                _update_run(conn, key, remote_dir=job["job_dir"], parent_remote_id=job_id)
                run = _get_run(conn, key, "rerun")
            finally:
                conn.close()
            _save_spec(key, saved)
            spec = {**saved, "wait": spec["wait"]}
            typer.echo(f"run key {key}")
        new_id = drive(host_name, host, key, spec, run)
        _redact_spec(key)
        _payload_path(key).unlink(missing_ok=True)
    except remote.RemoteTransportError as e:
        typer.echo(f"Error: {remote.safe_text(e)}", err=True)
        if key:
            typer.echo(f"The rerun is saved. Continue with: qur rerun --resume {key}", err=True)
        raise typer.Exit(code=1) from None
    except typer.Exit as e:
        if e.exit_code and key:
            typer.echo(f"The rerun is saved. Continue with: qur rerun --resume {key}", err=True)
        raise
    if spec["wait"]:
        raise typer.Exit(code=wait_for(host, new_id))


def _dry_run(host_name: str, host: dict[str, Any], job: dict[str, Any], spec: dict[str, Any], cmd: list[str], add_args: list[str]) -> None:
    folder = job["job_dir"]
    typer.echo(f"dry run: rerun of {host_name}:{job['id']} in {folder}; nothing was changed")
    for item in spec["replace"]:
        try:
            src, rel = _split_replace(item)
        except ValueError as e:
            _fail(str(e))
        typer.echo(f"  would replace: {rel}  (from {src})")
    for rel in spec["edit"]:
        typer.echo(f"  would open in $EDITOR: {rel}  (sent only if changed)")
    if spec["replace"] or spec["edit"]:
        typer.echo(f"  old copies would go to {folder}/.qur-prev/<timestamp>/")
    argv = ["add", "--dry-run", "--rerun-of", str(job["id"]), *add_args, "--", *cmd]
    code, out, err = worker_shell(host, add_script(host, folder, spec["venv"], spec["setup"], argv))
    if code == EXIT_SETUP_FAILED:
        _fail("the environment setup failed on the worker:\n" + (err or out).strip())
    typer.echo((out or err).rstrip())
    raise typer.Exit(code=0 if code == 0 else 1)
