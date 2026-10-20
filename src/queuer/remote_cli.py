"""Host side of remote workers (QUEUER_REMOTE_PLAN.md, phase 2.3 / 2.4 / 2.4b).

- `qur hosts` (list, add, remove, edit, check): manage `hosts.json` and preflight a worker.
- `qur -H HOST <anything>` (or `QUR_HOST=HOST`): run any qur command on a worker over ssh.
- `qur show vm3:42`: `HOST:ID` references, rewritten to `-H vm3` plus bare ids.

The passthrough is one hook: `RemoteGroup` (the class of the root Typer app) looks at the
arguments before click parses them. Nothing here keeps a list of worker commands.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import sqlite3
import sys
from collections.abc import Sequence
from typing import Any, Optional

import typer
from rich.console import Console
from rich.markup import escape
from rich.table import Table
from typer.core import TyperGroup

from queuer import client, db, hostcfg, notify, remote
from queuer.paths import _default_db_path
from queuer.remote_watch import cached_jobs
from queuer.remote_worker import remote_app

console = Console()

# Subcommands whose positional arguments are job ids and so may be written `HOST:ID`.
# (`pull` and `rerun` take HOST:ID too, but they are host-side commands that read the reference
# themselves; they are not forwarded.)
REF_COMMANDS = frozenset({"show", "logs", "cancel", "requeue", "wait", "rm", "edit"})
LOCAL_ONLY = frozenset({"hosts", "run", "rerun", "pull"})  # never forwarded: host-side commands (they drive workers themselves)
ENV_LOCAL = frozenset({"remote"})  # QUR_HOST never sends these to a worker (`qur -H HOST remote ...` still does)
ENV_NEEDS_EXPLICIT = frozenset({"daemon", "connect", "clear"})  # too destructive to reach a worker through an environment variable
ID_SPEC_RE = re.compile(r"^\d+(-\d+)?(,\d+(-\d+)?)*$")  # 42, 31-33, 12,14
REF_RE = re.compile(r"^([A-Za-z0-9_-]{1,16}):(\d+(?:-\d+)?(?:,\d+(?:-\d+)?)*)$")
HOST_ENV = "QUR_HOST"


# ---------------------------------------------------------------------------
# HOST:ID rewrite and -H passthrough
# ---------------------------------------------------------------------------


def split_ref(arg: str, known: Sequence[str]) -> tuple[str, str] | None:
    """`vm3:42` -> ("vm3", "42") when the whole argument matches and vm3 is a configured host."""
    m = REF_RE.match(arg)
    if m and m.group(1) in known:
        return m.group(1), m.group(2)
    return None


def _value_options(cmd: Any) -> set[str]:
    """Option strings of `cmd` that consume the next argument (their value is never rewritten)."""
    found: set[str] = set()
    for param in getattr(cmd, "params", []):
        # duck-typed: typer >= 0.27 bundles its own click, so isinstance against `click` would never match
        if getattr(param, "param_type_name", "") == "option" and not param.is_flag and not param.count and param.nargs == 1:
            found.update(param.opts)
            found.update(param.secondary_opts)
    return found


def rewrite_refs(
    args: Sequence[str], known: Sequence[str], value_options: set[str] | None = None
) -> tuple[str | None, list[str], bool]:
    """Rewrite `HOST:ID` positionals (before any `--`, never an option's value) to bare ids.

    Returns (host named by the references or None, new args, whether a bare local id is also
    present). Two different hosts raise typer.BadParameter."""
    value_options = value_options or set()
    host: str | None = None
    out: list[str] = []
    bare_id = False
    skip_next = False
    for i, arg in enumerate(args):
        if skip_next:
            out.append(arg)
            skip_next = False
            continue
        if arg == "--":
            out.extend(args[i:])
            break
        if arg.startswith("-"):
            out.append(arg)
            if arg in value_options:
                skip_next = True
            continue
        ref = split_ref(arg, known)
        if ref is None:
            if ID_SPEC_RE.match(arg):
                bare_id = True
            out.append(arg)
            continue
        if host is not None and ref[0] != host:
            raise typer.BadParameter(f"job references name two hosts ({host} and {ref[0]}); use one host per command")
        host = ref[0]
        out.append(ref[1])
    return host, out, bare_id


def _take_host(args: list[str]) -> tuple[str | None, list[str]]:
    """Strip a leading `-H NAME` / `--host NAME` / `--host=NAME` / `-HNAME` (before the subcommand)."""
    if not args:
        return None, args
    first = args[0]
    if first in ("-H", "--host"):
        if len(args) < 2:
            raise typer.BadParameter(f"Option '{first}' requires an argument.")
        return args[1], args[2:]
    if first.startswith("--host="):
        return first.split("=", 1)[1], args[1:]
    if first.startswith("-H") and len(first) > 2 and not first.startswith("--"):
        return first[2:], args[1:]
    return None, args


def _wants_tty(sub: str | None, rest: Sequence[str]) -> bool:
    """A terminal is needed by `edit` and by following logs; never for --json output."""
    if "--json" in rest or not (sys.stdin.isatty() and sys.stdout.isatty()):
        return False
    if sub == "edit":
        return True
    if sub == "logs":
        return "--tail" in rest
    if sub == "daemon":
        return "logs" in rest and ("-f" in rest or "--follow" in rest)
    return False


def _expand_ids(args: Sequence[str]) -> list[int]:
    ids: list[int] = []
    for a in args:
        if ID_SPEC_RE.match(a):
            for part in a.split(","):
                lo, _, hi = part.partition("-")
                ids += range(int(lo), int(hi or lo) + 1)
    return ids


def _show_cached(host_name: str, args: Sequence[str], why: str) -> int:
    """`qur show HOST:ID` while the worker cannot be reached: the snapshot the watcher stored, marked stale."""
    ids = _expand_ids(args[1:])
    cache = {j["remote_id"]: j for j in cached_jobs(host_name)}
    found = [cache[i] for i in ids if i in cache]
    if not found:
        typer.echo(f"Error: {why}; and no cached snapshot of {host_name}:{ids[0] if ids else '?'} (the watcher stores jobs when they finish)", err=True)
        return 1
    if "--json" in args:
        out = [{**j["snapshot"], "stale": True, "seen_at": j["seen_at"]} for j in found]
        typer.echo(json.dumps(out[0] if len(ids) == 1 else out, indent=2))
        return 0
    for j in found:
        s = j["snapshot"]
        typer.echo(f"{host_name}:{j['remote_id']}  {remote.safe_text(s.get('status'))}  (cached {remote.safe_text(j['seen_at'])}; {remote.safe_text(why)})")
        for label, value in (("command", notify._command(s)), ("duration", notify._duration(s)), ("exit code", s.get("exit_code")),
                             ("usage", notify._usage_line(s)), ("job folder", s.get("job_dir"))):
            if value not in (None, ""):
                typer.echo(f"  {label}: {remote.safe_text(value)}")
    return 0


def passthrough(host_name: str, args: Sequence[str]) -> int:
    """Run `qur <args>` on the worker with this terminal; return its exit code."""
    host = hostcfg.get(host_name)
    if host is None:
        raise typer.BadParameter(f"unknown host {host_name!r}; configured: {', '.join(hostcfg.names()) or 'none'} (qur hosts add)")
    sub = args[0] if args and not args[0].startswith("-") else None
    if list(args[:2]) == ["remote", "gc"] and "--jobs-dir" not in args and host.get("jobs_dir"):
        args = [*args, "--jobs-dir", host["jobs_dir"]]  # the worker's session does not know where this host sends folders
    if sub == "show":  # live when the worker answers, else the watcher's stored snapshot (works offline)
        try:
            code, out, err = remote.ssh_run(host, list(args), timeout=30)
            sys.stdout.write(remote.safe_text(out.decode("utf-8", "replace"), multiline=True))
            sys.stderr.write(remote.safe_text(err, multiline=True))
            return code
        except (remote.HostUnreachable, remote.AuthFailed) as e:
            return _show_cached(host_name, args, f"{host_name} is unreachable: {str(e).splitlines()[0] if str(e) else 'no answer'}")
        except remote.RemoteTransportError as e:
            typer.echo(f"Error: {remote.safe_text(e)}", err=True)
            return 1
    try:
        return remote.ssh_exec(host, list(args), tty=_wants_tty(sub, args))
    except remote.RemoteTransportError as e:
        typer.echo(f"Error: {remote.safe_text(e)}", err=True)
        return 1
    except KeyboardInterrupt:
        return 130


class RemoteGroup(TyperGroup):
    """Root group: forwards `-H HOST ...` / `QUR_HOST` / `HOST:ID` commands to the worker before click parses them."""

    def parse_args(self, ctx: Any, args: list[str]) -> list[str]:
        explicit, rest = _take_host(list(args))
        sub = rest[0] if rest and not rest[0].startswith("-") else None
        if sub in LOCAL_ONLY:
            if explicit:
                raise typer.BadParameter(f"`qur {sub}` runs on this machine and drives workers itself; it cannot be used with -H")
            return super().parse_args(ctx, args)
        host = explicit
        from_env = False
        if host is None and sub is not None and sub not in ENV_LOCAL:
            host = os.environ.get(HOST_ENV) or None
            from_env = host is not None
        ref_host: str | None = None
        if sub in REF_COMMANDS and hostcfg.default_path().exists():
            known = hostcfg.names()
            if known:
                ref_host, new_tail, bare = rewrite_refs(rest[1:], known, _value_options(self.commands.get(sub)))
                if ref_host is not None:
                    if explicit and ref_host != explicit:
                        raise typer.BadParameter(f"-H {explicit} conflicts with a {ref_host}: job reference")
                    if host is None and bare:
                        raise typer.BadParameter(f"mixes local job ids with {ref_host}: ids; write {ref_host}:ID for each")
                    rest = [rest[0], *new_tail]
                    host = ref_host
                    from_env = False
        if host is None:
            return super().parse_args(ctx, args)
        if from_env:
            if sub in ENV_NEEDS_EXPLICIT:
                raise typer.BadParameter(
                    f"{HOST_ENV}={host} is set, so `qur {sub}` would run on the worker; write `qur -H {host} {sub}` to do that, "
                    f"or unset {HOST_ENV} to run it here"
                )
            typer.echo(f"(on {host} via {HOST_ENV})", err=True)
        ctx.exit(passthrough(host, rest))
        return []  # unreachable: ctx.exit raises


# ---------------------------------------------------------------------------
# qur hosts
# ---------------------------------------------------------------------------

hosts_app = typer.Typer(
    help="Workers this machine can drive over ssh (hosts.json). Run any command on one with `qur -H NAME ...`.",
    no_args_is_help=False,
    invoke_without_command=True,
)


def _die(msg: str, code: int = 1) -> None:
    typer.echo(f"Error: {msg}", err=True)
    raise typer.Exit(code=code)


def _last_seen() -> dict[str, str | None]:
    """name -> last_ok_at from the host table the watcher fills (read-only; never creates the database)."""
    path = _default_db_path()
    if not path.exists():
        return {}
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            return {n: t for n, t in conn.execute("SELECT name, last_ok_at FROM remote_hosts")}
        finally:
            conn.close()
    except sqlite3.Error:
        return {}


def _check_not_local_id(name: str, entry: dict[str, Any]) -> None:
    """A worker id equal to this machine's own qur id would make Telegram buttons act on LOCAL jobs."""
    local = notify.load()
    worker_id = entry.get("id") or name
    if local is not None and worker_id == local["qur_id"]:
        _die(f"id {worker_id!r} is this machine's own qur id (notify.json); give the worker another one with --id")


def _save(cfg: dict[str, Any]) -> None:
    try:
        hostcfg.save(cfg)
    except (ValueError, OSError) as e:
        _die(str(e))


def _problem_note() -> None:
    problem = hostcfg.check_permissions()
    if problem:
        typer.echo(f"Warning: {problem}", err=True)


@hosts_app.callback()
def hosts_main(ctx: typer.Context) -> None:
    if ctx.invoked_subcommand is None:
        hosts_list(as_json=False)


@hosts_app.command("list")
def hosts_list(as_json: bool = typer.Option(False, "--json", help="Print JSON")) -> None:
    """List configured workers: name, ssh target, id, enabled, last seen."""
    _problem_note()
    cfg = hostcfg.load(check=False)["hosts"]
    seen = _last_seen()
    if as_json:
        typer.echo(json.dumps([{"name": n, **h, "last_seen": seen.get(n)} for n, h in sorted(cfg.items())], indent=2))
        return
    if not cfg:
        console.print("No workers configured. Add one: qur hosts add NAME --ssh TARGET")
        return
    table = Table(show_edge=False, pad_edge=False)
    for col in ("NAME", "SSH", "ID", "ENABLED", "LAST SEEN"):
        table.add_column(col)
    for name, h in sorted(cfg.items()):
        table.add_row(escape(name), escape(h["ssh"]), escape(h["id"]), "yes" if h["enabled"] else "no", escape(seen.get(name) or "-"))
    console.print(table)


def _apply(entry: dict[str, Any], *, ssh, id_, jobs_dir, setup, pull_dir, poll_secs, ssh_option, enabled) -> None:
    if ssh is not None:
        entry["ssh"] = ssh
    if id_ is not None:
        entry["id"] = id_
    if jobs_dir is not None:
        entry["jobs_dir"] = jobs_dir
    if setup is not None:
        entry["setup"] = setup or None  # --setup '' clears it
    if pull_dir is not None:
        entry["pull_dir"] = pull_dir
    if poll_secs is not None:
        entry["poll_secs"] = poll_secs
    if ssh_option is not None:
        entry["ssh_options"] = list(ssh_option)
    if enabled is not None:
        entry["enabled"] = enabled


@hosts_app.command("add")
def hosts_add(
    name: str = typer.Argument(..., help="Short name used with -H and in NAME:ID (letters, digits, _ -)"),
    ssh: str = typer.Option(..., "--ssh", metavar="TARGET", help="[user@]host or an alias from ~/.ssh/config"),
    id_: Optional[str] = typer.Option(None, "--id", help="Label shown in Telegram and output (default: NAME)"),
    jobs_dir: Optional[str] = typer.Option(None, "--jobs-dir", help="Where job folders go on the worker (default ~/qur-jobs)"),
    setup: Optional[str] = typer.Option(None, "--setup", metavar="CMD", help="Shell snippet run on the worker before every run, e.g. 'source ~/venvs/md/bin/activate'"),
    pull_dir: Optional[str] = typer.Option(None, "--pull-dir", help="Where pulled tarballs go (default ~/qur-results/ID)"),
    poll_secs: Optional[int] = typer.Option(None, "--poll-secs", help="Watcher poll interval, seconds (default 15)"),
    ssh_option: Optional[list[str]] = typer.Option(None, "--ssh-option", metavar="OPT", help="Extra ssh argument, repeatable (e.g. --ssh-option -i --ssh-option ~/.ssh/qur_vm3). qur never reads or copies the key."),
) -> None:
    """Add a worker. Access (your public key in the worker's authorized_keys) is set up by you, outside qur."""
    cfg = hostcfg.load(check=False)
    if name in cfg["hosts"]:
        _die(f"host {name!r} already exists (qur hosts edit {name})")
    entry: dict[str, Any] = {}
    _apply(entry, ssh=ssh, id_=id_, jobs_dir=jobs_dir, setup=setup, pull_dir=pull_dir, poll_secs=poll_secs, ssh_option=ssh_option, enabled=None)
    cfg["hosts"][name] = entry
    try:
        hostcfg.check_host(name, entry)
    except ValueError as e:
        _die(str(e))
    _check_not_local_id(name, entry)
    _save(cfg)
    console.print(f"[green]added {escape(name)}[/green]  next: qur hosts check {escape(name)}")


@hosts_app.command("edit")
def hosts_edit(
    name: str = typer.Argument(...),
    ssh: Optional[str] = typer.Option(None, "--ssh", metavar="TARGET"),
    id_: Optional[str] = typer.Option(None, "--id"),
    jobs_dir: Optional[str] = typer.Option(None, "--jobs-dir"),
    setup: Optional[str] = typer.Option(None, "--setup", metavar="CMD", help="New setup snippet; '' removes it"),
    pull_dir: Optional[str] = typer.Option(None, "--pull-dir"),
    poll_secs: Optional[int] = typer.Option(None, "--poll-secs"),
    ssh_option: Optional[list[str]] = typer.Option(None, "--ssh-option", metavar="OPT", help="Replaces the whole list; repeatable"),
    enabled: Optional[bool] = typer.Option(None, "--enabled/--disabled", help="--disabled: the watcher skips it; -H still works"),
) -> None:
    """Change fields of a worker; only the options you give are changed."""
    cfg = hostcfg.load(check=False)
    if name not in cfg["hosts"]:
        _die(f"no host {name!r} (qur hosts list)")
    entry = dict(cfg["hosts"][name])
    _apply(entry, ssh=ssh, id_=id_, jobs_dir=jobs_dir, setup=setup, pull_dir=pull_dir, poll_secs=poll_secs, ssh_option=ssh_option, enabled=enabled)
    try:
        cfg["hosts"][name] = hostcfg.check_host(name, entry)
    except ValueError as e:
        _die(str(e))
    _check_not_local_id(name, cfg["hosts"][name])
    _save(cfg)
    console.print(f"[green]updated {escape(name)}[/green]")


@hosts_app.command("remove")
def hosts_remove(name: str = typer.Argument(...)) -> None:
    """Remove a worker from hosts.json (nothing on the worker is touched)."""
    cfg = hostcfg.load(check=False)
    if name not in cfg["hosts"]:
        _die(f"no host {name!r} (qur hosts list)")
    del cfg["hosts"][name]
    _save(cfg)
    try:  # the cached jobs, runs and cursor of a removed worker would only be orphans
        conn = db.connect(_default_db_path())
        try:
            db.forget_remote_host(conn, name)
        finally:
            conn.close()
    except sqlite3.Error:
        pass
    console.print(f"[green]removed {escape(name)}[/green]")


# --- check -----------------------------------------------------------------

_JOBS_DIR_SCRIPT = (
    'd="${1/#\\~/$HOME}"; '
    'if [ -d "$d" ]; then [ -w "$d" ] && echo "$d writable" || { echo "$d not writable"; exit 1; }; '
    'elif [ -w "$(dirname "$d")" ]; then echo "$d will be created"; '
    'else echo "$d missing and its parent is not writable"; exit 1; fi'
)


def _env_script(expect: Sequence[str]) -> str:
    lines = [
        "python -V 2>&1 || echo 'python: not found'",
        'echo "python: $(command -v python || echo none)"',
        'echo "VIRTUAL_ENV=${VIRTUAL_ENV:-}"',
        "miss=0",
    ]
    for cmd in expect:
        q = shlex.quote(cmd)
        lines.append(f"p=$(command -v {q}) && printf '%s: %s\\n' {q} \"$p\" || {{ printf '%s: MISSING\\n' {q}; miss=1; }}")
    lines.append("exit $miss")
    return "\n".join(lines)


def _host_key_hint(host: dict[str, Any], err: object) -> str:
    """qur's ssh runs with BatchMode=yes and cannot ask about an unknown host key."""
    if "host key verification failed" in str(err).lower():
        return f"\n(an unknown host key: run `ssh {host.get('ssh', 'HOST')}` once by hand and answer yes, then check again)"
    return ""


_LOGIN_INFO = 'export PATH="$HOME/.local/bin:$PATH"; exec qur remote info --json'


def _env_mismatch(host: dict[str, Any], info: dict[str, Any]) -> str:
    """Empty when the worker's database and socket are the same in a plain ssh command (polling,
    pull, `qur -H`) and in the login shell `qur run` queues in; else what differs and what to do.
    QUEUER_DB_PATH / QUEUER_SOCKET_PATH exported only in ~/.profile (or a systemd override) are the usual cause."""
    try:
        code, out, _err = remote.ssh_run(host, ["bash", "-lc", _LOGIN_INFO], raw=True, timeout=30)
        login = remote.parse_json(out.decode("utf-8", "replace")) if code == 0 else None
    except (remote.RemoteTransportError, ValueError):
        return ""  # cannot compare: the steps around this one report the real problem
    if not isinstance(login, dict) or "db_path" not in info or "db_path" not in login:
        return ""  # an older worker without the fields
    diffs = [f"{k}: plain ssh {info.get(k)}, login shell {login.get(k)}" for k in ("db_path", "socket_path") if info.get(k) != login.get(k)]
    if not diffs:
        return ""
    return ("; ".join(diffs) + ". Jobs queued by `qur run` would go to a different database than the one the watcher polls. "
            "Export QUEUER_DB_PATH / QUEUER_SOCKET_PATH where non-login ssh commands read them (top of ~/.bashrc, above the "
            "`case $- in *i*)` guard), not only in ~/.profile")


def check_host(name: str, host: dict[str, Any], expect: Sequence[str] = ()) -> list[dict[str, Any]]:
    """Preflight one worker. Each step: {"step", "ok", "detail"}; stops after a step later ones depend on."""
    steps: list[dict[str, Any]] = []

    def add(step: str, ok: bool, detail: str) -> bool:
        steps.append({"step": step, "ok": ok, "detail": detail})
        return ok

    try:
        code, _out, err = remote.ssh_run(host, ["true"], raw=True, timeout=30)
        if not add("ssh reachable", code == 0, "login ok (BatchMode, no passphrase prompt)" if code == 0 else (err.strip() or f"exit {code}") + _host_key_hint(host, err)):
            return steps
    except remote.AuthFailed as e:
        add("ssh reachable", False, f"login refused: {e}{_host_key_hint(host, e)}")
        return steps
    except remote.RemoteTransportError as e:
        add("ssh reachable", False, str(e))
        return steps

    try:
        code, out, err = remote.ssh_run(host, ["remote", "info", "--json"], timeout=30)
    except remote.RemoteTransportError as e:
        add("remote qur found", False, str(e))
        return steps
    add("remote qur found", True, "qur runs on the worker")
    info: dict[str, Any] = {}
    if code == 0:
        try:
            info = remote.parse_json(out.decode("utf-8", "replace"))
            remote.check_protocol(info)
        except remote.ProtocolMismatch as e:
            add("protocol compatible", False, str(e))
            return steps
        except ValueError:
            info = {}
    if not info:
        add("protocol compatible", False, (err.strip() or f"`qur remote info` exited {code}") + " (is the worker's qur older than this one?)")
        return steps
    add("protocol compatible", True, f"protocol {info.get('protocol')}, queuer {info.get('queuer_version')}, machine {info.get('machine_id')}")
    add("daemon running", bool(info.get("daemon")), "queuerd answers" if info.get("daemon") else "queuerd is not running on the worker (qur daemon start)")

    mismatch = _env_mismatch(host, info)
    add("same environment in every ssh session", not mismatch, mismatch or "polling, pull and `qur run` see the same database and socket")

    jobs_dir = host.get("jobs_dir") or info.get("jobs_dir") or hostcfg.DEFAULT_JOBS_DIR
    try:
        code, out, err = remote.ssh_run(host, ["bash", "-c", _JOBS_DIR_SCRIPT, "_", jobs_dir], raw=True, timeout=30)
        add("jobs dir writable", code == 0, (out.decode("utf-8", "replace").strip() or err.strip() or f"exit {code}"))
    except remote.RemoteTransportError as e:
        add("jobs dir writable", False, str(e))

    try:
        code, out, _err = remote.ssh_run(host, ["doctor", "--json", "--no-hosts"], timeout=60)
        linger = None
        if code in (0, 1):
            try:
                checks = json.loads(out.decode("utf-8", "replace")).get("checks", [])
                linger = next((c for c in checks if c.get("name") == "linger"), None)
            except ValueError:
                linger = None
        if linger is None:
            add("linger enabled", False, "could not read the worker's `qur doctor --json`")
        else:
            add("linger enabled", linger.get("status") == "OK", linger.get("detail", ""))
    except remote.RemoteTransportError as e:
        add("linger enabled", False, str(e))

    try:
        code, out, err = remote.ssh_run(
            host, ["bash", "-c", _env_script(expect)], raw=True, use_setup=True, timeout=60,
        )
        text = out.decode("utf-8", "replace").strip() or err.strip()
        label = "setup works" if host.get("setup") else "environment"
        add(label, code == 0, text.replace("\n", "; ") if code == 0 else (text or f"exit {code}").replace("\n", "; "))
    except remote.RemoteTransportError as e:
        add("setup works", False, str(e))
    return steps


@hosts_app.command("check")
def hosts_check(
    name: Optional[str] = typer.Argument(None, help="Worker to check (default: every enabled one)"),
    expect: Optional[list[str]] = typer.Option(None, "--expect", metavar="CMD", help="Command that must be found after `setup` (repeatable), e.g. --expect lmp"),
    as_json: bool = typer.Option(False, "--json", help="Print JSON"),
    from_daemon: bool = typer.Option(False, "--from-daemon", help="Instead: ask queuerd to reach the worker, proving the daemon (not this shell) can log in"),
) -> None:
    """Preflight a worker: ssh, remote qur, protocol, daemon, jobs dir, linger, and that `setup` works.

    \b
    Examples:
      qur hosts check vm3
      qur hosts check vm3 --expect lmp --expect uv
    """
    cfg = hostcfg.load()["hosts"]
    if name is not None:
        if name not in cfg:
            _die(f"no host {name!r} (qur hosts list)")
        targets = [name]
    else:
        targets = [n for n, h in sorted(cfg.items()) if h["enabled"]]
        if not targets:
            _die("no enabled workers (qur hosts add)")
    if from_daemon:
        results: dict[str, list[dict[str, Any]]] = {}
        for n in targets:
            try:
                data = client.send("remote_check", host=n)
                results[n] = [{"step": "daemon ssh", "ok": bool(data["ok"]), "detail": data["detail"]}]
            except (client.DaemonNotRunning, client.DaemonError) as e:
                results[n] = [{"step": "daemon ssh", "ok": False, "detail": str(e)}]
    else:
        results = {n: check_host(n, {**cfg[n], "name": n}, expect or ()) for n in targets}
    failed = any(not s["ok"] for steps in results.values() for s in steps)
    if as_json:
        typer.echo(json.dumps({"ok": not failed, "hosts": results}, indent=2))
        raise typer.Exit(code=1 if failed else 0)
    for n, steps in results.items():
        console.print(f"[bold]{escape(n)}[/bold]")
        width = max(len(s["step"]) for s in steps)
        for s in steps:
            tag = "[green]ok  [/green]" if s["ok"] else "[bold red]FAIL[/bold red]"
            console.print(f"  {tag}  {escape(s['step']):<{width}}  {escape(s['detail'])}", highlight=False)
    raise typer.Exit(code=1 if failed else 0)


# ---------------------------------------------------------------------------
# qur remote list (host side: what the watcher has seen, works while a worker is offline)
# ---------------------------------------------------------------------------


@remote_app.command("list")
def remote_list(
    host: Optional[str] = typer.Argument(None, help="Only this worker"),
    as_json: bool = typer.Option(False, "--json", help="Print JSON"),
) -> None:
    """Worker jobs this host has seen finish (from the watcher's cache; stale while the worker is unreachable).

    Only final outcomes are cached: queued and running worker jobs are not listed here (ask the worker: `qur -H HOST list`).
    """
    rows = cached_jobs(host)
    if as_json:
        typer.echo(json.dumps([{k: v for k, v in r.items()} for r in rows], indent=2))
        return
    if not rows:
        typer.echo("no worker jobs cached yet (the daemon records them as they finish)")
        return
    table = Table()
    for col in ("job", "status", "duration", "usage", "folder", "seen"):
        table.add_column(col)
    for r in rows:
        s = r["snapshot"]
        seen = (r["seen_at"] or "-")[:19].replace("T", " ") + (" (stale)" if r["stale"] else "")
        def cell(v: Any) -> str:
            return escape(remote.safe_text(v))

        table.add_row(f"{cell(r['host'])}:{r['remote_id']}", cell(r["status"]), cell(notify._duration(s) or "-"),
                      cell(notify._usage_line(s) or "-"), cell(s.get("job_dir") or "-"), cell(seen))
    console.print(table)
