"""queuer: CLI client for the queuerd job queue daemon."""

from __future__ import annotations

import json
import os
import shlex
import shutil
import stat
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import typer
from rich.console import Console
from rich.table import Table

from queuer import client, db
from queuer.daemon import _default_db_path, _default_log_dir
from queuer.db import DEFAULT_CHANNEL
from queuer.pathresolve import SHELL_INTERPRETERS

app = typer.Typer(help="queuer: a background job queue with parallel channels")
console = Console()


SHELL_METACHARS = frozenset("|&;<>`$*?~\n")


def _is_explicit_shell_c(cmd: list[str]) -> bool:
    """True if the user hand-wrote `bash -c '...'` (or sh/zsh/etc) themselves."""
    return len(cmd) == 3 and os.path.basename(cmd[0]) in SHELL_INTERPRETERS and cmd[1] == "-c"


def _needs_shell(script: str) -> bool:
    """True if `script` contains syntax only a real shell can interpret --
    pipes, redirects, chaining, substitution, globbing, expansion. Anything
    without these is just a program name plus plain arguments and can be
    exec'd directly.

    Deliberately does NOT include `(`, `)`, `{`, `}`, `[`, `]` -- those show
    up constantly in ordinary inline code (`python -c "print(1)"`, a JSON
    payload, a regex) and would force bash for things that are not shell
    syntax at all. The chars kept here are ones with essentially no other
    reading in a single quoted-as-one-word command.
    """
    return any(c in SHELL_METACHARS for c in script)


def _build_job_argv(cmd: list[str]) -> list[str]:
    """Turn the words typed after `--` into the argv queuer actually execs.

    Bash is only used when the command genuinely needs shell syntax --
    &&, |, ;, <, >, $VARS, backticks, globs, subshells. Everything else is
    exec'd directly (no `bash -c` enclosure), so PATH/path resolution runs
    on the real argv instead of being buried inside an opaque script string,
    and there's no double round of shell-quoting to go messy on display.

    Why detecting shell syntax is only possible/needed in the single-word
    case: argv after `--` has already been through the *invoking* shell's
    own tokenizing once. Anything typed unquoted (a bare `<` or `&&`) was
    already consumed by that outer shell before queuer's process even
    started -- no code here can recover it. The only way such syntax
    reaches us intact is if the whole command was quoted as ONE shell word,
    e.g.:
        queuer add -- 'g16 < input.gjf'
        queuer add -- 'az login && ./deploy.sh'
    So a single-element `cmd` is checked for shell metacharacters and only
    wrapped in `bash -c` if it actually has any; otherwise it's split with
    shlex (POSIX quoting rules, same as a shell would) into a plain argv.

    2+ words (the common case: a program plus its own flags/args, possibly
    including one quoted multi-word argument like a JSON payload, e.g.
    `curl -d '{"key": "value"}' url`) can never carry surviving shell syntax
    -- the outer shell already tokenized them -- so they're used as-is,
    untouched, no bash, no re-quoting.

    Special case: if the user already hand-wrote `bash -c '...'` (the old
    workaround), use it as-is instead of wrapping it a second time.
    """
    if not cmd:
        raise ValueError("empty command")
    if _is_explicit_shell_c(cmd):
        return list(cmd)
    if len(cmd) == 1:
        script = cmd[0]
        if not script:
            raise ValueError("empty command")
        if _needs_shell(script):
            return ["bash", "-c", script]
        try:
            words = shlex.split(script)
        except ValueError:
            # unbalanced quotes etc -- can't safely split this ourselves,
            # so hand it to a real shell exactly as typed.
            return ["bash", "-c", script]
        if not words:
            raise ValueError("empty command")
        return words
    return list(cmd)


def _resolve_interpreter(cmd: list[str]) -> list[str]:
    """Resolve cmd[0] (the program name) to an absolute path via PATH lookup,
    if it's a bare command name. Leaves every other token completely
    untouched.

    Runs AFTER `_build_job_argv`, on the final argv queuer will exec. For a
    direct-exec argv (the common case now), this pins the exact interpreter
    (e.g. a venv's python) at enqueue time using the client's own PATH,
    instead of depending on the daemon reproducing it at run time. For a
    `bash -c '<script>'` shape (only produced when the command genuinely
    needs shell syntax), cmd[0] is "bash" itself and this is a no-op --
    resolving into the opaque script text is `pathresolve.py`'s job to
    deliberately avoid, not this function's.

    Deliberately narrower than pathresolve's own per-token resolution: it
    only ever touches cmd[0]. Running the same logic over every token would
    also walk into any inline source text that follows a `-c`/`-e` flag
    (`python -c "..."`, `perl -e "..."`, `node -e "..."`, etc.) -- those are
    opaque code, not paths, and rewriting a literal '/' inside them would
    silently corrupt the script. pathresolve.py's own shell -c exception
    exists for exactly this hazard, but only recognizes shell interpreters
    (SHELL_INTERPRETERS), not other languages' inline-eval flags, so this
    stays safe by never resolving past argv[0].
    """
    if not cmd:
        return cmd
    if _is_explicit_shell_c(cmd):
        # user hand-wrote `bash -c '...'` themselves, or `_build_job_argv`
        # produced this shape because the command needed real shell syntax
        # -- either way pathresolve resolves the interpreter itself
        # daemon-side, and the script text is deliberately left alone.
        return cmd
    prog = cmd[0]
    if "/" in prog or Path(prog).exists():
        # already a path, or a bare name that happens to exist relative to
        # cwd -- leave it for pathresolve's own (later, cwd-aware) handling
        return cmd
    which = shutil.which(prog)
    if which:
        return [os.path.abspath(which), *cmd[1:]]
    return cmd


def _stdin_was_redirected_from_file() -> bool:
    """True if fd 0 is a regular file, i.e. the invoking shell did `< somefile`
    on *this* process. A pipe (`cmd | queuer ...`) shows up as a FIFO, not a
    regular file, so that case is deliberately not flagged here."""
    try:
        return stat.S_ISREG(os.fstat(0).st_mode)
    except OSError:
        return False


def _call(cmd: str, **kwargs: Any) -> Any:
    """Call the daemon, printing a clean one-line error and exiting
    nonzero on any failure rather than letting a traceback surface."""
    try:
        return client.send(cmd, **kwargs)
    except (client.DaemonNotRunning, client.DaemonError) as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(code=1)


def _display_quote(word: str) -> str:
    """Quote one argv word for human display -- round-trips exactly via
    shlex.split (safe to copy-paste back into a real shell), but picks
    whichever quoting style is actually clean for this word instead of
    always defaulting to POSIX single-quote escaping.

    shlex.quote's escaping for a single quote *inside* a single-quoted
    string is the standard `'"'"'` dance -- correct, but it's real quote
    soup the moment a word contains more than one embedded `'` (e.g. a
    Python one-liner with two string literals). If the word has no
    characters that are special inside double quotes (", $, `, \\,
    newline), double-quoting it is just as valid and round-trips the same
    -- and it's the one case (an embedded single quote, nothing else
    exotic) that's extremely common in `-c` one-liners.
    """
    if not word:
        return "''"
    if shlex.quote(word) == word:
        # no quoting needed at all
        return word
    if not any(c in word for c in '"$`\\\n'):
        return f'"{word}"'
    return shlex.quote(word)


def _display_join(argv: list[str]) -> str:
    return " ".join(_display_quote(w) for w in argv)


def _cmd_str(job: dict[str, Any], full: bool) -> str:
    # _display_join (not " ".join) so that an argv element containing
    # spaces or shell metacharacters -- e.g. the "g16 < test.gjf" in
    # ["bash", "-c", "g16 < test.gjf"] -- round-trips as one quoted word
    # instead of printing as if it were several separate, unquoted tokens.
    key = "resolved_cmd" if full else "raw_cmd"
    argv = json.loads(job[key])
    return _display_join(argv)


def _fmt_time(ts: str | None) -> str:
    if not ts:
        return "-"
    return datetime.fromisoformat(ts).strftime("%Y-%m-%d %H:%M:%S")


def _fmt_duration(job: dict[str, Any]) -> str:
    started = job.get("started_at")
    if not started:
        return "-"
    start = datetime.fromisoformat(started)
    end = datetime.fromisoformat(job["finished_at"]) if job.get("finished_at") else datetime.now(start.tzinfo)
    secs = (end - start).total_seconds()
    if secs < 60:
        return f"{secs:.0f}s"
    minutes, secs = divmod(int(secs), 60)
    if minutes < 60:
        return f"{minutes}m{secs}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h{minutes}m"


# ---------------------------------------------------------------------------
# add
# ---------------------------------------------------------------------------

@app.command()
def add(
    cmd: list[str] = typer.Argument(
        ..., help="Command to run, exactly as you'd type it in a shell. Plain commands are "
        "exec'd directly, no shell involved: queuer add -- python train.py --epochs 5. "
        "If your command uses shell syntax (&&, |, ;, <, >, $VARS, globs), quote the WHOLE "
        "thing as one argument so your own shell hands it to queuer intact instead of "
        "acting on it itself -- queuer then runs it via `bash -c`: "
        "queuer add -- 'az login && ./deploy.sh'  or  queuer add -- 'g16 < input.gjf'"
    ),
    channel: int = typer.Option(
        DEFAULT_CHANNEL, "--channel", help="Channel to queue on. Each channel runs serially; "
        "different channels run in parallel. Defaults to channel 1."
    ),
    before: Optional[int] = typer.Option(None, "--before", help="Insert before this job ID (same channel)"),
    after: Optional[int] = typer.Option(None, "--after", help="Insert after this job ID (same channel)"),
    timeout: Optional[int] = typer.Option(
        None, "--timeout", help="Kill the job if it runs longer than this many seconds"
    ),
    now: bool = typer.Option(
        False, "--now",
        help="Jump the channel's queue: freezes (SIGSTOP) whatever is currently running "
        "on that channel and runs this immediately. Further --now jobs queue FIFO behind "
        "it; once the priority lane empties, the frozen job resumes (SIGCONT) untouched. "
        "Cannot be combined with --before/--after.",
    ),
) -> None:
    """Enqueue a job. Runs via `bash -c`, so shell syntax works -- quote the whole
    command as one argument if it uses any (see the argument help above).

    Examples:
      queuer add -- python train.py --epochs 5
      queuer add --channel 2 -- python train.py --epochs 5
      queuer add --timeout 60 -- ./run.sh
      queuer add --after 12 -- python eval.py
      queuer add --before 12 -- python setup.py
      queuer add --now -- python urgent_eval.py
      queuer add -- curl -d '{"key": "value"}' https://example.com/api
      queuer add -- 'g16 < input.gjf > output.log'
      queuer add -- 'az login && ./deploy.sh'
    """
    if not cmd:
        typer.echo("Error: no command given after --", err=True)
        raise typer.Exit(code=1)

    if _stdin_was_redirected_from_file():
        # queuer execs the job with its own fresh stdin -- it does NOT hand
        # the daemon's copy of *this* fd to the job. So a bare redirect
        # like `queuer add -- g16 < test.gjf` (unquoted) was consumed by
        # your shell to feed *queuer's* stdin before queuer even started,
        # and "test.gjf" never made it into argv at all. Warn rather than
        # silently queuing a job that's missing the input file it was
        # supposed to have.
        typer.echo(
            "Warning: it looks like stdin was redirected from a file on this "
            "queuer invocation itself (e.g. `queuer add -- g16 < test.gjf`). "
            "Your shell consumed that redirect for queuer, not for the job -- "
            "the job will run with no input file. If the redirect was meant "
            "for the job, quote the whole command (including the redirect) "
            "as one argument so your shell passes it through untouched:\n"
            "  queuer add -- 'g16 < test.gjf'\n",
            err=True,
        )

    # `argv` is what actually gets exec'd -- bash-wrapped if the command
    # needed shell syntax, with the interpreter pre-pinned to an absolute
    # path so the job doesn't depend on the daemon reproducing a venv's
    # PATH. `raw_argv` is `cmd` completely untouched -- exactly the words
    # typed after `--` -- kept separate purely for `raw command` in
    # `show`/`list` so that view reflects what the user wrote, not queuer's
    # own interpreter-pinning. `resolved command` (see pathresolve.py,
    # daemon-side) is where absolute executable/file paths belong.
    job_argv = _resolve_interpreter(_build_job_argv(cmd))

    data = _call(
        "add",
        argv=job_argv,
        raw_argv=cmd,
        cwd=os.getcwd(),
        path=os.environ.get("PATH", ""),
        virtual_env=os.environ.get("VIRTUAL_ENV"),
        channel=channel,
        before=before,
        after=after,
        timeout_secs=timeout,
        now=now,
    )
    console.print(f"Queued as job [bold]{data['id']}[/bold] on channel [bold]{data['channel']}[/bold]")
    if now:
        console.print("[yellow]--now[/yellow]: current job on this channel frozen (if any); this runs first")


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------

def _render_queue_and_backlog(channel_data: dict[str, Any], full: bool) -> None:
    if channel_data["paused"]:
        console.print("[yellow]channel is paused -- new jobs will not start[/yellow]\n")

    running = channel_data["running"]
    frozen = channel_data.get("frozen")
    priority_queue = channel_data.get("priority_queue") or []

    if frozen:
        console.print(
            f"[yellow]#{frozen['id']} frozen (--now preempted it)[/yellow]  {_cmd_str(frozen, full)}"
        )

    queue_table = Table(show_lines=False)
    queue_table.add_column("ID", justify="right")
    queue_table.add_column("Command")
    if running:
        marker = "* (priority)" if running.get("is_priority") else "*"
        queue_table.add_row(str(running["id"]), f"{marker} {_cmd_str(running, full)}", style="bold green")
    for job in priority_queue:
        queue_table.add_row(str(job["id"]), f"(priority) {_cmd_str(job, full)}")
    for job in channel_data["queue"]:
        queue_table.add_row(str(job["id"]), _cmd_str(job, full))

    if running or priority_queue or channel_data["queue"]:
        console.print(queue_table)
    else:
        console.print("[dim](idle -- nothing running or queued)[/dim]")

    backlog_table = Table(title="Recent (last 10)", show_lines=False)
    backlog_table.add_column("ID", justify="right")
    backlog_table.add_column("Status")
    backlog_table.add_column("Command")
    backlog_table.add_column("Duration", justify="right")
    backlog_table.add_column("Exit", justify="right")
    for job in channel_data["backlog"]:
        status_style = {"done": "green", "failed": "red", "cancelled": "yellow"}.get(job["status"], "")
        backlog_table.add_row(
            str(job["id"]),
            f"[{status_style}]{job['status']}[/{status_style}]" if status_style else job["status"],
            _cmd_str(job, full),
            _fmt_duration(job),
            str(job["exit_code"]) if job["exit_code"] is not None else "-",
        )
    console.print(backlog_table)


@app.command(name="list")
def list_jobs(
    full: bool = typer.Option(False, "--full", help="Show resolved absolute paths instead of what you typed"),
    as_json: bool = typer.Option(False, "--json", help="Print raw JSON instead of a table"),
) -> None:
    """Show every channel's currently running job, queue, and last 10
    finished jobs, each in its own section. Only channels that have had a
    job land on them show up here.

    Examples:
      queuer list
      queuer list --full
      queuer list --json
    """
    data = _call("list")
    channels = data["channels"]

    if as_json:
        console.print_json(json.dumps(data))
        return

    if not channels:
        console.print("[dim](no channels -- nothing has ever been queued)[/dim]")
        return

    for i, channel_id in enumerate(sorted(channels, key=int)):
        if i > 0:
            console.print()
        console.print(f"[bold]channel {channel_id}[/bold]")
        _render_queue_and_backlog(channels[channel_id], full)


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------

@app.command()
def status(as_json: bool = typer.Option(False, "--json")) -> None:
    """Show only the currently running job, for every channel.

    Examples:
      queuer status
      queuer status --json
    """
    data = _call("status")
    channels = data["channels"]

    if as_json:
        console.print_json(json.dumps(data))
        return

    if not channels:
        console.print("[dim](no channels -- nothing has ever been queued)[/dim]")
        return

    for i, channel_id in enumerate(sorted(channels, key=int)):
        if i > 0:
            console.print()
        chan = channels[channel_id]
        console.print(f"[bold]channel {channel_id}[/bold]")
        if chan["paused"]:
            console.print("[yellow]paused[/yellow]")
        frozen = chan.get("frozen")
        if frozen:
            console.print(f"[yellow]#{frozen['id']} frozen (--now preempted it)[/yellow]  {_cmd_str(frozen, full=False)}")
        running = chan["running"]
        if running:
            label = "(priority)" if running.get("is_priority") else ""
            console.print(f"[bold green]#{running['id']}[/bold green] {label}  {_cmd_str(running, full=False)}")
            console.print(f"started: {_fmt_time(running['started_at'])}  running for: {_fmt_duration(running)}")
        elif not frozen:
            console.print("[dim]idle -- no job running[/dim]")


# ---------------------------------------------------------------------------
# show
# ---------------------------------------------------------------------------

@app.command()
def show(job_id: int, as_json: bool = typer.Option(False, "--json")) -> None:
    """Show full detail for one job: raw+resolved command, cwd, timestamps, exit code.

    Examples:
      queuer show 12
      queuer show 12 --json
    """
    job = _call("show", id=job_id)
    if as_json:
        console.print_json(json.dumps(job))
        return

    table = Table(show_header=False, title=f"Job #{job['id']}")
    table.add_row("channel", str(job["channel"]))
    table.add_row("status", job["status"])
    table.add_row("raw command", _display_join(json.loads(job["raw_cmd"])))
    table.add_row("resolved command", _display_join(json.loads(job["resolved_cmd"])))
    table.add_row("cwd", job["cwd"])
    table.add_row("enqueued at", _fmt_time(job["enqueued_at"]))
    table.add_row("started at", _fmt_time(job["started_at"]))
    table.add_row("finished at", _fmt_time(job["finished_at"]))
    table.add_row("duration", _fmt_duration(job))
    table.add_row("exit code", str(job["exit_code"]) if job["exit_code"] is not None else "-")
    table.add_row("timeout", f"{job['timeout_secs']}s" if job["timeout_secs"] else "none")
    table.add_row("pid", str(job["pid"]) if job["pid"] else "-")
    table.add_row("log", job["log_path"])
    if job["note"]:
        table.add_row("note", job["note"])
    console.print(table)


# ---------------------------------------------------------------------------
# cancel / rm / requeue / pause / resume
# ---------------------------------------------------------------------------

@app.command()
def cancel(
    channel: int = typer.Option(DEFAULT_CHANNEL, "--channel", help="Channel whose running job to kill"),
) -> None:
    """Kill the currently running job on a channel (default: channel 1).

    Example:
      queuer cancel
      queuer cancel --channel 2
    """
    data = _call("cancel", channel=channel)
    console.print(f"Cancelled job [bold]{data['cancelled']}[/bold] on channel [bold]{data['channel']}[/bold]")


@app.command()
def clear(
    channel: Optional[int] = typer.Option(
        None, "--channel", help="Only clear this channel's history (default: every channel)"
    ),
) -> None:
    """Delete finished job records (done/failed/cancelled) -- the backlog
    section shown by `list`/`status`. Queued and running jobs are never
    touched. Log files on disk are untouched too -- use `clean-logs` for
    those.

    Examples:
      queuer clear
      queuer clear --channel 2
    """
    data = _call("clear", channel=channel)
    scope = f"channel {channel}" if channel is not None else "all channels"
    console.print(f"Cleared [bold]{data['cleared']}[/bold] finished job record(s) ({scope})")


@app.command()
def rm(job_id: int) -> None:
    """Remove a job from the queue. Fails if it's currently running -- use cancel for that.

    Example:
      queuer rm 12
    """
    _call("rm", id=job_id)
    console.print(f"Removed job [bold]{job_id}[/bold]")


@app.command()
def requeue(job_id: int) -> None:
    """Re-enqueue a finished job with its original command, cwd, timeout, and channel.

    Example:
      queuer requeue 12
    """
    data = _call("requeue", id=job_id)
    console.print(f"Requeued job {job_id} as new job [bold]{data['id']}[/bold]")


@app.command()
def pause(
    channel: int = typer.Option(DEFAULT_CHANNEL, "--channel", help="Channel to pause"),
) -> None:
    """Stop a channel from starting new jobs (default: channel 1). The
    channel's current job, if any, keeps running. Other channels are
    unaffected.

    Example:
      queuer pause
      queuer pause --channel 2
    """
    _call("pause", channel=channel)
    console.print(
        f"[yellow]Paused[/yellow] channel {channel} -- new jobs will not start until you run "
        f"`queuer resume --channel {channel}`"
    )


@app.command()
def resume(
    channel: int = typer.Option(DEFAULT_CHANNEL, "--channel", help="Channel to resume"),
) -> None:
    """Resume pulling new jobs from a channel's queue (default: channel 1).

    Example:
      queuer resume
      queuer resume --channel 2
    """
    _call("resume", channel=channel)
    console.print(f"[green]Resumed[/green] channel {channel}")


# ---------------------------------------------------------------------------
# logs
# ---------------------------------------------------------------------------

@app.command()
def logs(
    job_id: int,
    tail: bool = typer.Option(False, "--tail", help="Follow the log as it grows (Ctrl-C to stop)"),
) -> None:
    """Read a job's log file directly from disk (works even if the daemon isn't running).

    Examples:
      queuer logs 12
      queuer logs 12 --tail
    """
    log_path = _default_log_dir() / f"{job_id}.log"
    if not log_path.exists():
        typer.echo(f"Error: no log found for job {job_id} at {log_path}", err=True)
        raise typer.Exit(code=1)

    if not tail:
        console.print(log_path.read_text(errors="replace"), end="")
        return

    with open(log_path, "r", errors="replace") as f:
        f.seek(0, os.SEEK_END)
        try:
            while True:
                line = f.readline()
                if line:
                    console.print(line, end="")
                else:
                    time.sleep(0.5)
        except KeyboardInterrupt:
            pass


@app.command(name="clean-logs")
def clean_logs(
    days: int = typer.Option(30, "--days", help="Delete logs for jobs finished more than this many days ago"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be deleted without deleting"),
) -> None:
    """Delete old log files from disk to free space.

    Only touches finished jobs (done/failed/cancelled) whose finished_at
    is older than --days. Also removes orphaned logs (files whose job row
    no longer exists in the DB, e.g. pruned backlog). Never touches logs
    for queued/running/stopped jobs. Reads the DB directly -- works even
    if the daemon isn't running.

    Examples:
      queuer clean-logs --dry-run
      queuer clean-logs --days 7
    """
    log_dir = _default_log_dir()
    if not log_dir.exists():
        console.print("[dim](no log directory)[/dim]")
        return

    conn = db.connect(_default_db_path())
    cutoff = time.time() - days * 86400
    to_delete: list[Path] = []

    for log_file in log_dir.glob("*.log"):
        try:
            job_id = int(log_file.stem)
        except ValueError:
            continue  # not a job log, leave it alone

        job = db.get_job(conn, job_id)
        if job is None:
            to_delete.append(log_file)  # orphaned -- no matching row
            continue
        if job["status"] not in ("done", "failed", "cancelled"):
            continue  # queued/running/stopped -- never touch
        if not job["finished_at"]:
            continue
        finished_ts = datetime.fromisoformat(job["finished_at"]).timestamp()
        if finished_ts < cutoff:
            to_delete.append(log_file)

    conn.close()

    if not to_delete:
        console.print("[dim](nothing to clean)[/dim]")
        return

    total_bytes = sum(f.stat().st_size for f in to_delete)
    verb = "Would delete" if dry_run else "Deleting"
    console.print(f"{verb} {len(to_delete)} log(s), {total_bytes / 1024:.0f} KB")
    for f in to_delete:
        console.print(f"  {f}")
        if not dry_run:
            f.unlink()


if __name__ == "__main__":
    app()
