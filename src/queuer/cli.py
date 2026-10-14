"""queuer: CLI client for the queuerd job queue daemon."""

from __future__ import annotations

import json
import os
import shlex
import shutil
import socket
import stat
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import typer
from rich.console import Console
from rich.table import Table

from queuer import client, db, notify
from queuer.daemon import _default_db_path, _default_log_dir
from queuer.db import DEFAULT_CHANNEL
from queuer.pathresolve import SHELL_INTERPRETERS

app = typer.Typer(
    help="""Background job queue. Jobs on one channel run one at a time, in order; \
different channels run in parallel (default channel: 1). `qur` is a short alias for `queuer`.

\b
Quick start:
  qur add -- python train.py           queue a job
  qur add --retries 2 -- ./flaky.sh    auto-requeue up to 2 times if it fails
  qur list                             running / queued / recent jobs, all channels
  qur show 12 14                       full detail for jobs 12 and 14
  qur logs 12 --tail                   follow job 12's output
  qur cancel                           kill the running job
  qur cancel 12                        cancel job 12 (running, frozen or queued)
  qur connect                          get Telegram alerts when jobs finish

Run `qur <command> --help` for one command, or `qur --help-all` for every command's help.""",
)
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
    # stored timestamps are UTC; show them in the machine's local time
    return datetime.fromisoformat(ts).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def _fmt_secs(secs: float) -> str:
    if secs < 60:
        return f"{secs:.0f}s"
    minutes, secs = divmod(int(secs), 60)
    if minutes < 60:
        return f"{minutes}m{secs}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h{minutes}m"


def _fmt_duration(job: dict[str, Any]) -> str:
    started = job.get("started_at")
    if not started:
        return "-"
    start = datetime.fromisoformat(started)
    end = datetime.fromisoformat(job["finished_at"]) if job.get("finished_at") else datetime.now(start.tzinfo)
    return _fmt_secs((end - start).total_seconds())


def _fmt_origin(job: dict[str, Any]) -> str:
    """Where a job came from: queued fresh, auto-retried by the daemon after a
    failure, or requeued by hand."""
    origin, parent = job.get("origin"), job.get("parent_id")
    if origin == "retry":
        return f"retry of #{parent} ({job['attempt']}/{job['max_retries']})"
    if origin == "requeue":
        return f"requeue of #{parent}"
    return "new"


def _fmt_elapsed(ts: str | None) -> str:
    """Time since `ts` (an ISO timestamp), e.g. how long a job has been queued."""
    if not ts:
        return "-"
    then = datetime.fromisoformat(ts)
    return _fmt_secs(max(0.0, (datetime.now(then.tzinfo) - then).total_seconds()))


# ---------------------------------------------------------------------------
# add
# ---------------------------------------------------------------------------

def _help_all(ctx: typer.Context, value: bool) -> None:
    if not value or ctx.resilient_parsing:
        return
    group = typer.main.get_command(app)
    prog = ctx.find_root().info_name or "qur"
    for name in group.commands:
        console.rule(f"[bold]{prog} {name}[/bold]")
        try:
            group.main(args=[name, "--help"], prog_name=prog, standalone_mode=False)
        except SystemExit:
            pass
    raise typer.Exit()


@app.callback()
def main(
    help_all: bool = typer.Option(
        False, "--help-all", callback=_help_all, is_eager=True,
        help="Show the help for every command, one after another, and exit.",
    ),
) -> None:
    pass


@app.command()
def add(
    cmd: list[str] = typer.Argument(
        ..., metavar="CMD...",
        help="Command to run, written after `--`. Quote the WHOLE command as one "
        "argument if it needs shell syntax (&&, |, ;, <, >, $VARS, globs)."
    ),
    channel: int = typer.Option(
        DEFAULT_CHANNEL, "--channel", help="Channel to queue on. Jobs on one channel run one at a "
        "time; different channels run in parallel."
    ),
    before: Optional[int] = typer.Option(None, "--before", help="Insert before this queued job ID (same channel)"),
    after: Optional[int] = typer.Option(None, "--after", help="Insert after this queued job ID (same channel)"),
    timeout: Optional[int] = typer.Option(
        None, "--timeout", help="Kill the job if it runs longer than this many seconds (counts as a failure)"
    ),
    retries: int = typer.Option(
        0, "--retries", min=0,
        help="If the job fails (non-zero exit, timeout, or failed start), requeue it automatically at the "
        "end of its channel's queue, up to this many times. Cancelled jobs are never retried.",
    ),
    now: bool = typer.Option(
        False, "--now",
        help="Jump the channel's queue: freeze (SIGSTOP) the running job and run this first. "
        "More --now jobs run FIFO behind it, then the frozen job resumes (SIGCONT). "
        "Cannot be combined with --before/--after.",
    ),
) -> None:
    """Queue a job.

    Plain commands run directly, with no shell. If the command needs shell syntax,
    quote the whole thing as one argument and it runs via `bash -c`. Put `--` before
    the command so its own flags aren't parsed as queuer options.

    The executable is checked when you add the job, so a bad command is rejected
    immediately instead of when it reaches the front of the queue.

    \b
    Examples:
      qur add -- python train.py --epochs 5
      qur add --channel 2 -- python train.py --epochs 5
      qur add --timeout 60 -- ./run.sh
      qur add --retries 3 -- python flaky_download.py
      qur add --after 12 -- python eval.py
      qur add --before 12 -- python setup.py
      qur add --now -- python urgent_eval.py
      qur add -- curl -d '{"key": "value"}' https://example.com/api
      qur add -- 'g16 < input.gjf > output.log'
      qur add -- 'az login && ./deploy.sh'
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
        python_path=os.environ.get("PYTHONPATH"),
        channel=channel,
        before=before,
        after=after,
        timeout_secs=timeout,
        now=now,
        max_retries=retries,
    )
    console.print(f"Queued as job [bold]{data['id']}[/bold] on channel [bold]{data['channel']}[/bold]")
    if retries:
        console.print(f"auto-retry on failure: up to {retries} time(s)")
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
    queue_table.add_column("Origin")
    queue_table.add_column("Queued")
    queue_table.add_column("Elapsed", justify="right")
    if running:
        marker = "* (priority)" if running.get("is_priority") else "*"
        queue_table.add_row(
            str(running["id"]), f"{marker} {_cmd_str(running, full)}", _fmt_origin(running),
            _fmt_time(running["enqueued_at"]), _fmt_elapsed(running["enqueued_at"]),
            style="bold green",
        )
    for job in priority_queue:
        queue_table.add_row(
            str(job["id"]), f"(priority) {_cmd_str(job, full)}", _fmt_origin(job),
            _fmt_time(job["enqueued_at"]), _fmt_elapsed(job["enqueued_at"]),
        )
    for job in channel_data["queue"]:
        queue_table.add_row(
            str(job["id"]), _cmd_str(job, full), _fmt_origin(job),
            _fmt_time(job["enqueued_at"]), _fmt_elapsed(job["enqueued_at"]),
        )

    if running or priority_queue or channel_data["queue"]:
        console.print(queue_table)
    else:
        console.print("[dim](idle -- nothing running or queued)[/dim]")

    backlog_table = Table(title="Recent (last 10)", show_lines=False)
    backlog_table.add_column("ID", justify="right")
    backlog_table.add_column("Status")
    backlog_table.add_column("Command")
    backlog_table.add_column("Origin")
    backlog_table.add_column("Duration", justify="right")
    backlog_table.add_column("Exit", justify="right")
    for job in channel_data["backlog"]:
        status_style = {"done": "green", "failed": "red", "cancelled": "yellow"}.get(job["status"], "")
        backlog_table.add_row(
            str(job["id"]),
            f"[{status_style}]{job['status']}[/{status_style}]" if status_style else job["status"],
            _cmd_str(job, full),
            _fmt_origin(job),
            _fmt_duration(job),
            str(job["exit_code"]) if job["exit_code"] is not None else "-",
        )
    console.print(backlog_table)


@app.command(name="list")
def list_jobs(
    channel: Optional[int] = typer.Option(
        None, "--channel", help="Only list this channel (default: every channel)"
    ),
    full: bool = typer.Option(False, "--full", help="Show resolved absolute paths instead of what you typed"),
    as_json: bool = typer.Option(False, "--json", help="Print raw JSON instead of a table"),
) -> None:
    """List running, queued, and recent jobs, one section per channel.

    Each channel shows its running job (marked *), then its queue, with when each
    job was queued and how long ago that was ("Elapsed" counts from queue time,
    so it includes any wait). Below that: the last 10 finished jobs.

    The Origin column says where a job came from: "new" (queued by you),
    "retry of #N" (queued automatically by --retries after job N failed; shows
    the retry count), or "requeue of #N" (you ran `qur requeue N`). Only channels
    that have had a job show up. For one job's full detail, use `qur show`.

    \b
    Examples:
      qur list
      qur list --channel 2
      qur list --full
      qur list --json
    """
    data = _call("list")
    channels = data["channels"]

    if channel is not None:
        # daemon returns every channel; keys are strings after the JSON round-trip
        channels = {k: v for k, v in channels.items() if int(k) == channel}
        if not channels:
            typer.echo(f"Error: channel {channel} has no jobs (nothing has been queued on it)", err=True)
            raise typer.Exit(code=1)
        data = {"channels": channels}

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
def status(as_json: bool = typer.Option(False, "--json", help="Print raw JSON instead of text")) -> None:
    """Show the running job on every channel.

    Also shows whether a channel is paused, and any job frozen by a `--now`
    preemption. Does not list queued jobs -- use `qur list` for those.

    \b
    Examples:
      qur status
      qur status --json
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

def _render_job(job: dict[str, Any]) -> None:
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
    table.add_row("origin", _fmt_origin(job))
    if job.get("max_retries"):
        table.add_row("auto-retries", f"{job['attempt']} of {job['max_retries']} used")
    table.add_row("pid", str(job["pid"]) if job["pid"] else "-")
    table.add_row("log", job["log_path"])
    if job["note"]:
        table.add_row("note", job["note"])
    console.print(table)


@app.command()
def show(
    job_ids: list[int] = typer.Argument(..., help="One or more job IDs", metavar="JOB_ID..."),
    as_json: bool = typer.Option(False, "--json", help="Print raw JSON (one object, or a list for several IDs)"),
) -> None:
    """Show full detail for one or more jobs.

    Shows raw and resolved command, cwd, timestamps, duration, exit code, timeout,
    auto-retry count, pid, log path, and any note. With several IDs, an unknown ID
    prints an error but the others are still shown (exit code 1 at the end).

    \b
    Examples:
      qur show 12
      qur show 12 14 15
      qur show 12 --json
    """
    jobs: list[dict[str, Any]] = []
    failed = False
    for job_id in job_ids:
        try:
            jobs.append(client.send("show", id=job_id))
        except client.DaemonError as e:
            typer.echo(f"Error: {e}", err=True)
            failed = True
        except client.DaemonNotRunning as e:
            typer.echo(f"Error: {e}", err=True)
            raise typer.Exit(code=1)

    if as_json:
        if jobs:
            console.print_json(json.dumps(jobs[0] if len(job_ids) == 1 else jobs))
    else:
        for i, job in enumerate(jobs):
            if i > 0:
                console.print()
            _render_job(job)

    if failed:
        raise typer.Exit(code=1)


# ---------------------------------------------------------------------------
# cancel / rm / requeue / pause / resume
# ---------------------------------------------------------------------------

@app.command()
def cancel(
    job_id: Optional[int] = typer.Argument(
        None, metavar="[JOB_ID]", help="Cancel this job, wherever it is (running, frozen, or queued)"
    ),
    channel: Optional[int] = typer.Option(
        None, "--channel", help="Channel whose running job to kill (default 1). With JOB_ID, must match the job's channel."
    ),
) -> None:
    """Cancel a job.

    With no JOB_ID: kills a channel's running job. With a JOB_ID: cancels that job
    whatever state it is in -- running, frozen by `--now`, or still waiting in the
    queue (a queued job is kept in history as `cancelled`; `qur rm` deletes it
    instead).

    Killing sends SIGTERM to the job's whole process group, then SIGKILL if it is
    still alive after a short grace period. A cancelled job is never auto-retried.
    Errors if nothing is running on the channel, or if the job is already finished.

    \b
    Examples:
      qur cancel
      qur cancel --channel 2
      qur cancel 12
    """
    kwargs: dict[str, Any] = {}
    if job_id is not None:
        kwargs["id"] = job_id
    if channel is not None:
        kwargs["channel"] = channel
    data = _call("cancel", **kwargs)
    console.print(f"Cancelled job [bold]{data['cancelled']}[/bold] on channel [bold]{data['channel']}[/bold]")


@app.command()
def clear(
    channel: Optional[int] = typer.Option(
        None, "--channel", help="Only clear this channel's history (default: every channel)"
    ),
) -> None:
    """Delete finished-job records (done/failed/cancelled).

    Clears the "Recent" section of `qur list`. Queued, running, and frozen jobs are
    never touched. Channels left with no jobs at all drop off the list (channel 1
    always stays). Log files stay on disk -- use `qur clean-logs` for those.

    \b
    Examples:
      qur clear
      qur clear --channel 2
    """
    data = _call("clear", channel=channel)
    scope = f"channel {channel}" if channel is not None else "all channels"
    console.print(f"Cleared [bold]{data['cleared']}[/bold] finished job record(s) ({scope})")


@app.command()
def rm(job_id: int = typer.Argument(..., help="ID of a queued job")) -> None:
    """Remove a queued job.

    The job is deleted outright, not kept as history. Only works on jobs that are
    still waiting; for a running job use `qur cancel`.

    \b
    Example:
      qur rm 12
    """
    _call("rm", id=job_id)
    console.print(f"Removed job [bold]{job_id}[/bold]")


@app.command()
def requeue(job_id: int = typer.Argument(..., help="ID of a finished job (done/failed/cancelled)")) -> None:
    """Queue a finished job again.

    Makes a new job with the original command, cwd, environment, timeout, channel,
    and --retries setting (with a fresh retry count), added to the end of that
    channel's queue. Only works on done/failed/cancelled jobs.

    \b
    Example:
      qur requeue 12
    """
    data = _call("requeue", id=job_id)
    console.print(f"Requeued job {job_id} as new job [bold]{data['id']}[/bold]")


@app.command()
def pause(
    channel: int = typer.Option(DEFAULT_CHANNEL, "--channel", help="Channel to pause"),
) -> None:
    """Stop a channel from starting new queued jobs.

    The channel's running job, if any, finishes normally. Other channels are
    unaffected. The paused state survives daemon restarts. Undo with `qur resume`.

    \b
    Examples:
      qur pause
      qur pause --channel 2
    """
    _call("pause", channel=channel)
    console.print(
        f"[yellow]Paused[/yellow] channel {channel} -- new jobs will not start until you run "
        f"`qur resume --channel {channel}`"
    )


@app.command()
def resume(
    channel: int = typer.Option(DEFAULT_CHANNEL, "--channel", help="Channel to resume"),
) -> None:
    """Let a paused channel start queued jobs again.

    \b
    Examples:
      qur resume
      qur resume --channel 2
    """
    _call("resume", channel=channel)
    console.print(f"[green]Resumed[/green] channel {channel}")


# ---------------------------------------------------------------------------
# connect (Telegram notifications)
# ---------------------------------------------------------------------------

def _fail(msg: str) -> None:
    typer.echo(f"Error: {msg}", err=True)
    raise typer.Exit(code=1)


def _warn_hello(bot_cfg: dict[str, Any]) -> None:
    """Test message through one bot (flat view)."""
    errors = notify.send(bot_cfg, notify.hello_text(bot_cfg))
    for err in errors:
        typer.echo(f"Warning: could not send the test message ({err})", err=True)


def _fetch_export_over_ssh(target: str) -> str:
    """Run `qur connect --export` on `target` over ssh and return its string.
    ssh's own prompts (password, host key) show on the terminal as usual."""
    if not target or target.startswith("-"):
        _fail("--from takes [user@]host")
    # ~/.local/bin is where `uv tool` puts qur, but a non-interactive ssh
    # shell often lacks it on PATH
    remote = 'PATH="$HOME/.local/bin:$PATH" qur connect --export'
    typer.echo(f"Asking {target} over ssh...", err=True)
    try:
        res = subprocess.run(["ssh", "--", target, remote], stdout=subprocess.PIPE, text=True, timeout=120)
    except FileNotFoundError:
        _fail("ssh is not installed on this machine")
    except subprocess.TimeoutExpired:
        _fail(f"timed out talking to {target}")
    if res.returncode != 0:
        _fail(f"ssh to {target} failed (exit {res.returncode}); is qur set up there? (`qur connect` on that machine first)")
    for line in res.stdout.splitlines():
        if line.strip().startswith((notify.EXPORT_PREFIX, notify.EXPORT_PREFIX_MULTI)):
            return line.strip()
    _fail(f"{target} did not return a setup string; is qur connected there?")


def _button_state(bot: dict[str, Any], long: bool = True) -> str:
    if notify.listening(bot):
        return "on"
    return "off (another machine should handle it)" if long else "off"


def _connected_line(cfg: dict[str, Any], prefix: str) -> str:
    bots = cfg["bots"]
    if len(bots) == 1:
        bot = bots[0]
        return (f"{prefix} This qur is [bold]{cfg['qur_id']}[/bold] ({len(bot['chat_ids'])} chat(s)). "
                f"Requeue button: {_button_state(bot)}.")
    lines = [f"{prefix} This qur is [bold]{cfg['qur_id']}[/bold], connected to {len(bots)} bots:"]
    for bot in bots:
        lines.append(f"  {notify.bot_label(bot)}: {len(bot['chat_ids'])} chat(s), Requeue button {_button_state(bot, long=False)}")
    return "\n".join(lines)


def _bot_ids_hint(cfg: dict[str, Any]) -> str:
    return ", ".join(notify.bot_label(b) for b in cfg["bots"])


def _bots_for(cfg: dict[str, Any] | None, ref: str | None) -> list[dict[str, Any]]:
    """The bots a command applies to: the one named by --bot, else all."""
    if cfg is None:
        _fail("notifications are not set up on this machine -- run `qur connect`")
    if ref is None:
        return list(cfg["bots"])
    found = notify.find_bots(cfg, ref)
    if not found:
        _fail(f"no connected bot matches '{ref}'. This machine has: {_bot_ids_hint(cfg)} (see `qur connect --list`)")
    return found


def _bot_username(token: str, required: bool) -> str | None:
    """The bot's @username from Telegram. With `required`, an unusable token
    is an error; otherwise failures are ignored (best effort)."""
    try:
        return notify.check_token(token)
    except notify.NotifyError as e:
        if required:
            _fail(f"bot token rejected by Telegram: {e}")
        return None


@app.command()
def connect(
    export: bool = typer.Option(False, "--export", help="Print a string that sets up other machines (contains the bot tokens). With --bot, only that bot"),
    import_: Optional[str] = typer.Option(None, "--import", help="Set this machine up from another machine's --export string (adds its bots to this machine's)"),
    from_: Optional[str] = typer.Option(None, "--from", metavar="[USER@]HOST", help="Set this machine up over ssh from a machine that is already set up (no string to copy)"),
    off: bool = typer.Option(False, "--off", "--remove-bot", help="Remove a bot from this machine (--bot ID), or everything if it has only one or you pass --all"),
    list_: bool = typer.Option(False, "--list", help="Show this machine's setup: id, host, every bot with its paired chats and Requeue button on/off"),
    remove_chat: Optional[int] = typer.Option(None, "--remove-chat", metavar="CHAT_ID", help="Stop sending to one paired chat/phone (ids are shown by --list), on every bot or just --bot. Keeps the bot tokens"),
    id_: Optional[str] = typer.Option(None, "--id", help="Set this machine's id shown in messages (1-16 letters, digits, - or _)"),
    listen: Optional[bool] = typer.Option(None, "--listen/--no-listen", help="Whether this machine handles the Requeue/Details buttons for a bot (all its bots unless --bot; only one machine per bot should; --import turns it off for new bots)"),
    add_bot: bool = typer.Option(False, "--add-bot", help="Connect one more bot to this machine: asks for its token, then pairs a phone (QR) as usual"),
    bot: Optional[str] = typer.Option(None, "--bot", metavar="BOT", help="Which connected bot a command applies to: its id or @username (shown by --list)"),
    all_: bool = typer.Option(False, "--all", help="With --remove-bot: remove every bot, i.e. forget the whole setup"),
) -> None:
    """Get a Telegram message when a job finishes, on any machine running qur.

    Messages say done / failed / cancelled, the job id, how long it took, the
    command as you typed it, the exit code, and which machine sent it (hostname
    plus that qur's id, shown when you connect; change it with --id). They never
    include the working directory or resolved paths; use `qur show ID` on the
    machine for those. A job that is auto-retried stays silent until its final try.

    Each message has two buttons. Requeue re-runs the finished job (like `qur
    requeue ID`). Details replies with everything `qur show ID` prints, formatted
    for Telegram (this includes paths, sent only to your paired chats). Both are
    handled by the daemon of the machine that polls that bot.

    One qur can use several bots at once: every message goes through every bot
    it is connected to, and presses on any of them come back to it. Telegram
    lets only one program listen to a bot at a time, so give each machine its
    own bot if you want its buttons to always reach it (`--add-bot` on that
    machine), and use --import/--from on the others only to send through it.
    Machines set up with --import send through the bot but show no buttons
    (unless you pass --listen).

    Manage this machine: --list shows its id, host, bots and paired chat ids
    (other machines are not listed; run it on each). --id renames it.
    --remove-chat CHAT_ID stops one phone/account; --remove-bot (same as --off)
    removes a bot (--bot picks which; with one bot it removes the whole setup).
    With several bots, --bot picks the one for --remove-bot, --export, --listen
    and for pairing another phone. The other management options cannot be
    combined with each other.

    One-time setup, once per bot: in Telegram open @BotFather, send /newbot,
    copy the token.

    First machine: run `qur connect`, paste the token, scan the QR with your
    phone, press Start. Running it again adds another phone or account (with
    several bots, say which: `qur connect --bot @my_bot`). A second bot on the
    same machine: `qur connect --add-bot`.

    Other machines, easiest: `qur connect --from user@first-machine`. It logs in
    over ssh, runs `qur connect --export` there and imports the result (all its
    bots), so there is nothing to copy. Without ssh: run `qur connect --export`
    on the first one, then `qur connect --import <string>` on each other one. No
    scan needed. (Pair one machine at a time; sending from many machines at once
    is fine.)

    \b
    Examples:
      qur connect
      qur connect --add-bot
      qur connect --export
      qur connect --from sampk@laptop
      qur connect --import qurtg1:eyJ0Ijoi...
      qur connect --id bigbox
      qur connect --list
      qur connect --bot @laptop_bot --no-listen
      qur connect --remove-chat 123456789
      qur connect --remove-bot --bot @old_bot
      qur connect --remove-bot --all
    """
    actions = [export, import_ is not None, from_ is not None, off, list_, remove_chat is not None, add_bot]
    if sum(actions) > 1:
        _fail("use only one of --export, --import, --from, --off/--remove-bot, --list, --remove-chat, --add-bot")
    if (export or off or list_ or remove_chat is not None) and (id_ is not None or listen is not None):
        _fail("--id and --listen cannot be combined with --export, --off, --list or --remove-chat")
    if id_ is not None and not notify.valid_qur_id(id_):
        _fail("id must be 1-16 characters: letters, digits, - or _")
    if bot is not None and (import_ is not None or from_ is not None or list_ or add_bot or id_ is not None):
        _fail("--bot picks an already connected bot; it cannot be combined with --import, --from, --list, --add-bot or --id")
    if all_ and not off:
        _fail("--all only goes with --remove-bot")
    if all_ and bot is not None:
        _fail("use either --bot or --all, not both")

    if from_ is not None:
        import_ = _fetch_export_over_ssh(from_)

    existing = notify.load()

    # --id / --listen alone: change this machine's setup, no pairing
    if import_ is None and not add_bot and existing is not None and (id_ is not None or listen is not None):
        cfg = {"qur_id": id_ or existing["qur_id"], "bots": [dict(b) for b in existing["bots"]]}
        if listen is not None:
            targets = {b["token"] for b in _bots_for(existing, bot)}
            for b in cfg["bots"]:
                if b["token"] in targets:
                    b["listen"] = listen
        notify.save(cfg)
        console.print(_connected_line(notify.load() or cfg, "Updated."))
        return

    if list_:
        if existing is None:
            console.print("[dim](notifications are not set up on this machine -- run `qur connect`)[/dim]")
            return
        table = Table(show_header=False, title="This machine")
        table.add_row("id", existing["qur_id"])
        table.add_row("host", socket.gethostname())
        for i, b in enumerate(existing["bots"]):
            if i:
                table.add_section()
            table.add_row("bot", notify.bot_label(b))  # bot id; never print the secret half
            table.add_row("chats", ", ".join(str(c) for c in b["chat_ids"]) or "none -- run `qur connect` to pair one")
            table.add_row("Requeue/Details buttons", "on" if notify.listening(b) else "off (send-only)")
        table.add_section()
        table.add_row("config", str(notify.default_path()))
        console.print(table)
        console.print("[dim]Other machines are not listed: each qur only knows itself. Run this on each.[/dim]")
        return

    if remove_chat is not None:
        targets = [b for b in _bots_for(existing, bot) if remove_chat in b["chat_ids"]]
        if existing is None or not targets:
            _fail(f"chat {remove_chat} is not paired on this machine (see `qur connect --list`)")
        hit = {b["token"] for b in targets}
        cfg = {"qur_id": existing["qur_id"], "bots": [
            {**b, "chat_ids": [c for c in b["chat_ids"] if c != remove_chat]} if b["token"] in hit else b
            for b in existing["bots"]
        ]}
        notify.save(cfg)
        if len(existing["bots"]) == 1:
            left = len(cfg["bots"][0]["chat_ids"])
            console.print(f"Removed chat {remove_chat}. {left} chat(s) left"
                          + ("" if left else " -- no messages will be sent until you run `qur connect` to pair one") + ".")
        else:
            console.print(f"Removed chat {remove_chat} from {len(targets)} bot(s): "
                          + ", ".join(notify.bot_label(b) for b in targets) + ".")
            empty = [b for b in cfg["bots"] if not b["chat_ids"]]
            for b in empty:
                console.print(f"[yellow]{notify.bot_label(b)} has no chats left[/yellow] -- it sends nothing until you "
                              f"run `qur connect --bot {notify.bot_id(b['token'])}` to pair one.")
        return

    if off:
        if existing is None:
            if notify.remove():  # unreadable config: still clean it up
                console.print("Bot [yellow]removed[/yellow] from this machine.")
            else:
                console.print("[dim](notifications were not set up on this machine)[/dim]")
            return
        if bot is not None:
            gone = _bots_for(existing, bot)
            rest = existing
            for b in gone:
                rest = notify.without_bot(rest, b["token"]) if rest else None
            if rest is not None:
                notify.save(rest)
                console.print("Bot [yellow]removed[/yellow]: " + ", ".join(notify.bot_label(b) for b in gone)
                              + f". {len(rest['bots'])} bot(s) left. To invalidate the token everywhere, "
                              "send /revoke to @BotFather.")
                return
        elif len(existing["bots"]) > 1 and not all_:
            _fail(f"this machine has {len(existing['bots'])} bots ({_bot_ids_hint(existing)}); "
                  "use --bot ID to remove one, or --all to remove them all")
        notify.remove()
        console.print("Bot [yellow]removed[/yellow] from this machine"
                      + (" (all bots)" if len(existing["bots"]) > 1 else "") + ". "
                      "To invalidate the bot token everywhere, send /revoke to @BotFather.")
        return

    if export:
        chosen = [b for b in _bots_for(existing, bot) if b["chat_ids"]] if existing else []
        if existing is None or not chosen:
            _fail("nothing to export -- run `qur connect` first")
        typer.echo(notify.export_string(existing, chosen))
        typer.echo(f"Contains the bot token{'s' if len(chosen) > 1 else ''} -- keep it secret. "
                   "Run `qur connect --import <string>` on another machine.", err=True)
        return

    if import_ is not None:
        try:
            items = notify.parse_export_bots(import_)
        except ValueError as e:
            _fail(str(e))
        names: dict[str, str | None] = {}
        for token, _chats in items:
            try:
                names[token] = notify.check_token(token)
            except notify.NotifyError as e:
                _fail(f"bot token rejected by Telegram: {e}")
        cfg = existing
        imported: list[str] = []
        for token, chat_ids in items:
            old = next((b for b in cfg["bots"] if b["token"] == token), None) if cfg else None
            if old is not None:
                # already connected: keep its settings, add any chats we lack
                entry = {**old, "chat_ids": old["chat_ids"] + [c for c in chat_ids if c not in old["chat_ids"]]}
                if listen is not None:
                    entry["listen"] = listen
            else:
                # a bot another machine already uses: only one machine per bot
                # can poll for button presses, so this one starts send-only
                entry = {"token": token, "chat_ids": chat_ids, "listen": bool(listen)}
            if names.get(token):
                entry["username"] = names[token]
            cfg = notify.with_bot(cfg, entry, id_)
            imported.append(token)
        assert cfg is not None
        notify.save(cfg)
        for bcfg in notify.bot_cfgs(cfg):
            if bcfg["token"] in imported:
                _warn_hello(bcfg)
        console.print(_connected_line(cfg, "Connected."))
        return

    # pair a phone/account with a bot
    username: str | None = None
    if add_bot or existing is None:
        token = typer.prompt("Bot token (from @BotFather /newbot)", hide_input=True).strip()
        if not notify.TOKEN_RE.match(token):
            _fail("that does not look like a bot token (expected 123456789:ABC...)")
        username = _bot_username(token, required=add_bot)
    else:
        if bot is None and len(existing["bots"]) > 1:
            _fail(f"this machine has {len(existing['bots'])} bots ({_bot_ids_hint(existing)}); pick the one to pair a phone "
                  "with using --bot, or connect another bot with --add-bot")
        target = _bots_for(existing, bot)
        if len(target) > 1:
            _fail(f"--bot matched {len(target)} bots; use the numeric bot id")
        token = target[0]["token"]
        username = target[0].get("username") or _bot_username(token, required=False)

    def show_link(link: str) -> None:
        import segno

        typer.echo("Scan with your phone camera or Telegram, then press Start:\n")
        segno.make(link, error="m").terminal(compact=True)
        typer.echo(f"\n{link}\n")
        typer.echo("Waiting for Start (Ctrl-C to cancel)...")

    try:
        with notify.pairing_lock():  # the daemon stops polling while we pair
            chat_id = notify.pair(token, show_link)
    except notify.PairingConflict as e:
        _fail(str(e))
    except notify.NotifyError as e:
        _fail(str(e))
    except TimeoutError as e:
        _fail(str(e))
    except KeyboardInterrupt:
        _fail("cancelled")

    old = next((b for b in existing["bots"] if b["token"] == token), None) if existing else None
    entry = dict(old) if old else {"token": token, "chat_ids": []}
    if chat_id not in entry["chat_ids"]:
        entry["chat_ids"] = [*entry["chat_ids"], chat_id]
    if listen is not None:
        entry["listen"] = listen
    if username:
        entry["username"] = username
    cfg = notify.with_bot(existing, entry, id_)
    notify.save(cfg)
    for bcfg in notify.bot_cfgs(cfg):
        if bcfg["token"] == token:
            _warn_hello(bcfg)
    console.print(_connected_line(cfg, "Connected."))


# ---------------------------------------------------------------------------
# logs
# ---------------------------------------------------------------------------

@app.command()
def logs(
    job_id: int = typer.Argument(..., help="Job ID"),
    tail: bool = typer.Option(False, "--tail", help="Follow the log as it grows (Ctrl-C to stop)"),
) -> None:
    """Print a job's log (stdout and stderr combined).

    Reads the log file straight from disk, so it works even if the daemon isn't
    running. Each retry of a job is a separate job with its own log.

    \b
    Examples:
      qur logs 12
      qur logs 12 --tail
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
    """Delete old log files from disk.

    Removes logs of finished jobs (done/failed/cancelled) finished more than --days
    ago, plus orphaned logs whose job record no longer exists (e.g. pruned from
    Recent). Never touches logs of queued, running, or frozen jobs. Reads the DB
    directly, so it works even if the daemon isn't running.

    \b
    Examples:
      qur clean-logs --dry-run
      qur clean-logs --days 7
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
