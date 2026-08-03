"""queuer: CLI client for the queuerd job queue daemon."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import typer
from rich.console import Console
from rich.table import Table

from queuer import client
from queuer.daemon import _default_log_dir
from queuer.db import DEFAULT_CHANNEL

app = typer.Typer(help="queuer: a background job queue with parallel channels")
console = Console()


def _call(cmd: str, **kwargs: Any) -> Any:
    """Call the daemon, printing a clean one-line error and exiting
    nonzero on any failure rather than letting a traceback surface."""
    try:
        return client.send(cmd, **kwargs)
    except (client.DaemonNotRunning, client.DaemonError) as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(code=1)


def _cmd_str(job: dict[str, Any], full: bool) -> str:
    key = "resolved_cmd" if full else "raw_cmd"
    argv = json.loads(job[key])
    return " ".join(argv)


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
        ..., help="Command to run. Use -- before it if it has its own flags, "
        "e.g. queuer add --timeout 60 -- python train.py --epochs 5"
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
    """Enqueue a job.

    Examples:
      queuer add -- python train.py --epochs 5
      queuer add --channel 2 -- python train.py --epochs 5
      queuer add --timeout 60 -- ./run.sh
      queuer add --after 12 -- python eval.py
      queuer add --before 12 -- python setup.py
      queuer add --now -- python urgent_eval.py
    """
    data = _call(
        "add",
        argv=cmd,
        cwd=os.getcwd(),
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
    table.add_row("raw command", " ".join(json.loads(job["raw_cmd"])))
    table.add_row("resolved command", " ".join(json.loads(job["resolved_cmd"])))
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


if __name__ == "__main__":
    app()
