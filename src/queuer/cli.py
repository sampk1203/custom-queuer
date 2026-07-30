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

app = typer.Typer(help="queuer: a serial background job queue")
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
    before: Optional[int] = typer.Option(None, "--before", help="Insert before this job ID"),
    after: Optional[int] = typer.Option(None, "--after", help="Insert after this job ID"),
    timeout: Optional[int] = typer.Option(
        None, "--timeout", help="Kill the job if it runs longer than this many seconds"
    ),
) -> None:
    """Enqueue a job.

    Examples:
      queuer add -- python train.py --epochs 5
      queuer add --timeout 60 -- ./run.sh
      queuer add --after 12 -- python eval.py
      queuer add --before 12 -- python setup.py
    """
    data = _call(
        "add",
        argv=cmd,
        cwd=os.getcwd(),
        before=before,
        after=after,
        timeout_secs=timeout,
    )
    console.print(f"Queued as job [bold]{data['id']}[/bold]")


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------

@app.command(name="list")
def list_jobs(
    full: bool = typer.Option(False, "--full", help="Show resolved absolute paths instead of what you typed"),
    as_json: bool = typer.Option(False, "--json", help="Print raw JSON instead of a table"),
) -> None:
    """Show the currently running job, the queue, and the last 10 finished jobs.

    Examples:
      queuer list
      queuer list --full
      queuer list --json
    """
    data = _call("list")

    if as_json:
        console.print_json(json.dumps(data))
        return

    if data["paused"]:
        console.print("[yellow]daemon is paused -- new jobs will not start[/yellow]\n")

    running = data["running"]

    queue_table = Table(show_lines=False)
    queue_table.add_column("ID", justify="right")
    queue_table.add_column("Command")
    if running:
        queue_table.add_row(str(running["id"]), f"* {_cmd_str(running, full)}", style="bold green")
    for job in data["queue"]:
        queue_table.add_row(str(job["id"]), _cmd_str(job, full))

    if running or data["queue"]:
        console.print(queue_table)
    else:
        console.print("[dim](idle -- nothing running or queued)[/dim]")

    backlog_table = Table(title="Recent (last 10)", show_lines=False)
    backlog_table.add_column("ID", justify="right")
    backlog_table.add_column("Status")
    backlog_table.add_column("Command")
    backlog_table.add_column("Duration", justify="right")
    backlog_table.add_column("Exit", justify="right")
    for job in data["backlog"]:
        status_style = {"done": "green", "failed": "red", "cancelled": "yellow"}.get(job["status"], "")
        backlog_table.add_row(
            str(job["id"]),
            f"[{status_style}]{job['status']}[/{status_style}]" if status_style else job["status"],
            _cmd_str(job, full),
            _fmt_duration(job),
            str(job["exit_code"]) if job["exit_code"] is not None else "-",
        )
    console.print(backlog_table)


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------

@app.command()
def status(as_json: bool = typer.Option(False, "--json")) -> None:
    """Show only the currently running job.

    Examples:
      queuer status
      queuer status --json
    """
    data = _call("status")
    if as_json:
        console.print_json(json.dumps(data))
        return
    if data["paused"]:
        console.print("[yellow]daemon is paused[/yellow]")
    running = data["running"]
    if running:
        console.print(f"[bold green]#{running['id']}[/bold green]  {_cmd_str(running, full=False)}")
        console.print(f"started: {_fmt_time(running['started_at'])}  running for: {_fmt_duration(running)}")
    else:
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
def cancel() -> None:
    """Kill the currently running job.

    Example:
      queuer cancel
    """
    data = _call("cancel")
    console.print(f"Cancelled job [bold]{data['cancelled']}[/bold]")


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
    """Re-enqueue a finished job with its original command, cwd, and timeout.

    Example:
      queuer requeue 12
    """
    data = _call("requeue", id=job_id)
    console.print(f"Requeued job {job_id} as new job [bold]{data['id']}[/bold]")


@app.command()
def pause() -> None:
    """Stop the daemon from starting new jobs. The current job (if any) keeps running.

    Example:
      queuer pause
    """
    _call("pause")
    console.print("[yellow]Paused[/yellow] -- new jobs will not start until you run `queuer resume`")


@app.command()
def resume() -> None:
    """Resume pulling new jobs from the queue.

    Example:
      queuer resume
    """
    _call("resume")
    console.print("[green]Resumed[/green]")


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
