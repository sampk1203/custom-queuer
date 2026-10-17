"""queuer: CLI client for the queuerd job queue daemon."""

from __future__ import annotations

import csv
import io
import json
import os
import re
import shlex
import shutil
import socket
import stat
import subprocess  # nosec B404 - only used for the fixed `ssh --` call below
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, NoReturn, Optional

import click
import typer
from rich.console import Console
from rich.markup import escape
from rich.table import Table
from typer.core import TyperCommand

from queuer import client, db, jobenv, logclean, notify, selfcheck
from queuer.daemon import DEFAULT_GRACE_SECS, _default_db_path, _default_log_dir
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
  qur wait 12                          block until job 12 ends (exit code mirrors it)
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
        raise typer.Exit(code=1) from None


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


def _fmt_retry_delay(job: dict[str, Any]) -> str:
    """`qur show` text of a job's --retry-delay / --retry-backoff."""
    text = _fmt_secs(job["retry_delay_secs"])
    return text + ", doubling every attempt" if job.get("retry_backoff") else text


def _retry_delay_secs(delay: str | None, backoff: bool, retries: int) -> int | None:
    """`--retry-delay` / `--retry-backoff` -> seconds (None = no delay). Exits with an
    error if they make no sense: no --retries, backoff without a delay, too long."""
    if delay is None:
        if backoff:
            _fail("--retry-backoff needs --retry-delay")
        return None
    if not retries:
        _fail("--retry-delay needs --retries")
    try:
        secs = int(parse_delay(delay).total_seconds())
    except ValueError as e:
        _fail(str(e))
    if secs > db.MAX_RETRY_DELAY_SECS:
        _fail(f"--retry-delay is too long: at most {db.MAX_RETRY_DELAY_SECS // 86400} days")
    return secs


def _fmt_stop(sig: str | None, grace: int | None) -> str:
    """"SIGINT, SIGKILL after 30s" (what a job's stop settings mean; defaults spelled out)."""
    after = DEFAULT_GRACE_SECS if grace is None else grace
    return f"{sig or 'SIGTERM'}, SIGKILL after {_fmt_secs(after)}"


def _stop_signal(value: str | None, flag: str) -> str | None:
    """`-s` / `--stop-signal` -> canonical signal name (None = not given; blank = back to
    the default, used by `qur edit`). Exits with an error for an unusable signal."""
    try:
        return db.check_stop_signal(value, flag)
    except ValueError as e:
        _fail(str(e))


def _grace_secs(value: str | None, flag: str) -> int | None:
    """`--grace` / `--stop-grace` -> whole seconds (None = not given or blank). A bare number
    is seconds (`--grace 30`); units work too (`90s`, `2m`, `1h`). 0 is allowed: SIGKILL
    right after the signal. Exits with an error for anything else."""
    if value is None or not value.strip():
        return None
    text = value.strip()
    try:
        secs = int(text) if text.isdigit() else int(parse_delay(text).total_seconds())
        return db.check_stop_grace(secs, flag)
    except ValueError as e:
        _fail(str(e))


def _fmt_duration(job: dict[str, Any]) -> str:
    started = job.get("started_at")
    if not started:
        return "-"
    start = datetime.fromisoformat(started)
    end = datetime.fromisoformat(job["finished_at"]) if job.get("finished_at") else datetime.now(start.tzinfo)
    return _fmt_secs((end - start).total_seconds())


def _fmt_mem_kb(kb: float) -> str:
    """F10: a peak-RSS reading in KB as a short human size, e.g. "31 GB"."""
    mb = kb / 1024
    if mb >= 1024:
        return f"{mb / 1024:.1f} GB"
    if mb >= 1:
        return f"{mb:.0f} MB"
    return f"{kb:.0f} KB"


_STATUS_STYLES = {"done": "green", "failed": "red", "cancelled": "yellow", "interrupted": "magenta", "skipped": "dim"}


def _fmt_status(status: str) -> str:
    style = _STATUS_STYLES.get(status, "")
    return f"[{style}]{status}[/{style}]" if style else status


def _fmt_origin(job: dict[str, Any]) -> str:
    """Where a job came from: queued fresh, auto-retried by the daemon after a
    failure, or requeued by hand."""
    origin, parent = job.get("origin"), job.get("parent_id")
    if origin == "retry":
        return f"retry of #{parent} ({job['attempt']}/{job['max_retries']})"
    if origin == "requeue":
        return f"requeue of #{parent}"
    return "new"


def _fmt_waits(job: dict[str, Any]) -> str:
    """The "Waits on" cell of a queued job: the dependencies that have not ended yet
    ("12, 14"), or "-" when it has none / they all ended (it is then only waiting for
    the jobs ahead of it)."""
    waiting = job.get("waiting_on") or []
    parts = [_fmt_ids(waiting)] if waiting else []
    if job.get("editing"):
        parts.append("editing")  # `qur edit` has the job open: it is not started, its channel waits
    return ", ".join(parts) if parts else "-"


def _fmt_elapsed(ts: str | None) -> str:
    """Time since `ts` (an ISO timestamp), e.g. how long a job has been queued."""
    if not ts:
        return "-"
    then = datetime.fromisoformat(ts)
    return _fmt_secs(max(0.0, (datetime.now(then.tzinfo) - then).total_seconds()))


# ---------------------------------------------------------------------------
# add
# ---------------------------------------------------------------------------

def _parse_job_ids(values: list[str] | None, flag: str) -> list[int]:
    """Job ids from repeated `--depends-on` / `--after-any` values. Each value is a
    comma list of ids and `A-B` ranges ("12", "12,14", "31-33,40"); several flags
    combine. Returns the distinct ids, ascending. Exits with a clear error on junk."""
    return sorted(_collect_job_ids(values, flag))


def _collect_job_ids(values: list[str] | None, flag: str) -> list[int]:
    """Like `_parse_job_ids` but keeps the order given (first mention wins)."""
    ids: dict[int, None] = {}
    for value in values or []:
        for part in value.split(","):
            part = part.strip()
            m = re.fullmatch(r"(\d+)(?:-(\d+))?", part)
            if m is None:
                _fail(f"{flag}: {part!r} is not a job id or a range like 31-33")
            first = int(m.group(1))
            last = int(m.group(2)) if m.group(2) is not None else first
            if first < 1 or last < first:
                _fail(f"{flag}: {part!r} is not a valid job id range (ids start at 1, low end first)")
            if last - first + 1 > db.MAX_SWEEP:
                _fail(f"{flag}: {part!r} is more than {db.MAX_SWEEP} jobs")
            ids.update(dict.fromkeys(range(first, last + 1)))
            if len(ids) > db.MAX_SWEEP:
                _fail(f"{flag}: more than {db.MAX_SWEEP} jobs listed")
    return list(ids)


_DELAY_UNIT_SECS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def parse_delay(text: str) -> timedelta:
    """`--delay` value -> timedelta. Units s/m/h/d, which can be chained: "30m", "2h",
    "1h30m", "90s". A bare number is rejected (seconds or minutes? say which)."""
    value = text.strip().lower()
    if not re.fullmatch(r"(?:\d+[smhd])+", value):
        raise ValueError(f"{text!r} is not a duration: use a number with a unit s/m/h/d, e.g. 30m, 2h, 1h30m")
    secs = sum(int(n) * _DELAY_UNIT_SECS[u] for n, u in re.findall(r"(\d+)([smhd])", value))
    if secs <= 0:
        raise ValueError(f"{text!r}: the delay must be longer than zero")
    try:
        return timedelta(seconds=secs)
    except OverflowError:
        raise ValueError(f"{text!r} is too long a delay") from None


def parse_at(text: str, now: datetime) -> datetime:
    """`--at` value -> an absolute local time (timezone-aware), `now` being the current
    local time. "HH:MM" is the NEXT occurrence of that clock time (today if still ahead,
    else tomorrow); "YYYY-MM-DD HH:MM" is that exact moment and must be in the future."""
    value = text.strip()
    clock = re.fullmatch(r"(\d{1,2}):(\d{2})", value)
    if clock is not None:
        hour, minute = int(clock.group(1)), int(clock.group(2))
        if hour > 23 or minute > 59:
            raise ValueError(f"{text!r} is not a valid time of day (HH:MM, 00:00 to 23:59)")
        first = datetime(now.year, now.month, now.day, hour, minute).astimezone()
        if first > now:
            return first
        nxt = datetime(now.year, now.month, now.day) + timedelta(days=1)
        return datetime(nxt.year, nxt.month, nxt.day, hour, minute).astimezone()
    full = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})[ T](\d{1,2}):(\d{2})", value)
    if full is None:
        raise ValueError(f"{text!r} is not a time: use HH:MM (e.g. 22:00) or 'YYYY-MM-DD HH:MM'")
    year, month, day, hour, minute = (int(g) for g in full.groups())
    try:
        when = datetime(year, month, day, hour, minute).astimezone()
    except (ValueError, OverflowError):
        raise ValueError(f"{text!r} is not a valid date and time") from None
    if when <= now:
        raise ValueError(f"{text!r} is in the past (it is {now.strftime('%Y-%m-%d %H:%M')} now)")
    return when


def _start_time(delay: str | None, at: str | None) -> datetime | None:
    """The absolute start time asked for by `--delay` / `--at` (None if neither); exits
    with a clear error on junk or on both flags together."""
    if delay is not None and at is not None:
        _fail("--delay and --at cannot be combined; use one of them")
    now = datetime.now().astimezone()
    try:
        if delay is not None:
            return now + parse_delay(delay)
        if at is not None:
            return parse_at(at, now)
    except (ValueError, OverflowError) as e:
        _fail(f"{'--delay' if delay is not None else '--at'}: {e}")
    return None


def _starts_in_future(job: dict[str, Any]) -> bool:
    """True if the job has a start time (`--delay` / `--at`) that has not come yet."""
    not_before = job.get("not_before")
    if not not_before:
        return False
    when = datetime.fromisoformat(not_before)
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return when > datetime.now(timezone.utc)


def _fmt_start(job: dict[str, Any]) -> str:
    """The "Starts at" cell of a queued job: its start time while that is still ahead,
    else "-"."""
    return _fmt_time(job["not_before"]) if _starts_in_future(job) else "-"


def _expand_each(args: list[str]) -> list[str]:
    """`--each V1 V2 V3 -- CMD` -> `--each V1 --each V2 --each V3 -- CMD`: click options
    take one value each, so the values after `--each` (up to the next `--flag` or the
    `--` before the command) are split into repeated options before parsing. Everything
    after the first `--` is the command and is never touched."""
    out: list[str] = []
    i = 0
    while i < len(args):
        arg = args[i]
        if arg == "--":
            out.extend(args[i:])
            break
        if arg == "--each":
            j = i + 1
            while j < len(args) and not args[j].startswith("--"):
                out.extend(["--each", args[j]])
                j += 1
            if j == i + 1:  # click would swallow the `--` as the value, so say it here
                raise click.UsageError("--each needs at least one value, e.g. --each a b c -- CMD {}")
            i = j
            continue
        out.append(arg)
        i += 1
    return out


class _AddCommand(TyperCommand):
    """`qur add`, whose `--each` takes several values (see `_expand_each`)."""

    # ctx is typed Any on purpose: typer >= 0.27 bundles its own copy of click (typer._click),
    # so neither `click.Context` nor a private import matches the base class for mypy.
    def parse_args(self, ctx: Any, args: list[str]) -> list[str]:
        return super().parse_args(ctx, _expand_each(args))


def _sweep_jobs(cmd: list[str], each: list[str] | None, array: str | None) -> list[dict[str, Any]] | None:
    """The per-job requests of a `--each` / `--array` sweep (None if neither flag).

    --each: one job per value, `{}` replaced by the value in every word that has it;
    QUR_INDEX is the 1-based position. A value substituted into a command that a shell
    will parse (one quoted word, or a hand-written `bash -c`) is shell-quoted first.
    --array A-B: one job per number A..B inclusive, the same command, QUR_INDEX = the number."""
    if each and array:
        _fail("--each and --array cannot be combined")
    if each:
        if len(each) > db.MAX_SWEEP:
            _fail(f"--each: {len(each)} values is too many (at most {db.MAX_SWEEP} jobs per add)")
        if not any("{}" in word for word in cmd):
            _fail("--each needs {} somewhere in the command, e.g. qur add --each a b c -- ./run.sh {}")
        quote_at = {0} if len(cmd) == 1 else ({2} if _is_explicit_shell_c(cmd) else set())
        jobs = []
        for position, value in enumerate(each, 1):
            words = [
                word.replace("{}", shlex.quote(value) if n in quote_at else value) for n, word in enumerate(cmd)
            ]
            try:
                argv = _resolve_interpreter(_build_job_argv(words))
            except ValueError as e:
                _fail(f"--each value {value!r}: {e}")
            jobs.append({"argv": argv, "raw_argv": words, "index": position})
        return jobs
    if array is not None:
        m = re.fullmatch(r"(\d+)-(\d+)", array.strip())
        if m is None:
            _fail(f"--array: {array!r} is not a range like 1-20")
        low, high = int(m.group(1)), int(m.group(2))
        if high < low:
            _fail(f"--array: {array!r} is backwards (write the smaller number first)")
        if high - low + 1 > db.MAX_SWEEP:
            _fail(f"--array: {high - low + 1} jobs is too many (at most {db.MAX_SWEEP} per add)")
        argv = _resolve_interpreter(_build_job_argv(cmd))
        return [{"argv": argv, "raw_argv": cmd, "index": n} for n in range(low, high + 1)]
    return None


def _fmt_ids(ids: list[int]) -> str:
    """Job ids for display, runs collapsed: [31, 32, 33, 40] -> "31-33, 40"."""
    parts: list[str] = []
    i = 0
    while i < len(ids):
        j = i
        while j + 1 < len(ids) and ids[j + 1] == ids[j] + 1:
            j += 1
        parts.append(str(ids[i]) if j == i else (f"{ids[i]}, {ids[j]}" if j == i + 1 else f"{ids[i]}-{ids[j]}"))
        i = j + 1
    return ", ".join(parts)


def _render_dry_run(data: dict[str, Any], timeout: int | None) -> None:
    """Print what `qur add --dry-run` found out (F16): the daemon validated the request and
    placed it exactly as a real add would, then threw the rows away."""
    jobs: list[dict[str, Any]] = data["jobs"]
    first = jobs[0]
    sweep = first["index"] is not None
    console.print("[bold]Dry run[/bold]: nothing was queued.")
    if sweep:
        console.print(f"would add {len(jobs)} jobs on channel [bold]{data['channel']}[/bold] (one per value, QUR_INDEX set per job)")
    else:
        console.print(f"would be queued on channel [bold]{data['channel']}[/bold]")
    shown = jobs if not sweep else jobs[:5]
    for job in shown:
        prefix = f"job with QUR_INDEX={job['index']}: " if sweep else ""
        console.print(f"{prefix}raw command: {escape(_display_join(job['raw_argv']))}")
        console.print(f"{prefix}resolved command: {escape(_display_join(job['argv']))}")
    if sweep and len(jobs) > len(shown):
        console.print(f"... and {len(jobs) - len(shown)} more")
    console.print(f"cwd: {escape(data['cwd'])}")
    env_lines = jobenv.describe({**jobenv.DEFAULT_JOB_ENV, **data["env"]})
    console.print(f"environment: {escape(', '.join(env_lines))}")
    if timeout:
        console.print(f"timeout: {timeout}s")
    if data.get("on_finish"):
        console.print(f"on finish hook: {escape(data['on_finish'])}")
    if data.get("on_error"):
        console.print(f"on error hook: {escape(data['on_error'])}")
    if data.get("stop_signal") or data.get("stop_grace") is not None:
        console.print(f"stop: {_fmt_stop(data.get('stop_signal'), data.get('stop_grace'))}")
    running, frozen = data.get("running_id"), data.get("frozen_id")
    where = str(first["position"]) if not sweep else f"{first['position']}-{jobs[-1]['position']}"
    label = "queue position" if not sweep else "queue positions"
    if data["priority"]:
        console.print(f"{label}: {where} on the --now lane of channel {data['channel']}")
        if running:
            console.print(f"job {running} (running now) would be frozen and resumed after the --now jobs")
    else:
        ahead = f"{data['queued_before']} queued ahead" if first["position"] == data["queued_before"] + 1 else "inserted among queued jobs"
        busy = f"; job {running} is running now" if running else ("; a job is frozen" if frozen else "; the channel is idle")
        console.print(f"{label}: {where} ({ahead}{busy})")
    if data["paused"]:
        console.print(f"[yellow]channel {data['channel']} is paused[/yellow]: nothing starts until `qur resume`")
    for job in jobs:
        if job["dep_verdict"] in ("failed", "removed"):
            what = "was removed" if job["dep_verdict"] == "removed" else "has already failed"
            console.print(f"[yellow]would be skipped right away:[/yellow] dependency {job['dep_id']} {what}")
            break


def _help_all(ctx: typer.Context, value: bool) -> None:
    if not value or ctx.resilient_parsing:
        return
    group = typer.main.get_group(app)
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


@app.command(cls=_AddCommand)
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
    before: Optional[int] = typer.Option(
        None, "--before",
        help="Queue position only: insert before this still-queued job ID (same channel). "
        "It does NOT make this job wait for that one to finish or succeed.",
    ),
    after: Optional[int] = typer.Option(
        None, "--after",
        help="Queue position only: insert right after this still-queued job ID (same channel). "
        "It does NOT make this job wait for that one to finish or succeed; "
        "if that job already started, the add is rejected.",
    ),
    depends_on: Optional[list[str]] = typer.Option(
        None, "--depends-on", metavar="ID[,ID...]",
        help="Run this job only if every listed job ENDS SUCCESSFULLY (done, exit 0). If any of them ends "
        "failed/cancelled/skipped/interrupted (or was removed), this job is marked skipped and the queue moves "
        "on. IDs may be a comma list or a range (31-33); repeat the flag to add more. Unlike --before/--after "
        "this is about WHEN the job may run, not where it sits. Strict FIFO: while it waits it blocks the jobs "
        "behind it on its channel -- put it on another channel if something else must run meanwhile.",
    ),
    after_any: Optional[list[str]] = typer.Option(
        None, "--after-any", metavar="ID[,ID...]",
        help="Run this job once every listed job has ENDED, however it ended (done, failed, cancelled, skipped, "
        "interrupted). Same ID syntax and the same blocking behaviour as --depends-on; the two can be combined.",
    ),
    each: Optional[list[str]] = typer.Option(
        None, "--each", metavar="V1 V2 ...",
        help="Sweep: queue one independent job per value, in order, with {} in the command replaced by the "
        "value (in every word that contains it; an error if there is no {}). The values run up to the next "
        "--flag or the `--`. QUR_INDEX is set to the value's 1-based position. All jobs share the channel, "
        "timeout, retries and dependencies. At most 1000 jobs.",
    ),
    array: Optional[str] = typer.Option(
        None, "--array", metavar="A-B",
        help="Sweep: queue one job per number A..B inclusive, all running the same command; each job's "
        "environment has QUR_INDEX set to its number. Same sharing and 1000-job cap as --each. "
        "Prints the created id range for use with --depends-on.",
    ),
    on_fail: str = typer.Option(
        "continue", "--on-fail", metavar="pause|continue",
        help="What a FAILURE of this job does to its channel. continue (default): the next job starts as "
        "usual. pause: the channel is paused and a Telegram message says so; resume it with "
        "`qur resume`. Only `failed` counts (non-zero exit, timeout, failed start): not cancelled, skipped "
        "or interrupted jobs, and not a failure that --retries requeues (only the last one).",
    ),
    on_finish: Optional[str] = typer.Option(
        None, "--on-finish", metavar="COMMAND",
        help="Hook: a shell command (run with /bin/sh -c) started after this job's FINAL outcome: done, "
        "failed or cancelled while running (not skipped or interrupted jobs, and not a failure that --retries "
        "requeues). It runs in the job's cwd with the job's environment plus QUR_JOB_ID, QUR_STATUS, "
        "QUR_EXIT_CODE, QUR_CHANNEL, QUR_CWD, QUR_LOG, QUR_COMMAND, QUR_DURATION_SECS and QUR_HOOK, in the "
        "background (it never holds the channel; killed after 5 minutes). Its output and exit code are "
        "appended to the job's log. Example: --on-finish 'rsync -a out/ host:results/'.",
    ),
    on_error: Optional[str] = typer.Option(
        None, "--on-error", metavar="COMMAND",
        help="Hook like --on-finish, but only when the job ends `failed` (non-zero exit, timeout, failed "
        "start; the last failure when --retries is used). Runs after the global hook and --on-finish.",
    ),
    stop_signal: Optional[str] = typer.Option(
        None, "--stop-signal", metavar="SIG",
        help="Signal that stops this job when it is cancelled or times out, instead of SIGTERM: one of "
        "TERM, INT, HUP, QUIT, USR1, USR2, KILL (or its number). It goes to the job's whole process group. "
        "Use INT for a program that writes a checkpoint on Ctrl-C. `qur cancel -s` overrides it for one "
        "cancel. A daemon stop (reboot, `qur daemon stop`) still uses SIGTERM.",
    ),
    stop_grace: Optional[str] = typer.Option(
        None, "--stop-grace", metavar="SECONDS",
        help="How long the job gets between the stop signal and SIGKILL (default 5 seconds): a number of "
        "seconds, or a duration like 90s, 2m. 0 = SIGKILL at once. `qur cancel --grace` overrides it for "
        "one cancel. Not used when the daemon itself stops.",
    ),
    delay: Optional[str] = typer.Option(
        None, "--delay", metavar="DURATION",
        help="Do not start this job until this long from now: a number with a unit s/m/h/d "
        "(30m, 2h, 1h30m). Stored as an absolute time, so it survives daemon restarts. Strict FIFO: "
        "if this job reaches the front of its channel early it holds the channel until its time; "
        "put it on another channel if the queue must keep running meanwhile. After its time it also "
        "waits for any --depends-on/--after-any. The timeout counts run time only. "
        "Not with --at or --now.",
    ),
    at: Optional[str] = typer.Option(
        None, "--at", metavar="TIME",
        help="Do not start this job before this local time: HH:MM (the next time the clock shows it: "
        "today if still ahead, else tomorrow) or 'YYYY-MM-DD HH:MM'. Same holding rule as --delay. "
        "Not with --delay or --now.",
    ),
    env: Optional[list[str]] = typer.Option(
        None, "--env", metavar="KEY[=VAL]",
        help="Add an environment variable to the job; repeat for more. KEY alone copies the value from this "
        "shell, KEY=VAL sets it. Jobs always get PATH, HOME, VIRTUAL_ENV, PYTHONPATH and PYTHONUNBUFFERED=1, "
        "plus these when set in this shell: CUDA_VISIBLE_DEVICES, OMP_NUM_THREADS, MKL_NUM_THREADS, "
        "OPENBLAS_NUM_THREADS, LD_LIBRARY_PATH, LANG, LC_*, TERM, TZ (change that list with the "
        "QUEUER_ENV_PASSTHROUGH variable: comma-separated names, `*` allowed, empty = none). --env wins over "
        "all of these; QUR_INDEX of a sweep wins over --env. Values are stored in the job and are visible "
        "in `qur show`, except names containing KEY/TOKEN/SECRET/PASSWORD.",
    ),
    timeout: Optional[int] = typer.Option(
        None, "--timeout", help="Kill the job if it runs longer than this many seconds (counts as a failure)"
    ),
    retries: int = typer.Option(
        0, "--retries", min=0,
        help="If the job fails (non-zero exit, timeout, or failed start), requeue it automatically at the "
        "end of its channel's queue, up to this many times. Cancelled jobs and jobs interrupted by a daemon stop "
        "are never retried.",
    ),
    retry_delay: Optional[str] = typer.Option(
        None, "--retry-delay", metavar="DURATION",
        help="Wait this long before an automatic retry (--retries) may start, e.g. 5m, 1h30m (units s/m/h/d, "
        "at most 30d). Default: retry as soon as it reaches the front. Strict FIFO applies: the waiting retry "
        "holds its channel. A manual requeue is never delayed. Needs --retries.",
    ),
    retry_backoff: bool = typer.Option(
        False, "--retry-backoff",
        help="Double the --retry-delay for every retry already made (5m, 10m, 20m, ...; capped at 30d). "
        "Needs --retry-delay.",
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run",
        help="Check and place the job as a real add would, but queue nothing: prints the raw and resolved "
        "command, cwd, environment, channel and the queue position it would get (and whether it would be "
        "skipped at once). Errors are the same as for a real add. Works with every other option.",
    ),
    now: bool = typer.Option(
        False, "--now",
        help="Jump the channel's queue: freeze (SIGSTOP) the running job and run this first. "
        "More --now jobs run FIFO behind it, then the frozen job resumes (SIGCONT). "
        "While frozen it stays alive and keeps its memory, so two jobs of one channel can exist at once. "
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
      qur add --retries 3 --retry-delay 5m -- ./flaky.sh   # wait 5 min before each retry
      qur add --retries 4 --retry-delay 1m --retry-backoff -- ./flaky.sh   # 1m, 2m, 4m, 8m
      qur add --on-fail pause -- ./step1.sh          # a failure pauses the channel
      qur add --on-finish 'rsync -a out/ host:results/' -- ./run.sh   # hook after the job ends
      qur add --on-error 'notify-send failed' -- ./run.sh             # hook only if it fails
      qur add --env CUDA_VISIBLE_DEVICES=1 --env OMP_NUM_THREADS=8 -- python train.py
      qur add --env MY_SETTING -- ./run.sh            # copy MY_SETTING from this shell
      qur add --after 12 -- python eval.py
      qur add --before 12 -- python setup.py
      qur add --depends-on 12 -- python eval.py
      qur add --depends-on 31-33,40 --after-any 35 -- python collect.py
      qur add --each 0.1 0.2 0.5 -- python train.py --lr {}
      qur add --array 1-20 -- ./run.sh               # $QUR_INDEX is 1..20
      qur add --depends-on 31-33 -- python collect.py
      qur add --delay 30m -- ./later.sh              # start in 30 minutes
      qur add --at 22:00 -- ./overnight.sh           # start at 22:00 (tomorrow if it is past)
      qur add --at "2026-12-01 06:30" -- ./run.sh
      qur add --now -- python urgent_eval.py
      qur add --dry-run --delay 2h -- python train.py   # check it, queue nothing
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
    if on_fail not in db.ON_FAIL_MODES:
        _fail(f"--on-fail must be one of {', '.join(db.ON_FAIL_MODES)} (got {on_fail!r})")
    stop_signal_name = _stop_signal(stop_signal, "--stop-signal")
    stop_grace_secs = _grace_secs(stop_grace, "--stop-grace")
    sweep = _sweep_jobs(cmd, each, array)
    job_argv = _resolve_interpreter(_build_job_argv(cmd)) if sweep is None else []
    depends_ids = _parse_job_ids(depends_on, "--depends-on")
    after_any_ids = _parse_job_ids(after_any, "--after-any")
    if now and (depends_ids or after_any_ids):
        _fail("--now cannot be combined with --depends-on/--after-any")
    start = _start_time(delay, at)
    if now and start is not None:
        _fail("--now cannot be combined with --delay/--at")
    not_before = start.astimezone(timezone.utc).isoformat() if start is not None else None
    retry_delay_secs = _retry_delay_secs(retry_delay, retry_backoff, retries)
    try:
        env_explicit = jobenv.parse_env_options(env, os.environ)
    except ValueError as e:
        _fail(str(e))
    env_pass = {k: v for k, v in jobenv.collect_passthrough(os.environ).items() if k not in env_explicit}

    job_kwargs: dict[str, Any] = {"argv": job_argv, "raw_argv": cmd} if sweep is None else {"jobs": sweep}
    data = _call(
        "add",
        **job_kwargs,
        cwd=os.getcwd(),
        path=os.environ.get("PATH", ""),
        virtual_env=os.environ.get("VIRTUAL_ENV"),
        python_path=os.environ.get("PYTHONPATH"),
        env=env_explicit,
        env_pass=env_pass,
        channel=channel,
        before=before,
        after=after,
        timeout_secs=timeout,
        now=now,
        max_retries=retries,
        depends_on=depends_ids,
        after_any=after_any_ids,
        not_before=not_before,
        on_fail=on_fail,
        retry_delay_secs=retry_delay_secs,
        retry_backoff=retry_backoff,
        on_finish=on_finish,
        on_error=on_error,
        stop_signal=stop_signal_name,
        stop_grace=stop_grace_secs,
        **({"dry_run": True} if dry_run else {}),
    )
    if dry_run:
        _render_dry_run(data, timeout)
    elif sweep is None:
        console.print(f"Queued as job [bold]{data['id']}[/bold] on channel [bold]{data['channel']}[/bold]")
    else:
        ids = data.get("ids") or [data["id"]]
        span = f"{ids[0]}-{ids[-1]}" if len(ids) > 1 and ids == list(range(ids[0], ids[-1] + 1)) else _fmt_ids(ids)
        console.print(f"added jobs [bold]{span}[/bold] ({len(ids)}) on channel [bold]{data['channel']}[/bold]")
    if sweep is None and not dry_run:
        console.print(f"command: {escape(_display_join(job_argv))}")
    if not dry_run:
        console.print(f"cwd: {escape(os.getcwd())}")
    if depends_ids:
        console.print(f"runs only if job(s) {_fmt_ids(depends_ids)} finish successfully")
    if after_any_ids:
        console.print(f"runs after job(s) {_fmt_ids(after_any_ids)} have ended (any outcome)")
    if start is not None:
        console.print(f"starts no earlier than {_fmt_time(not_before)}")
    if data.get("skipped"):
        console.print(f"[yellow]skipped right away:[/yellow] {escape(str(data['skipped']))}")
    if env_explicit and not dry_run:
        console.print(f"environment: {escape(', '.join(env_explicit))}")
    if on_fail == "pause":
        console.print("on failure: this channel will be paused (`qur resume` to continue)")
    if on_finish:
        console.print(f"on finish: runs `{escape(on_finish)}`")
    if on_error:
        console.print(f"on error: runs `{escape(on_error)}`")
    if stop_signal_name or stop_grace_secs is not None:
        console.print(f"stopped with {_fmt_stop(stop_signal_name, stop_grace_secs)}")
    if retries:
        console.print(f"auto-retry on failure: up to {retries} time(s)")
    if retry_delay_secs:
        console.print(f"each retry waits {_fmt_secs(retry_delay_secs)}" + (", doubling every attempt" if retry_backoff else ""))
    if now and not dry_run:
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
    show_waits = any(j.get("depends_on") or j.get("after_any") or j.get("editing") for j in channel_data["queue"])
    if show_waits:
        queue_table.add_column("Waits on")
    show_starts = any(_starts_in_future(j) for j in channel_data["queue"])
    if show_starts:
        queue_table.add_column("Starts at")
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
        row = [
            str(job["id"]), _cmd_str(job, full), _fmt_origin(job),
            _fmt_time(job["enqueued_at"]), _fmt_elapsed(job["enqueued_at"]),
        ]
        if show_waits:
            row.append(_fmt_waits(job))
        if show_starts:
            row.append(_fmt_start(job))
        queue_table.add_row(*row)

    if running or priority_queue or channel_data["queue"]:
        console.print(queue_table)
    else:
        console.print("[dim](idle -- nothing running or queued)[/dim]")

    recent = channel_data["backlog"]
    title = f"Recent (last {len(recent)})" if recent else "Recent"
    if len(recent) >= db.LIST_BACKLOG:
        title += " -- more with `qur history`"
    backlog_table = Table(title=escape(title), show_lines=False)
    backlog_table.add_column("ID", justify="right")
    backlog_table.add_column("Status")
    backlog_table.add_column("Command")
    backlog_table.add_column("Origin")
    backlog_table.add_column("Duration", justify="right")
    backlog_table.add_column("Exit", justify="right")
    for job in recent:
        backlog_table.add_row(
            str(job["id"]),
            _fmt_status(job["status"]),
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
    so it includes any wait). Below that: the last 10 finished jobs (older ones and
    filters: `qur history`).

    The Origin column says where a job came from: "new" (queued by you),
    "retry of #N" (queued automatically by --retries after job N failed; shows
    the retry count), or "requeue of #N" (you ran `qur requeue N`). Only channels
    that have had a job show up. For one job's full detail, use `qur show`.

    A queued job that was added with --depends-on / --after-any stays "queued"; the
    "Waits on" column then lists the dependencies that have not ended yet. While it
    waits, the jobs behind it on that channel wait too (strict FIFO).

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
# history
# ---------------------------------------------------------------------------

def parse_since(text: str, now: datetime) -> datetime:
    """`--since` value -> an absolute local time (timezone-aware), `now` being the current
    local time. A duration ("2d", "6h", "1h30m": that long ago) or a date/time in the past
    ("2026-10-01" = midnight local, "2026-10-01 14:30")."""
    value = text.strip()
    try:
        return now - parse_delay(value)
    except ValueError:
        pass
    full = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})(?:[ T](\d{1,2}):(\d{2}))?", value)
    if full is None:
        raise ValueError(f"{text!r} is not a duration (2d, 6h, 1h30m) or a date (YYYY-MM-DD, 'YYYY-MM-DD HH:MM')")
    year, month, day = int(full.group(1)), int(full.group(2)), int(full.group(3))
    hour, minute = int(full.group(4) or 0), int(full.group(5) or 0)
    try:
        when = datetime(year, month, day, hour, minute).astimezone()
    except (ValueError, OverflowError):
        raise ValueError(f"{text!r} is not a valid date and time") from None
    return when


_TERMINAL_STATUSES_HELP = ", ".join(db.TERMINAL_STATUSES)


@app.command()
def history(
    status: Optional[list[str]] = typer.Option(
        None, "--status", metavar="STATUS",
        help=f"Only jobs that ended like this ({_TERMINAL_STATUSES_HELP}); repeat for several.",
    ),
    channel: Optional[int] = typer.Option(None, "--channel", min=1, help="Only this channel (default: all)"),
    since: Optional[str] = typer.Option(
        None, "--since", metavar="DURATION|DATE",
        help="Only jobs that finished since then: a duration (2d, 6h, 1h30m) or a date "
        "(YYYY-MM-DD, local midnight; or 'YYYY-MM-DD HH:MM').",
    ),
    grep: Optional[str] = typer.Option(
        None, "--grep", metavar="TEXT", help="Only jobs whose command contains TEXT (case-insensitive)."
    ),
    limit: int = typer.Option(50, "--limit", "-n", min=1, max=db.MAX_HISTORY, help="Most rows to show (newest first)"),
    full: bool = typer.Option(False, "--full", help="Show resolved absolute paths instead of what you typed"),
    as_json: bool = typer.Option(False, "--json", help="Print the jobs as a JSON list instead of a table"),
    as_csv: bool = typer.Option(False, "--csv", help="Print the jobs as CSV (header row first) instead of a table"),
) -> None:
    """Search finished jobs (done, failed, cancelled, skipped, interrupted), newest first.

    The daemon keeps the last 200 finished jobs per channel (set QUEUER_KEEP in the
    daemon's environment to change that); `qur list` shows only the last 10. Filters
    combine (all must match). The command is matched against what you typed after `--`.

    \b
    Examples:
      qur history
      qur history --status failed --since 2d
      qur history --status failed --status interrupted --channel 2
      qur history --since 2026-10-01 --grep train.py --limit 200
      qur history --json
      qur history --status failed --csv > failed.csv
    """
    if as_json and as_csv:
        _fail("--json and --csv cannot be combined")
    for s in status or []:
        if s not in db.TERMINAL_STATUSES:
            _fail(f"--status: {s!r} is not one of {', '.join(db.TERMINAL_STATUSES)}")
    since_ts: str | None = None
    if since is not None:
        try:
            since_ts = parse_since(since, datetime.now().astimezone()).astimezone(timezone.utc).isoformat()
        except (ValueError, OverflowError) as e:
            _fail(f"--since: {e}")
    data = _call(
        "history", statuses=list(status) if status else None, channel=channel, since=since_ts, grep=grep, limit=limit,
    )
    jobs = data["jobs"]
    if as_json:
        console.print_json(json.dumps(jobs))
        return
    if as_csv:
        typer.echo(_history_csv(jobs, full), nl=False)
        return
    if not jobs:
        console.print("[dim](no matching jobs)[/dim]")
        return
    table = Table(title=f"History ({len(jobs)})", show_lines=False)
    table.add_column("ID", justify="right")
    table.add_column("Ch", justify="right")
    table.add_column("Status")
    table.add_column("Command")
    table.add_column("Origin")
    table.add_column("Finished")
    table.add_column("Duration", justify="right")
    table.add_column("Exit", justify="right")
    for job in jobs:
        table.add_row(
            str(job["id"]),
            str(job["channel"]),
            _fmt_status(job["status"]),
            _cmd_str(job, full),
            _fmt_origin(job),
            _fmt_time(job["finished_at"]),
            _fmt_duration(job),
            str(job["exit_code"]) if job["exit_code"] is not None else "-",
        )
    console.print(table)
    if len(jobs) >= limit:
        console.print(f"[dim](limit {limit} reached; use --limit to see more)[/dim]")


CSV_COLUMNS = (
    "id", "channel", "status", "command", "cwd", "origin", "enqueued_at", "started_at", "finished_at",
    "duration_secs", "exit_code", "note",
)


def _csv_cell(value: Any) -> str:
    """One CSV cell. A text starting with = + - @ (or tab/CR) would run as a formula when the
    file is opened in a spreadsheet, so it gets a leading apostrophe."""
    text = "" if value is None else str(value)
    if text[:1] in ("=", "+", "-", "@", "\t", "\r"):
        return "'" + text
    return text


def _job_csv_row(job: dict[str, Any], full: bool) -> list[str]:
    secs: str = ""
    if job.get("started_at") and job.get("finished_at"):
        secs = f"{(datetime.fromisoformat(job['finished_at']) - datetime.fromisoformat(job['started_at'])).total_seconds():.1f}"
    values: list[Any] = [
        job["id"], job["channel"], job["status"], _cmd_str(job, full), job.get("cwd"), _fmt_origin(job),
        job.get("enqueued_at"), job.get("started_at"), job.get("finished_at"), secs, job.get("exit_code"),
        job.get("note"),
    ]
    return [_csv_cell(v) for v in values]


def _history_csv(jobs: list[dict[str, Any]], full: bool) -> str:
    """The `qur history --csv` text: a header row, then one row per job (UTC ISO timestamps)."""
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(CSV_COLUMNS)
    for job in jobs:
        writer.writerow(_job_csv_row(job, full))
    return buf.getvalue()


_EXPORT_STATUSES = db.ACTIVE_STATUSES + db.TERMINAL_STATUSES
_EXPORT_STATUS_HELP = f"Only jobs in this state ({', '.join(_EXPORT_STATUSES)}); repeat for several."  # module level: ruff B008 forbids calls in defaults


@app.command(name="export")
def export_jobs(
    status: Optional[list[str]] = typer.Option(
        None, "--status", metavar="STATUS",
        help=_EXPORT_STATUS_HELP,
    ),
    channel: Optional[int] = typer.Option(None, "--channel", min=1, help="Only this channel (default: all)"),
) -> None:
    """Print every job row as a JSON list (oldest first): commands, cwd, env, times, exit codes.

    Queued, running and finished jobs; no logs. Redirect it to a file. Values passed with
    `--env` are written as stored (unmasked), so keep the file private.

    \b
    Examples:
      qur export > jobs.json
      qur export --channel 2 --status failed --status done > ch2.json
    """
    for s in status or []:
        if s not in _EXPORT_STATUSES:
            _fail(f"--status: {s!r} is not one of {', '.join(_EXPORT_STATUSES)}")
    data = _call("export", statuses=list(status) if status else None, channel=channel)
    typer.echo(json.dumps(data["jobs"], indent=2))


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
    if job.get("max_rss_kb") is not None:
        table.add_row("peak RSS", _fmt_mem_kb(job["max_rss_kb"]))
    if job.get("cpu_user_secs") is not None or job.get("cpu_sys_secs") is not None:
        table.add_row(
            "cpu time",
            f"{_fmt_secs(job.get('cpu_user_secs') or 0.0)} user + {_fmt_secs(job.get('cpu_sys_secs') or 0.0)} sys",
        )
    if job.get("on_fail") == "pause":
        table.add_row("on fail", "pause the channel")
    if job.get("on_finish_cmd"):
        table.add_row("on finish", escape(job["on_finish_cmd"]))
    if job.get("on_error_cmd"):
        table.add_row("on error", escape(job["on_error_cmd"]))
    if job.get("stop_signal") or job.get("stop_grace_secs") is not None:
        table.add_row("stop", _fmt_stop(job.get("stop_signal"), job.get("stop_grace_secs")))
    if job.get("not_before"):
        waiting = " (still waiting)" if job["status"] == "queued" and _starts_in_future(job) else ""
        table.add_row("not before", _fmt_time(job["not_before"]) + waiting)
    if job.get("depends_on"):
        table.add_row("depends on", _fmt_ids(job["depends_on"]) + " (must finish done)")
    if job.get("after_any"):
        table.add_row("after any", _fmt_ids(job["after_any"]) + " (any outcome)")
    if job.get("waiting_on"):
        table.add_row("waiting on", _fmt_ids(job["waiting_on"]))
    if job.get("editing"):
        table.add_row("editing", f"being edited by process {job.get('edit_pid')}: it does not start, its channel waits")
    if job.get("max_retries"):
        table.add_row("auto-retries", f"{job['attempt']} of {job['max_retries']} used")
    if job.get("retry_delay_secs"):
        table.add_row("retry delay", _fmt_retry_delay(job))
    env_lines = jobenv.describe(job.get("env_extra"))
    if env_lines:
        table.add_row("environment", escape("\n".join(env_lines)))
    table.add_row("pid", str(job["pid"]) if job["pid"] else "-")
    table.add_row("log", job["log_path"])
    if job["note"]:
        table.add_row("note", job["note"])
    console.print(table)


@app.command()
def show(
    job_ids: list[str] = typer.Argument(
        ..., help="One or more job IDs; ranges (31-33) and comma lists (12,14) work too", metavar="JOB_ID...",
    ),
    as_json: bool = typer.Option(False, "--json", help="Print raw JSON (one object, or a list for several IDs)"),
) -> None:
    """Show full detail for one or more jobs.

    Shows raw and resolved command, cwd, timestamps, duration, exit code, timeout,
    dependencies (depends on / after any / waiting on), auto-retry count, the extra
    environment (names only for variables copied automatically; values hidden for
    names containing KEY/TOKEN/SECRET/PASSWORD), pid, log path, and any note.
    IDs can be ranges (31-33) or comma lists, as with --depends-on. With several IDs,
    an unknown ID prints an error but the others are still shown (exit code 1 at the end).

    \b
    Examples:
      qur show 12
      qur show 12 14 15
      qur show 31-33
      qur show 12 --json
    """
    ids = _collect_job_ids(job_ids, "JOB_ID")
    jobs: list[dict[str, Any]] = []
    failed = False
    for job_id in ids:
        try:
            jobs.append(client.send("show", id=job_id))
        except client.DaemonError as e:
            typer.echo(f"Error: {e}", err=True)
            failed = True
        except client.DaemonNotRunning as e:
            typer.echo(f"Error: {e}", err=True)
            raise typer.Exit(code=1) from None

    if as_json:
        if jobs:
            console.print_json(json.dumps(jobs[0] if len(ids) == 1 else jobs))
    else:
        for i, job in enumerate(jobs):
            if i > 0:
                console.print()
            _render_job(job)

    if failed:
        raise typer.Exit(code=1)


# ---------------------------------------------------------------------------
# wait
# ---------------------------------------------------------------------------

WAIT_POLL_SECS = 1.5  # how often `qur wait` asks the daemon
WAIT_EXIT_FAILED = 1
WAIT_EXIT_NOT_DONE = 2  # cancelled / skipped / interrupted / removed
WAIT_EXIT_NO_DAEMON = 3
WAIT_EXIT_USAGE = 4  # bad arguments or an unknown job id
WAIT_EXIT_TIMEOUT = 124  # like timeout(1)
WAIT_EXIT_INTERRUPTED = 130  # Ctrl-C
_RETRY_NOTE = re.compile(r"auto-retry \d+/\d+ queued as job (\d+)")


def _wait_code(status: str) -> int:
    """Exit code that mirrors a job's final status: 0 done, 1 failed, 2 anything else."""
    if status == "done":
        return 0
    return WAIT_EXIT_FAILED if status == "failed" else WAIT_EXIT_NOT_DONE


def _wait_worst(codes: list[int]) -> int:
    """The worst of several job codes: a failure (1) beats cancelled/skipped/... (2) beats done (0)."""
    if WAIT_EXIT_FAILED in codes:
        return WAIT_EXIT_FAILED
    return max(codes, default=0)


def _wait_stop(msg: str, code: int) -> NoReturn:
    typer.echo(f"Error: {msg}", err=True)
    raise typer.Exit(code=code)


def _wait_send(cmd: str, **kwargs: Any) -> Any:
    """client.send, but an unreachable daemon ends `qur wait` with exit code 3."""
    try:
        return client.send(cmd, **kwargs)
    except client.DaemonNotRunning as e:
        _wait_stop(str(e), WAIT_EXIT_NO_DAEMON)


def _print_json(obj: Any) -> None:
    """Plain JSON on stdout (no colours, no Rich markup)."""
    typer.echo(json.dumps(obj, indent=2))


def _wait_record(orig: int, job: dict[str, Any] | None, watched: int) -> dict[str, Any]:
    """The final state of one waited-for job. `watched` is the id of its last attempt
    (differs from `orig` when `--retries` requeued it); `job` None = the row is gone."""
    if job is None:
        return {"id": orig, "status": "removed", "exit_code": None, "channel": None, "ran_as": watched,
                "note": "the job no longer exists", "code": WAIT_EXIT_NOT_DONE, "finished": True}
    return {"id": orig, "status": job["status"], "exit_code": job["exit_code"], "channel": job["channel"],
            "ran_as": watched, "note": job["note"], "code": _wait_code(job["status"]), "finished": True}


def _wait_poll_one(orig: int, watching: dict[int, int], seen: dict[int, str], first: bool) -> dict[str, Any] | None:
    """Ask the daemon about one job. Returns its final record, or None while it is still
    queued/running. A failed job that `--retries` requeued is followed to its retry (the
    retry's id is in the note); raises LookupError for an unknown id on the first pass."""
    while True:
        watched = watching[orig]
        try:
            job = _wait_send("show", id=watched)
        except client.DaemonError:
            if first and watched == orig:
                raise LookupError(orig) from None
            return _wait_record(orig, None, watched)
        seen[orig] = job["status"]
        if job["status"] not in db.TERMINAL_STATUSES:
            return None
        if job["status"] == "failed" and job.get("attempt", 0) < job.get("max_retries", 0):
            m = _RETRY_NOTE.search(job.get("note") or "")
            if m and int(m.group(1)) != watched:
                watching[orig] = int(m.group(1))
                continue  # ask about the retry right away
        return _wait_record(orig, job, watched)


def _wait_sleep(deadline: float | None) -> bool:
    """Sleep one poll interval (less if the deadline is closer). True = the deadline passed."""
    if deadline is None:
        time.sleep(WAIT_POLL_SECS)
        return False
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        return True
    time.sleep(min(WAIT_POLL_SECS, remaining))
    return False


def _wait_for_jobs(ids: list[int], timeout: timedelta | None, announce: bool) -> tuple[list[dict[str, Any]], bool]:
    """Block until every id in `ids` is terminal. Returns (one record per id, timed_out)."""
    deadline = None if timeout is None else time.monotonic() + timeout.total_seconds()
    watching = {i: i for i in ids}
    seen: dict[int, str] = {}
    done: dict[int, dict[str, Any]] = {}
    first = True
    while True:
        missing: list[int] = []
        for orig in ids:
            if orig in done:
                continue
            try:
                rec = _wait_poll_one(orig, watching, seen, first)
            except LookupError:
                missing.append(orig)
                continue
            if rec is not None:
                done[orig] = rec
                if announce:
                    _announce_waited(rec)
        if missing:
            _wait_stop(f"job {_fmt_ids(missing)} does not exist" if len(missing) == 1
                       else f"jobs {_fmt_ids(missing)} do not exist", WAIT_EXIT_USAGE)
        first = False
        if len(done) == len(ids):
            return [done[i] for i in ids], False
        if _wait_sleep(deadline):
            pending = {"finished": False, "exit_code": None, "channel": None, "note": None, "code": None}
            return [done.get(i) or {"id": i, "status": seen.get(i, "unknown"), "ran_as": watching[i], **pending}
                    for i in ids], True


def _announce_waited(rec: dict[str, Any]) -> None:
    text = f"job {rec['id']}: {rec['status']}"
    if rec["exit_code"] is not None:
        text += f" (exit {rec['exit_code']})"
    if rec["ran_as"] != rec["id"]:
        text += f" -- last attempt was job {rec['ran_as']}"
    console.print(escape(text))


def _wait_for_channel(channel: int, timeout: timedelta | None, quiet: bool) -> bool:
    """Block until `channel` has nothing running, frozen or queued. True = timed out.
    A channel that has never had a job (or was cleared) counts as idle."""
    deadline = None if timeout is None else time.monotonic() + timeout.total_seconds()
    hinted = False
    while True:
        data = _wait_send("list")
        chan = {int(k): v for k, v in data["channels"].items()}.get(channel)
        if not chan or not (chan.get("running") or chan.get("frozen") or chan.get("queue") or chan.get("priority_queue")):
            return False
        if chan.get("paused") and not chan.get("running") and not chan.get("frozen") and not hinted and not quiet:
            typer.echo(f"Note: channel {channel} is paused; it stays busy until `qur resume --channel {channel}`", err=True)
            hinted = True
        if _wait_sleep(deadline):
            return True


@app.command()
def wait(
    job_ids: Optional[list[str]] = typer.Argument(
        None, help="Job IDs to wait for; ranges (31-33) and comma lists (12,14) work too", metavar="[JOB_ID...]",
    ),
    channel: Optional[int] = typer.Option(None, "--channel", min=1, help="Instead of jobs: wait until this channel is idle"),
    timeout: Optional[str] = typer.Option(
        None, "--timeout", metavar="DURATION", help="Give up after this long (30s, 10m, 2h, 1h30m); exit code 124",
    ),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Print nothing (the exit code says how it went)"),
    as_json: bool = typer.Option(False, "--json", help="Print the final states as JSON"),
) -> None:
    """Block until jobs finish; the exit code says how they ended.

    Waits for the given jobs to reach a final state (done, failed, cancelled, skipped,
    interrupted), or with --channel until that channel has nothing running or queued.
    A job that failed and was requeued by --retries is followed to its retry, so you hear
    about the last attempt. Polls the daemon every 1.5 s.

    \b
    Exit codes:
      0    done (--channel: the channel is idle)
      1    failed (with several jobs: any failure)
      2    cancelled / skipped / interrupted / removed
      3    the daemon is not reachable
      4    bad arguments, or a job id that does not exist
      124  --timeout ran out
      130  Ctrl-C
    With several jobs, the worst code wins: 1, then 2, then 0.

    \b
    Examples:
      qur wait 12
      qur wait 31-33 --timeout 2h
      qur add -- ./prep.sh && qur wait 40 && echo prepared
      qur wait --channel 2 --quiet
      qur wait 12 --json
    """
    ids = _collect_job_ids(job_ids, "JOB_ID") if job_ids else []
    if ids and channel is not None:
        _wait_stop("give job ids or --channel, not both", WAIT_EXIT_USAGE)
    if not ids and channel is None:
        _wait_stop("give at least one job id, or --channel N", WAIT_EXIT_USAGE)
    limit: timedelta | None = None
    if timeout is not None:
        try:
            limit = parse_delay(timeout)
        except ValueError as e:
            _wait_stop(f"--timeout: {e}", WAIT_EXIT_USAGE)

    try:
        if channel is not None:
            timed_out = _wait_for_channel(channel, limit, quiet)
            code = WAIT_EXIT_TIMEOUT if timed_out else 0
            if as_json:
                _print_json({"channel": channel, "idle": not timed_out, "timed_out": timed_out, "exit_code": code})
            elif not quiet:
                console.print(f"channel {channel}: " + ("still busy (timed out)" if timed_out else "idle"))
        else:
            records, timed_out = _wait_for_jobs(ids, limit, announce=not quiet and not as_json)
            code = WAIT_EXIT_TIMEOUT if timed_out else _wait_worst([r["code"] for r in records])
            if as_json:
                _print_json({"exit_code": code, "timed_out": timed_out, "jobs": records})
            elif timed_out and not quiet:
                waiting = [r["id"] for r in records if not r["finished"]]
                console.print(f"timed out; still waiting for {_fmt_ids(waiting)}")
    except KeyboardInterrupt:
        if not quiet:
            typer.echo("Interrupted", err=True)
        raise typer.Exit(code=WAIT_EXIT_INTERRUPTED) from None
    if code:
        raise typer.Exit(code=code)


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
    signal_name: Optional[str] = typer.Option(
        None, "-s", "--signal", metavar="SIG",
        help="Stop signal for this cancel: TERM (default), INT, HUP, QUIT, USR1, USR2 or KILL (or its number). "
        "Overrides the job's --stop-signal. Ignored for a job that is still queued.",
    ),
    grace: Optional[str] = typer.Option(
        None, "--grace", metavar="SECONDS",
        help="Seconds the job gets between the signal and SIGKILL (default 5): a number, or a duration like "
        "90s, 2m. 0 = SIGKILL at once. Overrides the job's --stop-grace. Ignored for a job that is still queued.",
    ),
) -> None:
    """Cancel a job.

    With no JOB_ID: kills a channel's running job. With a JOB_ID: cancels that job
    whatever state it is in -- running, frozen by `--now`, or still waiting in the
    queue (a queued job is kept in history as `cancelled`; `qur rm` deletes it
    instead). Queued jobs that needed it to succeed (--depends-on) become `skipped`.

    Killing sends SIGTERM (or -s SIG, or the job's --stop-signal) to the job's whole
    process group, then SIGKILL if it is still alive after a grace period (5 seconds,
    or --grace, or the job's --stop-grace). A cancelled job is never auto-retried.
    Errors if nothing is running on the channel, or if the job is already finished.

    \b
    Examples:
      qur cancel
      qur cancel --channel 2
      qur cancel 12
      qur cancel 12 -s INT --grace 30   # Ctrl-C, then 30 s to write a checkpoint
    """
    kwargs: dict[str, Any] = {}
    sig_name = _stop_signal(signal_name, "--signal")
    grace_secs = _grace_secs(grace, "--grace")
    if sig_name is not None:
        kwargs["signal"] = sig_name
    if grace_secs is not None:
        kwargs["grace"] = grace_secs
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
    """Delete finished-job records (done/failed/cancelled/interrupted).

    Clears the finished-job history (`qur list` "Recent" and `qur history`). Queued, running, and frozen jobs are
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
    still waiting; for a running job use `qur cancel`. Refused while another queued
    job depends on it (--depends-on / --after-any): remove or cancel those first,
    or `qur cancel` this one, which skips them.

    \b
    Example:
      qur rm 12
    """
    _call("rm", id=job_id)
    console.print(f"Removed job [bold]{job_id}[/bold]")


@app.command()
def requeue(job_id: int = typer.Argument(..., help="ID of a finished job (done/failed/cancelled/interrupted)")) -> None:
    """Queue a finished job again.

    Makes a new job with the original command, cwd, environment, timeout, channel,
    and --retries setting (with a fresh retry count), added to the end of that
    channel's queue. Works on any finished job (done/failed/cancelled/interrupted).

    \b
    Example:
      qur requeue 12
    """
    data = _call("requeue", id=job_id)
    console.print(f"Requeued job {job_id} as new job [bold]{data['id']}[/bold]")


# ---------------------------------------------------------------------------
# edit
# ---------------------------------------------------------------------------

_EDIT_HEADER = """\
# Edit job {id} (channel {channel}). Save and quit to apply; quit without changes to abandon.
# Lines starting with # are ignored. While this file is open the job is locked: it does not
# start, and (strict FIFO) nothing behind it on its channel starts either.
# command: written like `qur add -- 'COMMAND'` (one string; shell syntax runs via bash -c).
# timeout: seconds, a duration like 2h, or none.
"""


def _editor_argv() -> list[str]:
    """The editor to open: $VISUAL, $EDITOR, else nano, else vi."""
    for var in ("VISUAL", "EDITOR"):
        value = os.environ.get(var, "").strip()
        if value:
            try:
                words = shlex.split(value)
            except ValueError:
                continue
            if words:
                return words
    for name in ("nano", "vi"):
        found = shutil.which(name)
        if found:
            return [found]
    _fail("no editor found: set $EDITOR (or $VISUAL), or edit with flags, e.g. `qur edit ID --cwd DIR`")


def _run_editor(path: str) -> None:
    """Open `path` in the user's editor and wait for it. Raises RuntimeError if it fails."""
    argv = _editor_argv() + [path]
    try:
        # the editor is the user's own $VISUAL/$EDITOR and the argument is our temp file
        res = subprocess.run(argv)  # nosec B603
    except OSError as e:
        raise RuntimeError(f"could not start the editor {argv[0]!r}: {e.strerror or e}") from e
    if res.returncode != 0:
        raise RuntimeError(f"the editor exited with status {res.returncode}")


def _edit_text(job: dict[str, Any]) -> str:
    timeout = job.get("timeout_secs")
    return (
        _EDIT_HEADER.format(id=job["id"], channel=job["channel"])
        + f"command: {_display_join(json.loads(job['raw_cmd']))}\n"
        + f"cwd: {job['cwd']}\n"
        + f"timeout: {timeout if timeout else 'none'}\n"
    )


def _parse_edit_text(text: str) -> dict[str, str]:
    """The `key: value` lines of the edit file (comments and blank lines ignored)."""
    out: dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, sep, value = line.partition(":")
        key = key.strip().lower()
        if not sep or key not in ("command", "cwd", "timeout"):
            raise ValueError(f"cannot read the line {line!r}: use `command: ...`, `cwd: ...` or `timeout: ...`")
        out[key] = value.strip()
    for key in ("command", "cwd", "timeout"):
        if not out.get(key):
            raise ValueError(f"the `{key}:` line is missing or empty")
    return out


def _edit_timeout(text: str) -> int:
    """`timeout:` value of the edit file -> seconds (0 = none)."""
    value = text.strip().lower()
    if value in ("none", "0"):
        return 0
    if value.isdigit():
        return int(value)
    return int(parse_delay(value).total_seconds())


def _command_request(words: list[str]) -> dict[str, Any]:
    """The `edit` request keys for a new command (what `add` sends for one)."""
    try:
        argv = _resolve_interpreter(_build_job_argv(words))
    except ValueError as e:
        _fail(str(e))
    return {
        "argv": argv, "raw_argv": words, "path": os.environ.get("PATH", ""),
        "virtual_env": os.environ.get("VIRTUAL_ENV"), "python_path": os.environ.get("PYTHONPATH"),
    }


def _edit_interactive(job_id: int) -> None:
    """`qur edit ID` with no flags: lock the job, edit command/cwd/timeout in $EDITOR, apply."""
    pid = os.getpid()
    job = _call("edit_lock", id=job_id, pid=pid)
    applied = False
    path: str | None = None
    try:
        original = _edit_text(job)
        fd, path = tempfile.mkstemp(prefix=f"qur-edit-{job_id}-", suffix=".txt")
        with os.fdopen(fd, "w") as f:
            f.write(original)
        try:
            _run_editor(path)
            with open(path) as f:
                edited = f.read()
        except (RuntimeError, OSError) as e:
            _fail(f"{e}; job {job_id} left unchanged")
        if edited == original:
            console.print(f"No changes: job [bold]{job_id}[/bold] left as it was")
            return
        try:
            fields = _parse_edit_text(edited)
            old = _parse_edit_text(original)
            request: dict[str, Any] = {}
            if fields["command"] != old["command"]:
                request.update(_command_request([fields["command"]]))
            if fields["cwd"] != old["cwd"]:
                cwd = os.path.abspath(os.path.expanduser(fields["cwd"]))
                request["cwd"] = cwd
                if "argv" not in request:  # a new cwd re-resolves the stored command
                    request.update(_command_request(json.loads(job["raw_cmd"])))
            if fields["timeout"] != old["timeout"]:
                request["timeout_secs"] = _edit_timeout(fields["timeout"]) or None
        except ValueError as e:
            _fail(f"{e}; job {job_id} left unchanged")
        if not request:
            console.print(f"No changes: job [bold]{job_id}[/bold] left as it was")
            return
        data = _call("edit", id=job_id, pid=pid, **request)
        applied = True
        console.print(f"Edited job [bold]{job_id}[/bold]: changed {', '.join(data['changed'])}")
    finally:
        if path is not None:
            try:
                os.unlink(path)
            except OSError:
                pass
        if not applied:  # abandoned or failed: let the channel go on
            try:
                client.send("edit_unlock", id=job_id, pid=pid)
            except (client.DaemonNotRunning, client.DaemonError) as e:
                typer.echo(
                    f"Warning: could not release the edit lock on job {job_id} ({e}); "
                    "the daemon drops it by itself when the lock gets stale",
                    err=True,
                )


@app.command()
def edit(
    job_id: int = typer.Argument(..., help="ID of a queued job"),
    cmd: Optional[list[str]] = typer.Argument(
        None, metavar="[CMD...]", help="New command, written after `--` (like `qur add`).",
    ),
    cmd_text: Optional[str] = typer.Option(
        None, "--cmd", metavar="COMMAND", help="New command as ONE string (shell syntax runs via bash -c). "
        "Same as words after `--`.",
    ),
    cwd: Optional[str] = typer.Option(None, "--cwd", help="New working directory (relative to this shell's)."),
    timeout: Optional[int] = typer.Option(None, "--timeout", min=0, help="New timeout in seconds; 0 removes it."),
    channel: Optional[int] = typer.Option(
        None, "--channel", min=1, help="Move the job to this channel, at the END of its queue.",
    ),
    retries: Optional[int] = typer.Option(None, "--retries", min=0, help="New --retries count."),
    on_fail: Optional[str] = typer.Option(None, "--on-fail", metavar="pause|continue", help="New --on-fail."),
    on_finish: Optional[str] = typer.Option(
        None, "--on-finish", metavar="COMMAND", help="New --on-finish hook; an empty string removes it.",
    ),
    on_error: Optional[str] = typer.Option(
        None, "--on-error", metavar="COMMAND", help="New --on-error hook; an empty string removes it.",
    ),
    stop_signal: Optional[str] = typer.Option(
        None, "--stop-signal", metavar="SIG", help="New --stop-signal; an empty string goes back to SIGTERM.",
    ),
    stop_grace: Optional[str] = typer.Option(
        None, "--stop-grace", metavar="SECONDS", help="New --stop-grace; an empty string goes back to the default (5 s).",
    ),
    delay: Optional[str] = typer.Option(None, "--delay", metavar="DURATION", help="Start no earlier than this from now."),
    at: Optional[str] = typer.Option(None, "--at", metavar="TIME", help="Start no earlier than this local time."),
    clear_delay: bool = typer.Option(False, "--clear-delay", help="Remove the start time."),
    depends_on: Optional[list[str]] = typer.Option(
        None, "--depends-on", metavar="ID[,ID...]", help="REPLACE the --depends-on list with these jobs.",
    ),
    after_any: Optional[list[str]] = typer.Option(
        None, "--after-any", metavar="ID[,ID...]", help="REPLACE the --after-any list with these jobs.",
    ),
    clear_deps: bool = typer.Option(False, "--clear-deps", help="Remove every dependency."),
    env: Optional[list[str]] = typer.Option(
        None, "--env", metavar="KEY[=VAL]", help="Add or change an environment variable (as in `qur add`); repeatable.",
    ),
) -> None:
    """Edit a queued job.

    With flags, the listed settings change and everything else stays; all of it is
    checked like `qur add` (executable, dependencies, deadlocks) and applied all or
    nothing. With no flags, command, cwd and timeout open in $VISUAL/$EDITOR (else
    nano/vi) as a small text file.

    Only a job that is still `queued` can be edited. While an edit is open the job is
    LOCKED: it does not start, and since each channel is strict FIFO the jobs behind it
    wait too, even if the job ahead of it has finished. When you save (or abandon) the
    edit the channel goes on and runs the edited job. A lock of an editor that died, or
    older than 30 minutes, is ignored by the daemon, and a daemon restart drops all locks
    (the edit then fails with a clear message). `qur rm` / `qur cancel` still work on a
    locked job (the editor then reports that the job is gone).

    --channel moves the job to the end of the other channel's queue. --depends-on /
    --after-any replace that kind of dependency; --clear-deps removes all. A new command
    or cwd is resolved against this shell's PATH, which also replaces the job's PATH,
    VIRTUAL_ENV and PYTHONPATH. Setting --retries 0 also drops the job's retry delay.

    \b
    Examples:
      qur edit 12                              # edit command/cwd/timeout in $EDITOR
      qur edit 12 -- python train.py --epochs 10
      qur edit 12 --cwd ~/run2 --timeout 7200
      qur edit 12 --channel 2 --delay 1h
      qur edit 12 --depends-on 10,11 --env OMP_NUM_THREADS=4
      qur edit 12 --clear-delay --clear-deps
    """
    words = list(cmd or [])
    if cmd_text is not None:
        if words:
            _fail("give the new command either with --cmd or after `--`, not both")
        words = [cmd_text]
    if cmd_text is not None and not cmd_text.strip():
        _fail("--cmd is empty")
    flagged = any(
        v is not None for v in (cwd, timeout, channel, retries, on_fail, on_finish, on_error, stop_signal, stop_grace, delay, at, depends_on, after_any, env)
    )
    if not words and not flagged and not clear_delay and not clear_deps:
        _edit_interactive(job_id)
        return

    request: dict[str, Any] = {}
    if on_fail is not None:
        if on_fail not in db.ON_FAIL_MODES:
            _fail(f"--on-fail must be one of {', '.join(db.ON_FAIL_MODES)} (got {on_fail!r})")
        request["on_fail"] = on_fail
    if on_finish is not None:
        request["on_finish"] = on_finish
    if on_error is not None:
        request["on_error"] = on_error
    if stop_signal is not None:
        request["stop_signal"] = _stop_signal(stop_signal, "--stop-signal")
    if stop_grace is not None:
        request["stop_grace"] = _grace_secs(stop_grace, "--stop-grace")
    if clear_delay and (delay is not None or at is not None):
        _fail("--clear-delay cannot be combined with --delay/--at")
    start = _start_time(delay, at)
    if start is not None:
        request["not_before"] = start.astimezone(timezone.utc).isoformat()
    elif clear_delay:
        request["not_before"] = None
    if clear_deps and (depends_on or after_any):
        _fail("--clear-deps cannot be combined with --depends-on/--after-any")
    if clear_deps:
        request["clear_deps"] = True
    if depends_on:
        request["depends_on"] = _parse_job_ids(depends_on, "--depends-on")
    if after_any:
        request["after_any"] = _parse_job_ids(after_any, "--after-any")
    if env:
        try:
            request["env"] = jobenv.parse_env_options(env, os.environ)
        except ValueError as e:
            _fail(str(e))
    if timeout is not None:
        request["timeout_secs"] = timeout or None
    if channel is not None:
        request["channel"] = channel
    if retries is not None:
        request["max_retries"] = retries
    if cwd is not None:
        request["cwd"] = os.path.abspath(os.path.expanduser(cwd))
    if words or cwd is not None:
        if not words:  # a new cwd re-resolves the stored command
            words = json.loads(_call("show", id=job_id)["raw_cmd"])
        request.update(_command_request(words))
    data = _call("edit", id=job_id, **request)
    console.print(f"Edited job [bold]{job_id}[/bold]: changed {', '.join(data['changed'])}")


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

def _fail(msg: str) -> NoReturn:
    typer.echo(f"Error: {msg}", err=True)
    raise typer.Exit(code=1)


def _warn_hello(bot_cfg: dict[str, Any]) -> None:
    """Test message through one bot (flat view)."""
    menu = notify.menu_markup() if notify.listening(bot_cfg) else None  # Status/List keys only where presses are handled
    errors = notify.send(bot_cfg, notify.hello_text(bot_cfg), menu)
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
        res = subprocess.run(["ssh", "--", target, remote], stdout=subprocess.PIPE, text=True, timeout=120)  # nosec B603 B607 - fixed argv, `--` guards target
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
    import_: Optional[str] = typer.Option(None, "--import", help="Set this machine up from another machine's --export string (adds its bots to this machine's). Use `--import -` to read the string from stdin so the token stays out of ps and shell history"),
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

    Messages say done / failed / cancelled / interrupted (daemon stopped), the job id, how long it took, the
    command as you typed it, the exit code, and which machine sent it (hostname
    plus that qur's id, shown when you connect; change it with --id). They never
    include the working directory or resolved paths; use `qur show ID` on the
    machine for those. A job that is auto-retried stays silent until its final try.

    Each message has three buttons. Requeue re-runs the finished job (like `qur
    requeue ID`). Details replies with everything `qur show ID` prints, formatted
    for Telegram (this includes paths, sent only to your paired chats). Logs
    replies with the last lines of the job's output, so anything the job prints
    (secrets included) goes through Telegram when you press it. All are
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
      qur connect --import -   (string read from stdin)
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
    elif import_ == "-":
        import_ = sys.stdin.read().strip()
        if not import_:
            _fail("--import -: nothing on stdin")
    elif import_ is not None:
        typer.echo("Warning: the string holds bot tokens and is now visible in `ps` and shell history. "
                   "Prefer `--from [user@]host` or `qur connect --import -` (string on stdin).", err=True)

    existing = notify.load()

    # --id / --listen alone: change this machine's setup, no pairing
    if import_ is None and not add_bot and existing is not None and (id_ is not None or listen is not None):
        cfg: dict[str, Any] = {"qur_id": id_ or existing["qur_id"], "bots": [dict(b) for b in existing["bots"]]}
        if listen is not None:
            listen_tokens = {b["token"] for b in _bots_for(existing, bot)}
            for b in cfg["bots"]:
                if b["token"] in listen_tokens:
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
        problem = notify.check_permissions(fix=False)
        if problem:
            console.print("[yellow]Warning:[/yellow] " + escape(f"{problem}. Fix: chmod 600 {notify.default_path()}"))
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
            rest: dict[str, Any] | None = existing
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
        merged: dict[str, Any] | None = existing
        imported: list[str] = []
        for token, chat_ids in items:
            old = next((b for b in merged["bots"] if b["token"] == token), None) if merged else None
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
            merged = notify.with_bot(merged, entry, id_)
            imported.append(token)
        if merged is None:  # items is never empty, so with_bot ran at least once
            _fail("nothing to import")
        notify.save(merged)
        for bcfg in notify.bot_cfgs(merged):
            if bcfg["token"] in imported:
                _warn_hello(bcfg)
        console.print(_connected_line(merged, "Connected."))
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

def _tail_offset(f: Any, n: int) -> int:
    """Byte offset where the last `n` lines of the open binary file `f` begin
    (0 if it has fewer). Reads backwards in blocks, never the whole file."""
    end = f.seek(0, os.SEEK_END)
    if end == 0:
        return 0
    f.seek(end - 1)
    need = n + 1 if f.read(1) == b"\n" else n  # newlines to pass; the last line's own ending is not a boundary
    pos = end
    while pos > 0:
        step = min(8192, pos)
        pos -= step
        f.seek(pos)
        block = f.read(step)
        idx = len(block)
        while True:
            idx = block.rfind(b"\n", 0, idx)
            if idx == -1:
                break
            need -= 1
            if need == 0:
                return pos + idx + 1
    return 0


@app.command()
def logs(
    job_id: Optional[int] = typer.Argument(None, help="Job ID (not needed with --running)"),
    running: bool = typer.Option(
        False, "--running", help="Use the job running now on a channel instead of an ID (see --channel)"
    ),
    channel: int = typer.Option(DEFAULT_CHANNEL, "--channel", min=1, help="Channel for --running (default 1)"),
    tail: bool = typer.Option(False, "--tail", help="Follow the log as it grows (Ctrl-C to stop)"),
    lines: Optional[int] = typer.Option(
        None, "--lines", "-n", min=1,
        help="Print only the last N lines (with --tail: show them first, then follow)",
    ),
) -> None:
    """Print a job's log (stdout and stderr combined).

    Reads the log file straight from disk, so it works even if the daemon isn't
    running. Each retry of a job is a separate job with its own log. A big log is
    streamed, never loaded whole.

    \b
    Examples:
      qur logs 12
      qur logs 12 -n 50
      qur logs 12 --tail
      qur logs 12 -n 20 --tail
      qur logs --running --tail
      qur logs --running --channel 2 -n 30
    """
    if running:
        if job_id is not None:
            _fail("give a job ID or --running, not both")
        data = _call("status")
        chan = data["channels"].get(str(channel)) or data["channels"].get(channel) or {}
        current = chan.get("running")
        if not current:
            _fail(f"no job is running on channel {channel}")
        job_id = int(current["id"])
        typer.echo(f"(job {job_id})", err=True)
    elif job_id is None:
        _fail("give a job ID, or --running")
    assert job_id is not None
    log_path = _default_log_dir() / f"{job_id}.log"
    if not log_path.exists():
        typer.echo(f"Error: no log found for job {job_id} at {log_path}", err=True)
        raise typer.Exit(code=1)

    # raw output, no Rich: it would eat [brackets]
    if lines is not None:
        with open(log_path, "rb") as fb:
            fb.seek(_tail_offset(fb, lines))
            data = fb.read()
        sys.stdout.write(data.decode(errors="replace").replace("\r\n", "\n").replace("\r", "\n"))
        sys.stdout.flush()
    elif not tail:
        with open(log_path, "r", errors="replace") as f:  # text mode: \r\n and \r read as \n
            while True:
                chunk = f.read(65536)
                if not chunk:
                    break
                sys.stdout.write(chunk)
        sys.stdout.flush()
    if not tail:
        return

    with open(log_path, "r", errors="replace") as f:
        f.seek(0, os.SEEK_END)
        try:
            while True:
                line = f.readline()
                if line:
                    sys.stdout.write(line)
                    sys.stdout.flush()
                else:
                    time.sleep(0.5)
        except KeyboardInterrupt:
            pass


@app.command(name="clean-logs")
def clean_logs(
    days: int = typer.Option(30, "--days", help="Delete logs for jobs finished more than this many days ago"),
    max_mb: Optional[int] = typer.Option(
        None, "--max-mb", min=1,
        help="Also delete the oldest finished jobs' logs until the log folder is at most this many MB "
        "(logs of queued/running/frozen jobs count but are never deleted)",
    ),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be deleted without deleting"),
) -> None:
    """Delete old log files from disk.

    Removes logs of finished jobs (done/failed/cancelled/interrupted) finished more than --days
    ago, plus orphaned logs whose job record no longer exists (e.g. pruned from
    the history). With --max-mb it also removes the oldest of the remaining ones until the
    folder fits. Never touches logs of queued, running, or frozen jobs. Reads the DB
    directly, so it works even if the daemon isn't running. The daemon can do this by itself
    on a timer (see the README, "Automatic log cleaning").

    \b
    Examples:
      qur clean-logs --dry-run
      qur clean-logs --days 7
      qur clean-logs --days 90 --max-mb 2000
    """
    log_dir = _default_log_dir()
    if not log_dir.exists():
        console.print("[dim](no log directory)[/dim]")
        return

    db_file = _default_db_path()
    if not db_file.is_file():
        # db.connect() would create an empty database, and every log would then look
        # orphaned. Never delete anything on that basis.
        _fail(f"no job database at {db_file}; refusing to clean logs (is QUEUER_DB_PATH the same as the daemon's?)")
    conn = db.connect(db_file)
    try:
        to_delete = logclean.plan(
            conn, log_dir, max_age_days=days, max_bytes=max_mb * 1024 * 1024 if max_mb is not None else None
        )
    finally:
        conn.close()

    if not to_delete:
        console.print("[dim](nothing to clean)[/dim]")
        return

    total_bytes = sum(size for _, size in to_delete)
    verb = "Would delete" if dry_run else "Deleting"
    console.print(f"{verb} {len(to_delete)} log(s), {total_bytes / 1024:.0f} KB")
    for f, _ in to_delete:
        console.print(f"  {f}")
    if not dry_run:
        try:
            logclean.delete(to_delete)
        except OSError as e:
            _fail(f"could not delete a log: {e}")


# ---------------------------------------------------------------------------
# doctor
# ---------------------------------------------------------------------------

_DOCTOR_STYLES = {selfcheck.OK: "green", selfcheck.WARN: "yellow", selfcheck.FAIL: "bold red"}


@app.command()
def doctor(
    fix: bool = typer.Option(False, "--fix", help="Repair permission problems only (loose modes on files/folders you own); nothing else is changed"),
    as_json: bool = typer.Option(False, "--json", help="Print the results as JSON"),
) -> None:
    """Check that queuer is set up and locked down properly.

    Checks: daemon reachable, socket mode and owner, config folder/file modes
    (notify.json 0600, folders 0700, no symlinks), database mode, log folder writable
    and free disk space, systemd linger, unit file (and whether it still matches what
    install.sh writes) and service state, Telegram token
    (one getMe call, only if configured; offline is a warning), and the database
    schema version. Every problem comes with a fix hint.

    Exit code 1 if any check FAILs (warnings do not fail), so it works in scripts.

    \b
    Examples:
      qur doctor
      qur doctor --fix
      qur doctor --json
    """
    checks = selfcheck.run_checks(fix=fix)
    code = selfcheck.exit_code(checks)
    counts = selfcheck.summary(checks)
    if as_json:
        _print_json({"ok": code == 0, "summary": counts, "checks": [c.as_dict() for c in checks]})
        raise typer.Exit(code=code)
    width = max(len(c.name) for c in checks)
    for c in checks:
        style = _DOCTOR_STYLES[c.status]
        console.print(f"[{style}]{c.status:<4}[/{style}]  {escape(c.name):<{width}}  {escape(c.detail)}", highlight=False)
        if c.hint:
            console.print(f"      {'':<{width}}  [dim]fix: {escape(c.hint)}[/dim]", highlight=False)
    console.print(f"\n{counts[selfcheck.OK]} ok, {counts[selfcheck.WARN]} warning(s), {counts[selfcheck.FAIL]} failure(s)")
    raise typer.Exit(code=code)


# ---------------------------------------------------------------------------
# daemon (thin wrappers over systemctl / journalctl)
# ---------------------------------------------------------------------------

daemon_app = typer.Typer(
    help="Start, stop and inspect the queuerd service (thin wrappers over `systemctl --user` and `journalctl --user`).",
    no_args_is_help=True,
)
app.add_typer(daemon_app, name="daemon")


def _run_service_tool(argv: list[str]) -> int:
    """Run systemctl/journalctl with a fixed argv on the terminal; return its exit code."""
    try:
        # fixed argv built from constants and one validated integer, no shell
        return subprocess.run(argv, check=False).returncode  # nosec B603 B607
    except FileNotFoundError:
        _fail(f"{argv[0]} not found; these commands need systemd")
    except KeyboardInterrupt:  # Ctrl-C while following the logs
        return 130


def _running_job_ids() -> list[int]:
    """Ids of jobs the daemon is running (or has frozen); empty if it cannot be asked."""
    try:
        data = client.send("status")
    except (client.DaemonNotRunning, client.DaemonError):
        return []
    ids: list[int] = []
    for chan in data.get("channels", {}).values():
        for key in ("running", "frozen"):
            job = chan.get(key)
            if job:
                ids.append(int(job["id"]))
    return sorted(ids)


def _confirm_stop(verb: str, yes: bool) -> None:
    ids = _running_job_ids()
    if not ids or yes:
        return
    jobs = ", ".join(f"#{i}" for i in ids)
    console.print(
        f"[yellow]{len(ids)} job(s) running ({jobs}).[/yellow] A {verb} kills them; they are recorded "
        "as [magenta]interrupted[/magenta] (not failed, not retried) and the queue continues afterwards."
    )
    if not typer.confirm("Continue?", default=False):
        raise typer.Exit(code=1)


def _service_action(action: str, done: str) -> None:
    code = _run_service_tool(["systemctl", "--user", action, selfcheck.UNIT_NAME])
    if code != 0:
        typer.echo(f"Error: systemctl {action} failed (exit {code}); see: qur daemon logs", err=True)
        raise typer.Exit(code=code)
    console.print(f"[green]{done}[/green]")


@daemon_app.command("start")
def daemon_start() -> None:
    """Start the queuerd service (`systemctl --user start queuerd`)."""
    _service_action("start", "queuerd started")


@daemon_app.command("stop")
def daemon_stop(
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask when a job is running"),
) -> None:
    """Stop the queuerd service. Queued jobs stay queued; a running job is killed and recorded as interrupted."""
    _confirm_stop("stop", yes)
    _service_action("stop", "queuerd stopped")


@daemon_app.command("restart")
def daemon_restart(
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask when a job is running"),
) -> None:
    """Restart the queuerd service. A running job is killed and recorded as interrupted; the queue then continues."""
    _confirm_stop("restart", yes)
    _service_action("restart", "queuerd restarted")


@daemon_app.command("status")
def daemon_status() -> None:
    """Show the service state (`systemctl --user status queuerd`). Exit code is systemctl's (3 = not running)."""
    raise typer.Exit(code=_run_service_tool(["systemctl", "--user", "status", selfcheck.UNIT_NAME, "--no-pager"]))


@daemon_app.command("logs")
def daemon_logs(
    lines: int = typer.Option(50, "--lines", "-n", min=1, help="How many of the latest lines to show"),
    follow: bool = typer.Option(False, "--follow", "-f", help="Keep following new lines (Ctrl-C to stop)"),
) -> None:
    """Show the daemon's own log (`journalctl --user -u queuerd`), not a job's output (that is `qur logs ID`)."""
    argv = ["journalctl", "--user", "-u", selfcheck.UNIT_NAME, "-n", str(lines), "--no-pager"]
    if follow:
        argv.append("-f")
    raise typer.Exit(code=_run_service_tool(argv))


if __name__ == "__main__":
    app()
