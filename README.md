# queuer

A serial background job queue. `queuerd` runs one job at a time, in order,
as a `systemd --user` service; `queuer` is the CLI you use to add, inspect,
reorder, and cancel jobs.

## Requirements

- Python 3.12
- [uv](https://docs.astral.sh/uv/)
- Linux with `systemd --user` support (this was built and tested on Pop!_OS)

## Install

```bash
git clone git@github.com:sampk1203/custom-queuer.git
cd custom-queuer
./install.sh
```

`install.sh` is safe to re-run any time (after a `git pull`, or if you move
the project directory) — it syncs the venv, runs the test suite, installs
`queuer` / `qur` / `queuerd` onto your `PATH` via `uv tool install
--editable .` (edits to the code take effect without reinstalling), stops
shell tab-completion for `qur`/`queuer` (for your login shell), stops
any existing `queuerd` process (systemd-managed or stray), rewrites the
systemd unit with the correct path, enables + starts it, and enables
linger so it survives logout.

`qur` is a short alias for `queuer` — same command, less typing.

To confirm it worked:
```bash
systemctl --user status queuerd
qur status
```

## Command reference

`qur` works everywhere `queuer` does below — it's the same command, just shorter.
Every command also has worked examples under `qur <command> --help`, and `qur --help-all`
prints the help for every command at once.

```
queuer add [--channel N] [--before ID | --after ID | --now] [--timeout SECONDS] [--retries N] -- <cmd...>
queuer list [--channel N] [--full] [--json]
queuer status [--json]
queuer show ID [ID...] [--json]
queuer cancel [--channel N]
queuer clear [--channel N]
queuer rm ID
queuer requeue ID
queuer pause [--channel N]
queuer resume [--channel N]
queuer logs ID [--tail]
queuer clean-logs [--days N] [--dry-run]
```

### `add`

Enqueues a job. Always put `--` before the command itself if it has its
own flags, or `queuer` will try to parse them as its own options:

```bash
qur add -- echo hello
qur add --timeout 300 -- python train.py --epochs 50
qur add --after 12 -- ./run_simulation.sh input.yaml
```

The command is captured two ways:
- **raw** — exactly what you typed
- **resolved** — path-like tokens (anything containing `/`, or a bare
  filename that exists in the current directory, or the executable itself
  looked up on `PATH`) canonicalized to absolute paths. Symlinks are
  **not** followed (deliberately — this matters for venvs: resolving
  `.venv/bin/python3` to its symlink target would silently break venv
  activation for the job).

`--retries N` requeues the job automatically (at the end of its channel's
queue) if it fails — non-zero exit, timeout, or failed start — up to N
times. Each retry is a new job with its own ID and log; the failed job's
note says which job replaced it. Cancelled jobs are never retried, and
jobs interrupted by a daemon restart are not retried either.

`add` validates the executable exists and is executable *before*
accepting the job — a bad command is rejected immediately, not an hour
later when it reaches the front of the queue.

`--timeout SECONDS` kills the job automatically if it runs longer than
that (SIGTERM, then SIGKILL if it doesn't exit within a grace period),
and marks it `failed` with `note: timed out`.

### `list`

Shows each channel in its own section: the running job first (marked
with `*`), then the queue, with the time each job was queued and how long
ago that was, then the last 10 finished jobs (done/failed/cancelled).

- `--channel N` lists only that channel (default: all channels)
- The `Origin` column says where a job came from: `new` (you queued it),
  `retry of #N (k/max)` (automatic, after job N failed), or `requeue of #N`
  (manual, via `qur requeue N`)
- `--full` shows resolved absolute paths instead of what you typed
- `--json` prints the raw response instead of a table, for scripting

### `show ID [ID...]`

Full detail on one or more jobs — both command forms, cwd, every timestamp,
duration, exit code, timeout, auto-retry count, pid, and log path. With
`--json`, one ID prints an object; several print a list.

### `cancel`

Kills the currently running job (SIGTERM to its whole process group,
then SIGKILL if it's still alive after the grace period). Errors clearly
if nothing is running.

### `rm ID`

Removes a **queued** job entirely — it's gone, not kept as a backlog
entry. Errors if the ID is currently running (use `cancel` instead) or
doesn't exist.

### `requeue ID`

Re-enqueues a finished job (done/failed/cancelled) with its exact
original command, working directory, timeout, and retry setting, appended to the end
of the queue. Handy for rerunning a job that failed without retyping it.

### `pause` / `resume`

`pause` stops the daemon from starting new jobs — the current job (if
any) keeps running to completion. State persists across daemon
restarts, so a paused queue doesn't silently resume after a crash.

### `logs ID [--tail]`

Reads the job's log file straight off disk — this doesn't go through
the daemon at all, so it works even if `queuerd` isn't running.
`--tail` follows the file as it grows (Ctrl-C to stop). Each job's log
is capped at 50MB; once hit, further output is dropped and a single
`[queuer] log truncated at 50MB` marker is appended.

## Managing the daemon

```bash
systemctl --user status queuerd          # is it running
systemctl --user restart queuerd         # restart it
systemctl --user stop queuerd            # stop it (queue state is preserved)
journalctl --user -u queuerd -f          # daemon's own logs (not job logs)
```

Job output goes to `~/.local/state/queuer/logs/<id>.log` (via `queuer
logs`), separate from the daemon's own stderr/stdout, which systemd
sends to the journal.

## Crash / restart recovery

If `queuerd` dies while a job is running (crash, `kill -9`, reboot), that
job is never silently lost or blindly re-run. On startup, any job still
marked `running` from a previous instance is marked `failed` with
`note: interrupted by daemon restart`. Note this only updates the job's
record — if the job's own subprocess is still alive (it runs in its own
process group, independent of the daemon's), it keeps running
unsupervised until it finishes or you kill it manually (`pgrep`/`kill`).

Queued jobs and the last-10 backlog persist in SQLite at
`~/.local/state/queuer/queuer.db` and survive daemon restarts and
reboots without any special handling.

## File locations

| What | Where |
|---|---|
| Job database | `~/.local/state/queuer/queuer.db` |
| Job logs | `~/.local/state/queuer/logs/<id>.log` |
| Socket | `$XDG_RUNTIME_DIR/queuer.sock` |
| systemd unit | `~/.config/systemd/user/queuerd.service` |

All three of the first paths can be overridden with `QUEUER_DB_PATH`,
`QUEUER_LOG_DIR`, and `QUEUER_SOCKET_PATH` env vars — mainly useful for
running the daemon against a scratch location during manual testing,
without touching real state.

## Development

```bash
uv sync
uv run pytest -v          # storage layer + path-resolution unit tests
```

For a design/architecture writeup and the phase-by-phase build plan, see
`queuer_plan.md` in the repo root.

## Known limitations (by design, not oversights)

- Serial within a channel — one job at a time per channel (channels run in parallel).
- Retries are opt-in per job (`--retries`), immediate, and go to the end of the queue.
- No sandboxing — jobs run with your own user permissions, same as
  running them directly in a shell.
- Nothing currently prevents two `queuerd` processes from running
  against the same database at once if you start one manually alongside
  the systemd-managed one — always manage it through `systemctl --user`,
  not by backgrounding `queuerd` directly.
