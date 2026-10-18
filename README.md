# queuer

A serial background job queue. `queuerd` runs one job at a time, in order,
as a `systemd --user` service; `queuer` is the CLI you use to add, inspect,
reorder, and cancel jobs.

## Requirements

- Python 3.10 or newer (`requires-python >= 3.10`; CI tests 3.10-3.15, `.python-version` pins 3.12 for development)
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
the existing `queuerd` service (asking first if a job is running, see below), rewrites the
systemd unit with the correct path, enables + starts it, and enables
linger so it survives logout. It also makes any state left by an older version
owner-only (db, logs, config; see [Security and permissions](#security-and-permissions)).
If uv is missing it downloads a pinned uv release (version and SHA-256 are written
in `install.sh`, Linux x86_64 or aarch64) from GitHub over HTTPS, refuses it if the
hash does not match, and only then installs it to `~/.local/bin`. Nothing from the
net is run unchecked. On another platform, or if you prefer your package manager,
install uv first and the script skips the download. To move to a newer uv, change
`UV_VERSION` and both hashes together (the release page publishes a `.sha256` file
per archive).

`qur` is a short alias for `queuer` — same command, less typing.

Running `./install.sh` asks what to do: **Enter** installs (or re-installs), **1**
uninstalls. `./install.sh install` / `./install.sh uninstall` skip the question.
`./install.sh install --skip-tests` skips the test suite (about 50 s) and goes straight to
installing; without the flag nothing changes: a failing test still stops the install before
the running daemon is touched.

`./install.sh uninstall --yes` (or `-y`) uninstalls without any prompt, so it also works with
no terminal (a script, ssh without a tty). It answers the safe way: the job logs are kept and
linger stays enabled.

**Running jobs.** Stopping the daemon kills its jobs (recorded `interrupted`, not failed, not
retried), so a re-install after a `git pull` could end a multi-day run. Before it stops the
service, `./install.sh install` and `./install.sh uninstall` ask the daemon (`qur status`)
which jobs are running (or frozen by `--now`), list them and ask `Kill them and continue? [y/N]`.
The default is no: nothing is stopped, the script exits with 1 and says so, and you re-run it when
the jobs have finished (the tests, venv and `qur` commands are already updated by then). With no
terminal the answer is also no, so a script never kills a job by accident. `--yes` (`-y`) answers
yes: `./install.sh install --yes`. A daemon that does not answer is not asked about. Only the
`queuerd` systemd service is stopped; the old blanket `pkill -x queuerd` is gone, so a second
`queuerd` of yours (a dev copy) is no longer killed silently. A `queuerd` process that the service does not
manage (started by hand) is listed and stopped only after its own `Stop it? [y/N]` (or `--yes`).

The project path is written into the systemd unit (`ExecStart` points at the project's
`.venv`). If you move or delete the project folder, run `./install.sh install` again from the
new place; until then the service cannot start.

**Uninstall** lists what it will remove and asks to proceed (default no), then asks
separately whether to delete the job logs and whether to disable linger (both
default no); if a job is running it lists it and asks once more before the daemon is stopped
(see "Running jobs" above). It stops and disables `queuerd`, deletes the systemd unit, runs
`uv tool uninstall queuer`, removes the shell completion files and the lines typer
added to `~/.bashrc` (the old file is kept as `~/.bashrc.queuer-bak`), deletes the job database (queue and history), the Telegram
setup (`~/.config/queuer`, bot token included), the socket (and its private
`/tmp/queuer-<uid>` folder if that was used) and the project `.venv`. The job database and
logs are looked up in `~/.local/state/queuer`, the same place the daemon uses. It does not delete the project folder, uv itself, or the PATH line `uv tool
update-shell` added to your shell rc, since other tools use those. Logs stay unless
you say yes. The Telegram bot token is not revoked; send `/revoke` to @BotFather
for that.

To confirm it worked:
```bash
qur doctor
qur daemon status
qur status
```

## Command reference

`qur` works everywhere `queuer` does below — it's the same command, just shorter.
Every command also has worked examples under `qur <command> --help`, and `qur --help-all`
prints the help for every command at once.

```
queuer add [--channel N] [--before ID | --after ID | --now] [--depends-on IDS] [--after-any IDS] [--each V... | --array A-B] [--on-fail pause|continue] [--on-finish CMD] [--on-error CMD] [--delay DURATION | --at TIME] [--timeout SECONDS] [--retries N [--retry-delay DURATION [--retry-backoff]]] [--dry-run] -- <cmd...>
queuer list [--channel N] [--full] [--json]
queuer status [--json]
queuer show ID|RANGE [ID|RANGE...] [--json]
queuer wait ID|RANGE [ID|RANGE...] | --channel N [--timeout DURATION] [--quiet] [--json]
queuer history [--status S]... [--channel N] [--since DURATION|DATE] [--grep TEXT] [--limit N] [--full] [--json]
queuer cancel [JOB_ID] [--channel N] [-s SIGNAL] [--grace SECONDS]
queuer clear [--channel N]
queuer rm ID
queuer requeue ID
queuer edit ID [--cmd CMD | -- <cmd...>] [--cwd DIR] [--timeout SECONDS] [--channel N] [--retries N] [--on-fail pause|continue] [--on-finish CMD] [--on-error CMD] [--stop-signal SIG] [--stop-grace SECONDS] [--delay DURATION | --at TIME | --clear-delay] [--depends-on IDS] [--after-any IDS] [--clear-deps] [--env KEY[=VAL]]...
queuer pause [--channel N]
queuer resume [--channel N]
queuer doctor [--fix] [--json]
queuer daemon start | stop [--yes] | restart [--yes] | status | logs [-n N] [-f]
queuer logs ID [--tail]
queuer clean-logs [--days N] [--max-mb N] [--dry-run]
queuer connect [--export | --import STRING | --from [USER@]HOST | --add-bot | --off] [--id NAME] [--listen | --no-listen] [--yes] [--list] [--remove-chat CHAT_ID] [--remove-bot [--all]] [--bot ID_OR_@NAME]
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
- **resolved** — the same words, except that the executable (the first word) is made
  an absolute path: looked up on `PATH` (your shell's `PATH` at `qur add` time), or made
  absolute against the current directory if it contains a `/` or exists there. Every
  other word reaches the job exactly as you typed it (the job runs in the directory you
  added it from, so relative paths mean the same thing; `--config=conf/a.yaml`,
  `facebook/opt-125m` or a URL are never rewritten). Symlinks are **not** followed
  (deliberately — this matters for venvs: resolving `.venv/bin/python3` to its symlink
  target would silently break venv activation for the job).

`--before ID` / `--after ID` choose the job's **position** in the queue and nothing
more: the new job does not wait for that job to finish or succeed, and the ID must
still be queued on the same channel (once it started, the add is rejected).
`--now` freezes the running job (SIGSTOP) while the priority job runs; a frozen job
stays alive and keeps its memory, so briefly one channel can have two live processes (one
frozen, one running). A normal job never starts beside a running `--now` job: the queue
waits until the `--now` lane is empty.

**Dependencies.** `--depends-on IDS` runs the job only if every listed job ended
`done` (exit 0); if any ended failed, cancelled, skipped, interrupted or was removed,
this job is marked `skipped` (note `dependency N failed`) and the queue moves on.
Skips cascade to the jobs that depended on the skipped one. `--after-any IDS` runs
the job once every listed job has *ended*, however it ended. `IDS` is a comma list
and/or a range (`--depends-on 31-33,40`); the flags can be repeated and combined (an
id in both counts as `--depends-on`). They are about *when* the job may run;
`--before/--after` only choose its queue position.

```bash
qur add -- ./prepare.sh                        # job 12
qur add --depends-on 12 -- ./train.sh          # only if 12 succeeded
qur add --after-any 12,13 -- ./cleanup.sh      # once both ended, whatever happened
```

The queue stays strict FIFO per channel: a job waiting for an unfinished dependency
**blocks the jobs behind it** on its channel; nothing jumps ahead. If something must
run meanwhile, put it on another channel. A waiting job stays `queued`; `qur list`
shows a "Waits on" column and `qur show` lists `depends on` / `after any` /
`waiting on`. `add` checks the ids exist and rejects a dependency that could never
run (a job queued *behind* this one on the same channel, or any longer cycle through
queue order and dependencies). `qur rm` of a still-queued job that others depend on is
refused with the list of dependants; `qur cancel` of it skips them. A job that
`--retries` requeues still counts as failed for its dependants (they are skipped: the
retry goes to the end of the queue, so waiting for it could deadlock). Requeueing a
job does not copy its dependencies. Timeouts count run time only, as always. Finished
jobs that a queued job still needs are kept in history until it has run or been skipped.

**Sweeps.** `--each V1 V2 V3 -- CMD ... {} ...` queues one independent job per
value, in order, with `{}` replaced by the value in every word of the command that
contains it (an error if there is no `{}`). `--array A-B -- CMD ...` queues one job
per number A..B inclusive; each job's environment has `QUR_INDEX` set to its number
(for `--each`, to the value's 1-based position). `--array` does not touch `{}`.
A sweep's request stays small: `--array` sends the command once with the range (the daemon
expands it), so `--array 1-1000` is no bigger than `--array 1-2`.

```bash
qur add --each 0.1 0.2 0.5 -- python train.py --lr {}     # added jobs 31-33
qur add --array 1-20 -- ./run.sh                          # $QUR_INDEX is 1..20
qur add --depends-on 31-33 -- python collect.py           # waits for the whole sweep
```

All jobs share the channel, timeout, `--retries`, `--before/--after` position and
dependencies (`--depends-on` applies to every created job). The jobs are created
all-or-nothing, at most 1000 per `add`, and `add` prints the id range it created.
`--each` values run up to the next `--flag` or the `--` before the command, so a value
can't start with `--`. If the command is one quoted word (or a hand-written
`bash -c`), a value substituted into it is shell-quoted; with separate words it is
used as is. `--before ID` puts the jobs before ID in order; `--after ID` puts them
after ID in order.

**`--on-fail pause|continue`** (default `continue`, today's behaviour). A job added
with `pause` that ends `failed` — non-zero exit, timeout or failed start; not
cancelled, skipped or interrupted — pauses its channel and sends the Telegram message
"channel N paused after job X failed". The jobs behind it stay queued; `qur resume`
(with `--channel N`) carries on. With `--retries`, only the final failure pauses
(retries copy the setting). Requeued jobs and sweep jobs keep it.

**Hooks (`--on-finish CMD`, `--on-error CMD`).** A hook is a shell command (run with
`/bin/sh -c`) the daemon starts after the job's **final** outcome, for example to copy results
when a long run ends:

```bash
qur add --on-finish 'rsync -a out/ host:results/' -- ./run.sh
qur add --on-error 'echo "job $QUR_JOB_ID failed ($QUR_EXIT_CODE)" | mail -s queuer me' -- ./run.sh
```

- `--on-finish` runs when the job ended `done`, `failed`, or was cancelled while running;
  `--on-error` only when it ended `failed` (non-zero exit, timeout, failed start). With
  `--retries` only the last attempt counts (retries and requeues copy the hooks). Skipped jobs,
  jobs cancelled while still queued, and jobs `interrupted` by a daemon stop run no hooks.
- Order: the global hook (below), then `--on-finish`, then `--on-error`, one after another.
- The hook runs in the job's cwd with the job's environment plus `QUR_HOOK`
  (`post-job`/`on-finish`/`on-error`), `QUR_JOB_ID`, `QUR_STATUS`, `QUR_EXIT_CODE` (empty if
  none), `QUR_CHANNEL`, `QUR_CWD`, `QUR_LOG`, `QUR_COMMAND` (the raw command, shell-quoted) and
  `QUR_DURATION_SECS`.
- It runs in the background: it never holds the channel, the next job starts at once. A hook
  still running after 5 minutes is killed (so is one still running when the daemon stops, after
  a few seconds' grace). Its output (first 64 KB) and exit code are appended to the **job's log**
  (`qur logs ID` shows `[queuer] hook on-finish: ...` and `... exited 0`); a non-zero exit or a
  timeout is also noted in the daemon journal. A failing hook never changes the job's status.
- A hook is stored and shown in `qur show` and Telegram Details exactly as typed, so do not
  put secrets in it (pass them through the environment, like for commands). Hooks are not
  redacted in `qur show`. `qur edit ID --on-finish CMD` / `--on-error CMD` set them on a queued job;
  an empty string removes one. At most 4096 characters.
- **Global post-job hook.** Set `QUEUER_POST_JOB_HOOK='command'` in the daemon's environment
  (`systemctl --user edit queuerd`: `[Service]` / `Environment=QUEUER_POST_JOB_HOOK=...`, then
  `qur daemon restart`) to run one command after every job that ends `done`, `failed` or
  `cancelled` (same rules and variables as above, `QUR_HOOK=post-job`). A bad value (too long) is
  ignored with a warning in the journal.
- Not done: more notifiers (email, ntfy, webhook, Matrix) as built-in, Telegram-style channels.
  A hook can call `curl`, `mail` or `ntfy` itself.

**Delayed start.** `--delay 30m` (units `s`/`m`/`h`/`d`, chainable: `1h30m`) or
`--at 22:00` (the next time your local clock shows that; today if it is still ahead,
else tomorrow) or `--at "2026-12-01 06:30"` (that exact local moment, which must be in
the future) keeps the job from starting before that time. The time is stored in the
database as an absolute timestamp, so it survives daemon restarts and reboots; if the
daemon was off at that time the job starts as soon as it is back and it reaches the
front. The daemon sleeps until exactly that moment (it does not poll for it).

```bash
qur add --delay 30m -- ./later.sh
qur add --at 22:00 -- ./overnight.sh
qur add --at "2026-12-01 06:30" --array 1-20 -- ./run.sh     # applies to every job
```

Same strict FIFO rule as dependencies: a delayed job that reaches the front of its
channel **holds the channel until its time**; the jobs behind it wait too, and nothing
jumps ahead. If the queue must keep running meanwhile, put the delayed job on another
channel. A job with both a delay and `--depends-on`/`--after-any` starts when its time
has come **and** its dependencies have ended (a dependency that already failed skips it
at once, without waiting for its time). A paused channel does not start it when its time
comes; it starts after `qur resume`. The job stays `queued` until then: `qur list` shows
a "Starts at" column and `qur show` / the Telegram Details show `not before`. `qur rm`
and `qur cancel` work as on any queued job and free the channel at once. The timeout
counts run time only, so the delay never eats into it. `--delay` and `--at` cannot be
combined with each other or with `--now`; a requeued job (button or `--retries`) is not
delayed again.

**Environment.** A job does not get your whole shell environment (that would copy secrets
into the job row and logs). It gets:

- Standard input is `/dev/null` (a job never reads the daemon's terminal, even when the daemon was started by hand).
- `PATH`, `HOME`, `VIRTUAL_ENV` and `PYTHONPATH` as before, and `PYTHONUNBUFFERED=1` so Python
  output reaches the log and `qur logs` as it is printed (override it with `--env PYTHONUNBUFFERED=0`);
- `USER`, `LOGNAME`, `SHELL` and `TMPDIR`, taken from the daemon's own environment (the systemd user
  manager sets the first three) when it has them there; a name the daemon does not have is not
  invented. Some MPI, conda and licence tools refuse to start without them. `--env` overrides them,
  and hooks get the same;
- a **passthrough list**, copied from the shell that runs `qur add` when set there:
  `CUDA_*` (`CUDA_HOME`, `CUDA_VISIBLE_DEVICES`, ...), `OMP_*` (`OMP_NUM_THREADS`, `OMP_PLACES`,
  `OMP_PROC_BIND`, ...), `MKL_*`, `OPENBLAS_*`, `NCCL_*`, `OMPI_*`, `UCX_*`, `CONDA_*`,
  `LD_LIBRARY_PATH`, `LANG`, `LC_*`, `TERM`, `TZ`. Replace the list with the variable
  `QUEUER_ENV_PASSTHROUGH` in your shell (comma-separated names, `*` allowed;
  `QUEUER_ENV_PASSTHROUGH=` empty means pass nothing). For anything else your tools read
  (`LAMMPS_POTENTIALS`, a licence server, ...) list it there, for example
  `QUEUER_ENV_PASSTHROUGH='CUDA_*,OMP_*,LD_LIBRARY_PATH,LAMMPS_*'` (that REPLACES the built-in list, so
  repeat what you still want), or give it per job with `--env NAME`;
- anything you give with `--env KEY=VAL` (sets it) or `--env KEY` (copies the value from your shell;
  an error if it is not set). Repeatable; names must match `[A-Za-z_][A-Za-z0-9_]*`. `--env` beats the
  passthrough list, and a sweep's `QUR_INDEX` beats `--env`.

```bash
qur add --env CUDA_VISIBLE_DEVICES=1 --env OMP_NUM_THREADS=8 -- python train.py
qur add --env MY_SETTING -- ./run.sh
```

The values are stored in the job (so requeues and `--retries` repeat them) and `qur show` lists
them: variables copied automatically by name only, `--env` ones as `NAME=value`, with the value shown as
`***` when the name contains `KEY`, `TOKEN`, `SECRET` or `PASSWORD`. The Telegram Details shows the
same list. Don't pass secrets with `--env` if you do not want them in the database; use a file the
job reads.

`--retries N` requeues the job automatically (at the end of its channel's
queue) if it fails — non-zero exit, timeout, or failed start — up to N
times. Each retry is a new job with its own ID and log; the failed job's
note says which job replaced it. Cancelled jobs are never retried, and
jobs interrupted by a daemon stop (see "Daemon stop / restart" below) are not
retried either and do not use up `--retries`.

`--retry-delay DURATION` (units s/m/h/d, chainable: `5m`, `1h30m`; at most 30d; needs
`--retries`) makes each automatic retry wait that long before it may start, so a job that
fails at once does not burn all its retries in seconds. The retry is stored with a start
time exactly like `--delay`, so the same rules apply: strict FIFO (the waiting retry holds
its channel, jobs queued behind it wait), it survives a daemon restart, and a paused
channel waits for `qur resume`. Dependants of the failed job are still skipped at once.
`--retry-backoff` (needs `--retry-delay`) doubles the wait with every retry already made:
`--retry-delay 1m --retry-backoff` waits 1m, 2m, 4m, 8m, ... (never more than 30d). A
manual requeue (`qur requeue`, the Telegram button) is never delayed but keeps the setting.
`qur show` and the Telegram Details list it as `retry delay`. Without `--retry-delay`
retries start as soon as they reach the front, as before.

`add` validates the executable exists and is executable *before*
accepting the job — a bad command is rejected immediately, not an hour
later when it reaches the front of the queue.

`--dry-run` checks and places the job exactly as a real `add` would, then queues nothing:

```
qur add --dry-run --delay 2h --env OMP_NUM_THREADS=8 -- python train.py
```

It prints the raw command (what you typed) and the resolved command (the executable as an
absolute path, what will really be exec'd), the working directory, the
environment the job would get (same masking as `qur show`: copied names only, `NAME=***` for
secret-looking names), the channel, and the queue position (`queue position: 3 (2 queued
ahead; job 7 is running now)`; with `--now` the position on the `--now` lane and the job that
would be frozen). It also says when the channel is paused and when a `--depends-on` job has
already failed, so the new job would be skipped at once. The daemon does the real validation
and placement inside a transaction that it rolls back, so every error (bad command, unknown
`--before`/`--after`/dependency id, deadlock, bad cwd) is exactly the one a real add gives,
no job id is used up, no worker is woken, no job is frozen and no Telegram message is sent.
Works with every other option; a sweep (`--each`/`--array`) prints its first 5 jobs and the
position range. The daemon must be running (the check needs the queue).

`--timeout SECONDS` kills the job automatically if it runs longer than
that (SIGTERM, then SIGKILL if it doesn't exit within a grace period),
and marks it `failed` with `note: timed out`. The signal and the grace period are
configurable per job, see below.

**Stop signal and grace (`--stop-signal SIG`, `--stop-grace SECONDS`).** By default a job is
stopped (by `qur cancel` or its `--timeout`) with SIGTERM to its whole process group and gets
5 seconds before SIGKILL. A job that must shut down cleanly (write a checkpoint, flush
output) can say how:

```bash
qur add --stop-signal INT --stop-grace 120 -- ./lmp -in run.in   # Ctrl-C, then up to 2 minutes
```

- `--stop-signal` is one of `TERM`, `INT`, `HUP`, `QUIT`, `USR1`, `USR2`, `KILL` (also `SIGINT`
  or the number). `STOP`/`CONT` are refused: queuer uses them to freeze jobs for `--now`.
- `--stop-grace` is a number of seconds, or a duration (`90s`, `2m`, `1h`), at most 1 day; `0`
  sends SIGKILL right after the signal. The grace is how long the job's WHOLE process group gets to
  exit; if any process of it (a background child that ignores the signal, an MPI helper) is
  still alive then, the whole group is killed.
- Both apply to `qur cancel` and to a timeout. They do NOT apply when the daemon itself stops
  (reboot, `qur daemon stop`): that keeps its fixed SIGTERM and 5-second budget so the stop
  stays inside systemd's `TimeoutStopSec=30`.
- Retries, requeues and sweeps copy them; `qur edit ID --stop-signal SIG --stop-grace N` changes
  them on a queued job, an empty string goes back to the default. `qur show`, Telegram Details
  and `qur add --dry-run` show them when set.

### `list`

Shows each channel in its own section: the running job first (marked
with `*`), then the queue, with the time each job was queued and how long
ago that was, then the last 10 finished jobs (done/failed/cancelled/skipped/interrupted;
the table title says how many are shown). The daemon keeps more (see `history`).

- `--channel N` lists only that channel (default: all channels)
- The `Origin` column says where a job came from: `new` (you queued it),
  `retry of #N (k/max)` (automatic, after job N failed), or `requeue of #N`
  (manual, via `qur requeue N`)
- `--full` shows the resolved command (executable as an absolute path) instead of what you typed
- `--json` prints the raw response instead of a table, for scripting

### `show ID [ID...]`

IDs can also be ranges and comma lists, like `--depends-on`: `qur show 31-33`, `qur show 12,15 40`.
They are shown in the order you give them.

Full detail on one or more jobs — both command forms, cwd, every timestamp,
duration, exit code, timeout, auto-retry count and retry delay, extra environment (see "Environment" under
`add`), peak memory and CPU time of a finished job (see "Resource accounting" below), pid, and
log path. With `--json`, one ID prints an object; several print a list.

#### Resource accounting

While a job runs, the daemon samples its process from `/proc` every 0.2 s and stores the result
when the job ends, whatever way it ends (done, failed, timed out, cancelled, interrupted).
`qur show` then has two more rows, and the Telegram Details button the same two:

```
peak RSS   30.0 GB
cpu time   2m14s user + 3s sys
```

- **peak RSS**: the largest resident memory the job's WHOLE process group had at once: the sum of
  `VmRSS` over every process in the group (the process `qur` started, `mpirun`'s ranks, a shell
  script's children), the biggest sum seen while it ran, never lowered when a rank exits. The
  started process's own `VmHWM` counts too. Shown as KB, MB or GB. Use it to size the next DFT or
  LAMMPS run.
- **cpu time**: user and system CPU seconds of every process in the group (`utime`/`stime`) plus
  the children each one has already waited for (`cutime`/`cstime`), so `mpirun -np 16 lmp` shows
  the ranks' CPU time, not the launcher's. Wall time is the existing `duration`.
- The finished-job message in Telegram gets a short tail: `... · exit 0 · used 31.0 GB · 1h58m cpu`.
  Whichever of the two was captured is shown; a job that has nothing is shown as before.
- Raw numbers are in `qur show --json`, `qur history --json` and `qur export` as `max_rss_kb`,
  `cpu_user_secs`, `cpu_sys_secs` (`null` = not captured). They are not in the `list` table or in
  `history --csv`. A requeued job starts with empty usage and gets its own numbers when it runs.

What it does NOT measure (read before trusting a number):

- Only processes in the job's process group count (queuer starts every job in its own group). A
  daemon a job starts with `setsid`/`nohup setsid`, or a process that makes its own group
  (`setpgid`), is outside it. A scheduler-style tool that moves work to another host (`srun`,
  `ssh`) shows only its local part.
- The RSS sum counts a page once per process that has it mapped, so ranks that share memory
  (MPI shared-memory windows, a mapped library, copy-on-write pages after a fork) make the sum higher
  than the real use. It is an upper-leaning estimate of what to ask for, not an exact figure.
  Memory that only a process's children had, once they have all exited, is not in a later sum, but
  the peak keeps it.
- The group is scanned about once a second (one pass over `/proc`; the started process itself every
  0.2 s at first, then every second). A job shorter than about 0.2 s has no numbers (it ends before
  the first sample). A memory spike between two scans can be missed, and the CPU time can lag the
  real end by up to a second (what a child used in the last moments, before its parent collects it).
- Linux only (`/proc`). Where `/proc` is unreadable a job just has no numbers; it never fails.

### `wait ID|RANGE... | --channel N`

Blocks until the jobs reach a final state (done, failed, cancelled, skipped, interrupted),
or, with `--channel N`, until that channel has nothing running, frozen or queued. The **exit
code mirrors the outcome**, so it works in scripts:

| code | meaning |
|------|---------|
| 0 | done (`--channel`: idle) |
| 1 | failed (with several jobs: any failure) |
| 2 | cancelled, skipped, interrupted, or removed while waiting |
| 3 | the daemon is not reachable |
| 4 | bad arguments, or a job id that does not exist (reported at once) |
| 124 | `--timeout` ran out |
| 130 | Ctrl-C |

With several ids the worst code wins (1, then 2, then 0). A job that failed and was requeued
by `--retries` is followed to its retry, so you get the result of the last attempt. A paused
channel stays busy, so `--channel` on it waits until `qur resume`. A channel that never had a job counts as idle.
`--timeout 2h`, `--quiet` (no output), `--json` (`{"exit_code", "timed_out", "jobs": [{"id", "status",
"exit_code", "channel", "ran_as", "note", "code", "finished"}]}`; with `--channel`:
`{"channel", "idle", "timed_out", "exit_code"}`). It polls the daemon every 1.5 s, asking about all
the jobs in one request (so `qur wait 1-1000` costs one round trip per poll, not 1000).

```
qur add -- ./prep.sh            # "Queued as job 40", then echoes the command and cwd
qur wait 40 && qur add -- ./use_prep.sh
qur wait 31-33 --timeout 2h || echo "sweep did not finish cleanly"
```

`qur show` and `qur status` also take `--json` (like `list`): plain JSON on stdout, no colours.

### `history`

Searches the finished jobs the daemon still has (done, failed, cancelled, skipped,
interrupted), newest first. The daemon keeps the last **200 finished jobs per channel**
(the old fixed 10 is gone); set `QUEUER_KEEP=N` in the daemon's environment (for example with
`systemctl --user edit queuerd`: `[Service]` / `Environment=QUEUER_KEEP=500`, then restart) to
change that. A bad value is ignored with a warning in the journal. Pruning happens when a job
finishes; a finished job that a still-queued job depends on is never pruned, and log files are
never touched (`qur clean-logs` handles those). `qur list` and the Telegram list still show only
the last 10.

- `--status S` (repeatable): `done`, `failed`, `cancelled`, `skipped`, `interrupted`
- `--channel N`
- `--since DURATION|DATE`: finished since then. A duration (`2d`, `6h`, `1h30m`) or a date
  (`2026-10-01` = local midnight, or `"2026-10-01 14:30"`)
- `--grep TEXT`: the command you typed contains TEXT (case-insensitive; the words are joined by
  spaces, so `--grep "--lr 0.1"` works). The resolved command is not searched
- `--limit N` / `-n N`: most rows, default 50, at most 10000
- `--full`: resolved commands; `--json`: a JSON list of the job records; `--csv`: CSV with a header row
  (`id, channel, status, command, cwd, origin, enqueued_at, started_at, finished_at, duration_secs,
  exit_code, note`; UTC timestamps; a command, cwd, origin or note cell starting with `=`, `+`, `-` or `@` gets a leading `'` so a
  spreadsheet does not run it as a formula; the number columns, e.g. an exit code of `-9`, are left as they are). `--csv` and `--json` cannot be combined

Filters combine (all must match). Exit code 0 even when nothing matches.

```bash
qur history --status failed --since 2d
qur history --channel 2 --grep train.py --limit 200
qur history --status failed --status interrupted --json
qur history --status failed --csv > failed.csv
```

### `export [--channel N] [--status S]`

Prints every job row as a JSON list on stdout (oldest id first): queued, running, frozen and
finished jobs, with commands, cwd, `env_extra`, times, exit codes, retry/dependency settings,
resource usage (`max_rss_kb`, `cpu_user_secs`, `cpu_sys_secs`) and `depends_on`/`after_any`. No logs. `--status` (repeatable) takes any job status (`queued`,
`running`, `stopped`, `done`, `failed`, `cancelled`, `skipped`, `interrupted`). It is a read-only
backup/inspection dump; there is no import. Values given with `--env` are written as stored
(unmasked), so keep the file private.

```bash
qur export > jobs.json
qur export --channel 2 --status queued > ch2-queue.json
```

### `cancel [JOB_ID]`

With no `JOB_ID`, kills the currently running job on a channel (default 1;
`--channel N` for another) — SIGTERM to its whole process group, then SIGKILL
if it's still alive after the grace period (5 seconds). Errors clearly if
nothing is running there.

`-s/--signal SIG` and `--grace SECONDS` change that for this one cancel, for
example `qur cancel 12 -s INT --grace 30` (Ctrl-C, then 30 seconds to write a
checkpoint). They override the job's own `--stop-signal` / `--stop-grace` (see `add`).
`--grace` is a number of seconds or a duration (`90s`, `2m`); `0` means SIGKILL at once.
They have no effect on a job that is still queued.

With a `JOB_ID`, cancels **that job**, wherever it is:

- **running** — killed as above.
- **frozen** by a `--now` job — continued and killed (a stopped process never
  acts on SIGTERM, so it is sent SIGCONT first). The `--now` job that froze it
  keeps running.
- **queued** — marked `cancelled` and kept in history (`qur rm` deletes it
  instead).
- already finished, or no such ID — error.

The job's channel is looked up from its ID, so you don't need `--channel`; if
you give both and they disagree, it errors rather than guessing. Cancelled jobs
are never auto-retried.

### `rm ID`

Removes a **queued** job entirely — it's gone, not kept as a backlog
entry. Errors if the ID is currently running (use `cancel` instead) or
doesn't exist, or if another queued job depends on it (`--depends-on` /
`--after-any`; remove those first, or `cancel` this one to skip them).

### `requeue ID`

Re-enqueues a finished job (done/failed/cancelled) with its exact
original command, working directory, timeout, and retry setting, appended to the end
of the queue. Handy for rerunning a job that failed without retyping it.

### `edit ID`

Changes a job that is still `queued`; a running or finished job gives a clear error.

```
qur edit 12                                   # command, cwd, timeout in $VISUAL/$EDITOR (else nano, vi)
qur edit 12 -- python train.py --epochs 10    # new command
qur edit 12 --cwd ~/run2 --timeout 7200
qur edit 12 --channel 2 --delay 1h
qur edit 12 --depends-on 10,11 --env OMP_NUM_THREADS=4
qur edit 12 --clear-delay --clear-deps
```

With flags, only what you list changes. Everything is checked like `qur add` (executable,
cwd, dependencies, deadlocks) and applied all or nothing. `--timeout 0` removes the timeout,
`--channel` moves the job to the END of that channel's queue, `--depends-on` / `--after-any`
replace that kind of dependency, `--clear-deps` removes all, `--env` adds or changes variables
(a sweep's `QUR_INDEX` stays). A new command or `--cwd` is resolved against this shell's PATH,
which also replaces the job's PATH, VIRTUAL_ENV and PYTHONPATH. `--retries 0` also drops the
job's retry delay. In the editor file, `command:` is read like `qur add -- 'COMMAND'` (shell
syntax runs via `bash -c`); an untouched command line keeps the original words.

**Edit lock.** While the editor is open the job is locked: it does not start, and since each
channel is strict FIFO the jobs behind it wait too, even after the job ahead has finished.
When you save or abandon the edit the channel goes on and runs the edited job. `qur list`
shows "editing" in the Waits on column, `qur show` an `editing` row. A lock whose editor
process died, or that is older than 30 minutes, is ignored and cleared by the daemon, and a
daemon restart drops all locks (the edit then fails with a clear message), so a crashed editor
can never block a channel. `qur rm` / `qur cancel` still work on a locked job. A job that is
being started at that very moment can no longer be edited. Locking is per job: other
channels are not affected.

### `pause` / `resume`

`pause` stops the daemon from starting new jobs — the current job (if
any) keeps running to completion. State persists across daemon
restarts, so a paused queue doesn't silently resume after a crash. `qur clear` forgets a channel that has no jobs left (except channel 1) together with its pause flag, so reusing that number later starts unpaused; pausing a channel before its first job still works.

### `doctor [--fix] [--json]`

Checks that queuer is set up and locked down, prints one line per check (`OK` / `WARN` /
`FAIL`) and, under every problem, a `fix:` hint. Exit code 1 if any check FAILs (warnings do
not fail it), so it also works in scripts.

| Check | OK when | Otherwise |
|---|---|---|
| `daemon` | `queuerd` answers on the socket | FAIL |
| `socket` / `socket dir` | a socket you own, mode 0600 (and the private `/tmp/queuer-<uid>` fallback folder 0700) | loose mode or foreign owner FAIL; missing WARN |
| `config dir`, `notify.json`, `redact.json` | files 0600, folder 0700, real files you own, no symlinks | loose file FAIL, loose folder WARN, symlink/foreign owner FAIL |
| `db dir`, `db file`, `db wal/shm` | database and its `-wal`/`-shm` files 0600, folder 0700 | same rules |
| `schema` | database at this queuer's schema version (read only, never migrates) | older WARN (the daemon upgrades it on start), newer FAIL |
| `db filesystem` | the database folder is on a local filesystem (read from `/proc/self/mountinfo`; nothing is opened or created) | on NFS, CIFS/SMB, Lustre, GPFS, Ceph, 9p or a FUSE mount (`fuse`, `fuse.*`) WARN: SQLite WAL may not work there, set `QUEUER_DB_PATH` to a local disk; `/proc/self/mountinfo` unreadable WARN. Cannot be fixed by `--fix` |
| `log dir`, `log writable`, `disk space` | log folder private and writable, at least 1 GB free where the logs go | under 1 GB WARN, under 100 MB FAIL, not writable FAIL |
| `linger` | `loginctl` linger is on (the daemon survives logout) | WARN |
| `unit file`, `service` | `~/.config/systemd/user/queuerd.service` exists and `systemctl --user is-active queuerd` says active | WARN |
| `unit exec` | the unit's `ExecStart` program exists, is executable and is this project's `.venv/bin/queuerd` | no `ExecStart` or program missing (project moved or deleted) FAIL; a different project folder WARN |
| `unit settings` | the installed unit has the restart/stop lines `install.sh` writes now (`Type`, `Restart`, `RestartSec`, `TimeoutStopSec`, `OOMPolicy=continue`) and either all or none of the hardening lines (`UMask`, `NoNewPrivileges`, `LockPersonality`, `RestrictAddressFamilies`); `hardening: none` is fine (`QUEUER_HARDEN=none`, or systemd refused `basic`) | a missing/different setting or a half-hardened unit WARN: re-run `./install.sh` |
| `telegram ...` | only if configured: one `getMe` per bot accepts the token | rejected token (401/404) FAIL; offline or other API error WARN; not configured is OK |

`--fix` repairs **permission problems only**: it chmods files and folders you own to 0600/0700.
It never starts services, edits the unit (the two `unit ...` checks only read it), deletes or moves anything, follows a symlink, or
touches the database contents. A folder you moved with `QUEUER_DB_PATH` / `QUEUER_LOG_DIR` /
`QUEUER_NOTIFY_PATH` is reported but never chmod'ed (it may be `$HOME`; the same rule the daemon
uses). A symlink or a path owned by someone else is only reported. `--json` prints
`{"ok", "summary", "checks": [{"name", "status", "detail", "hint", "fixed"}]}`.

```bash
qur doctor
qur doctor --fix
qur doctor --json | jq '.checks[] | select(.status != "OK")'
```

### `daemon start|stop|restart|status|logs`

Thin wrappers over `systemctl --user` and `journalctl --user -u queuerd` (fixed argument lists,
no shell), so you do not have to remember them:

| Command | Runs |
|---|---|
| `qur daemon start` | `systemctl --user start queuerd` |
| `qur daemon stop [--yes]` | `systemctl --user stop queuerd` |
| `qur daemon restart [--yes]` | `systemctl --user restart queuerd` |
| `qur daemon status` | `systemctl --user status queuerd --no-pager` (exit code is systemctl's; 3 = not running) |
| `qur daemon logs [-n N] [-f]` | `journalctl --user -u queuerd -n N --no-pager [-f]` (default 50 lines; `-f` follows, Ctrl-C stops) |

`stop` and `restart` first ask the daemon whether a job is running (or frozen by `--now`). If so
they say which and that it will be recorded as `interrupted` (not failed, not retried), and ask
`Continue? [y/N]`; `--yes` / `-y` skips the question. A daemon that cannot be reached is not asked about.
These need systemd; on a machine without `systemctl` they say so and exit 1.

### `connect`

Telegram messages when a job finishes, from every machine running qur, into
one chat on your phone.

```
✅ done · job 12 · 2h13m
$ python train.py --epochs 50
bigbox [a3f9] · ch 1 · exit 0 · used 31.0 GB · 2h9m cpu
[ 🔁 Requeue ] [ ℹ️ Details ] [ 📄 Logs ]
```

Sent for **done**, **failed** (including timeouts and failed starts),
**cancelled** (running or queued) and **interrupted** (`interrupted (daemon
stopped)`: the daemon stopped while the job ran; the Requeue button works on it). Each
qur has its own short id (`a3f9` above, shown when you connect), so with
several machines you can see exactly which one a message came from. A job with
`--retries` stays silent while retries remain; you get one message for its
final outcome, marked `attempt 3/3`. Messages show the command as you typed it
(one line, capped at 1000 characters) but never the working directory, resolved
paths or raw error text — use `qur show ID` on that machine for those. Secrets in the
command are masked as `***` first (see [Secret redaction](#secret-redaction-telegram-only)).

**Details button.** Replies with everything `qur show ID` prints (raw and
resolved command, cwd, timestamps, duration, exit code, timeout, origin, peak RSS and CPU time,
environment, pid, log path, note), laid out for Telegram: bold labels, monospace commands and paths.
This is the one place paths leave the machine, and only to your paired chats, on
request. `qur show` itself is unchanged. Secrets are masked as `***` here too.

**Logs button.** Replies with the last ~20 lines of the job's output (colour codes
stripped), to your paired chats only and only when you press it. Known secret shapes are
masked as `***` first, but a log can hold things no pattern can recognise: don't press it
for jobs that print secrets.

#### Secret redaction (Telegram only)

Everything sent to Telegram (finished-job message, Details, the Logs tail, and the list and
status text) is masked first. `qur show`, `qur logs` and `qur list` on the machine are NOT
redacted: you see your own data. What is masked (replaced by `***`):

- `NAME=VALUE` when NAME ends in key/token/secret/password/passwd/passphrase (`API_KEY=`,
  `GITHUB_TOKEN=`, `db.password=`, also inside URLs: `?access_token=...`), or has a
  `secret`/`password` part (`SECRET_KEY_BASE=`), or has a whole segment (split on `_ . -`) that is
  `pass`, `pwd`, `cred(s)`, `credential(s)`, `auth`, `cookie(s)` or `session` (`MYSQL_PWD=`, `DB_PASS=`,
  `AWS_CREDENTIALS=`, `API_AUTH=`, `SESSION_COOKIE=`; `QUEUER_ENV_PASSTHROUGH` and `author` are not).
  Names that only *contain* the word are left
  alone, so `--tokenizer=gpt2`, `max_tokens=100` and `token_count=5` stay readable.
- `--password=VAL`, `--password VAL`, `--api-key VAL`, `--access-token VAL` (same name rule;
  `--key-file PATH` is not touched).
- `Authorization: Bearer ...` (also Basic/Token) and a bare `Bearer <long token>`.
- AWS access key ids (`AKIA...`), GitHub tokens (`ghp_`, `gho_`, `ghu_`, `ghs_`, `ghr_`,
  `github_pat_`), Telegram bot tokens (`123456789:AAH...`).
- The password in `scheme://user:pass@host`.

Your own patterns go in `~/.config/queuer/redact.json` (or the path in `QUEUER_REDACT_PATH`;
`redact.json` sits next to `notify.json` if you moved that one): a JSON list of regexes,
for example

```json
["hunter\\d", "internal-host-\\w+", "my-project-(\\w+)-secret"]
```

A match is replaced by `***`; if the regex has a group, only the first group is replaced
(the third example keeps `my-project-` and `-secret`). The file must be yours and not
writable by others; a loose read mode is tightened to 0600, a symlink or someone else's
file is ignored. A pattern that is not a valid regex, or that matches the empty string, is
skipped and the daemon logs one line to the journal (never the pattern itself); it can never
stop the daemon. Changes are picked up without a restart.

This is pattern matching, not a guarantee. A secret in a plain argument (`mysql -pSECRET`,
`sshpass -p SECRET`) or printed without a label is not recognised. **Pass secrets through the
environment or a file, not through command arguments** (`--env` values are stored in the job
row, so a file is better still; see Environment above).

**Requeue button.** Pressing it re-runs the finished job exactly like
`qur requeue ID` (same command, cwd, timeout, retries and retry delay; end of its channel's
queue) and replies `🔁 job 12 requeued as job 13`; the Requeue button is then
removed from that message (Details stays). A second Requeue press of the same job
within 30 s (a double tap) is answered `already requeued as job N` and does nothing.
Telegram puts no time on a button press, so after the daemon was off for more than
5 minutes, presses that piled up meanwhile are dropped (the pressing chat gets an
`offline - press again` toast if Telegram still accepts it), never run late. Only chats you paired can use either
button, and these are the only actions the bot accepts. Both need the daemon to listen
to Telegram, which Telegram allows for one machine per bot: the machine that owns
the bot (`listen` on by default) handles them, and bots added with `--import` are
send-only and show no buttons. Change that with `qur connect --listen` /
`--no-listen` (add `--bot` for one bot). **Best setup: one bot per machine** (see
"Several bots on one machine" below), so a press always reaches the machine that
sent the message. Old buttons stop working if you
rename the machine, and a press is ignored if the job is not finished.

**See this machine's setup:** `qur connect --list` shows its id, hostname, every
bot (id and @username), its paired chat ids, whether the buttons are on, and the
config path. Other machines
are not listed — each qur only knows itself, so run it on each machine.

**Remove one phone/account:** `qur connect --remove-chat CHAT_ID` (ids from
`--list`). It stops getting messages and can no longer press buttons; the bot
token and other chats stay (with several bots it is removed from all of them
unless you add `--bot`). **Remove a bot from this machine:**
`qur connect --remove-bot` (same as `--off`; with several bots add `--bot ID`, or
`--all` to forget the whole setup).

**Rename this machine:** `qur connect --id bigbox` (1–16 letters, digits, `-`,
`_`). Takes effect on the next message; nothing else changes. `--id` also works
on first setup and with `--import`.

**One-time setup, once per person:** in Telegram open **@BotFather**, send
`/newbot`, pick a name, copy the token.

**First machine:**

```bash
qur connect          # paste the token, scan the QR with your phone, press Start
```

If the QR won't scan, open the `t.me` link printed under it. Run `qur connect` again to add another phone or
account — every paired chat gets every message.

**Every other machine** — no scan, no waiting. Easiest, over ssh:

```bash
qur connect --from sampk@laptop       # on the new machine; nothing to copy
qur connect --from sampk@laptop --id desktop   # same, and name it
```

It runs `qur connect --export` on that machine over ssh (adding `~/.local/bin` to
the remote PATH, since non-interactive ssh often lacks it) and imports the result.
ssh prompts (password, host key) appear as usual. The other machine must already
be set up. Without ssh, copy the string by hand:

```bash
qur connect --export                  # on the first machine: prints one string
qur connect --import qurtg1:eyJ0...   # on each other machine
echo 'qurtg1:eyJ0...' | qur connect --import - --yes   # same, string from stdin (no terminal to ask on, so --yes)
```

A string given on the command line is visible in `ps` and your shell history, so
`qur connect` prints a warning. Prefer `--from user@host` (nothing to copy) or
`--import -` (string on stdin).

`--import` and `--from` also show which bot and which chats the string would make this machine send job
messages (commands, paths, log tails) to, and ask `[y/N]` (default no) before saving anything; only chats this
machine does not already send to are listed, and nothing is asked when there are none. `--yes` / `-y` skips the question,
and is required when there is no terminal to ask on, which includes `--import -` (stdin is the string).

Each machine gets its own id on import and is send-only (no buttons; the first
machine keeps them). The string contains the bot token, so keep it private.

`qur connect --off` deletes this machine's saved setup. To invalidate the bot
token on all machines, send `/revoke` to @BotFather.

**How it works / limits:**

- Any number of machines can *send* through one bot at the same time. Listening
  happens in two places only: the daemon on the one `listen` machine (polling for
  Requeue presses), and the pairing step in `qur connect`. Telegram allows one
  listener per bot token, so the daemon steps aside while `qur connect` pairs.
  Pair **one machine at a time**; if two overlap you get a clear "another qur may
  be pairing" error — wait and retry. That is why other machines use `--import`
  instead of scanning.
- If two machines both have `listen` on, they take turns killing each other's
  polls and the button is unreliable. Turn one off with `--no-listen`.
- While Telegram is unreachable the daemon retries every 30 s and logs the
  error once to the journal. A button pressed during an outage is handled when
  it is back (Telegram keeps unconfirmed presses for ~24 h).
- The config (`notify.json`, mode 0600) holds the bot token. It is re-read for
  every message, so connecting or disconnecting needs no daemon restart.
- Telegram can read message content (bot messages are not end-to-end
  encrypted). That is why messages are kept minimal.
- Pairing only accepts a `/start` carrying the random code from your own QR, so
  a stranger who finds the bot can't subscribe themselves.
- A failed send (offline, Telegram down) is logged to the daemon's journal and
  dropped. It never delays or fails a job. Messages are not queued for later.

### `logs ID [-n N] [--tail]` / `logs --running [--channel N]`

Reads the job's log file straight off disk. It asks the daemon where the file is (the daemon may have a `QUEUER_LOG_DIR` your shell does not), and falls back to your shell's default log folder when the daemon is down or no longer knows the job, so it works even if `queuerd` isn't running. A big log is
streamed in chunks, never loaded whole. `-n N` / `--lines N` prints only the last
N lines. `--tail` follows the file as it grows (Ctrl-C to stop); `-n 20 --tail`
shows the last 20 lines first, then follows. `qur logs --running` picks the job running now
on a channel (default 1, `--channel N` for another) so you do not need its id; it prints
`(job N)` on stderr first, and fails with a clear message if nothing is running there. Each job's log
is capped at 50MB; once hit, further output is dropped and a single
`[queuer] log truncated at 50MB` marker is appended. If `<id>.log` already exists with content when job `<id>` starts (the database was deleted or reset, so ids began at 1 again), the old file is moved to `<id>.log.old` instead of being overwritten; `clean-logs` does not touch `*.log.old` files, delete them by hand.

### `clean-logs [--days N] [--max-mb N] [--dry-run]`

Deletes the logs of finished jobs (done, failed, cancelled, skipped, interrupted) that ended
more than `--days` ago (default 30; `0` or more, a negative number is refused), plus orphaned logs whose job row is gone from the
history (an orphan only when its file is also older than `--days`). `--max-mb N` then also
deletes the oldest remaining finished logs until the whole log folder is at most N MB
(logs of queued, running and frozen jobs count towards the total but are never deleted, so
the folder can stay above the limit while they are big). Only files named `<job id>.log`
are considered. `--dry-run` lists what would go. It reads the database directly, so it works
without the daemon, and refuses to run when the database is missing.

#### Automatic log cleaning

The daemon can run the same cleaning by itself on a timer, so you do not have to run
`qur clean-logs` by hand. It is **off unless you set a limit**, because it deletes files.
Set these in the daemon's environment (for example `systemctl --user edit queuerd`:
`[Service]` / `Environment=QUEUER_LOG_MAX_AGE_DAYS=30`, then `qur daemon restart`):

| Variable | Meaning |
| --- | --- |
| `QUEUER_LOG_MAX_AGE_DAYS=N` | delete logs of jobs finished more than N days ago (positive integer) |
| `QUEUER_LOG_MAX_MB=N` | keep the log folder at most N MB, deleting the oldest finished logs first (positive integer) |
| `QUEUER_LOG_CLEAN_HOURS=H` | how often to clean, in hours (positive number, may be fractional; default 6, never shorter than 1 minute) |

Either limit alone is enough; with both, age is applied first and then the size limit. The
first clean runs one minute after the daemon starts, then every `QUEUER_LOG_CLEAN_HOURS`.
The rules are exactly those of `qur clean-logs` (same `queuer.logclean` code): unfinished
jobs' logs are never touched and a log whose job row was pruned is judged by its file time.
When something was deleted the daemon writes one line to its journal (`qur daemon logs`:
`cleaned N log(s), X KB`); a failure is reported once and the timer keeps going. A bad
value (not a positive number) is ignored with a warning in the journal. Nothing is
deleted from the database, only log files.

## Managing the daemon

```bash
qur daemon status                        # is it running (systemctl --user status queuerd)
qur daemon restart                       # restart it; asks first if a job is running
qur daemon stop                          # stop it (queue state is preserved)
qur daemon logs -f                       # daemon's own logs (not job logs)
qur doctor                               # check the whole setup
```

These wrap `systemctl --user` / `journalctl --user -u queuerd`, which still work directly
(`systemctl --user restart queuerd`, `journalctl --user -u queuerd -f`). See `doctor` and
`daemon` in the command reference.

Job output goes to `~/.local/state/queuer/logs/<id>.log` (via `queuer
logs`), separate from the daemon's own stderr/stdout, which systemd
sends to the journal.

## Daemon stop / restart

Jobs do **not** survive a daemon stop. A stopped daemon never marks a running job
`failed`; it records the new terminal status **`interrupted`** (`note: daemon
stopped`). An interrupted job is not auto-retried, does not count against
`--retries`, and shows up in `qur list/status/show`, `--json`, `qur clear` and
`qur clean-logs` like any other finished job. Requeue it with `qur requeue ID` or
the Telegram button.

- **Clean stop** (`systemctl --user stop/restart queuerd`, reboot, SIGTERM/SIGINT): the
  daemon stops starting jobs, sends SIGTERM to every running (and `--now`-frozen)
  job's process group, SIGKILLs what is left after 5 s, writes `interrupted` for each
  job, sends the Telegram messages, then exits. That takes at most about 20 s, inside
  the unit's `TimeoutStopSec=30`. A second SIGTERM or Ctrl-C while that runs is ignored (the handlers stay installed until the jobs are recorded). Queued jobs stay queued. The process ends as soon as that
  shutdown is done; it does not wait for an in-flight Telegram poll.
- **Unclean stop** (crash, `kill -9`, power loss): the daemon could not write anything. On
  the next start every row still `running` (or `stopped`) from the previous instance is
  marked `interrupted` and you get a message for it. If the job's own process is still
  alive (it runs in its own process group), it keeps running unsupervised until it
  finishes or you kill it (`pgrep`/`kill`); systemd may also kill it when it cleans up
  the dead service (unsure, depends on the unit's KillMode).

With `install.sh` the daemon is a systemd user service with lingering enabled, so it
starts at boot (no login needed) and is restarted if it crashes. After a restart or reboot
the queue continues by itself: the interrupted job is recorded and you are told, then
the next queued job of each channel starts. Nothing can send
a message while the daemon itself is down; if you need to hear about that, watch it
from outside (e.g. a ping from another machine).

Queued jobs and the finished-job history (last 200 per channel, see `history`) persist in SQLite at
`~/.local/state/queuer/queuer.db` and survive daemon restarts and
reboots without any special handling. The database runs in SQLite WAL mode (so `clean-logs` and
the daemon can use it at the same time; you will see `queuer.db-wal` and `-shm` next to it, also 0600)
and carries a `schema_version`: an older database is upgraded automatically, in transactions,
the first time the daemon (or `clean-logs`) opens it. A database written by a newer queuer is refused.
WAL needs working shared memory and file locks, so keep the database on a **local disk**: on NFS/CIFS or some FUSE mounts (a cluster home directory is the usual case) SQLite may not refuse WAL and can still corrupt or lock up. Check with `findmnt -T ~/.local/state/queuer`; `qur doctor` warns (`db filesystem`) when the folder is on such a filesystem. The fix is to point `QUEUER_DB_PATH` at a local disk (for example in the unit: `systemctl --user edit queuerd`, `[Service]` / `Environment=QUEUER_DB_PATH=/var/tmp/<you>/queuer.db`, then restart, and set the same variable in your shell so `qur` finds the same file; the logs and socket can stay where they are).
A queued job's `position` only orders its queue; the numbers are spaced 1024 apart, so starting, removing or cancelling a job (and appending to a long queue or a big `--array` sweep) does not rewrite the other rows. Seeing gaps in the column is normal.

## File locations

| What | Where |
|---|---|
| Job database | `~/.local/state/queuer/queuer.db` |
| Job logs | `~/.local/state/queuer/logs/<id>.log` |
| Socket | `$XDG_RUNTIME_DIR/queuer.sock`, or `/tmp/queuer-<uid>/queuer.sock` (private dir) if that is unset (the `qur` client also tries `/run/user/<uid>/queuer.sock` when nothing is at the `/tmp` path) |
| Notification config (bot token, chats, this qur's id) | `~/.config/queuer/notify.json` (0600) |
| Extra Telegram redaction patterns (optional, JSON list of regexes) | `~/.config/queuer/redact.json` (0600) |
| systemd unit | `~/.config/systemd/user/queuerd.service` |

The database, log, socket and notification paths can be overridden with `QUEUER_DB_PATH`,
`QUEUER_LOG_DIR`, `QUEUER_SOCKET_PATH`, `QUEUER_NOTIFY_PATH` and `QUEUER_REDACT_PATH` env vars (and `QUEUER_KEEP`, the
number of finished jobs kept per channel, default 200; `QUEUER_LOG_MAX_AGE_DAYS`, `QUEUER_LOG_MAX_MB` and
`QUEUER_LOG_CLEAN_HOURS` for automatic log cleaning, see `clean-logs`; `QUEUER_POST_JOB_HOOK`, the global post-job hook, see `add`;
`QUEUER_JOB_UMASK`, the umask of jobs and hooks, see "Service hardening") — mainly useful for
running the daemon against a scratch location during manual testing,
without touching real state.

## Security and permissions

queuerd runs any command a connected client sends, so the socket is the trust
boundary. What is enforced:

- **Socket.** Created mode 0600 (the daemon sets a private umask while binding, so your
  umask doesn't matter). On Linux the daemon also checks the connecting process's uid
  (`SO_PEERCRED`) and drops anyone who isn't you.
- **Socket location.** `$XDG_RUNTIME_DIR/queuer.sock`. If that is unset or empty (cron,
  `sudo`), the fallback is `/tmp/queuer-<uid>/queuer.sock`, never a bare `/tmp`. The folder
  is created 0700; an existing one is used only if it is a real folder (not a symlink)
  owned by you, and a loose mode is tightened. Otherwise the daemon refuses to start.
  A `qur` run from cron, `sudo -u` or ssh without a login session has no `XDG_RUNTIME_DIR` either, so it would look in
  `/tmp/queuer-<uid>` while the systemd service listens in `/run/user/<uid>`: when nothing is at the `/tmp` path and the
  systemd socket exists, the client uses that one. An explicit `QUEUER_SOCKET_PATH` or `XDG_RUNTIME_DIR` is never second-guessed.
- **Service hardening.** `install.sh` writes the unit with `UMask=0077`, `NoNewPrivileges`,
  `LockPersonality` and `RestrictAddressFamilies` (level `basic`, the default; jobs inherit
  these, so `sudo` inside a job fails: use `QUEUER_HARDEN=none` for that).
  If systemd refuses the unit the installer falls back to `QUEUER_HARDEN=none` and says so.
- **Job umask.** The unit's `UMask=0077` is inherited by every job, so result files are created
  `0600` and a collaborator, or a group-shared project folder, cannot read them. To change that for
  jobs only, set `QUEUER_JOB_UMASK` in the daemon's environment, as an octal number
  (`systemctl --user edit queuerd`: `[Service]` / `Environment=QUEUER_JOB_UMASK=027`, then restart):
  `022` = everyone can read, `027` = your group can read, `077` = private. Unset or empty keeps the
  daemon's own umask (`0077` with the default unit); a bad value is ignored with a line in the journal.
  Jobs and hooks get it; the daemon's own files (database, logs, config, socket) stay private
  whatever it says, because the daemon sets their modes itself. Nothing else about the unit changes.
- **One OOM-killed job does not stop the service.** The unit sets `OOMPolicy=continue` (also at
  hardening level `none`). Jobs live in the daemon's cgroup; with systemd's default policy the kernel
  OOM killer (or systemd-oomd) killing ONE job process can make systemd stop the whole unit, so every
  channel's job would get SIGTERM and be recorded `interrupted`. Now only that job ends (it is
  recorded `failed`, like any job killed by a signal) and the other channels go on.
  Needs systemd 243 or newer (an older one ignores the line). Units written by an older
  `install.sh` do not have it: `qur doctor` shows `unit settings` as WARN, and `./install.sh` fixes it. To check what
  systemd uses: `systemctl --user show queuerd -p OOMPolicy`.
- **One daemon per socket.** A second `queuerd` exits with `queuerd is already running`
  before it opens the database, so it can't steal the socket or mark the first one's
  running job as failed. A stale socket file left by a crash does not block startup.
- **Database, logs, config.** Files are created 0600 (database, its `-wal`/`-shm`, every job
  log, `notify.json`), and folders queuer creates are 0700, whatever your umask. queuer's
  own default folders (`~/.local/state/queuer`, its `logs`) are tightened to 0700 on
  start, and `install.sh` also fixes older files. A folder you chose with
  `QUEUER_DB_PATH` / `QUEUER_LOG_DIR` is never chmod'ed (it could be `$HOME`); the files in
  it are still 0600 and the daemon prints a warning in the journal if the folder is open to
  other users. Job logs hold whatever your jobs print and the database holds your commands.
- **Accepted risk: your own processes.** Any process running as your user can queue commands
  through the socket (and read your 0600 files). There is no client auth token by design;
  the daemon only keeps other users out.
- **Bad requests.** A request that isn't valid JSON or isn't UTF-8 gets `invalid request`,
  and one longer than 16 MiB gets `request too large` (a 1000-job sweep is far below that);
  the daemon carries on (it used to log a traceback and send no reply). A database error
  or a number out of range inside a command is answered with `ok: false` and no traceback;
  an editor pid must be 1..4194304; one failing step never ends a channel's worker (it is
  logged and retried after 2 s). `add` rejects wrongly typed fields (timeout, channel, argv, cwd,
  before/after) with an error instead of failing later inside the worker, answers a missing `cwd`/`argv` by name,
  and refuses numbers beyond the limits: channel 1..9999, `--retries` at most 1000, `--timeout` at most 10 years,
  a sweep index 0..1000000000. Removing or cancelling a job in the instant its process is being created is refused
  ("is starting right now"). A job whose row vanishes during start-up has its process group killed. A job log that
  cannot be opened or written (disk full) is logged to the journal and the job's output is
  dropped; the job and the daemon keep going. The log file is opened without following
  symlinks.
- **One bad job never stalls a channel.** `add` also rejects non-string `path`,
  `virtual_env`, `python_path` and `raw_argv`, NUL bytes, and a working directory that
  does not exist. If anything still goes wrong while starting or running a job, that job
  is marked `failed` ("internal error: ...") and the channel's worker carries on; a
  worker that has died is respawned by the next `add`. Handlers (`cancel`, `rm`, `show`,
  `requeue`, `pause`, `resume`, `clear`) reject wrongly typed ids and channels with a
  clear error. A client that connects and sends nothing is dropped after 10 s.
- **Timeouts and `--now`.** `--timeout` counts run time only; time a job spends frozen
  by a `--now` job does not count.
- **Bot token.** Never appears in logs or error text. `qur connect --import - --yes` takes the
  setup string on stdin (an import first shows the bot and chats it will send job data to and asks); on the command line it is visible in `ps` and shell history
  (a warning is printed). The setup string and `notify.json` contain the token, so
  keep them private; `/revoke` to @BotFather invalidates it. The daemon checks
  `notify.json` and its folder (regular file, yours, no group/other access): a loose
  mode is tightened to 600/700 and logged, a symlink or someone else's file is logged as
  a warning. `qur connect --list` prints the same warning. Anything running as **you**
  can still read the file (like `~/.ssh` keys). The token stays in that plain 0600 file by
  design (no keyring or encrypted store).
- **Telegram buttons.** The update offset is saved in the database, so a press that was
  already handled is not replayed after a restart, and a repeated press of the same
  button (double tap) is handled once. A press on a message Telegram no longer shows
  gets an answer ("message too old") instead of a spinner.
- **Telegram.** Only chats paired with a bot can press its buttons, and pairing needs your
  own random code. Finished-job messages include the command as typed (up to 1000
  characters; Details up to 1200 per command), and the **Logs** button sends output (see above).
  Known secret shapes (`KEY=...`, `--password ...`, bearer tokens, cloud keys, `user:pass@`
  URLs, plus your own `redact.json` patterns) are masked as `***` before anything is sent, and
  only there: local `qur show` / `qur logs` are unchanged. It is pattern matching: pass secrets
  through the environment or a file, not as arguments, and don't press Logs for jobs that
  print them.
- **Errors you may see.** `permission denied on <socket>` means the socket isn't yours
  with mode 0600 (is `queuerd` running as another user?). `stale socket` means the
  file exists but nothing answers: `systemctl --user restart queuerd`. The CLI gives up
  with `did not answer` if the daemon is silent for 60 s.

- **Proxies.** The bot token is part of every Telegram URL, so queuerd ignores
  `HTTPS_PROXY` / `ALL_PROXY` / `HTTP_PROXY` for Telegram and connects directly. If your
  network only reaches Telegram through a proxy, start the daemon with
  `QUEUER_USE_PROXY=1` in its environment (e.g. `systemctl --user edit queuerd`:
  `[Service]` then `Environment=QUEUER_USE_PROXY=1`); the proxy then sees the token.
- **Connections.** At most 32 socket connections are served at once; a client that
  sends nothing is dropped after 10 s.

Not covered: jobs are not sandboxed (see below), and a process running as **you** can
always queue commands, which is by design.

## Development

```bash
uv sync
uv run pytest -v          # storage layer + path-resolution unit tests
```

The suite is isolated from your own setup: `tests/conftest.py` points the database, logs, socket and
notify config at a temporary folder and removes every other `QUEUER_*` variable from the environment
for the length of each test (so a `QUEUER_POST_JOB_HOOK` or `QUEUER_KEEP` exported in the shell that runs
`./install.sh` is never read by a test daemon). A test that needs one sets it itself with `monkeypatch.setenv`.

`.github/workflows/ci.yml` runs the same tests, the lint job (mypy, `ruff check src tests`, bandit) and a dependency audit
(`pip-audit` on the locked dependencies; it also fails when the committed `requirements.txt` is not what `uv.lock` exports:
after changing dependencies run `uv export --frozen --no-dev --no-hashes --no-emit-project -o requirements.txt` and commit it),
on every push and pull request, and
once a week so new advisories show up without a push. The GitHub actions are pinned to commit SHAs and the tools to exact
versions (mypy, ruff, bandit, pip-audit), so a new release cannot change CI behind your back: bump them by hand. The
build backend range in `pyproject.toml` (`uv_build`) follows the uv version `install.sh` installs; a test checks it. It needs a GitHub repo to run;
locally the commands above are the same thing.

## Known limitations (by design, not oversights)

- Serial within a channel — one job at a time per channel (channels run in parallel). To run
  jobs in parallel, use several channels.
- A job waiting on a dependency blocks the jobs behind it on its channel (strict FIFO).
- A job ends only when its whole process tree has ended: a command that backgrounds a child
  (`cmd &`) keeps its channel busy until that child exits too. `qur cancel` kills the whole
  group.
- Retries are opt-in per job (`--retries`), immediate unless `--retry-delay` is set, and go to the end of the queue.
- Hooks (`--on-finish`, `--on-error`, `QUEUER_POST_JOB_HOOK`) are best-effort background commands: no retry, a 5-minute limit, and a hook still running when the daemon stops is killed. Their output (at most 64 KB) goes to the job's log, even when that log has already hit the 50 MB cap.
- Notifications are Telegram-only, best-effort (a send that fails is dropped,
  not retried), and visible to Telegram. Pair one machine at a time (see
  `connect`).
- `qur clean-logs` refuses to run when the job database is missing (it would otherwise
  treat every log as orphaned), and deletes an orphaned log (its row was pruned) only when
  the file is older than `--days`. `qur logs` prints job output verbatim (`[brackets]`
  are not interpreted).
- A job that starts a background child (`bash -c "sleep 8 & echo hi"`) stays `running`
  until that child exits too: the daemon waits for the job's output pipe to close, and
  the child holds it open. `qur cancel ID` ends the whole process group.
- Resource accounting (`peak RSS`, `cpu time`) covers the job's whole process group (a shell
  wrapper's children, `mpirun` and its ranks, subprocesses), is sampled about once a second and is
  Linux-only; shared memory is counted once per process. See "Resource accounting" under `show`.
- No sandboxing — jobs run with your own user permissions, same as
  running them directly in a shell.
- A second `queuerd` refuses to start while one already answers on the socket
  (`queuerd is already running`, exit 1) and touches nothing. Different
  `QUEUER_SOCKET_PATH` values against the same database are not detected, so
  manage the daemon through `systemctl --user`.


## Several bots on one machine

One qur can be connected to any number of Telegram bots. Every finished-job
message goes through every bot (to every chat paired with it), and the daemon
polls each bot that has `listen` on, so Requeue / Details presses on any of
them come back to this machine and are answered through the same bot.

Telegram allows only one program to poll a bot, so for buttons that always
reach the right machine, give each machine its own bot (`/newbot` in
@BotFather) and add them to the same Telegram group if you want one feed.

```
qur connect                         # first bot: paste token, scan QR, press Start
qur connect --add-bot               # another bot on this machine (token prompt + QR)
qur connect --list                  # every bot, its chats, buttons on/off
qur connect --bot @second_bot       # pair another phone with one bot (needed when there are several)
qur connect --bot @second_bot --no-listen   # per-bot buttons (no --bot = all bots)
qur connect --remove-chat 123456789 [--bot ID]   # all bots that have it, or just one
qur connect --remove-bot --bot ID   # drop one bot; --all drops everything
qur connect --export [--bot ID]     # all bots (qurtg2:) or one (qurtg1:)
qur connect --import STRING         # adds the string's bots, send-only unless --listen
qur connect --import - --yes        # same, string read from stdin (keeps token out of ps/history)
qur connect --import STRING --yes   # skip the "send job messages to these chats?" question
```

`--bot` takes the numeric bot id or the @username shown by `--list`. `--import`
and `--from` merge into what the machine already has. Old single-bot
`notify.json` files keep working and are rewritten in the new layout on the
next change. Pairing a phone pauses button polling for all bots while it runs.
