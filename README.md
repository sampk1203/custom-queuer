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

**Uninstall** lists what it will remove and asks to proceed (default no), then asks
separately whether to delete the job logs and whether to disable linger (both
default no). It stops and disables `queuerd`, deletes the systemd unit, runs
`uv tool uninstall queuer`, removes the shell completion files and the lines typer
added to `~/.bashrc`, deletes the job database (queue and history), the Telegram
setup (`~/.config/queuer`, bot token included), the socket (and its private
`/tmp/queuer-<uid>` folder if that was used) and the project `.venv`. The job database and
logs are looked up in `~/.local/state/queuer`, the same place the daemon uses. It does not delete the project folder, uv itself, or the PATH line `uv tool
update-shell` added to your shell rc, since other tools use those. Logs stay unless
you say yes. The Telegram bot token is not revoked; send `/revoke` to @BotFather
for that.

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
queuer add [--channel N] [--before ID | --after ID | --now] [--depends-on IDS] [--after-any IDS] [--each V... | --array A-B] [--on-fail pause|continue] [--delay DURATION | --at TIME] [--timeout SECONDS] [--retries N] -- <cmd...>
queuer list [--channel N] [--full] [--json]
queuer status [--json]
queuer show ID [ID...] [--json]
queuer cancel [JOB_ID] [--channel N]
queuer clear [--channel N]
queuer rm ID
queuer requeue ID
queuer pause [--channel N]
queuer resume [--channel N]
queuer logs ID [--tail]
queuer clean-logs [--days N] [--dry-run]
queuer connect [--export | --import STRING | --from [USER@]HOST | --add-bot | --off] [--id NAME] [--listen | --no-listen] [--list] [--remove-chat CHAT_ID] [--remove-bot [--all]] [--bot ID_OR_@NAME]
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

`--before ID` / `--after ID` choose the job's **position** in the queue and nothing
more: the new job does not wait for that job to finish or succeed, and the ID must
still be queued on the same channel (once it started, the add is rejected).
`--now` freezes the running job (SIGSTOP) while the priority job runs; a frozen job
stays alive and keeps its memory, so briefly one channel can have two live jobs.

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

`--retries N` requeues the job automatically (at the end of its channel's
queue) if it fails — non-zero exit, timeout, or failed start — up to N
times. Each retry is a new job with its own ID and log; the failed job's
note says which job replaced it. Cancelled jobs are never retried, and
jobs interrupted by a daemon stop (see "Daemon stop / restart" below) are not
retried either and do not use up `--retries`.

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

### `cancel [JOB_ID]`

With no `JOB_ID`, kills the currently running job on a channel (default 1;
`--channel N` for another) — SIGTERM to its whole process group, then SIGKILL
if it's still alive after the grace period. Errors clearly if nothing is
running there.

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

### `pause` / `resume`

`pause` stops the daemon from starting new jobs — the current job (if
any) keeps running to completion. State persists across daemon
restarts, so a paused queue doesn't silently resume after a crash.

### `connect`

Telegram messages when a job finishes, from every machine running qur, into
one chat on your phone.

```
✅ done · job 12 · 2h13m
$ python train.py --epochs 50
bigbox [a3f9] · ch 1 · exit 0
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
paths or raw error text — use `qur show ID` on that machine for those.

**Details button.** Replies with everything `qur show ID` prints (raw and
resolved command, cwd, timestamps, duration, exit code, timeout, origin, pid, log
path, note), laid out for Telegram: bold labels, monospace commands and paths.
This is the one place paths leave the machine, and only to your paired chats, on
request. `qur show` itself is unchanged.

**Logs button.** Replies with the last ~20 lines of the job's output (colour codes
stripped). Whatever the job printed, secrets included, then goes through Telegram, to
your paired chats only and only when you press it. Don't press it for jobs that print
secrets.

**Requeue button.** Pressing it re-runs the finished job exactly like
`qur requeue ID` (same command, cwd, timeout, retries; end of its channel's
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
echo 'qurtg1:eyJ0...' | qur connect --import -   # same, string from stdin
```

A string given on the command line is visible in `ps` and your shell history, so
`qur connect` prints a warning. Prefer `--from user@host` (nothing to copy) or
`--import -` (string on stdin).

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

### `logs ID [-n N] [--tail]`

Reads the job's log file straight off disk — this doesn't go through
the daemon at all, so it works even if `queuerd` isn't running. A big log is
streamed in chunks, never loaded whole. `-n N` / `--lines N` prints only the last
N lines. `--tail` follows the file as it grows (Ctrl-C to stop); `-n 20 --tail`
shows the last 20 lines first, then follows. Each job's log
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
  the unit's `TimeoutStopSec=30`. Queued jobs stay queued.
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

Queued jobs and the last-10 backlog persist in SQLite at
`~/.local/state/queuer/queuer.db` and survive daemon restarts and
reboots without any special handling.

## File locations

| What | Where |
|---|---|
| Job database | `~/.local/state/queuer/queuer.db` |
| Job logs | `~/.local/state/queuer/logs/<id>.log` |
| Socket | `$XDG_RUNTIME_DIR/queuer.sock`, or `/tmp/queuer-<uid>/queuer.sock` (private dir) if that is unset |
| Notification config (bot token, chats, this qur's id) | `~/.config/queuer/notify.json` (0600) |
| systemd unit | `~/.config/systemd/user/queuerd.service` |

The database, log, socket and notification paths can be overridden with `QUEUER_DB_PATH`,
`QUEUER_LOG_DIR`, `QUEUER_SOCKET_PATH`, and `QUEUER_NOTIFY_PATH` env vars — mainly useful for
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
- **Bad requests.** A request that isn't valid JSON, isn't UTF-8, or is longer than 64 KB
  gets `invalid request` and the daemon carries on (it used to log a traceback and
  send no reply). `add` rejects wrongly typed fields (timeout, channel, argv, cwd,
  before/after) with an error instead of failing later inside the worker. A job log that
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
- **Bot token.** Never appears in logs or error text. `qur connect --import -` takes the
  setup string on stdin; on the command line it is visible in `ps` and shell history
  (a warning is printed). The setup string and `notify.json` contain the token, so
  keep them private; `/revoke` to @BotFather invalidates it. The daemon checks
  `notify.json` and its folder (regular file, yours, no group/other access): a loose
  mode is tightened to 600/700 and logged, a symlink or someone else's file is logged as
  a warning. `qur connect --list` prints the same warning. Anything running as **you**
  can still read the file (like `~/.ssh` keys).
- **Telegram buttons.** The update offset is saved in the database, so a press that was
  already handled is not replayed after a restart, and a repeated press of the same
  button (double tap) is handled once. A press on a message Telegram no longer shows
  gets an answer ("message too old") instead of a spinner.
- **Telegram.** Only chats paired with a bot can press its buttons, and pairing needs your
  own random code. Finished-job messages include the command as typed (up to 1000
  characters; Details up to 1200 per command), and the **Logs** button sends output (see above). Don't put secrets in
  command arguments, and don't press Logs for jobs that print them.
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

`.github/workflows/ci.yml` runs the same tests, and a dependency audit
(`pip-audit` on the locked dependencies), on every push and pull request, and
once a week so new advisories show up without a push. It needs a GitHub repo to run;
locally the commands above are the same thing.

For a design/architecture writeup and the phase-by-phase build plan, see
`queuer_plan.md` in the repo root.

## Known limitations (by design, not oversights)

- Serial within a channel — one job at a time per channel (channels run in parallel). To run
  jobs in parallel, use several channels.
- A job waiting on a dependency blocks the jobs behind it on its channel (strict FIFO).
- Retries are opt-in per job (`--retries`), immediate, and go to the end of the queue.
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
qur connect --import -              # same, string read from stdin (keeps token out of ps/history)
```

`--bot` takes the numeric bot id or the @username shown by `--list`. `--import`
and `--from` merge into what the machine already has. Old single-bot
`notify.json` files keep working and are rewritten in the new layout on the
next change. Pairing a phone pauses button polling for all bots while it runs.
