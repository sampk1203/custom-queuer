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

Running `./install.sh` asks what to do: **Enter** installs (or re-installs), **1**
uninstalls. `./install.sh install` / `./install.sh uninstall` skip the question.

**Uninstall** lists what it will remove and asks to proceed (default no), then asks
separately whether to delete the job logs and whether to disable linger (both
default no). It stops and disables `queuerd`, deletes the systemd unit, runs
`uv tool uninstall queuer`, removes the shell completion files and the lines typer
added to `~/.bashrc`, deletes the job database (queue and history), the Telegram
setup (`~/.config/queuer`, bot token included), the socket and the project
`.venv`. It does not delete the project folder, uv itself, or the PATH line `uv tool
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
queuer add [--channel N] [--before ID | --after ID | --now] [--timeout SECONDS] [--retries N] -- <cmd...>
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
doesn't exist.

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
[ 🔁 Requeue ] [ ℹ️ Details ]
```

Sent for **done**, **failed** (including timeouts, failed starts, and jobs
interrupted by a daemon restart) and **cancelled** (running or queued). Each
qur has its own short id (`a3f9` above, shown when you connect), so with
several machines you can see exactly which one a message came from. A job with
`--retries` stays silent while retries remain; you get one message for its
final outcome, marked `attempt 3/3`. Messages show the command as you typed it
(one line, capped at 120 characters) but never the working directory, resolved
paths or raw error text — use `qur show ID` on that machine for those.

**Details button.** Replies with everything `qur show ID` prints (raw and
resolved command, cwd, timestamps, duration, exit code, timeout, origin, pid, log
path, note), laid out for Telegram: bold labels, monospace commands and paths.
This is the one place paths leave the machine, and only to your paired chats, on
request. `qur show` itself is unchanged.

**Requeue button.** Pressing it re-runs the finished job exactly like
`qur requeue ID` (same command, cwd, timeout, retries; end of its channel's
queue) and replies `🔁 job 12 requeued as job 13`; the Requeue button is then
removed from that message (Details stays). Only chats you paired can use either
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
```

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
`note: interrupted by daemon restart` (and, if notifications are set up, you get
a message for it). Note this only updates the job's
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
| Notification config (bot token, chats, this qur's id) | `~/.config/queuer/notify.json` |
| systemd unit | `~/.config/systemd/user/queuerd.service` |

The database, log, socket and notification paths can be overridden with `QUEUER_DB_PATH`,
`QUEUER_LOG_DIR`, `QUEUER_SOCKET_PATH`, and `QUEUER_NOTIFY_PATH` env vars — mainly useful for
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
- Notifications are Telegram-only, best-effort (a send that fails is dropped,
  not retried), and visible to Telegram. Pair one machine at a time (see
  `connect`).
- No sandboxing — jobs run with your own user permissions, same as
  running them directly in a shell.
- Nothing currently prevents two `queuerd` processes from running
  against the same database at once if you start one manually alongside
  the systemd-managed one — always manage it through `systemctl --user`,
  not by backgrounding `queuerd` directly.


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
```

`--bot` takes the numeric bot id or the @username shown by `--list`. `--import`
and `--from` merge into what the machine already has. Old single-bot
`notify.json` files keep working and are rewritten in the new layout on the
next change. Pairing a phone pauses button polling for all bots while it runs.
