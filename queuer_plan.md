# `queuer` — Background Job Scheduler / Queuer

Implementation plan for a systemd-`--user`-managed job queue daemon with a CLI client.
Written so any contributor (human or agent) can pick up a phase without needing the
original design conversation.

---

## 1. Goal

A background daemon (`queuerd`) runs jobs one at a time, in order, from a persistent
queue. A CLI (`queuer`) talks to it over a Unix socket to enqueue, inspect, reorder,
and cancel jobs. State survives daemon restarts and machine reboots.

## 2. Locked design decisions

These are settled — do not re-litigate without checking with the project owner.

| Decision | Choice |
|---|---|
| Service scope | `systemd --user` unit (not system-level). No sudo to run the daemon. |
| Concurrency | Strictly serial — one job running at a time, FIFO queue with insert points. |
| State store | SQLite (not flat JSON) for atomic writes under concurrent CLI access. |
| Persistence | Queue survives daemon restart and reboot. A job marked `running` at daemon startup with a dead PID is marked `failed` (interrupted), never silently re-run. |
| Job identity | Persistent unique integer ID (autoincrement), never a shifting queue position. All of `--before/--after/--rm` reference this ID. |
| IPC | Unix domain socket at `$XDG_RUNTIME_DIR/queuer.sock`. All state mutation happens inside the daemon; CLI never touches the DB directly. |
| Path resolution | Tokens containing `/` or resolving to an existing file/executable relative to enqueue-time cwd get canonicalized to absolute paths. Bare flags and non-path args are left untouched. Both raw and resolved forms are stored. |
| Kill semantics | Job runs in its own process group (`os.setsid`). Cancel sends SIGTERM to the group, waits a grace period, then SIGKILL. Sudo is only ever suggested to the user as a manual fallback if the daemon cannot signal the process — the daemon itself never shells out to sudo. |
| Backlog | Last 10 completed jobs (done/failed/cancelled) kept in `list` output, FIFO evicted. Logs of evicted jobs are kept on disk indefinitely (not auto-deleted) unless the owner says otherwise later. |
| Tech stack | Python 3, `asyncio` for the daemon's socket server and process supervision, `typer` + `rich` for the CLI, stdlib `sqlite3` for storage. |
| Job timeout | Optional `--timeout SECONDS` on `add`. If set and exceeded, daemon kills the job the same way `cancel` does (SIGTERM → grace → SIGKILL) and marks it `failed` with `note='timed out'`. |
| Daemon pause | `queuer pause` stops the worker from pulling new jobs; the currently running job (if any) is left alone. `queuer resume` re-enables pulling. Pause state persists across daemon restarts (stored in DB, not in-memory only) so a paused queue doesn't silently resume after a crash. |
| Log size cap | Per-job log capped at 50MB by default; once hit, further output is dropped and a single `[queuer] log truncated at 50MB` marker line is appended. Prevents a runaway job from filling disk. |

## 3. Non-goals (explicitly out of scope for v1)

- No parallel/concurrent job execution.
- No automatic retries on failure.
- No remote/network access to the daemon (Unix socket only).
- No sandboxing of job commands — this is a personal tool, jobs run with the user's own permissions.
- No web UI.

## 4. Data model

SQLite DB at `~/.local/state/queuer/queuer.db`.

```sql
CREATE TABLE jobs (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    status        TEXT NOT NULL CHECK (status IN
                    ('queued','running','done','failed','cancelled')),
    position      INTEGER,           -- NULL once job leaves the queue
    raw_cmd       TEXT NOT NULL,     -- JSON array, argv as typed by the user
    resolved_cmd  TEXT NOT NULL,     -- JSON array, argv with paths canonicalized
    cwd           TEXT NOT NULL,     -- captured at enqueue time
    env_extra     TEXT,              -- JSON object, opt-in extra env vars captured at enqueue time
    pid           INTEGER,           -- set once running
    pgid          INTEGER,           -- process group id, for group-kill
    enqueued_at   TEXT NOT NULL,     -- ISO8601
    started_at    TEXT,
    finished_at   TEXT,
    exit_code     INTEGER,
    log_path      TEXT NOT NULL,
    note          TEXT,              -- e.g. "interrupted by daemon restart", "timed out"
    timeout_secs  INTEGER            -- NULL = no timeout
);

CREATE TABLE daemon_state (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
-- single row: ('paused', 'true'|'false'), read by the worker loop on each cycle
```

Notes:
- `position` is only meaningful while `status = 'queued'`; use it purely for ordering,
  never for identity.
- Always forward `PATH` from the enqueuing shell; `env_extra` covers anything beyond
  that the user explicitly opts to pass through (design TBD in Phase 2, keep minimal
  for v1 — just `PATH` and `HOME` forwarded automatically, nothing else unless asked).

## 5. IPC protocol

Newline-delimited JSON over the Unix socket. Simple request/response, no streaming
needed except for `logs --tail` (Phase 4, can just poll the log file directly from
the CLI instead of proxying through the daemon — simpler, avoids extra IPC complexity).

Request shape:
```json
{"cmd": "add", "args": {"argv": ["...", "..."], "cwd": "...", "before": null, "after": null}}
```
Response shape:
```json
{"ok": true, "data": {...}}
```
or
```json
{"ok": false, "error": "message"}
```

Commands the daemon must handle: `add`, `list`, `status`, `cancel`, `rm`, `show`,
`requeue`, `pause`, `resume`.

## 6. CLI surface

```
queuer add [--before ID | --after ID] [--timeout SECONDS] -- <cmd...>
queuer list [--full] [--json]     # queued + currently running + last 10 done/failed/cancelled
queuer status [--json]             # currently running job only, or "idle"
queuer show ID [--json]            # full detail: raw+resolved argv, cwd, env, timestamps, exit code
queuer cancel                      # kill current running job (SIGTERM -> grace -> SIGKILL)
queuer rm ID                       # remove a queued job (error if ID is running or already gone)
queuer requeue ID                  # re-enqueue a done/failed/cancelled job with its original argv+cwd
queuer pause                       # stop pulling new jobs; current job (if any) keeps running
queuer resume                      # resume pulling new jobs
queuer logs ID [--tail]            # read the job's log file directly, no daemon round-trip needed
```

`add` reads the command after `--` verbatim; do not attempt to shell-interpret it
(no shell=True execution — spawn argv directly to avoid injection surprises and to
keep path-resolution well-defined).

`add` also validates the resolved executable exists (and is executable) before
accepting the job, and returns a clear error immediately rather than accepting
a job that's guaranteed to fail once it reaches the front of the queue an hour
later.

`--json` on `list`/`status`/`show` prints the raw response payload as JSON
instead of a rich table — for scripting against the queuer from other tools
(e.g. your orchestrator/pipeline code) without screen-scraping table output.

## 7. Phases

Each phase should be independently testable and leaves the tool in a working (if
incomplete) state.

### Phase 0 — Scaffolding
- Repo layout: `queuer/` package with `daemon.py`, `client.py`, `cli.py`, `db.py`, `pathresolve.py`.
- `pyproject.toml` via `uv`, entry points for `queuer` (CLI) and `queuerd` (daemon).
- Directory setup helpers: ensure `~/.local/state/queuer/logs/` and
  `$XDG_RUNTIME_DIR` socket path exist/are writable.
- **Deliverable:** package installs with `uv pip install -e .`, `queuer --help` and
  `queuerd --help` both run (no real logic yet).

### Phase 1 — Storage layer
- Implement `db.py`: schema creation/migration, and typed functions:
  `enqueue()`, `get_queue()`, `get_running()`, `get_backlog(limit=10)`,
  `mark_running()`, `mark_finished()`, `remove_from_queue()`,
  `reorder_before()`, `reorder_after()`, `get_job(id)`, `requeue(id)`
  (copies raw_cmd/resolved_cmd/cwd/env_extra/timeout_secs from an existing
  row into a fresh queued row, appended to the end of the queue),
  `get_paused()` / `set_paused(bool)` (backed by `daemon_state`).
- Unit tests against a throwaway sqlite file (not the real state dir) covering
  ordering, ID stability after removals, backlog eviction at >10 entries, and
  `requeue()` producing a correct new row without mutating the original.
- **Deliverable:** `db.py` fully unit-tested in isolation, no daemon/socket involved yet.

### Phase 2 — Daemon core (no CLI yet)
- `pathresolve.py`: implement the path-canonicalization rule from §2, with tests
  covering flags, relative files, nonexistent paths, and paths with `/` that don't exist.
- `daemon.py`:
  - asyncio Unix socket server, one handler per command in §5, including
    `show` (return full row for an ID), `requeue` (call `db.requeue()`),
    `pause`/`resume` (call `db.set_paused()`).
  - `add` handler validates the resolved executable exists and is executable
    (`os.access(path, os.X_OK)`) before inserting the row; on failure return
    `{"ok": false, "error": ...}` immediately, nothing is queued.
  - Worker loop: on idle, first check `db.get_paused()` — if paused, skip.
    Otherwise pull next queued job by `position`, spawn via
    `asyncio.create_subprocess_exec` with `start_new_session=True` (this sets the
    new process group), capture stdout/stderr to `log_path` with a size-capped
    writer (default 50MB, append `[queuer] log truncated at 50MB` once hit and
    stop writing further output), await completion, write
    `exit_code`/`finished_at`, move to backlog.
  - Timeout enforcement: if the job has `timeout_secs` set, run the wait with
    `asyncio.wait_for`; on `TimeoutError`, kill the same way `cancel` does
    (below) and mark `failed` with `note='timed out'` instead of `cancelled`.
  - Startup recovery: on boot, any row still `status='running'` gets checked —
    since the daemon just started, that PID cannot be alive from a prior run,
    so mark it `failed` with `note='interrupted by daemon restart'`. Paused
    state is read from `daemon_state` as-is (doesn't reset on restart).
  - Cancel handling: SIGTERM to `-pgid`, wait grace period (default 5s, make
    configurable), SIGKILL if still alive, mark `cancelled`.
- Manual test: drive the daemon with `socat` or a small test script sending raw
  JSON lines, confirm add/list/cancel/rm all work without the CLI existing yet.
- **Deliverable:** daemon runnable standalone (`python -m queuer.daemon`), fully
  functional over the socket, verified with raw socket test scripts.

### Phase 3 — CLI client
- `client.py`: connect to socket, send request, parse response, raise a clear
  error if the daemon isn't running (e.g. "queuerd not running — start it with
  `systemctl --user start queuerd`").
- `cli.py` with `typer`: implement all commands from §6 (`add`, `list`, `status`,
  `show`, `cancel`, `rm`, `requeue`, `pause`, `resume`, `logs`), `rich` tables for
  `list`/`status`/`show` output (columns: ID, status, cmd (raw or resolved per
  `--full`), started/duration, exit code where applicable), with `--json` on
  `list`/`status`/`show` bypassing rich and printing the raw response as JSON.
  `add` gains `--timeout SECONDS`, forwarded to the daemon and stored on the row.
- **Deliverable:** end-to-end usable tool via CLI, daemon started manually
  (`queuerd &`) for testing.

### Phase 4 — systemd integration
- Write the `--user` unit file, e.g. `~/.config/systemd/user/queuerd.service`:
  `Type=simple`, `ExecStart=<path to queuerd entrypoint>`, `Restart=on-failure`.
- Document (in README) the `loginctl enable-linger <user>` step for jobs to
  survive logout.
- Confirm reboot-persistence behavior end-to-end: enqueue jobs, kill -9 the
  daemon process to simulate crash, restart via systemctl, verify recovery
  logic from Phase 2 kicks in correctly.
- **Deliverable:** `systemctl --user enable --now queuerd` is the only setup
  step needed; survives daemon crash and full reboot.

### Phase 5 — Logs command + polish
- `queuer logs ID` / `--tail`: read directly from `log_path`, no daemon
  round-trip (daemon already writes there in Phase 2). `--tail` = `tail -f`-style
  follow, stop cleanly on Ctrl-C.
- Error-message pass: every failure mode (daemon down, bad ID, removing a
  running job, cancel when idle, non-existent executable) should produce a
  clear one-line CLI error, not a stack trace.
- README with install steps, command reference, and the systemd setup from Phase 4.
- **Deliverable:** tool is dogfoodable for daily use.

### Phase 6 (optional, defer until v1 is in daily use)
- Desktop notification (`notify-send`) on job completion/failure.
- Config file for grace period, backlog size, log retention policy.
- `queuer add --dry-run` to preview raw vs resolved command before enqueueing.

## 8. Open items to revisit later (not blocking v1)

- Whether evicted backlog logs should eventually get a retention/cleanup policy.
- Whether `env_extra` needs a real opt-in mechanism (e.g. `--env VAR` flags on
  `add`) or whether forwarding `PATH`/`HOME` only is sufficient in practice.
- Whether `--before`/`--after` need any validation against referencing a
  non-queued (running/done) ID — should be a clear CLI error, not a crash.

## 9. Testing expectations across all phases

- `db.py` and `pathresolve.py`: pure unit tests, no daemon/socket needed.
- `daemon.py`: integration tests using a temp socket path and temp sqlite file
  (never point tests at the real `~/.local/state/queuer/` — pass overridable
  paths via env var or CLI flag for test isolation).
- Manual smoke test checklist before closing any phase: add 3 jobs, reorder
  with `--before`/`--after`, cancel the running one, remove a queued one,
  confirm `list` and `list --full` both render correctly, confirm backlog
  caps at 10, `show` a job and a backlog entry, `requeue` a failed job and
  confirm it re-runs with the same argv/cwd, `pause` and confirm the queue
  stops advancing while a running job finishes normally, `resume` and confirm
  it picks back up, add a job with `--timeout 2` running `sleep 10` and
  confirm it's killed and marked `note='timed out'`, add a job pointing at a
  nonexistent executable and confirm `add` itself rejects it rather than
  queuing a guaranteed failure.

---

## 10. Deferred — not part of this plan

**Do not start on this until everything above is built, working, and in daily
use.** Noted here only so it isn't forgotten.

- **SMS/phone notification instead of (or alongside) desktop notify-send.**
  E.g. text a phone number when a job finishes or fails. This needs an
  external service (Twilio or similar), which means an API key, a network
  call from the daemon (currently fully offline/local), and a decision about
  what's worth interrupting a phone over (every job? failures only?). Revisit
  as a Phase 7+ item once the core tool has been lived with for a while and
  it's clear which jobs actually warrant a phone ping.
