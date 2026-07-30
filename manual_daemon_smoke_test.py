"""Manual smoke test for queuerd -- drives the daemon directly over its
Unix socket with raw JSON requests, no CLI required (Phase 2 of the plan
predates Phase 3's CLI, so this is how you verify the daemon works).

Usage:
  1. In one terminal, start the daemon against a scratch DB/socket so you
     don't touch your real state:

       QUEUER_DB_PATH=/tmp/queuer-test.db \
       QUEUER_SOCKET_PATH=/tmp/queuer-test.sock \
       QUEUER_LOG_DIR=/tmp/queuer-test-logs \
       uv run python -m queuer.daemon

  2. In another terminal, run this script the same way:

       QUEUER_SOCKET_PATH=/tmp/queuer-test.sock \
       uv run python manual_daemon_smoke_test.py

It walks through: add a few jobs, list the queue, reorder isn't exercised
here (that's exercised directly in the db.py tests), cancel the running
job, remove a queued job, and confirm the responses look right.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path


SOCKET_PATH = Path(os.environ.get("QUEUER_SOCKET_PATH", "/tmp/queuer-test.sock"))


async def send(cmd: str, **args) -> dict:
    reader, writer = await asyncio.open_unix_connection(str(SOCKET_PATH))
    req = {"cmd": cmd, "args": args}
    writer.write((json.dumps(req) + "\n").encode())
    await writer.drain()
    line = await reader.readline()
    writer.close()
    await writer.wait_closed()
    return json.loads(line.decode())


def show(label: str, resp: dict) -> None:
    print(f"\n--- {label} ---")
    print(json.dumps(resp, indent=2, default=str))


async def main() -> None:
    if not SOCKET_PATH.exists():
        print(f"no socket at {SOCKET_PATH} -- is the daemon running?", file=sys.stderr)
        sys.exit(1)

    cwd = "/tmp"

    r1 = await send("add", argv=["sleep", "3"], cwd=cwd)
    show("add sleep 3", r1)

    r2 = await send("add", argv=["echo", "hello"], cwd=cwd)
    show("add echo hello", r2)

    r3 = await send("add", argv=["/bin/does-not-exist"], cwd=cwd)
    show("add nonexistent executable (should fail)", r3)
    assert r3["ok"] is False, "expected rejection of a nonexistent executable"

    await asyncio.sleep(0.5)  # let the worker loop pick up job 1

    r4 = await send("list")
    show("list", r4)

    r5 = await send("cancel")
    show("cancel currently-running job", r5)

    await asyncio.sleep(0.5)

    r6 = await send("list")
    show("list after cancel", r6)

    queued_ids = [j["id"] for j in r6["data"]["queue"]]
    if queued_ids:
        target = queued_ids[0]
        r7 = await send("rm", id=target)
        show(f"rm job {target}", r7)

    r8 = await send("list")
    show("final list", r8)

    print("\nsmoke test finished -- eyeball the output above for correctness")


if __name__ == "__main__":
    asyncio.run(main())
