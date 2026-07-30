"""Thin socket client for talking to queuerd.

Deliberately synchronous (plain `socket`, not asyncio) since the CLI is a
one-shot script per invocation -- no need to run an event loop just to
send one request and read one response.
"""

from __future__ import annotations

import json
import socket
from typing import Any

from queuer.daemon import _default_socket_path


class DaemonNotRunning(Exception):
    """Raised when queuerd's socket doesn't exist or refuses connection."""


class DaemonError(Exception):
    """Raised when the daemon responded but reported ok=false."""


def send(cmd: str, **args: Any) -> Any:
    """Send a request to queuerd and return its `data` payload.

    Raises DaemonNotRunning if the daemon isn't reachable, DaemonError if
    it responded with an application-level error.
    """
    socket_path = _default_socket_path()
    if not socket_path.exists():
        raise DaemonNotRunning(
            "queuerd is not running -- start it with: systemctl --user start queuerd"
        )

    req = {"cmd": cmd, "args": args}
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.connect(str(socket_path))
    except OSError as e:
        raise DaemonNotRunning(f"could not connect to queuerd: {e}") from e

    try:
        sock.sendall((json.dumps(req) + "\n").encode())
        sock.shutdown(socket.SHUT_WR)
        chunks: list[bytes] = []
        while True:
            chunk = sock.recv(65536)
            if not chunk:
                break
            chunks.append(chunk)
    finally:
        sock.close()

    if not chunks:
        raise DaemonNotRunning("queuerd closed the connection without responding")

    resp = json.loads(b"".join(chunks).decode())
    if not resp.get("ok"):
        raise DaemonError(resp.get("error", "unknown error"))
    return resp.get("data")
