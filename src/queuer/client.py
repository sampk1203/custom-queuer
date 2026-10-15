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

REQUEST_TIMEOUT_SECS = 60


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
    try:
        present = socket_path.exists()
    except OSError as e:
        raise DaemonNotRunning(f"cannot access {socket_path}: {e.strerror}") from e
    if not present:
        raise DaemonNotRunning(
            "queuerd is not running -- start it with: systemctl --user start queuerd"
        )

    req = {"cmd": cmd, "args": args}
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(REQUEST_TIMEOUT_SECS)
    try:
        sock.connect(str(socket_path))
    except PermissionError as e:
        sock.close()
        raise DaemonNotRunning(
            f"permission denied on {socket_path}: it must belong to you with mode 0600 "
            "(is queuerd running as another user?)"
        ) from e
    except ConnectionRefusedError as e:
        sock.close()
        raise DaemonNotRunning(
            f"{socket_path} exists but nothing answers (stale socket?) -- "
            "try: systemctl --user restart queuerd"
        ) from e
    except OSError as e:
        sock.close()
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
    except socket.timeout as e:
        raise DaemonNotRunning(f"queuerd did not answer within {REQUEST_TIMEOUT_SECS}s") from e
    finally:
        sock.close()

    if not chunks:
        raise DaemonNotRunning("queuerd closed the connection without responding")

    try:
        resp = json.loads(b"".join(chunks).decode())
    except ValueError:
        raise DaemonNotRunning("queuerd sent a garbled reply") from None
    if not isinstance(resp, dict) or not resp.get("ok"):
        if not isinstance(resp, dict):
            raise DaemonError("unexpected reply from queuerd")
        raise DaemonError(resp.get("error", "unknown error"))
    return resp.get("data")
