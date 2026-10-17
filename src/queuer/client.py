"""Thin socket client for talking to queuerd.

Deliberately synchronous (plain `socket`, not asyncio) since the CLI is a
one-shot script per invocation -- no need to run an event loop just to
send one request and read one response.
"""

from __future__ import annotations

import json
import os
import socket
from pathlib import Path
from typing import Any

from queuer.paths import _default_socket_path

REQUEST_TIMEOUT_SECS = 60
RUN_USER_DIR = Path("/run/user")  # L6: where systemd's XDG_RUNTIME_DIR lives


class DaemonNotRunning(Exception):
    """Raised when queuerd's socket doesn't exist or refuses connection."""


class DaemonError(Exception):
    """Raised when the daemon responded but reported ok=false."""


def _socket_path() -> Path:
    """The daemon's socket. Normally `_default_socket_path()`. L6: in cron, ssh without
    pam_systemd or `sudo -u` XDG_RUNTIME_DIR is unset, so that path is the /tmp fallback while
    the systemd service listens in /run/user/<uid>: when nothing is at the fallback but the
    systemd socket exists, use it. Never when QUEUER_SOCKET_PATH or XDG_RUNTIME_DIR is set."""
    path = _default_socket_path()
    if os.environ.get("QUEUER_SOCKET_PATH") or os.environ.get("XDG_RUNTIME_DIR"):
        return path
    alt = RUN_USER_DIR / str(os.getuid()) / "queuer.sock"
    try:
        if not path.exists() and alt.exists():
            return alt
    except OSError:
        pass
    return path


def send(cmd: str, **args: Any) -> Any:
    """Send a request to queuerd and return its `data` payload.

    Raises DaemonNotRunning if the daemon isn't reachable, DaemonError if
    it responded with an application-level error.
    """
    socket_path = _socket_path()
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
        try:
            sock.sendall((json.dumps(req) + "\n").encode())
            sock.shutdown(socket.SHUT_WR)
        except (BrokenPipeError, ConnectionResetError):
            pass  # H4: the daemon answered early (e.g. "request too large") and closed: read its reply below
        chunks: list[bytes] = []
        while True:
            try:
                chunk = sock.recv(65536)
            except ConnectionResetError:
                break
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
