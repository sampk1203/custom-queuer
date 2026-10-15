import os
import socket

import pytest

from queuer import client


@pytest.fixture
def sock_path(tmp_path, monkeypatch):
    p = tmp_path / "q.sock"
    monkeypatch.setenv("QUEUER_SOCKET_PATH", str(p))
    return p


def test_missing_socket_says_not_running(sock_path):
    with pytest.raises(client.DaemonNotRunning, match="not running"):
        client.send("status")


def test_stale_socket_hint(sock_path):
    s = socket.socket(socket.AF_UNIX)
    s.bind(str(sock_path))
    s.close()  # file stays, nobody listens
    with pytest.raises(client.DaemonNotRunning, match="stale socket"):
        client.send("status")


@pytest.mark.skipif(os.getuid() == 0, reason="root ignores modes")
def test_permission_denied_hint(sock_path):
    s = socket.socket(socket.AF_UNIX)
    s.bind(str(sock_path))
    s.listen()
    os.chmod(sock_path, 0o000)
    try:
        with pytest.raises(client.DaemonNotRunning, match="permission denied"):
            client.send("status")
    finally:
        s.close()


def test_silent_daemon_times_out(sock_path, monkeypatch):
    monkeypatch.setattr(client, "REQUEST_TIMEOUT_SECS", 0.2)
    s = socket.socket(socket.AF_UNIX)
    s.bind(str(sock_path))
    s.listen()  # accepts at kernel level, never replies
    try:
        with pytest.raises(client.DaemonNotRunning, match="did not answer"):
            client.send("status")
    finally:
        s.close()


def test_garbled_reply_is_a_clean_error(sock_path):
    import threading

    srv = socket.socket(socket.AF_UNIX)
    srv.bind(str(sock_path))
    srv.listen(1)

    def serve():
        c, _ = srv.accept()
        c.recv(65536)
        c.sendall(b"not json\n")
        c.close()

    t = threading.Thread(target=serve)
    t.start()
    try:
        with pytest.raises(client.DaemonNotRunning, match="garbled"):
            client.send("status")
    finally:
        t.join()
        srv.close()
