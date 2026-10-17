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


def test_early_reply_of_a_daemon_that_closes_without_reading_is_still_shown(sock_path):
    # H4: "request too large" is sent before the daemon has read the whole request; the
    # client's send then breaks, but the reply must still reach the user
    import threading

    srv = socket.socket(socket.AF_UNIX)
    srv.bind(str(sock_path))
    srv.listen(1)

    def serve():
        c, _ = srv.accept()
        c.sendall(b'{"ok": false, "error": "request too large (over 16 MiB)"}\n')
        c.close()  # nothing of the request was read

    t = threading.Thread(target=serve)
    t.start()
    try:
        with pytest.raises(client.DaemonError, match="request too large"):
            client.send("add", blob="x" * (8 * 1024 * 1024))
    finally:
        t.join()
        srv.close()


# ---------------------------------------------------------------------------
# 5.todo L6: no XDG_RUNTIME_DIR (cron, ssh without pam_systemd, sudo -u)
# ---------------------------------------------------------------------------


@pytest.fixture
def no_xdg(tmp_path, monkeypatch):
    """No overrides at all: the client works out the socket path itself."""
    monkeypatch.delenv("QUEUER_SOCKET_PATH", raising=False)
    monkeypatch.delenv("XDG_RUNTIME_DIR", raising=False)
    run_user = tmp_path / "run-user"
    (run_user / str(os.getuid())).mkdir(parents=True)
    monkeypatch.setattr(client, "RUN_USER_DIR", run_user)
    monkeypatch.setattr("queuer.paths._fallback_socket_dir", lambda: tmp_path / "fallback")
    return run_user / str(os.getuid()) / "queuer.sock"


def test_client_uses_the_systemd_socket_when_the_fallback_has_none(no_xdg):
    no_xdg.write_text("")
    assert client._socket_path() == no_xdg


def test_client_prefers_the_fallback_socket_when_it_exists(no_xdg, tmp_path):
    no_xdg.write_text("")
    (tmp_path / "fallback").mkdir()
    (tmp_path / "fallback" / "queuer.sock").write_text("")
    assert client._socket_path() == tmp_path / "fallback" / "queuer.sock"


def test_client_keeps_the_fallback_path_when_neither_exists(no_xdg, tmp_path):
    assert client._socket_path() == tmp_path / "fallback" / "queuer.sock"


def test_explicit_settings_are_never_second_guessed(no_xdg, tmp_path, monkeypatch):
    no_xdg.write_text("")
    monkeypatch.setenv("QUEUER_SOCKET_PATH", str(tmp_path / "mine.sock"))
    assert client._socket_path() == tmp_path / "mine.sock"
    monkeypatch.delenv("QUEUER_SOCKET_PATH")
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path / "xdg"))
    assert client._socket_path() == tmp_path / "xdg" / "queuer.sock"
