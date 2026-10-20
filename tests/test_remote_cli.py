"""Phase 2: `qur hosts`, `qur -H`, `HOST:ID` references, the fake_ssh fixture (no real network)."""

import json

import pytest
import typer
from typer.testing import CliRunner

from queuer import hostcfg, remote
from queuer.cli import app
from queuer.remote_cli import rewrite_refs, split_ref

runner = CliRunner()
KNOWN = ["vm3", "vm4"]


def _add_host(name="vm3", **kw):
    cfg = hostcfg.load()
    cfg["hosts"][name] = {"ssh": name, **kw}
    hostcfg.save(cfg)


# --- HOST:ID parsing ---------------------------------------------------------

def test_split_ref_needs_known_host_and_whole_match():
    assert split_ref("vm3:42", KNOWN) == ("vm3", "42")
    assert split_ref("vm3:31-33", KNOWN) == ("vm3", "31-33")
    assert split_ref("vm3:12,14", KNOWN) == ("vm3", "12,14")
    assert split_ref("other:42", KNOWN) is None
    assert split_ref("vm3:42x", KNOWN) is None
    assert split_ref("a:1", KNOWN) is None


def test_rewrite_refs_basic_and_ranges():
    host, out, bare = rewrite_refs(["vm3:1", "vm3:5-7", "--json"], KNOWN)
    assert (host, out, bare) == ("vm3", ["1", "5-7", "--json"], False)


def test_rewrite_refs_two_hosts_rejected():
    with pytest.raises(typer.BadParameter):
        rewrite_refs(["vm3:1", "vm4:2"], KNOWN)


def test_rewrite_refs_leaves_after_double_dash_and_option_values():
    host, out, _ = rewrite_refs(["--lines", "vm3:5", "--", "vm3:9"], KNOWN, {"--lines"})
    assert host is None and out == ["--lines", "vm3:5", "--", "vm3:9"]


def test_rewrite_refs_flags_bare_local_ids():
    host, out, bare = rewrite_refs(["vm3:1", "2"], KNOWN)
    assert host == "vm3" and bare is True


# --- qur hosts ---------------------------------------------------------------

def test_hosts_add_list_edit_remove(fake_ssh):
    r = runner.invoke(app, ["hosts", "add", "vm3", "--ssh", "vm3", "--setup", "true", "--ssh-option", "-i", "--ssh-option", "~/k"])
    assert r.exit_code == 0, r.output
    assert hostcfg.get("vm3")["setup"] == "true"
    assert hostcfg.get("vm3")["ssh_options"] == ["-i", "~/k"]
    assert runner.invoke(app, ["hosts", "add", "vm3", "--ssh", "x"]).exit_code == 1  # exists
    rows = json.loads(runner.invoke(app, ["hosts", "list", "--json"]).output)
    assert rows[0]["name"] == "vm3" and rows[0]["last_seen"] is None
    assert "vm3" in runner.invoke(app, ["hosts"]).output
    assert runner.invoke(app, ["hosts", "edit", "vm3", "--setup", "", "--disabled"]).exit_code == 0
    h = hostcfg.get("vm3")
    assert h["setup"] is None and h["enabled"] is False
    assert runner.invoke(app, ["hosts", "remove", "vm3"]).exit_code == 0
    assert hostcfg.names() == []


def test_hosts_add_rejects_bad_name_and_target(fake_ssh):
    assert runner.invoke(app, ["hosts", "add", "bad name", "--ssh", "vm3"]).exit_code == 1
    assert runner.invoke(app, ["hosts", "add", "vm3", "--ssh", "-oProxyCommand=x"]).exit_code == 1


# --- qur hosts check ----------------------------------------------------------

def test_hosts_check_against_fake_worker(fake_ssh):
    fake_ssh.add_worker("vm3")
    _add_host("vm3")
    r = runner.invoke(app, ["hosts", "check", "vm3", "--expect", "bash", "--json"])
    steps = {s["step"]: s for s in json.loads(r.output)["hosts"]["vm3"]}
    assert steps["ssh reachable"]["ok"] and steps["remote qur found"]["ok"] and steps["protocol compatible"]["ok"]
    assert steps["jobs dir writable"]["ok"]
    assert steps["environment"]["ok"] and "bash:" in steps["environment"]["detail"]
    assert not steps["daemon running"]["ok"]  # no daemon in the fake worker
    assert r.exit_code == 1


def test_hosts_check_missing_expected_command_fails(fake_ssh):
    fake_ssh.add_worker("vm3")
    _add_host("vm3")
    steps = json.loads(runner.invoke(app, ["hosts", "check", "vm3", "--expect", "no-such-cmd-xyz", "--json"]).output)["hosts"]["vm3"]
    env = next(s for s in steps if s["step"] == "environment")
    assert not env["ok"] and "MISSING" in env["detail"]


def test_hosts_check_setup_runs_before_expect(fake_ssh):
    fake_ssh.add_worker("vm3")
    _add_host("vm3", setup="export QUR_T=1")
    steps = json.loads(runner.invoke(app, ["hosts", "check", "vm3", "--json"]).output)["hosts"]["vm3"]
    assert next(s for s in steps if s["step"] == "setup works")["ok"]


def test_hosts_check_failing_setup(fake_ssh):
    fake_ssh.add_worker("vm3")
    _add_host("vm3", setup="false")
    steps = json.loads(runner.invoke(app, ["hosts", "check", "vm3", "--json"]).output)["hosts"]["vm3"]
    assert not next(s for s in steps if s["step"] == "setup works")["ok"]


@pytest.mark.parametrize("mode,needle", [("unreachable", "refused"), ("denied", "refused")])
def test_hosts_check_error_classes(fake_ssh, mode, needle):
    fake_ssh.add_worker("vm3")
    fake_ssh.set_mode("vm3", mode)
    _add_host("vm3")
    steps = json.loads(runner.invoke(app, ["hosts", "check", "vm3", "--json"]).output)["hosts"]["vm3"]
    assert len(steps) == 1 and not steps[0]["ok"] and needle in steps[0]["detail"].lower()


def test_hosts_check_hints_at_the_unknown_host_key(fake_ssh):
    fake_ssh.add_worker("vm3")
    fake_ssh.set_mode("vm3", "hostkey")
    _add_host("vm3")
    steps = json.loads(runner.invoke(app, ["hosts", "check", "vm3", "--json"]).output)["hosts"]["vm3"]
    assert len(steps) == 1 and not steps[0]["ok"]
    assert "host key" in steps[0]["detail"].lower() and "ssh vm3" in steps[0]["detail"]


def test_transport_error_classes(fake_ssh):
    fake_ssh.add_worker("vm3")
    host = {"ssh": "vm3"}
    fake_ssh.set_mode("vm3", "unreachable")
    with pytest.raises(remote.HostUnreachable):
        remote.ssh_run(host, ["true"], raw=True)
    fake_ssh.set_mode("vm3", "denied")
    with pytest.raises(remote.AuthFailed):
        remote.ssh_run(host, ["true"], raw=True)
    with pytest.raises(remote.HostUnreachable):
        remote.ssh_run({"ssh": "nowhere"}, ["true"], raw=True)


def test_protocol_mismatch(fake_ssh, monkeypatch):
    with pytest.raises(remote.ProtocolMismatch):
        remote.check_protocol({"protocol": 99})


# --- -H passthrough and HOST:ID ------------------------------------------------

def test_dash_h_runs_command_on_worker(fake_ssh):
    fake_ssh.add_worker("vm3")
    _add_host("vm3")
    r = runner.invoke(app, ["-H", "vm3", "remote", "info", "--json"])
    assert r.exit_code == 0, r.output
    # passthrough bypasses CliRunner capture (ssh writes to the real fd), so assert on the call log
    assert any(c.startswith("vm3 :: ") and "qur remote info --json" in c for c in fake_ssh.calls)


def test_dash_h_passes_exit_code(fake_ssh):
    fake_ssh.add_worker("vm3")
    _add_host("vm3")
    assert runner.invoke(app, ["-H", "vm3", "show", "999"]).exit_code != 0


def test_dash_h_unknown_host_is_usage_error(fake_ssh):
    r = runner.invoke(app, ["-H", "nope", "list"])
    assert r.exit_code == 2


def test_dash_h_not_allowed_for_hosts(fake_ssh):
    assert runner.invoke(app, ["-H", "vm3", "hosts", "list"]).exit_code == 2


def test_qur_host_env(fake_ssh, monkeypatch):
    fake_ssh.add_worker("vm3")
    _add_host("vm3")
    monkeypatch.setenv("QUR_HOST", "vm3")
    runner.invoke(app, ["list", "--json"])  # forwarded; the fake worker has no daemon, so the exit code is not the point
    assert any("qur list" in c for c in fake_ssh.calls)
    # `qur remote` is host-side too: QUR_HOST never forwards it (only an explicit -H does)
    n = len(fake_ssh.calls)
    runner.invoke(app, ["remote", "info", "--json"])
    assert not any("qur remote info" in c for c in fake_ssh.calls[n:])
    assert runner.invoke(app, ["-H", "vm3", "remote", "info", "--json"]).exit_code == 0
    assert any("qur remote info" in c for c in fake_ssh.calls[n:])
    # `qur hosts` stays local even with QUR_HOST set
    n = len(fake_ssh.calls)
    assert runner.invoke(app, ["hosts", "list", "--json"]).exit_code == 0
    assert len(fake_ssh.calls) == n


def test_host_id_reference_rewritten_to_dash_h(fake_ssh):
    fake_ssh.add_worker("vm3")
    _add_host("vm3")
    runner.invoke(app, ["show", "vm3:42"])
    assert any(c.startswith("vm3 :: ") and "qur show 42" in c for c in fake_ssh.calls)


def test_references_to_two_hosts_rejected(fake_ssh):
    _add_host("vm3")
    _add_host("vm4")
    assert runner.invoke(app, ["show", "vm3:1", "vm4:2"]).exit_code == 2


def test_add_arguments_never_rewritten(fake_ssh):
    _add_host("vm3")
    runner.invoke(app, ["add", "--dry-run", "--", "echo", "vm3:1"])
    assert fake_ssh.calls == []


def test_local_ids_unchanged_without_hosts(fake_ssh):
    runner.invoke(app, ["show", "1"])
    assert fake_ssh.calls == []
