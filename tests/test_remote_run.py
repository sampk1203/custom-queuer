"""Phase 3: `qur run` end to end over fake_ssh with a real worker daemon."""

import json
import os
import tarfile

import pytest
from typer.testing import CliRunner

from queuer import hostcfg
from queuer.cli import app
from queuer.remote_run import build_payload, parse_args

runner = CliRunner()


@pytest.fixture
def worker(fake_ssh):
    paths = fake_ssh.add_worker("vm3")
    fake_ssh.start_daemon("vm3")
    cfg = hostcfg.load()
    cfg["hosts"]["vm3"] = {"ssh": "vm3", "jobs_dir": str(paths["QUEUER_JOBS_DIR"])}
    hostcfg.save(cfg)
    return paths


def _worker_json(*argv):
    """Run `qur <argv>` on the worker and parse its JSON."""
    import subprocess
    out = subprocess.run(["ssh", "--", "vm3", "qur " + " ".join(argv)], capture_output=True, text=True, check=True).stdout
    return json.loads(out)


# --- argument parsing -------------------------------------------------------

def test_parse_splits_own_forwarded_and_cmd():
    s = parse_args(["vm3", "./job", "--channel", "2", "--venv", "~/v", "--link", "a=/b", "--pull", "--retries=1", "--", "lmp", "-in", "x"])
    assert (s["host"], s["folder"]) == ("vm3", "./job")
    assert s["forward"] == ["--channel", "2", "--retries=1"]
    assert s["venv"] == "~/v" and s["link"] == ["a=/b"] and s["pull"] is True
    assert s["cmd"] == ["lmp", "-in", "x"]


@pytest.mark.parametrize("bad", ["--each", "--array", "--run-key", "--json"])
def test_parse_rejects(bad):
    import typer
    with pytest.raises(typer.BadParameter):
        parse_args(["vm3", "./j", bad, "x", "--", "true"])


def test_parse_needs_command_and_resume_key():
    import typer
    with pytest.raises(typer.BadParameter):
        parse_args(["vm3", "./j"])
    k = "a" * 32
    assert parse_args(["--resume", k])["resume_key"] == k


# --- payload ----------------------------------------------------------------

def test_payload_skips_keys_honours_qurignore_and_extras(tmp_path):
    f = tmp_path / "job"
    (f / ".ssh").mkdir(parents=True)
    (f / ".ssh" / "id_rsa").write_text("k")
    (f / "id_ed25519").write_text("k")
    (f / "scratch.dump").write_text("x")
    (f / "in.x").write_text("hi")
    (f / ".qurignore").write_text("# c\n*.dump\n")
    extra = tmp_path / "pot.eam"
    extra.write_text("p")
    out = tmp_path / "p.tgz"
    stats = build_payload(f, [str(extra), f"{extra}:sub/pot2"], out)
    names = set(tarfile.open(out).getnames())
    assert {"in.x", ".qurignore", "pot.eam", "sub/pot2"} <= names
    assert not any("id_" in n or ".ssh" in n or n.endswith(".dump") for n in names)
    assert stats["ignored"] == 1 and stats["skipped_secrets"]
    assert oct(os.stat(out).st_mode & 0o777) == "0o600"


def test_payload_duplicate_destination_rejected(tmp_path):
    f = tmp_path / "job"
    f.mkdir()
    (f / "a").write_text("1")
    e = tmp_path / "a"
    e.write_text("2")
    with pytest.raises(ValueError):
        build_payload(f, [str(e)], tmp_path / "p.tgz")


# --- end to end -------------------------------------------------------------

def test_run_sends_folder_and_queues_job(worker, tmp_path):
    folder = tmp_path / "myjob"
    folder.mkdir()
    (folder / "in.txt").write_text("data")
    r = runner.invoke(app, ["run", "vm3", str(folder), "--", "ls", "in.txt"])
    assert r.exit_code == 0, r.output
    job = _worker_json("show", "1", "--json")
    assert job["job_dir"].startswith(str(worker["QUEUER_JOBS_DIR"]) + "/myjob-")
    assert os.path.exists(os.path.join(job["job_dir"], "in.txt"))
    assert job["submitted_from"]
    assert job["cwd"] == job["job_dir"]


def test_run_setup_variable_in_job_env_and_qur_env_sh(worker, tmp_path):
    folder = tmp_path / "j"
    folder.mkdir()
    (folder / "qur-env.sh").write_text("export CUDA_FROM_FOLDER=yes\n")
    r = runner.invoke(app, ["run", "vm3", str(folder), "--setup", "export CUDA_FROM_SETUP=1", "--", "true"])
    assert r.exit_code == 0, r.output
    env = _worker_json("show", "1", "--json")["env_extra"]
    env = env if isinstance(env, dict) else json.loads(env)
    assert env.get("CUDA_FROM_SETUP") == "1" and env.get("CUDA_FROM_FOLDER") == "yes"


def test_failing_setup_queues_nothing(worker, tmp_path):
    folder = tmp_path / "j"
    folder.mkdir()
    r = runner.invoke(app, ["run", "vm3", str(folder), "--setup", "false", "--", "true"])
    assert r.exit_code == 1
    assert "setup failed" in (r.output + (r.stderr or "")).lower()
    assert runner.invoke(app, ["-H", "vm3", "show", "1"]).exit_code != 0  # no job 1 on the worker


def test_missing_require_aborts_before_queueing(worker, tmp_path):
    folder = tmp_path / "j"
    folder.mkdir()
    r = runner.invoke(app, ["run", "vm3", str(folder), "--require", "/no/such/path", "--", "true"])
    assert r.exit_code == 1


def test_link_and_dry_run(worker, tmp_path):
    folder = tmp_path / "j"
    folder.mkdir()
    model = tmp_path / "model.pt"
    model.write_text("w")
    r = runner.invoke(app, ["run", "vm3", str(folder), "--link", f"m.pt={model}", "--", "true"])
    assert r.exit_code == 0, r.output
    job = _worker_json("show", "1", "--json")
    assert os.path.islink(os.path.join(job["job_dir"], "m.pt"))
    before = len(os.listdir(worker["QUEUER_JOBS_DIR"]))
    r = runner.invoke(app, ["run", "vm3", str(folder), "--dry-run", "--", "true"])
    assert r.exit_code == 0, r.output
    assert len(os.listdir(worker["QUEUER_JOBS_DIR"])) == before


def test_resume_does_not_duplicate(worker, tmp_path):
    from queuer.remote_run import _conn
    folder = tmp_path / "j"
    folder.mkdir()
    assert runner.invoke(app, ["run", "vm3", str(folder), "--", "true"]).exit_code == 0
    conn = _conn()
    key = conn.execute("SELECT run_key FROM remote_runs").fetchone()[0]
    conn.execute("UPDATE remote_runs SET state='sent' WHERE run_key=?", (key,))  # pretend the add step never got recorded
    conn.commit()
    conn.close()
    r = runner.invoke(app, ["run", "--resume", key])
    assert r.exit_code == 0, r.output
    assert "already queued" in r.output
    assert len(os.listdir(worker["QUEUER_JOBS_DIR"])) == 1


# --- P0-3 gap tests (see 6.todo); the P1-3, P1-4a and P1-4b ones lost their xfail marker with the fix ------

def test_wait_for_reports_an_ssh_failure_instead_of_a_traceback(monkeypatch, capsys):
    from queuer import remote, remote_run

    def denied(*a, **k):
        raise remote.AuthFailed("vm3: Permission denied (publickey).")

    monkeypatch.setattr(remote, "ssh_run", denied)
    assert remote_run.wait_for({"ssh": "vm3"}, 7) == 1
    captured = capsys.readouterr()
    assert "Permission denied" in captured.out + captured.err


def test_run_survives_a_login_shell_that_prints_before_the_json(worker, fake_ssh, tmp_path):
    fake_ssh.set_flag("vm3", "banner", "Welcome to vm3. Last login: yesterday\n")
    folder = tmp_path / "j"
    folder.mkdir()
    (folder / "in.txt").write_text("data")
    r = runner.invoke(app, ["run", "vm3", str(folder), "--", "true"])
    assert r.exit_code == 0, r.output
    assert "unexpected answer" not in r.output
    assert _worker_json("show", "1", "--json")["id"] == 1


def test_remote_command_works_under_a_non_posix_login_shell(worker, fake_ssh):
    from queuer import remote

    fake_ssh.set_flag("vm3", "fishlike")
    code, out, err = remote.ssh_run(hostcfg.get("vm3"), ["remote", "info", "--json"])
    assert code == 0, err
    assert json.loads(out)["protocol"]
