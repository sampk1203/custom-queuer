"""Phase 3B: `qur rerun` over fake_ssh with a real worker daemon."""

import json
import os
import subprocess

import pytest
from typer.testing import CliRunner

from queuer import hostcfg
from queuer.cli import app
from queuer.remote_rerun import _chain_ids, carried_options, parse_args

runner = CliRunner()


@pytest.fixture
def worker(fake_ssh):
    paths = fake_ssh.add_worker("vm3")
    fake_ssh.start_daemon("vm3")
    cfg = hostcfg.load()
    cfg["hosts"]["vm3"] = {"ssh": "vm3", "jobs_dir": str(paths["QUEUER_JOBS_DIR"])}
    hostcfg.save(cfg)
    return paths


def _wj(*argv):
    out = subprocess.run(["ssh", "--", "vm3", "qur " + " ".join(argv)], capture_output=True, text=True, check=True).stdout
    return json.loads(out)


def _first_run(tmp_path, *extra):
    folder = tmp_path / "job"
    folder.mkdir()
    (folder / "in.lammps").write_text("old\n")
    r = runner.invoke(app, ["run", "vm3", str(folder), *extra, "--", "true"])
    assert r.exit_code == 0, r.output
    runner.invoke(app, ["-H", "vm3", "wait", "1"])
    return _wj("show", "1", "--json")


def test_parse_splits_own_forwarded_and_cmd():
    s = parse_args(["vm3:42", "--replace", "a=b", "--retries", "2", "--force", "--", "lmp", "-in", "x"])
    assert s["ref"] == "vm3:42" and s["replace"] == ["a=b"] and s["forward"] == ["--retries", "2"]
    assert s["force"] and s["cmd"] == ["lmp", "-in", "x"]


@pytest.mark.parametrize("bad", [["vm3:1", "--extra", "x"], ["vm3:1", "--each", "a:1"], ["vm3:1", "--rerun-of", "3"]])
def test_parse_rejects(bad):
    from typer import BadParameter
    with pytest.raises(BadParameter):
        parse_args(bad)


def test_chain_ids_and_carried_options_skip_captured_env_and_secrets():
    assert _chain_ids(["--depends-on", "3,5-6", "--after-any=9"]) == {3, 5, 6, 9}
    job = {"channel": 2, "timeout_secs": 60, "max_retries": 1, "on_fail": "pause",
           "env_extra": {"MYVAR": "1", "API_KEY": "s", "PATH": "/x", "CUDA_HOME": "/c", "QUR_ENV_PASSTHROUGH": "CUDA_HOME"}}
    opts, secret = carried_options(job, ["--timeout", "5"])
    assert "--timeout" not in opts and "--channel" in opts and "--on-fail" in opts
    assert ["--env", "MYVAR=1"] == opts[-2:] and secret == ["API_KEY"]
    assert not any("PATH" in o or "CUDA_HOME" in o for o in opts)


def test_rerun_replaces_file_backs_up_and_queues_in_same_folder(worker, tmp_path):
    job = _first_run(tmp_path)
    new = tmp_path / "in.lammps"
    new.write_text("new\n")
    r = runner.invoke(app, ["rerun", "vm3:1", "--replace", str(new), "--", "cat", "in.lammps"])
    assert r.exit_code == 0, r.output
    job2 = _wj("show", "2", "--json")
    assert job2["job_dir"] == job["job_dir"] and job2["parent_id"] == 1 and job2["origin"] == "rerun"
    assert open(os.path.join(job["job_dir"], "in.lammps")).read() == "new\n"
    prev = os.path.join(job["job_dir"], ".qur-prev")
    assert any(open(os.path.join(d, "in.lammps")).read() == "old\n" for d, _, f in os.walk(prev) if "in.lammps" in f)


def test_rerun_without_files_reuses_original_command(worker, tmp_path):
    _first_run(tmp_path)
    r = runner.invoke(app, ["rerun", "vm3:1"])
    assert r.exit_code == 0, r.output
    assert json.loads(_wj("show", "2", "--json")["raw_cmd"]) == ["true"]


def test_refused_while_folder_in_use_but_chaining_allowed(worker, tmp_path):
    folder = tmp_path / "j"
    folder.mkdir()
    assert runner.invoke(app, ["run", "vm3", str(folder), "--", "sleep", "30"]).exit_code == 0
    assert runner.invoke(app, ["rerun", "vm3:1"]).exit_code == 6
    f = tmp_path / "x"
    f.write_text("x")
    assert runner.invoke(app, ["rerun", "vm3:1", "--depends-on", "1", "--replace", str(f)]).exit_code == 6
    assert runner.invoke(app, ["rerun", "vm3:1", "--depends-on", "1"]).exit_code == 0
    runner.invoke(app, ["-H", "vm3", "cancel", "1,2"])


def test_replace_path_escape_and_key_files_rejected(worker, tmp_path):
    _first_run(tmp_path)
    f = tmp_path / "a"
    f.write_text("x")
    assert runner.invoke(app, ["rerun", "vm3:1", "--replace", f"{f}=../escape"]).exit_code != 0
    k = tmp_path / "id_rsa"
    k.write_text("secret")
    assert runner.invoke(app, ["rerun", "vm3:1", "--replace", str(k)]).exit_code != 0


def test_edit_round_trip_with_fake_editor(worker, tmp_path, monkeypatch):
    job = _first_run(tmp_path)
    ed = tmp_path / "ed.sh"
    ed.write_text('#!/bin/sh\necho edited > "$1"\n')
    ed.chmod(0o755)
    monkeypatch.setenv("VISUAL", str(ed))
    r = runner.invoke(app, ["rerun", "vm3:1", "--edit", "in.lammps"])
    assert r.exit_code == 0, r.output
    assert open(os.path.join(job["job_dir"], "in.lammps")).read() == "edited\n"
    ed.write_text("#!/bin/sh\ntrue\n")
    r = runner.invoke(app, ["rerun", "vm3:2", "--edit", "in.lammps"])
    assert r.exit_code != 0 and "nothing to rerun" in r.output


def test_environment_comes_from_worker_not_host(worker, tmp_path, monkeypatch):
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "HOST_VALUE")
    _first_run(tmp_path, "--setup", "export CUDA_VISIBLE_DEVICES=WORKER_VALUE")
    r = runner.invoke(app, ["rerun", "vm3:1", "--setup", "export CUDA_VISIBLE_DEVICES=WORKER_VALUE2"])
    assert r.exit_code == 0, r.output
    env = _wj("show", "2", "--json")["env_extra"]
    env = env if isinstance(env, dict) else json.loads(env)
    assert env["CUDA_VISIBLE_DEVICES"] == "WORKER_VALUE2"


def test_dry_run_changes_nothing(worker, tmp_path):
    _first_run(tmp_path)
    f = tmp_path / "in.lammps"
    f.write_text("zzz")
    r = runner.invoke(app, ["rerun", "vm3:1", "--replace", str(f), "--dry-run"])
    assert r.exit_code == 0 and "would replace: in.lammps" in r.output
    assert len(_wj("list", "--json")) == 1


def test_resume_does_not_duplicate(worker, tmp_path):
    _first_run(tmp_path)
    r = runner.invoke(app, ["rerun", "vm3:1"])
    key = [ln.split()[-1] for ln in r.output.splitlines() if ln.startswith("run key")][0]
    r = runner.invoke(app, ["rerun", "--resume", key])
    assert r.exit_code == 0 and "already queued" in r.output
    assert len(_wj("history", "--json")) == 2


# --- P6: a malformed recorded command is an error message, not a traceback -------------------------------------


def test_rerun_with_an_unreadable_recorded_command_says_so(worker, monkeypatch, tmp_path):
    from queuer import remote_rerun

    monkeypatch.setattr(remote_rerun, "_show", lambda host, job_id: {"job_dir": str(tmp_path), "raw_cmd": "not json {"})
    monkeypatch.setattr(remote_rerun, "folder_users", lambda host, job: [])
    r = runner.invoke(app, ["rerun", "vm3:1"])
    assert r.exit_code == 1 and "unreadable recorded command" in r.output and "Traceback" not in r.output
