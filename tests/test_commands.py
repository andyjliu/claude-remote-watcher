"""Control commands (watcher/commands.py): dry-run default, live actions, source routing, the resubmit guard."""
import subprocess
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from watcher import commands as cm
from watcher import config, fixes
from watcher import hubdata as hd
from watcher.state import State

TZ = ZoneInfo("America/New_York")
SRC, OTHER_SRC, SPEC = "s" * 32, "o" * 32, "a" * 32


def prop(t, v):
    if t == "title":
        return {"type": "title", "title": [{"plain_text": v}]}
    if t == "rich_text":
        return {"type": "rich_text", "rich_text": [{"plain_text": v}]}
    if t == "relation":
        return {"type": "relation", "relation": [{"id": x} for x in v]}
    return {"type": t, t: ({"name": v} if t == "select" else v)}


class FakeNotion:
    def __init__(self, dbs):
        self.dbs, self.updates = dbs, []

    def query(self, db, filter=None, sorts=None, limit=None):
        rows = self.dbs.get(db, [])
        if filter and "select" in filter:
            want = filter["select"]["equals"]
            rows = [r for r in rows if (r["properties"].get(filter["property"], {}).get("select") or {}).get("name") == want]
        return rows

    def update(self, page_id, props):
        self.updates.append((page_id, props))
        for rows in self.dbs.values():
            for r in rows:
                if r["id"] == page_id:
                    for k, v in props.items():
                        t = next(iter(v))
                        r["properties"][k] = {"type": t, **v}
        return {}


def command(action, dry=False, source=SRC, args=""):
    return {"id": f"cmd-{action}", "properties": {
        "Name": prop("title", action), "Action": prop("select", action), "Status": prop("select", "pending"),
        "Dry run": prop("checkbox", dry), "Source": prop("relation", [source] if source else []),
        "Spec": prop("relation", [SPEC]), "Args": prop("rich_text", args)}}


@pytest.fixture
def env(tmp_path, monkeypatch):
    repo = tmp_path / "advsel"
    (repo / ".watcher" / "research").mkdir(parents=True)
    monkeypatch.setattr(config, "load", lambda d: {"_state": str(Path(d) / ".watcher")})
    spec_page = {"id": SPEC, "properties": {"Name": prop("title", "flawed benchmarks"), "Spec ID": prop("rich_text", "advsel"),
                                            "State": prop("select", "running")}}
    mapping = {"id": "map1", "properties": {"Spec": prop("relation", [SPEC]), "Source": prop("relation", [SRC]),
                                            "Repo dir": prop("rich_text", str(repo))}}
    notion = FakeNotion({"specs": [spec_page], "mappings": [mapping], "commands": []})
    st = State(tmp_path / "hubstate")
    hub = SimpleNamespace(cfg={"hub": {}}, ids={"specs": "specs", "mappings": "mappings", "commands": "commands"},
                          notion=notion, state=st, tz=TZ, dry=False, quota=8, prefix="cw:", m={},
                          source_row=lambda: {"id": SRC})
    hub.m = st.meta.setdefault("hub", {})
    cmds = cm.Commands(hub)
    return SimpleNamespace(repo=repo, sd=repo / ".watcher", notion=notion, hub=hub, cmds=cmds, spec=spec_page)


def status_of(env, action):
    row = next(r for r in env.notion.dbs["commands"] if r["id"] == f"cmd-{action}")
    p = row["properties"]
    return p["Status"]["select"]["name"], "".join(x.get("text", {}).get("content", "") for x in p["Message"]["rich_text"])


def test_dry_run_is_the_default(env):
    env.notion.dbs["commands"] = [command("pause_loop"), command("stop_new_jobs", dry=True)]
    assert env.cmds.poll(force=True) == 2
    assert not (env.sd / "research" / "PAUSED").exists() and not (env.sd / "NO_NEW_JOBS").exists()
    st, msg = status_of(env, "pause_loop")
    assert st == "done" and msg.startswith("[dry run: hub.commands_live is off] would pause advsel")
    assert status_of(env, "stop_new_jobs")[1].startswith("[dry run: the row's Dry run box is on]")
    assert not any(pid == SPEC for pid, _ in env.notion.updates)      # spec State untouched


def test_live_pause_resume_and_new_jobs(env):
    env.cmds.live = True
    env.notion.dbs["commands"] = [command("pause_loop"), command("stop_new_jobs")]
    env.cmds.poll(force=True)
    assert (env.sd / "research" / "PAUSED").exists() and (env.sd / "NO_NEW_JOBS").exists()
    assert env.spec["properties"]["State"]["select"]["name"] == "no new jobs"
    env.notion.dbs["commands"] = [command("resume_loop"), command("allow_new_jobs")]
    env.cmds.poll(force=True)
    assert not (env.sd / "research" / "PAUSED").exists() and not (env.sd / "NO_NEW_JOBS").exists()
    assert status_of(env, "allow_new_jobs")[0] == "done"


def test_stop_and_kill_cancels_only_this_specs_jobs(env, monkeypatch):
    env.cmds.live = True
    def j(id, name, workdir, comment=""):
        return hd.Job(id=id, name=name, state="RUNNING", partition="general", gpus=4, cpus=8, mem_gb=64, comment=comment,
                      workdir=workdir, start=None, submit=None, limit=timedelta(hours=48), node="n1")
    jobs = [j("1", "o1", str(env.repo)), j("2", "eval", "/elsewhere", "cc:advsel"), j("3", "other", "/x/taskvec"),
            j("4", "cw:advsel", str(env.sd))]
    monkeypatch.setattr(cm.hd, "squeue_jobs", lambda user, tz: jobs)
    cancelled = []
    monkeypatch.setattr(cm.slurm, "run", lambda cmd, **kw: (cancelled.append(cmd[1]), subprocess.CompletedProcess(cmd, 0, "", ""))[1])
    env.notion.dbs["commands"] = [command("stop_and_kill")]
    env.cmds.poll(force=True)
    assert cancelled == ["1", "2"]                                    # not taskvec's job, not the watcher
    assert all((env.sd / f).exists() for f in ("STOP", "NO_NEW_JOBS", "research/PAUSED"))
    st, msg = status_of(env, "stop_and_kill")
    assert st == "done" and "cancel 2 job(s)" in msg


def test_rows_for_another_source_are_left_alone(env):
    env.notion.dbs["commands"] = [command("pause_loop", source=OTHER_SRC)]
    assert env.cmds.poll(force=True) == 0
    assert env.notion.updates == []


def test_queue_needs_gpus_and_starts_when_free(env, monkeypatch):
    env.cmds.live = True
    env.notion.dbs["commands"] = [command("queue", args='{"gpus": 4}')]
    env.cmds.poll(force=True)
    assert env.spec["properties"]["State"]["select"]["name"] == "queued"
    assert env.cmds.m["queued"]["advsel"]["gpus"] == 4
    started = []
    monkeypatch.setattr(env.cmds, "do_start", lambda spec, args, dry: started.append(spec["slug"]) or "started")
    env.cmds.start_queued({}, {}, gpus_used=6)                      # only 2 free
    assert started == []
    env.cmds.start_queued({}, {}, gpus_used=4)
    assert started == ["advsel"]


def test_bad_rows_fail_with_a_message(env):
    env.notion.dbs["commands"] = [command("queue", args='{"gpus": 99}'), command("summarize")]
    env.cmds.poll(force=True)
    assert status_of(env, "queue") == ("failed", "failed: GPU request 99 is outside 1..8")
    assert status_of(env, "summarize")[0] == "failed"


def test_resubmit_refused_while_new_jobs_are_stopped(tmp_path):
    st = State(tmp_path)
    (tmp_path / "NO_NEW_JOBS").write_text("x")
    with pytest.raises(fixes.FixError, match="new jobs are paused"):
        fixes.resubmit(st, {"id": "1", "submit_line": "sbatch run.sbatch", "workdir": str(tmp_path)})
