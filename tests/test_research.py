"""Research mode: background turns, footer handling, wake reasons, control-file blocking, session resume,
and failure routing from Loop.handle."""
import json
import time
from pathlib import Path

import pytest

from watcher import loop as loop_mod
from watcher.research import Research, parse_wait
from watcher.state import State


class FakeSlack:
    def __init__(self):
        self.posts = []

    def post(self, text, thread_ts=None):
        self.posts.append(text); return "1.0"

    post_long = post


def _fake_claude(tmp_path: Path, result: str, rc: int = 0, session: str = "sess-1") -> str:
    """A `claude` that logs its argv and prints one result object."""
    out = json.dumps({"type": "result", "is_error": rc != 0, "session_id": session, "result": result})
    bin_ = tmp_path / "claude"
    bin_.write_text(f"#!/bin/sh\necho \"$@\" >> {tmp_path}/argv.log\ncat >/dev/null\ncat <<'EOF'\n{out}\nEOF\nexit {rc}\n")
    bin_.chmod(0o755)
    return str(bin_)


def _cfg(tmp_path: Path, result: str, **research) -> dict:
    target = tmp_path / "exp"
    target.mkdir(exist_ok=True)
    (target / "SPEC.md").write_text("spec v1\n")
    r = {"enabled": True, "prompt_files": ["SPEC.md"], "control_files": ["SPEC.md"], "min_window_min": 0, "working_gap_sec": 0}
    r.update(research)
    return {"_target": str(target), "_state": str(target / ".watcher"), "slurm": {"cluster": "t"},
            "claude": {"bin": _fake_claude(tmp_path, result), "tiers": {"research": {"model": "m", "budget_usd": 1}}},
            "research": r}


def _research(tmp_path, result, **kw):
    cfg = _cfg(tmp_path, result, **kw)
    st = State(Path(cfg["_state"]))
    return Research(cfg, st, FakeSlack(), "100"), st


def _run_one(r: Research) -> dict:
    """Start a due turn, wait for the fake claude to exit, collect it."""
    assert r.tick("table", float("inf"), True) is None and r.proc is not None
    r.proc.wait(10)
    return r.tick("table", float("inf"), True)


def test_parse_wait():
    now = time.time()
    until, jobs = parse_wait("WAITING 123456 123457_3", now, 15, 120)
    assert jobs == ["123456", "123457_3"] and abs(until - (now + 7200)) < 1
    until, jobs = parse_wait("WAITING", now, 15, 120)
    assert jobs == [] and abs(until - (now + 900)) < 1
    until, _ = parse_wait("WAITING until 2000-01-01T00:00:00+00:00", now, 15, 120)
    assert until < now                                   # past times wake immediately
    until, _ = parse_wait("WAITING 2999-01-01T00:00", now, 15, 120)
    assert abs(until - (now + 7200)) < 1                  # capped at wait_max


def test_waiting_on_job_then_wake_on_state_change(tmp_path):
    r, st = _research(tmp_path, "did a thing\nSLACK: MILESTONE: M2 done\nSTATUS: WAITING 5550")
    st.jobs["5550"] = {"id": "5550", "state": "RUNNING"}
    res = _run_one(r)
    assert res["ok"] and r.m["status"] == "WAITING" and r.m["wait_jobs"] == ["5550"]
    assert r.slack.posts == ["[research] MILESTONE: M2 done"]
    assert r.m["session"] == "sess-1" and r.m["session_job"] == "100"
    assert r.wake_reason() is None
    st.jobs["5550"]["state"] = "COMPLETED"
    assert r.wake_reason() == "job 5550 is now COMPLETED"


def test_second_turn_resumes_session_and_routine_slack_is_not_sent(tmp_path):
    r, _ = _research(tmp_path, "x\nSLACK: routine progress\nSTATUS: WORKING")
    _run_one(r)
    assert r.slack.posts == [] and r.wake_reason() == "continuing (last turn said WORKING)"
    _run_one(r)
    argv = (tmp_path / "argv.log").read_text().splitlines()
    assert "--resume" not in argv[0] and "--resume sess-1" in argv[1]
    # a new allocation starts a fresh session
    r2 = Research(r.cfg, r.state, r.slack, "200")
    _run_one(r2)
    assert "--resume" not in (tmp_path / "argv.log").read_text().splitlines()[2]


def test_new_orders_wake_and_are_passed_once(tmp_path):
    r, st = _research(tmp_path, "x\nSLACK:\nSTATUS: WAITING 2999-01-01T00:00")
    _run_one(r)
    assert r.wake_reason() is None
    with open(st.orders, "a") as f:
        f.write("- now via slack: E1: go with the draft list\n")
    assert r.wake_reason() == "new standing orders from the user"
    _run_one(r)
    prompt = sorted(st.turns.glob("*_research_*.prompt.md"))[-1].read_text()
    assert "E1: go with the draft list" in prompt
    assert r.new_orders() == ""


def test_agent_blocked_clears_on_any_message(tmp_path):
    r, _ = _research(tmp_path, "x\nSLACK: need E1 and E8\nSTATUS: BLOCKED")
    _run_one(r)
    assert r.blocked().startswith("agent: need E1") and r.slack.posts == ["[research] need E1 and E8"]
    assert r.wake_reason() is None
    assert r.unblock(explicit=False) and r.blocked() is None


def test_control_file_change_blocks_until_explicit_resume(tmp_path):
    r, _ = _research(tmp_path, "x\nSLACK:\nSTATUS: WORKING")
    _run_one(r)
    (Path(r.cfg["_target"]) / "SPEC.md").write_text("spec v2\n")
    assert r.tick("table", float("inf"), True) is None and r.proc is None
    assert r.blocked().startswith("control:") and "SPEC.md" in r.slack.posts[-1]
    assert not r.unblock(explicit=False) and r.blocked()
    assert r.unblock(explicit=True) and r.blocked() is None
    assert r.control_changed() == []                     # new baseline accepted


def test_failed_turns_block_after_cap(tmp_path):
    cfg = _cfg(tmp_path, "boom", max_turn_failures=2)
    cfg["claude"]["bin"] = _fake_claude(tmp_path, "Traceback", rc=1)
    st = State(Path(cfg["_state"]))
    r = Research(cfg, st, FakeSlack(), "100")
    _run_one(r)
    assert r.blocked() is None and r.m["failures"] == 1
    _run_one(r)
    assert r.blocked().startswith("2 consecutive") and "BLOCKER" in r.slack.posts[-1]


def test_no_turn_without_window_or_quota(tmp_path):
    r, _ = _research(tmp_path, "x\nSTATUS: WORKING", min_window_min=30)
    assert r.tick("table", 600, True) is None and r.proc is None
    assert r.tick("table", float("inf"), False) is None and r.proc is None


@pytest.fixture
def lp(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path, "x\nSTATUS: WORKING")
    cfg["claude"]["classify_unknown"] = True
    cfg["watcher"] = {"quarantine_after": 3, "quarantine_min_nodes": 2}
    L = loop_mod.Loop(Path(cfg["_target"]), cfg)
    turns = []
    monkeypatch.setattr(loop_mod, "run_turn", lambda *a, **k: turns.append(a[2]) or {})
    monkeypatch.setattr(L, "post_job", lambda rec, text: None)
    L._turns = turns
    return L


def test_handle_routes_real_failures_to_research(lp, tmp_path):
    log = tmp_path / "o.log"; log.write_text("RuntimeError: shape mismatch\n")
    rec = {"id": "700", "name": "m0_smoke", "workdir": "/w", "state": "FAILED", "exit_code": "1:0", "nodes": "n1",
           "stdout_path": str(log), "klass": "CRASHED", "tier": 3, "directives": {}, "fixes": []}
    lp.state.jobs["700"] = rec
    lp.research.m.update(status="WAITING", wait_until=time.time() + 3600)
    lp.handle(rec, [3])
    assert lp._turns == []                               # neither classify nor tier-3
    assert rec["handled"] and rec["routed_to_research"]
    ev = lp.research.m["job_events"]
    assert ev[0]["id"] == "700" and "shape mismatch" in ev[0]["last_line"]
    assert lp.research.wake_reason() == "a job needs attention"


def test_research_shortcuts(lp):
    posts = []
    lp.slack.post = lambda text, thread_ts=None: posts.append(text)
    assert lp.shortcut("research pause", None, None)
    assert lp.research.sentinel("PAUSED").exists() and "PAUSED" in posts[-1]
    assert lp.research.wake_reason() is None
    assert lp.shortcut("research resume", None, None)
    assert not lp.research.sentinel("PAUSED").exists()
    assert lp.shortcut("research", None, None) and posts[-1].startswith("research:")


def test_control_globs_cover_files_under_directories(tmp_path):
    r, _ = _research(tmp_path, "x\nSTATUS: WORKING", control_files=["SPEC.md", "docs/**"])
    docs = Path(r.cfg["_target"]) / "docs" / "sub"
    docs.mkdir(parents=True)
    (docs / "loop.md").write_text("v1")
    assert sorted(r.hash_control()) == ["SPEC.md", "docs/sub/loop.md"]
    r.control_changed()
    (docs / "loop.md").write_text("v2")
    assert r.control_changed() == ["docs/sub/loop.md"]
