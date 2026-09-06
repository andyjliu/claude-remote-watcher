"""Loop.handle end to end with Slack and Claude stubbed: array siblings dying identically produce one
Claude turn, then threaded deltas, then a quarantine; a reply re-arms escalation."""
from pathlib import Path

import pytest

from watcher import loop as loop_mod


def _cfg(tmp_path: Path) -> dict:
    return {"_target": str(tmp_path), "_state": str(tmp_path / ".watcher"), "slurm": {"cluster": "t"},
            "claude": {"classify_unknown": False}, "watcher": {"quarantine_after": 3, "quarantine_min_nodes": 2, "escalation_delta_min": 0}}


@pytest.fixture
def lp(tmp_path, monkeypatch):
    L = loop_mod.Loop(tmp_path, _cfg(tmp_path))
    posts, turns = [], []
    monkeypatch.setattr(L, "post_job", lambda rec, text: posts.append((rec["id"], text)))
    monkeypatch.setattr(loop_mod, "run_turn", lambda cfg, st, kind, prompt, tag="": (turns.append(tag) or
                        {"rc": 0, "ok": True, "result": "x", "status": "ESCALATE", "slack": "need you", "quota": False, "path": ""}))
    L._posts, L._turns = posts, turns
    return L


def _task(L, idx, node, log: Path):
    r = {"id": f"500_{idx}", "name": "aft", "workdir": "/w", "state": "FAILED", "exit_code": "1:0", "nodes": node,
         "stdout_path": str(log), "klass": "CRASHED", "tier": 3, "directives": {}, "fixes": [], "submit": "2026-09-05T01:00:00"}
    L.state.jobs[r["id"]] = r
    return r


def test_identical_siblings_one_turn_then_quarantine(lp, tmp_path):
    log = tmp_path / "out.log"; log.write_text("stuff\nRuntimeError: shape mismatch at step 12\n")
    r0, r1, r2 = (_task(lp, i, n, log) for i, n in enumerate(["n1", "n1", "n2"]))
    lp.handle(r0, [3]); lp.handle(r1, [3]); lp.handle(r2, [3])
    assert lp._turns == ["500_0"]                      # one Claude turn for the whole array
    assert all(r["handled"] for r in (r0, r1, r2)) and r0["escalated"] and not r1["escalated"]
    kinds = [t.split(":")[0] for _, t in lp._posts]
    assert kinds == ["ESCALATE", "+1 identical failure (2 so far", "auto-quarantined aft"]
    assert lp.esc.quarantined("aft")["count"] == 3
    # a fourth sibling while quarantined: silent
    r3 = _task(lp, 3, "n3", log); lp.handle(r3, [3])
    assert lp._turns == ["500_0"] and len(lp._posts) == 3 and r3["handled"]
    # a different failure of the same job name is still swallowed by the quarantine
    log2 = tmp_path / "o2.log"; log2.write_text("ValueError: nope\n")
    r4 = _task(lp, 4, "n1", log2); lp.handle(r4, [3])
    assert lp._turns == ["500_0"]
    # lift: the next failure gets a turn again
    assert lp.esc.lift("aft")
    r5 = _task(lp, 5, "n1", log2); lp.handle(r5, [3])
    assert lp._turns == ["500_0", "500_5"]


def test_reply_rearms_escalation(lp, tmp_path):
    log = tmp_path / "out.log"; log.write_text("RuntimeError: x\n")
    r0 = _task(lp, 0, "n1", log); lp.handle(r0, [3])
    r1 = _task(lp, 1, "n1", log); lp.handle(r1, [3])
    assert lp._turns == ["500_0"]
    lp.esc.resolve_name("aft")                          # what process_inbox does on a thread reply
    r2 = _task(lp, 2, "n1", log); lp.handle(r2, [3])
    assert lp._turns == ["500_0", "500_2"]


def test_controller_owned_array_preemption_is_left_alone(lp, tmp_path):
    lp.state.jobs["400"] = {"id": "400", "name": "aft_ctl", "workdir": "/w", "state": "RUNNING", "submit": "2026-09-05T00:00:00"}
    r = _task(lp, 0, "n1", tmp_path / "none.log"); r.update(klass="PREEMPTED", tier=0, state="PREEMPTED")
    lp.handle(r, [3])
    assert r["handled"] and lp._turns == [] and lp._posts == []
    assert r["directives_effective"]["resume"] == "none" and r["directives_effective"]["controller"] == "400"
    # a controller that finished before the task was submitted does not own it
    lp.state.jobs["400"].update(state="COMPLETED", end="2026-09-05T00:30:00")
    assert lp.controller_of(r) is None


def test_hourly_budget(lp):
    lp.cfg["watcher"]["max_llm_turns_per_hour"] = 2
    b = [10]
    assert lp.take_turn_budget(b) and lp.take_turn_budget(b) and not lp.take_turn_budget(b)
    assert b == [8]
