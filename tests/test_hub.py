import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from watcher import hubdata as hd
from watcher.notion import P_text, norm_id, read, rich

TZ = ZoneInfo("America/New_York")
NOW = datetime(2026, 11, 21, 0, 0, tzinfo=TZ)


def job(id, part="general", state="RUNNING", gpus=4, start=NOW - timedelta(hours=10), limit=48, workdir="/x/advsel",
        comment="", name="j"):
    return hd.Job(id=id, name=name, state=state, partition=part, gpus=gpus, cpus=8, mem_gb=64, comment=comment,
                  workdir=workdir, start=start, submit=start, limit=timedelta(hours=limit), node="n1")


def test_parse_squeue_line_and_gpus():
    line = ("11226987|o1_cc_hacker_v1|RUNNING|general|cpu=32,mem=192G,node=1,billing=112,gres/gpu=4,gres/gpu:l40s=4|"
            "gres/gpu:4|N/A|32|32000M|1|(null)|/home/a/adverse-selection|2026-11-20T14:35:43|2026-11-20T14:35:20|"
            "2-00:00:00|9:16:36|babel-m5-24|None|")
    j = hd.parse_squeue_line(line, TZ)
    assert (j.gpus, j.cpus, j.mem_gb, j.comment, j.limit) == (4, 32, 192.0, "", timedelta(days=2))
    assert hd.gpus_of("cpu=2,mem=4G", "gres/gpu:l40s:2", "N/A", 2) == 4
    assert hd.gpus_of("cpu=2", "N/A", "gres/gpu:3", 1) == 3
    assert hd.gpus_of("cpu=2", "N/A", "N/A", 1) == 0


def test_weekly_gpu_hours_clips_window_and_counts_requeues():
    rows = [
        {"Partition": "general", "Start": "2026-11-20T20:00:00", "End": "2026-11-20T22:00:00", "AllocTRES": "gres/gpu=4"},
        {"Partition": "general", "Start": "2026-11-20T22:00:00", "End": "Unknown", "AllocTRES": "gres/gpu=4"},  # requeued attempt, running
        {"Partition": "general", "Start": "2026-11-13T22:00:00", "End": "2026-11-14T02:00:00", "AllocTRES": "gres/gpu=1"},  # half in window
        {"Partition": "preempt", "Start": "2026-11-20T20:00:00", "End": "2026-11-20T22:00:00", "AllocTRES": "gres/gpu=8"},
        {"Partition": "general", "Start": "None", "End": "Unknown", "AllocTRES": ""},
    ]
    assert abs(hd.weekly_gpu_hours(rows, {"general"}, NOW, TZ) - (8 + 8 + 2)) < 1e-6


def test_map_jobs_order():
    jobs = [job("1", comment="cc:taskvec"), job("2"), job("3", workdir="/elsewhere"), job("4"), job("5", workdir="/x/advsel/sub")]
    tags = {"4": "taskvec"}  # seen live earlier; comment no longer visible
    hd.map_jobs(jobs, {"/x/advsel": "advsel"}, tags, {"advsel", "taskvec"}, manual={"2": "taskvec"})
    assert [(j.spec, j.mapped_by) for j in jobs] == [("taskvec", "tag"), ("taskvec", "manual"), (None, "untracked"),
                                                     ("taskvec", "tag"), ("advsel", "workdir")]
    assert tags["1"] == "taskvec"


def test_time_until_dry_eta_beats_limit():
    jobs = [job("1"), job("2", start=NOW - timedelta(hours=1))]
    for j in jobs:
        j.spec = "advsel"
    d = hd.time_until_dry(NOW, jobs, {"general"}, 8, {"advsel": (NOW + timedelta(hours=5), False)}, [])
    assert (round(d.hours, 2), d.lower_bound) == (5.0, False)
    d = hd.time_until_dry(NOW, jobs, {"general"}, 8, {}, [])  # no ETA: time limits
    assert round(d.hours, 2) == 47.0 and not d.lower_bound and "job 2" in d.detail


def test_time_until_dry_pending_and_queued_lower_bound():
    jobs = [job("1", gpus=8, start=NOW - timedelta(hours=40)), job("2", state="PENDING", start=None, limit=6, gpus=4)]
    d = hd.time_until_dry(NOW, jobs, {"general"}, 8, {}, [("newspec", 4, 10.0)])
    # queued spec waits for job 1 to free GPUs at +8 h, then runs 10 h
    assert round(d.hours, 1) == 18.0 and d.lower_bound  # job 2's start is unknown -> whole number is a lower bound
    d = hd.time_until_dry(NOW, [], {"general"}, 8, {}, [("x", 4, None)])
    assert d.hours == 0 and d.lower_bound
    assert hd.time_until_dry(NOW, [job("9", part="preempt")], {"general"}, 8, {}, []).hours == 0


def test_spec_eta_and_overdue():
    cps = {"checkpoints": [{"status": "running", "eta": "2026-11-21T03:00-05:00"},
                           {"status": "done", "eta": "2026-11-25T03:00-05:00"}]}
    eta, overdue = hd.spec_eta(cps, TZ, NOW)
    assert eta == datetime(2026, 11, 21, 3, 0, tzinfo=TZ) and not overdue
    cps["spec_eta"] = "2026-11-20T12:00-05:00"
    assert hd.spec_eta(cps, TZ, NOW)[1] is True
    assert hd.spec_eta(None, TZ, NOW) == (None, False)


def test_derive_state():
    i = hd.DirInfo(path=Path("/x"), has_state=True, watcher_live=True)
    assert hd.derive_state(i, 0, "not started") == "running"
    i.no_new_jobs = True
    assert hd.derive_state(i, 0, None) == "no new jobs"
    i.paused = True
    assert hd.derive_state(i, 0, None) == "paused"
    i = hd.DirInfo(path=Path("/x"), has_state=True, watcher_live=False, stop=True)
    assert hd.derive_state(i, 0, "running") == "paused"
    assert hd.derive_state(i, 3, "running") == "running"
    assert hd.derive_state(i, 0, "queued") == "queued"
    assert hd.derive_state(None, 0, None) == "not started"


def test_ledger_and_cap(tmp_path):
    (tmp_path / "results").mkdir()
    (tmp_path / "results" / "spend.jsonl").write_text(
        json.dumps({"node": "O2", "usd": 1.5, "tokens_in": 100, "tokens_out": 50}) + "\n" +
        json.dumps({"node": "O2", "usd": 0.5, "tokens_in": 10, "tokens_out": None}) + "\nnot json\n")
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "defaults.yaml").write_text("budget:\n  api_usd: 50\n")
    usd, tok, _, by = hd.read_ledger(tmp_path / "results" / "spend.jsonl")
    assert (usd, tok, by["O2"]["usd"]) == (2.0, 160.0, 2.0)
    assert hd.api_cap_of(tmp_path) == 50.0


def test_notion_helpers():
    assert norm_id("41f95cc0892f8133befaf111cd2ec759") == "41f95cc0-892f-8133-befa-f111cd2ec759"
    assert len(rich("x" * 4500)) == 3
    page = {"properties": {"S": {"type": "rich_text", "rich_text": [{"plain_text": "ab"}, {"plain_text": "c"}]},
                           "R": {"type": "relation", "relation": [{"id": "a"}]}}}
    assert read(page, "S") == "abc" and read(page, "R") == ["a"] and read(page, "missing") is None
    assert P_text(None) == {"rich_text": []}


def test_eta_reason(tmp_path):
    from watcher.research import Research
    from watcher.state import State
    st = State(tmp_path / ".watcher")
    cfg = {"_target": str(tmp_path), "research": {"enabled": True}}
    r = Research(cfg, st, None, "1")
    now = time.time()
    r.m.update(status="WAITING", wait_until=now + 3600)
    assert r.eta_reason(now) == "seed"
    old = datetime.fromtimestamp(now - 7200).astimezone().isoformat()
    (st.dir / "checkpoints.json").write_text(json.dumps({"checkpoints": [{"key": "A", "status": "running", "eta_as_of": old}]}))
    assert r.eta_reason(now) == "refresh"
    r.m["wait_until"] = now + 300  # the loop wakes soon anyway
    assert r.eta_reason(now) is None
    r.m.update(wait_until=now + 3600, eta_last_end=now - 600)  # refreshed recently
    assert r.eta_reason(now) is None


def test_hub_model_configurable(tmp_path):
    from watcher.hub import Hub
    from watcher.state import State
    tok = tmp_path / "tok"; tok.write_text("x")
    ids = {k: "41f95cc0892f81299f12eb76d8ee0d83" for k in ("sources", "specs", "checkpoints", "history", "mappings", "jobs")}
    cfg = {"hub": {"notion": ids, "notion_token_file": str(tok)}}
    assert Hub(cfg, State(tmp_path / "s"), dry_run=True).model == "opus"
    cfg["claude"] = {"tiers": {"hub": {"model": "sonnet", "budget_usd": 2}}}
    h = Hub(cfg, State(tmp_path / "s"), dry_run=True)
    assert (h.model, h.budget) == ("sonnet", 2.0)
