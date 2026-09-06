"""Daily digest: grouped by job name, ordered by what the user must do; stale escalations get closed."""
from datetime import datetime, timedelta

import pytest

from watcher import loop as loop_mod
from watcher.report import digest, summary
from watcher.state import State


def _cfg(tmp_path):
    return {"_target": str(tmp_path / "value-generalization"), "_state": str(tmp_path / ".watcher"), "slurm": {"cluster": "babel"},
            "claude": {"classify_unknown": False}, "watcher": {"report_hour": 0, "timezone": "UTC", "report_llm_summary": True}}


def _iso(hours_ago: float) -> str:
    return (datetime.now().astimezone() - timedelta(hours=hours_ago)).isoformat(timespec="seconds")


def _job(st, jid, name, state, ago=1.0, **kw):
    r = {"id": jid, "name": name, "state": state, "first_seen": _iso(ago + 1), "last_change": _iso(ago), "fixes": [], "klass": "OK"}
    r.update(kw)
    st.jobs[jid] = r
    return r


@pytest.fixture
def st(tmp_path):
    s = State(tmp_path / ".watcher")
    s.meta.update(cluster="babel", watcher_job="10285589", watcher_started=_iso(11), slack_inbound=True, slack_last_poll=_iso(0))
    # open escalation across two array tasks, same fingerprint, with a thread on the first
    _job(s, "10297978_22", "sweep_persona", "FAILED", 5, klass="CRASHED", escalated=True, resolved=False, thread_ts="111.1",
         fingerprint="CRASHED:ValueError: bad token id # in batch #", exit_code="1:0")
    _job(s, "10298005_22", "sweep_persona", "FAILED", 5, klass="CRASHED", escalated=True, resolved=False,
         fingerprint="CRASHED:ValueError: bad token id # in batch #", exit_code="1:0")
    _job(s, "10290031", "eos_probe", "FAILED", 22, klass="CRASHED", escalated=True, resolved=False, evidence="exit 2:0", exit_code="2:0")
    # stale escalations: completed on its own, cancelled by the user, and a retry that completed
    _job(s, "10296564_22", "tenets69", "COMPLETED", 7, escalated=True, resolved=False)
    _job(s, "10296604_8", "tenets69", "CANCELLED by 23377", 8, escalated=True, resolved=False)
    _job(s, "10297643_20", "sweep_persona", "PREEMPTED", 3, escalated=True, resolved=False,
         fixes=[{"at": _iso(3), "action": "resubmit", "new_id": "10298004", "args": [], "reason": "PREEMPTED"}])
    _job(s, "10298004", "sweep_persona", "COMPLETED", 1, parent="10297643_20")
    # plain finished work
    for i in range(12):
        _job(s, f"10269726_{i}", "aft_pilot8", "COMPLETED", 2 + i * 0.1, elapsed="01:00:00", timelimit="12:00:00")
    _job(s, "10296562", "pred_tenets66_qwen3_ctl", "RUNNING", 8, elapsed="08:27:59", timelimit="5-00:00:00")
    for i in range(8):
        _job(s, f"10301571_{i}", "predict_ws", "RUNNING", 0.05, elapsed="00:02:44", timelimit="01:00:00")
    for i in range(30):
        _job(s, f"10278062_{i}", "aft_pilot8", "PENDING", 48, reason="MaxNodePerAccount" if i % 2 else "Priority")
    return s


def test_summary_groups_and_orders(st, tmp_path):
    txt = summary(st, str(tmp_path / "value-generalization"), link=lambda ts: f"https://x/{ts}", prev_needs={"eos_probe"})
    secs = txt.split("\n\n")
    assert secs[0].startswith("Daily digest ·")
    assert "value-generalization@babel · watcher 10285589 up 11h" in secs[0]
    needs = secs[1]
    assert needs.startswith("*NEEDS YOU (")
    # one line per job name, newest first, thread link, error line, (new) marker only for names not in prev_needs
    assert needs.index("sweep_persona (new)") < needs.index("eos_probe ")
    assert "<https://x/111.1|thread>" in needs and "ValueError: bad token id" in needs
    assert "eos_probe (new)" not in needs
    assert "_22" in needs and "10297978_22" not in needs           # array tasks shown by suffix
    fixed = secs[2]
    assert fixed.startswith("*FIXED AUTOMATICALLY") and "sweep_persona  1 job: PREEMPTED → 1 done" in fixed
    fin = secs[3]
    assert fin.startswith("*FINISHED (24h): 14 done · 4 failed · 1 cancelled") and "aft_pilot8  12 done" in fin
    assert fin.index("sweep_persona") < fin.index("aft_pilot8")   # failures first
    run = secs[4]
    assert "*RUNNING (9)*" in run and "predict_ws  8 running · longest 00:02:44/01:00" in run
    assert "*PENDING*: 30 across 1 job names, oldest 2d (aft_pilot8); MaxNodePerAccount ×15" in secs[5]
    assert secs[-1].endswith("Full table: reply `status` (or `status 48h`).")
    assert "```" not in txt


def test_reconcile_closes_stale_escalations(st, tmp_path, monkeypatch):
    L = loop_mod.Loop(tmp_path, _cfg(tmp_path)); L.state = st; L.esc = loop_mod.escalation.Escalations(st)
    assert L.reconcile_escalations() == 3
    assert st.jobs["10296564_22"]["resolved_by"] == "it completed"
    assert st.jobs["10296604_8"]["resolved_by"] == "it was cancelled"
    assert st.jobs["10297643_20"]["resolved_by"] == "its retry 10298004 completed"
    assert not st.jobs["10297978_22"].get("resolved") and not st.jobs["10290031"].get("resolved")
    txt = summary(st, str(tmp_path))
    assert "*NEEDS YOU (2)*" in txt and "tenets69" not in txt.split("*FIXED")[0]


def test_digest_puts_sonnet_paragraph_under_header_and_splits_long_posts(st, tmp_path, monkeypatch):
    L = loop_mod.Loop(tmp_path, _cfg(tmp_path)); L.state = st; L.esc = loop_mod.escalation.Escalations(st)
    L.slack.state = st; L.slack.enabled = True; L.slack.channel = "D1"
    posts = []
    monkeypatch.setattr(L.slack, "_api", lambda method, payload: (posts.append(payload) or {"ok": True, "ts": f"{len(posts)}.0"}))
    monkeypatch.setattr(L.slack, "_api_get", lambda method, params: {"ok": True, "permalink": f"https://slack/{params['message_ts']}"})
    monkeypatch.setattr(loop_mod, "run_turn", lambda cfg, s, kind, prompt, tag="": {"rc": 0, "ok": True, "result": "", "status": "OK",
                                                                                    "slack": "Look at sweep_persona first.", "quota": False, "path": ""})
    st.meta["last_report_date"] = "1999-01-01"
    L.maybe_digest()
    body = posts[0]["text"]
    lines = body.split("\n\n")
    assert lines[0].startswith("[babel] Daily digest") and lines[1] == "Look at sweep_persona first."
    assert lines[2].startswith("*NEEDS YOU") and "<https://slack/111.1|thread>" in lines[2]
    assert st.meta["digest_prev_needs"] == ["eos_probe", "sweep_persona"]
    # a long body is split on blank lines and continued in the first post's thread
    posts.clear()
    L.slack.post_long("A" * 2000 + "\n\n" + "B" * 2000 + "\n\n" + "C" * 10)
    assert len(posts) == 2 and posts[1]["thread_ts"] == "1.0" and posts[1]["text"].endswith("B" * 2000 + "\n\n" + "C" * 10)
