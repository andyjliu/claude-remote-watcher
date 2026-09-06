import json
from pathlib import Path

from watcher import escalation
from watcher.state import State


TB = """Traceback (most recent call last):
  File "/home/u/exp/train.py", line 212, in <module>
    main()
  File "/home/u/exp/train.py", line 88, in main
    x = torch.load("/data/run7/ckpt_00012.pt")
FileNotFoundError: [Errno 2] No such file or directory: '/data/run7/ckpt_00012.pt'
srun: error: node0412: task 0: Exited with exit code 1
"""


def test_fingerprint_ignores_paths_numbers_and_srun_trailer():
    a = escalation.fingerprint("CRASHED_TRIVIAL", "1:0", TB)
    b = escalation.fingerprint("CRASHED_TRIVIAL", "1:0", TB.replace("run7", "run9").replace("00012", "00031").replace("node0412", "node0007"))
    assert a == b
    assert a.startswith("CRASHED_TRIVIAL:FileNotFoundError")


def test_fingerprint_state_only_for_scheduler_failures():
    assert escalation.fingerprint("PREEMPTED", "0:0", TB) == "PREEMPTED"
    assert escalation.fingerprint("OOM", "0:125", "") == "OOM"


def test_fingerprint_without_log_uses_exit_code():
    assert escalation.fingerprint("CRASHED", "137:0", "") == "CRASHED:exit=137:0"


def test_key_groups_array_siblings_by_name():
    fp = "CRASHED:X"
    assert escalation.key({"id": "10326853_0", "name": "aft"}, fp) == escalation.key({"id": "10326853_41", "name": "aft"}, fp)
    assert escalation.key({"id": "1_0", "name": "aft"}, fp) != escalation.key({"id": "1_0", "name": "other"}, fp)


def test_map_counts_once_per_job_and_quarantines(tmp_path: Path):
    st = State(tmp_path)
    esc = escalation.Escalations(st)
    k = "aft|CRASHED:X"
    r0 = {"id": "1_0", "name": "aft", "nodes": "n1"}
    e = esc.count(r0, k); e = esc.count(r0, k)  # second call is a no-op
    assert e["count"] == 1
    esc.count({"id": "1_1", "name": "aft", "nodes": "n1"}, k)
    e = esc.count({"id": "1_2", "name": "aft", "nodes": "n2"}, k)
    assert e["count"] == 3 and sorted(e["nodes"]) == ["n1", "n2"]
    assert not esc.should_quarantine(e, after=3, min_nodes=3)
    assert esc.should_quarantine(e, after=3, min_nodes=2)
    esc.put_quarantine("aft", e, k)
    assert esc.quarantined("aft") and esc.quarantined("zzz") is None
    st.save()
    assert json.loads((tmp_path / "meta.json").read_text())["quarantine"]["aft"]["count"] == 3
    assert esc.lift("aft") and not esc.lift("aft")


def test_awaiting_user_until_reply(tmp_path: Path):
    esc = escalation.Escalations(State(tmp_path))
    k = "aft|CRASHED:X"
    esc.count({"id": "1_0", "name": "aft"}, k)
    assert not esc.awaiting_user(k)
    esc.mark(k, "ESCALATE", "171.1")
    assert esc.awaiting_user(k) and esc.get(k)["thread_ts"] == "171.1"
    assert esc.resolve_name("aft") == 1
    assert not esc.awaiting_user(k)
    esc.mark(k, "ESCALATE"); esc.mark(k, "ACTED")
    assert not esc.awaiting_user(k)
