import json
from pathlib import Path

from watcher import claude_turn
from watcher.state import State


def _fake_claude(tmp_path: Path, stdout: str, rc: int = 0) -> str:
    bin_ = tmp_path / "claude"
    bin_.write_text("#!/bin/sh\ncat >/dev/null\ncat <<'EOF'\n" + stdout + "\nEOF\nexit " + str(rc) + "\n")
    bin_.chmod(0o755)
    return str(bin_)


def _run(tmp_path, stdout, rc=0):
    cfg = {"claude": {"bin": _fake_claude(tmp_path, stdout, rc), "tiers": {"tier3": {"model": "m", "budget_usd": 1}}}}
    (tmp_path / "prompts").mkdir(exist_ok=True)
    return claude_turn.run_turn(cfg, State(tmp_path / "st"), "tier3", "hi")


def test_429_session_limit_is_quota(tmp_path):
    out = json.dumps({"type": "result", "is_error": True, "api_error_status": 429,
                      "result": "You've hit your session limit · resets 7:20pm"})
    res = _run(tmp_path, out, rc=1)
    assert res["quota"] and not res["ok"]


def test_529_is_quota_without_text_match(tmp_path):
    res = _run(tmp_path, json.dumps({"type": "result", "is_error": True, "api_error_status": 529, "result": "boom"}), rc=1)
    assert res["quota"]


def test_real_failure_is_not_quota(tmp_path):
    res = _run(tmp_path, json.dumps({"type": "result", "is_error": True, "result": "Traceback ... ValueError"}), rc=1)
    assert not res["quota"] and not res["ok"]


def test_ok_turn(tmp_path):
    res = _run(tmp_path, json.dumps({"type": "result", "is_error": False, "result": "did stuff\nSLACK: fine\nSTATUS: OK"}))
    assert res["ok"] and res["status"] == "OK" and res["slack"] == "fine" and not res["quota"]
