"""Slack inbound by polling: addressing forms, peer learning, last-speaker routing, thread replies."""
import json
from pathlib import Path

import pytest

from watcher import loop as loop_mod
from watcher.slack import Slack
from watcher.state import State


def _cfg(tmp_path: Path) -> dict:
    tok = tmp_path / "tok"; tok.write_text("xoxb-x"); uid = tmp_path / "uid"; uid.write_text("U1")
    return {"_target": str(tmp_path), "_state": str(tmp_path / ".watcher"), "slurm": {"cluster": "babel"},
            "claude": {"classify_unknown": False},
            "slack": {"bot_token_file": str(tok), "user_id_file": str(uid), "peer_clusters": ["orchard"], "poll_sec": 0}}


class FakeAPI:
    """Scripted Slack Web API: a channel history plus per-thread replies."""
    def __init__(self):
        self.history: list[dict] = []
        self.replies: dict[str, list[dict]] = {}
        self.calls: list[str] = []

    def __call__(self, method: str, payload: dict) -> dict:
        self.calls.append(method)
        if method == "conversations.open":
            return {"ok": True, "channel": {"id": "D1"}}
        if method == "conversations.history":
            old = float(payload["oldest"])
            return {"ok": True, "messages": [m for m in self.history if float(m["ts"]) > old], "has_more": False}
        if method == "conversations.replies":
            old = float(payload["oldest"])
            return {"ok": True, "messages": [m for m in self.replies.get(payload["ts"], []) if float(m["ts"]) > old]}
        if method == "chat.postMessage":
            ts = f"{9000 + len(self.history):.6f}"
            self.history.append({"ts": ts, "bot_id": "B1", "text": payload["text"]})
            return {"ok": True, "ts": ts}
        raise AssertionError(method)


@pytest.fixture
def sl(tmp_path, monkeypatch):
    st = State(tmp_path / ".watcher")
    s = Slack(_cfg(tmp_path), st)
    api = FakeAPI()
    monkeypatch.setattr(s, "_api", api)
    s.api = api
    assert s.start_listener()
    st.meta["slack_cursor"] = "1000.000000"
    return s


def user(ts, text, **kw):
    return {"ts": f"{ts:.6f}", "user": "U1", "text": text, **kw}


def bot(ts, text):
    return {"ts": f"{ts:.6f}", "bot_id": "B1", "text": text}


@pytest.mark.parametrize("text,addr,rest", [
    ("[orchard] status", "orchard", "status"),
    ("@orchard status", "orchard", "status"),
    ("orchard: status", "orchard", "status"),
    ("orchard, status", "orchard", "status"),
    ("orchard - status", "orchard", "status"),
    ("Orchard> status", "orchard", "status"),
    ("(babel) sure", "babel", "sure"),
    ("babel sure", "babel", "sure"),
    ("BABEL", "babel", ""),
    ("@all stop", "all", "stop"),
    ("all: stop", "all", "stop"),
    ("[all] stop", "all", "stop"),
    ("both: stop", "all", "stop"),
    ("all done, thanks", None, "all done, thanks"),
    ("sure", None, "sure"),
    ("nautilus: status", None, "nautilus: status"),  # unknown name is plain text
])
def test_address_forms(sl, text, addr, rest):
    assert sl.address(text) == (addr, rest)


def test_poll_learns_peers_and_tags_last_speaker(sl):
    sl.api.history = [bot(1001, "[orchard] Truncation report ..."), bot(1002, "[babel] [123 job] ESCALATE: x"),
                      user(1003, "Sure"), bot(1004, "[orchard] something"), user(1005, "[orchard] status"),
                      bot(1006, "[nautilus] hi"), user(1007, "ok")]
    assert sl.poll_inbound() == 3
    msgs = [json.loads(p.read_text()) for p in sl.state.inbox.glob("*.json")]
    msgs.sort(key=lambda m: m["ts"])
    assert [m["last_speaker"] for m in msgs] == ["babel", "orchard", "nautilus"]
    assert "nautilus" in sl.peers and sl.state.meta["slack_cursor"] == "1007.000000"
    assert sl.poll_inbound(force=True) == 0  # nothing new: cursor advanced


def test_poll_threads_only_ours(sl):
    root = sl.post("hello")                          # our root -> watched
    sl.api.replies[root] = [user(9500, "approve", thread_ts=root)]
    sl.api.replies["777.000000"] = [user(9501, "other cluster's thread", thread_ts="777.000000")]
    assert sl.poll_inbound(force=True) == 1
    m = json.loads(next(sl.state.inbox.glob("*.json")).read_text())
    assert m["thread_ts"] == root and m["text"] == "approve"
    assert sl.is_our_thread(root, {}) and not sl.is_our_thread("777.000000", {})
    assert sl.poll_inbound(force=True) == 0        # reply cursor advanced


def test_rate_limit_backs_off(sl, monkeypatch):
    import urllib.error
    def boom(method, payload):
        raise urllib.error.HTTPError("u", 429, "rate", {"Retry-After": "120"}, None)
    monkeypatch.setattr(sl, "_api", boom)
    import time
    before = time.time()
    assert sl.poll_inbound(force=True) == 0
    assert sl._next_poll >= before + 119


# ---- routing in Loop.process_inbox --------------------------------------------------------------
@pytest.fixture
def lp(tmp_path, monkeypatch):
    L = loop_mod.Loop(tmp_path, _cfg(tmp_path))
    api = FakeAPI(); monkeypatch.setattr(L.slack, "_api", api); L.slack.api = api
    turns = []
    monkeypatch.setattr(loop_mod, "run_turn", lambda cfg, st, kind, prompt, tag="": (turns.append(prompt) or
                        {"rc": 0, "ok": True, "result": "x", "status": "OK", "slack": "done", "quota": False, "path": ""}))
    L._turns = turns
    return L


def _drop(L, text, last_speaker=None, thread=None, ts="1234.5"):
    (L.state.inbox / f"{ts.replace('.', '_')}.json").write_text(json.dumps(
        {"ts": ts, "thread_ts": thread, "text": text, "source": "slack", "received": "now", "last_speaker": last_speaker}))


def test_unaddressed_goes_to_last_speaker(lp):
    _drop(lp, "Sure", last_speaker="orchard")
    lp.process_inbox([3])
    assert lp._turns == [] and not list(lp.state.inbox.glob("*.json"))
    _drop(lp, "Sure", last_speaker="babel", ts="1235.5")
    lp.process_inbox([3])
    assert len(lp._turns) == 1
    _drop(lp, "hello?", last_speaker=None, ts="1236.5")    # nobody has spoken: everyone answers
    lp.process_inbox([3])
    assert len(lp._turns) == 2


def test_addressed_overrides_last_speaker_and_shortcuts_skip_budget(lp):
    _drop(lp, "[babel] status", last_speaker="orchard")
    lp.process_inbox([0])                                   # no Claude budget, still answered
    posts = [m["text"] for m in lp.slack.api.history]
    assert len(posts) == 1 and posts[0].startswith("[babel]") and lp._turns == []
    _drop(lp, "[orchard] explain", last_speaker="babel", ts="1235.5")
    lp.process_inbox([3])
    assert lp._turns == []                                  # for orchard, not us
    _drop(lp, "babel: explain", last_speaker="orchard", ts="1236.5")
    lp.process_inbox([3])
    assert len(lp._turns) == 1
