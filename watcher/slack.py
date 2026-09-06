"""Slack DM to a single user, stdlib-only. Inbound is polled (conversations.history / .replies), not pushed:
Socket Mode load-balances each event across the open connections of one app, so with several clusters
sharing the bot only one of them would ever hear a given DM."""
from __future__ import annotations

import json
import re
import time
import urllib.error
import urllib.request
from pathlib import Path

from .config import cluster_name, get
from .config import path as cfg_path
from .state import State, now_iso


def _read(p: Path | None) -> str | None:
    try:
        return p.read_text().strip() if p and p.is_file() else None
    except OSError:
        return None


class Slack:
    def __init__(self, cfg: dict, state: State):
        self.state = state
        self.token = _read(cfg_path(cfg, "slack.bot_token_file"))
        self.user_id = _read(cfg_path(cfg, "slack.user_id_file"))
        self.enabled = bool(self.token and self.user_id)
        self.channel: str | None = state.meta.get("slack_channel")
        self.poll_sec = float(get(cfg, "slack.poll_sec", 60))
        self.thread_watch_hours = float(get(cfg, "slack.thread_watch_hours", 24))
        self._next_poll = 0.0
        self._cold_idx = 0
        # Several clusters (and directories) may share this bot and this DM channel, so every
        # outbound message is prefixed "[<cluster>]" and inbound DMs can be addressed to one cluster.
        self.tag = cluster_name(cfg)
        self.peers: set[str] = {str(x).lower() for x in (get(cfg, "slack.peer_clusters") or [])}
        self.peers |= {str(x).lower() for x in state.meta.get("peer_clusters", [])}
        self.peers.discard(self.tag.lower())

    # ---- multi-cluster routing --------------------------------------------
    # "[name] cmd", "(name) cmd", "@name cmd", "name: cmd", "name, cmd", "name - cmd", "name> cmd", "name cmd"
    _ADDR = re.compile(r"^\s*(?P<wrap>[\[(<{]|@|#)?\s*(?P<name>[A-Za-z][\w.-]*)\s*(?P<close>[\])>}])?(?P<sep>\s*[:,;>-]+\s*|\s+|$)(?P<rest>.*)$", re.S)

    def address(self, text: str) -> tuple[str | None, str]:
        """Parse a leading cluster name in any common form -- '[name] ...', '@name ...', 'name: ...',
        'name, ...', 'name - ...', 'name ...' -- where name is this cluster, a known peer, or
        'all'/'everyone'/'both' (those need a wrapper or punctuation so 'all done' stays plain text).
        Returns (name.lower() or None, remaining text)."""
        m = self._ADDR.match(text or "")
        if not m:
            return None, text
        name_l = m.group("name").lower()
        marked = bool(m.group("wrap") or m.group("close") or m.group("sep").strip())
        if name_l in ("all", "everyone", "both", "everybody"):
            return ("all", m.group("rest").strip()) if marked else (None, text)
        if name_l == self.tag.lower() or name_l in self.peers:
            return name_l, m.group("rest").strip()
        return None, text

    def for_us(self, name: str | None) -> bool:
        return name is None or name == "all" or name == self.tag.lower()

    def _remember(self, key: str, ts: str | None, cap: int = 400) -> None:
        if not ts:
            return
        lst = self.state.meta.setdefault(key, [])
        if ts not in lst:
            lst.append(ts)
            del lst[:-cap]

    def remember_user_ts(self, ts: str | None) -> None:
        """Remember the ts of a top-level user DM so a thread the user starts on their own message is
        not mistaken for another cluster's job thread, and so its replies are polled."""
        self._remember("seen_user_ts", ts)
        self._touch_thread(ts)

    def is_our_thread(self, thread_ts: str, jobs: dict) -> bool:
        if any(r.get("thread_ts") == thread_ts for r in jobs.values()):
            return True
        return thread_ts in self.state.meta.get("posted_ts", []) or thread_ts in self.state.meta.get("seen_user_ts", [])

    def _learn_peer(self, text: str) -> str | None:
        """Cluster name from a '[name] ...' bot post (ours or a peer's); learns new peers."""
        m = re.match(r"^\[([A-Za-z][\w.-]*)\]\s", text or "")
        if not m:
            return None
        name = m.group(1).lower()
        if name != self.tag.lower() and name not in self.peers:
            self.peers.add(name)
            self.state.meta["peer_clusters"] = sorted(self.peers)
            self.state.logline(f"slack: learned peer cluster {name!r} from its posts")
        return name

    # ---- outbound -------------------------------------------------------
    def _api(self, method: str, payload: dict) -> dict:
        req = urllib.request.Request(
            f"https://slack.com/api/{method}", data=json.dumps(payload).encode(),
            headers={"Authorization": f"Bearer {self.token}", "Content-Type": "application/json; charset=utf-8"})
        with urllib.request.urlopen(req, timeout=30) as r:
            out = json.loads(r.read().decode())
        if not out.get("ok"):
            raise RuntimeError(f"slack {method}: {out.get('error')}")
        return out

    def _ensure_channel(self) -> str:
        if not self.channel:
            self.channel = self._api("conversations.open", {"users": self.user_id})["channel"]["id"]
            self.state.meta["slack_channel"] = self.channel
        return self.channel

    def post(self, text: str, thread_ts: str | None = None) -> str | None:
        """Best-effort. Returns message ts (usable as a thread root) or None."""
        if not self.enabled:
            self.state.logline(f"[slack disabled] {text[:200]}")
            return None
        payload = {"channel": self._ensure_channel(), "text": f"[{self.tag}] {text}"[:3800], "unfurl_links": False}
        if thread_ts:
            payload["thread_ts"] = thread_ts
        try:
            ts = self._api("chat.postMessage", payload).get("ts")
        except Exception as e:  # noqa: BLE001
            self.state.logline(f"slack post failed: {e}")
            return None
        if not thread_ts:
            self._remember("posted_ts", ts)  # roots of threads that belong to this watcher
        self._touch_thread(thread_ts or ts)
        return ts

    # ---- inbound (polled) ---------------------------------------------------
    def _threads(self) -> dict:
        return self.state.meta.setdefault("slack_threads", {})

    def _touch_thread(self, root: str | None, cursor: str | None = None) -> None:
        """Mark a thread root as ours/active so its replies are polled. Activity in the last
        thread_watch_hours keeps it watched; recent activity (<2h) makes it 'hot' (polled every cycle)."""
        if not root:
            return
        t = self._threads().setdefault(root, {"cursor": root})
        t["active"] = time.time()
        if cursor and float(cursor) > float(t.get("cursor", "0")):
            t["cursor"] = cursor

    def _prune_threads(self) -> None:
        cutoff = time.time() - self.thread_watch_hours * 3600
        th = self._threads()
        for root in [r for r, t in th.items() if t.get("active", 0) < cutoff]:
            del th[root]

    def start_listener(self) -> bool:
        """Inbound needs only the bot token + user id (scope im:history). Returns whether it is on."""
        if not self.enabled:
            return False
        try:
            self._ensure_channel()
        except Exception as e:  # noqa: BLE001
            self.state.logline(f"slack: cannot open DM channel: {e}")
            return False
        if "slack_cursor" not in self.state.meta:
            self.state.meta["slack_cursor"] = f"{time.time():.6f}"  # never replay history from before this watcher
        self._next_poll = 0.0
        return True

    def poll_inbound(self, force: bool = False) -> int:
        """Fetch new DMs (and replies in our threads) into <state>/inbox/*.json. Self-paced to
        slack.poll_sec; cheap to call every second. Returns number of messages written."""
        if not self.enabled or (not force and time.time() < self._next_poll):
            return 0
        self._next_poll = time.time() + self.poll_sec
        n = 0
        try:
            n += self._poll_channel()
            n += self._poll_threads()
            self.state.meta["slack_last_poll"] = now_iso()
            self.state.meta.pop("slack_poll_error", None)
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = float(e.headers.get("Retry-After", "60") or 60)
                self._next_poll = time.time() + max(wait, self.poll_sec)
                self.state.logline(f"slack: rate limited; next poll in {wait:.0f}s")
            else:
                self.state.meta["slack_poll_error"] = f"{now_iso()} HTTP {e.code}"
                self.state.logline(f"slack poll failed: HTTP {e.code}")
        except Exception as e:  # noqa: BLE001
            self.state.meta["slack_poll_error"] = f"{now_iso()} {type(e).__name__}: {e}"
            self.state.logline(f"slack poll failed: {type(e).__name__}: {e}")
        return n

    def _poll_channel(self) -> int:
        """Top-level DMs since the cursor, oldest first. Bot posts teach us peer names and who spoke
        last; the user's posts land in the inbox tagged with that last speaker."""
        ch = self._ensure_channel()
        cursor = str(self.state.meta.get("slack_cursor", "0"))
        msgs: list[dict] = []
        payload: dict = {"channel": ch, "oldest": cursor, "inclusive": False, "limit": 200}
        while True:
            out = self._api("conversations.history", payload)
            msgs.extend(out.get("messages", []))
            nxt = (out.get("response_metadata") or {}).get("next_cursor")
            if not (out.get("has_more") and nxt):
                break
            payload["cursor"] = nxt
        msgs.sort(key=lambda m: float(m.get("ts", "0")))
        n = 0
        speaker = self.state.meta.get("slack_last_speaker")
        for m in msgs:
            if m.get("bot_id") or m.get("subtype") == "bot_message":
                name = self._learn_peer(m.get("text", ""))
                if name:
                    speaker = name
                if m.get("ts") in self.state.meta.get("posted_ts", []):
                    speaker = self.tag.lower()
                continue
            if self._is_user_msg(m):
                self.remember_user_ts(m.get("ts"))
                self._inbox(m, thread_ts=None, last_speaker=speaker)
                n += 1
        if msgs:
            self.state.meta["slack_cursor"] = msgs[-1]["ts"]
        self.state.meta["slack_last_speaker"] = speaker
        return n

    def _poll_threads(self) -> int:
        """Replies never show up in channel history, so poll conversations.replies per watched root:
        hot roots (activity < 2h) every cycle, the rest round-robin, three per cycle."""
        self._prune_threads()
        th = self._threads()
        if not th:
            return 0
        now = time.time()
        hot = [r for r, t in th.items() if now - t.get("active", 0) < 7200]
        cold = sorted(r for r in th if r not in hot)
        pick = list(hot)
        if cold:
            for i in range(min(3, len(cold))):
                pick.append(cold[(self._cold_idx + i) % len(cold)])
            self._cold_idx = (self._cold_idx + 3) % len(cold)
        n = 0
        ch = self._ensure_channel()
        for root in pick:
            t = th[root]
            out = self._api("conversations.replies", {"channel": ch, "ts": root, "oldest": t.get("cursor", root),
                                                      "inclusive": False, "limit": 100})
            replies = sorted((m for m in out.get("messages", []) if m.get("ts") != root), key=lambda m: float(m["ts"]))
            for m in replies:
                if self._is_user_msg(m):
                    self._inbox(m, thread_ts=root, last_speaker=None)
                    t["active"] = now
                    n += 1
            if replies:
                t["cursor"] = replies[-1]["ts"]
        return n

    def _is_user_msg(self, m: dict) -> bool:
        return (m.get("user") == self.user_id and not m.get("bot_id")
                and m.get("subtype") in (None, "thread_broadcast", "file_share"))

    def _inbox(self, m: dict, thread_ts: str | None, last_speaker: str | None) -> None:
        msg = {"ts": m.get("ts"), "thread_ts": thread_ts, "text": m.get("text", ""), "channel": self.channel,
               "received": now_iso(), "source": "slack", "last_speaker": last_speaker}
        (self.state.inbox / f"{str(m.get('ts', '0')).replace('.', '_')}.json").write_text(json.dumps(msg))
