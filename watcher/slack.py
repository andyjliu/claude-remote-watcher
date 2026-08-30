"""Slack DM to a single user. Outbound is stdlib-only; inbound (Socket Mode) needs slack_sdk."""
from __future__ import annotations

import json
import re
import threading
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
        self.app_token = _read(cfg_path(cfg, "slack.app_token_file"))
        self.enabled = bool(self.token and self.user_id)
        self.channel: str | None = state.meta.get("slack_channel")
        self._listener: threading.Thread | None = None
        # Several clusters (and directories) may share this bot and this DM channel, so every
        # outbound message is prefixed "[<cluster>]" and inbound DMs can be addressed to one cluster.
        self.tag = cluster_name(cfg)
        self.peers: set[str] = {str(x).lower() for x in (get(cfg, "slack.peer_clusters") or [])}
        self.peers |= {str(x).lower() for x in state.meta.get("peer_clusters", [])}
        self.peers.discard(self.tag.lower())

    # ---- multi-cluster routing --------------------------------------------
    _ADDR = re.compile(r"^(@)?([A-Za-z][\w.-]*)(\s*[:,]\s*|\s+|$)(.*)$", re.S)

    def address(self, text: str) -> tuple[str | None, str]:
        """Parse a leading '@name', 'name:' or 'name <cmd>' where name is this cluster, a known peer,
        or 'all'/'everyone' (those two need the '@' or ':' form so 'all done' stays plain text).
        Returns (name.lower() or None, remaining text)."""
        m = self._ADDR.match(text.strip())
        if not m:
            return None, text
        at, name, sep, rest = m.groups()
        name_l = name.lower()
        if name_l in ("all", "everyone"):
            return (name_l, rest.strip()) if (at or ":" in sep or "," in sep) else (None, text)
        if name_l == self.tag.lower() or name_l in self.peers:
            return name_l, rest.strip()
        return None, text

    def for_us(self, name: str | None) -> bool:
        return name is None or name in ("all", "everyone") or name == self.tag.lower()

    def _remember(self, key: str, ts: str | None, cap: int = 400) -> None:
        if not ts:
            return
        lst = self.state.meta.setdefault(key, [])
        if ts not in lst:
            lst.append(ts)
            del lst[:-cap]

    def remember_user_ts(self, ts: str | None) -> None:
        """Every user DM reaches every cluster; remember its ts so a thread the user starts on their
        own message is not mistaken for another cluster's job thread."""
        self._remember("seen_user_ts", ts)

    def is_our_thread(self, thread_ts: str, jobs: dict) -> bool:
        if any(r.get("thread_ts") == thread_ts for r in jobs.values()):
            return True
        return thread_ts in self.state.meta.get("posted_ts", []) or thread_ts in self.state.meta.get("seen_user_ts", [])

    def _learn_peer(self, text: str) -> None:
        m = re.match(r"^\[([A-Za-z][\w.-]*)\]\s", text or "")
        if m and m.group(1).lower() != self.tag.lower() and m.group(1).lower() not in self.peers:
            self.peers.add(m.group(1).lower())
            self.state.meta["peer_clusters"] = sorted(self.peers)
            self.state.logline(f"slack: learned peer cluster {m.group(1)!r} from its posts")

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
        return ts

    # ---- inbound --------------------------------------------------------
    def start_listener(self) -> bool:
        """Socket Mode listener writing DMs to <state>/inbox/*.json. Returns whether it started."""
        if not (self.enabled and self.app_token):
            return False
        try:
            from slack_sdk.socket_mode import SocketModeClient
            from slack_sdk.socket_mode.request import SocketModeRequest
            from slack_sdk.socket_mode.response import SocketModeResponse
            from slack_sdk.web import WebClient
        except ImportError:
            self.state.logline("slack_sdk not installed; inbound Slack disabled")
            return False

        client = SocketModeClient(app_token=self.app_token, web_client=WebClient(token=self.token))

        def handle(c: SocketModeClient, req: SocketModeRequest) -> None:
            if req.type == "events_api":
                c.send_socket_mode_response(SocketModeResponse(envelope_id=req.envelope_id))
                ev = req.payload.get("event", {})
                if ev.get("type") == "message" and ev.get("channel_type") == "im" and ev.get("bot_id") and not ev.get("thread_ts"):
                    self._learn_peer(ev.get("text", ""))  # another cluster's "[name] ..." post
                if (ev.get("type") == "message" and ev.get("channel_type") == "im"
                        and ev.get("user") == self.user_id and not ev.get("bot_id") and not ev.get("subtype")):
                    msg = {"ts": ev.get("ts"), "thread_ts": ev.get("thread_ts"), "text": ev.get("text", ""),
                           "channel": ev.get("channel"), "received": now_iso(), "source": "slack"}
                    (self.state.inbox / f"{ev.get('ts', '0').replace('.', '_')}.json").write_text(json.dumps(msg))

        client.socket_mode_request_listeners.append(handle)

        def run() -> None:
            try:
                client.connect()
                threading.Event().wait()  # keep thread alive; client reconnects on its own
            except Exception as e:  # noqa: BLE001
                self.state.logline(f"slack listener died: {e}")

        self._listener = threading.Thread(target=run, name="slack-listener", daemon=True)
        self._listener.start()
        return True
