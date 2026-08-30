"""Slack DM to a single user. Outbound is stdlib-only; inbound (Socket Mode) needs slack_sdk."""
from __future__ import annotations

import json
import threading
import urllib.request
from pathlib import Path

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
        payload = {"channel": self._ensure_channel(), "text": text[:3800], "unfurl_links": False}
        if thread_ts:
            payload["thread_ts"] = thread_ts
        try:
            return self._api("chat.postMessage", payload).get("ts")
        except Exception as e:  # noqa: BLE001
            self.state.logline(f"slack post failed: {e}")
            return None

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
