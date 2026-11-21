"""Minimal Notion REST client for the hub, stdlib only. Rate limited below Notion's ~3 req/s average,
retries 429/5xx, and has a dry-run mode that records writes to a JSONL file instead of sending them."""
from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any

API = "https://api.notion.com/v1"
VERSION = "2022-06-28"
TEXT_CHUNK = 2000          # Notion's limit per rich_text object
MAX_RICH_TEXT = 100        # and per rich_text array


class NotionError(RuntimeError):
    def __init__(self, status: int, code: str, message: str):
        super().__init__(f"notion {status} {code}: {message}")
        self.status, self.code = status, code


class Notion:
    def __init__(self, token: str, min_interval: float = 0.4, dry_run_log: Path | None = None, log=print):
        self.token = token
        self.min_interval = min_interval
        self.dry_run_log = dry_run_log
        self.log = log
        self._last = 0.0
        self.calls = 0
        self._fake = 0

    # ------------------------------------------------------------------ transport
    def request(self, method: str, path: str, body: dict | None = None, write: bool = False) -> dict:
        if write and self.dry_run_log is not None:
            self._fake += 1
            with open(self.dry_run_log, "a") as f:
                f.write(json.dumps({"ts": datetime.now().isoformat(timespec="seconds"), "method": method,
                                    "path": path, "body": body}) + "\n")
            return {"id": f"dry-run-{self._fake}", "object": "page", "dry_run": True}
        data = json.dumps(body).encode() if body is not None else None
        for attempt in range(6):
            wait = self.min_interval - (time.time() - self._last)
            if wait > 0:
                time.sleep(wait)
            req = urllib.request.Request(f"{API}{path}", data=data, method=method, headers={
                "Authorization": f"Bearer {self.token}", "Notion-Version": VERSION, "Content-Type": "application/json"})
            self._last = time.time()
            self.calls += 1
            try:
                with urllib.request.urlopen(req, timeout=60) as r:
                    return json.loads(r.read() or b"{}")
            except urllib.error.HTTPError as e:
                try:
                    err = json.loads(e.read() or b"{}")
                except json.JSONDecodeError:
                    err = {}
                if e.code == 429 or e.code >= 500:
                    delay = float(e.headers.get("Retry-After") or 0) or min(60, 2 ** attempt)
                    time.sleep(delay)
                    continue
                raise NotionError(e.code, err.get("code", ""), err.get("message", str(e))) from None
            except (urllib.error.URLError, TimeoutError, ConnectionError):
                time.sleep(min(60, 2 ** attempt))
                continue
        raise NotionError(0, "retries_exhausted", f"{method} {path}")

    # --------------------------------------------------------------------- pages
    def query(self, database_id: str, filter: dict | None = None, sorts: list | None = None, limit: int | None = None) -> list[dict]:
        out, cursor = [], None
        while True:
            body: dict[str, Any] = {"page_size": 100}
            if filter:
                body["filter"] = filter
            if sorts:
                body["sorts"] = sorts
            if cursor:
                body["start_cursor"] = cursor
            r = self.request("POST", f"/databases/{database_id}/query", body)
            out += r.get("results", [])
            if limit and len(out) >= limit:
                return out[:limit]
            if not r.get("has_more"):
                return out
            cursor = r.get("next_cursor")

    def create(self, database_id: str, props: dict, children: list | None = None) -> dict:
        body: dict[str, Any] = {"parent": {"database_id": database_id}, "properties": props}
        if children:
            body["children"] = children
        return self.request("POST", "/pages", body, write=True)

    def update(self, page_id: str, props: dict) -> dict:
        return self.request("PATCH", f"/pages/{page_id}", {"properties": props}, write=True)

    def archive(self, page_id: str) -> dict:
        return self.request("PATCH", f"/pages/{page_id}", {"archived": True}, write=True)

    def child_pages(self, block_id: str) -> list[dict]:
        out, cursor = [], None
        while True:
            q = f"?page_size=100" + (f"&start_cursor={cursor}" if cursor else "")
            r = self.request("GET", f"/blocks/{block_id}/children{q}")
            out += [b for b in r.get("results", []) if b.get("type") == "child_page"]
            if not r.get("has_more"):
                return out
            cursor = r.get("next_cursor")

    def me(self) -> dict:
        return self.request("GET", "/users/me")


# ---------------------------------------------------------------- property builders
def rich(text: str | None) -> list[dict]:
    text = "" if text is None else str(text)
    chunks = [text[i:i + TEXT_CHUNK] for i in range(0, len(text), TEXT_CHUNK)][:MAX_RICH_TEXT]
    return [{"type": "text", "text": {"content": c}} for c in chunks]


def P_title(text: str) -> dict:
    return {"title": rich(text)[:1] or []}


def P_text(text: str | None) -> dict:
    return {"rich_text": rich(text)}


def P_num(x: float | int | None) -> dict:
    return {"number": None if x is None else round(float(x), 4)}


def P_select(name: str | None) -> dict:
    return {"select": {"name": name} if name else None}


def P_date(iso: str | None) -> dict:
    return {"date": {"start": iso} if iso else None}


def P_check(b: bool) -> dict:
    return {"checkbox": bool(b)}


def P_url(u: str | None) -> dict:
    return {"url": u or None}


def P_rel(ids: list[str] | str | None) -> dict:
    if isinstance(ids, str):
        ids = [ids]
    return {"relation": [{"id": i} for i in (ids or []) if i]}


# ----------------------------------------------------------------- property readers
def read(page: dict, name: str) -> Any:
    """Plain value of a property: str for text/title/select/url, float for number, iso str for date,
    bool for checkbox, list[str] of page ids for relation."""
    p = (page.get("properties") or {}).get(name)
    if not p:
        return None
    t = p.get("type")
    if t in ("title", "rich_text"):
        return "".join(x.get("plain_text") or x.get("text", {}).get("content", "") for x in p.get(t) or [])
    if t == "number":
        return p.get("number")
    if t == "select":
        return (p.get("select") or {}).get("name")
    if t == "date":
        return (p.get("date") or {}).get("start")
    if t == "checkbox":
        return bool(p.get("checkbox"))
    if t == "url":
        return p.get("url")
    if t == "relation":
        return [r["id"] for r in p.get("relation") or []]
    if t == "last_edited_by":
        return (p.get("last_edited_by") or {}).get("id")
    return None


def norm_id(i: str) -> str:
    """Notion ids with or without dashes -> dashed lowercase uuid."""
    h = i.replace("-", "").lower()[-32:]
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}" if len(h) == 32 else i
