"""Status table + daily digest. Rendered as monospace text for Slack code blocks / terminal."""
from __future__ import annotations

from datetime import datetime, timedelta

from .slurm import is_live, norm_state
from .state import State

_ORDER = ["RUNNING", "PENDING", "FAILED", "OUT_OF_MEMORY", "TIMEOUT", "PREEMPTED", "NODE_FAIL", "CANCELLED", "COMPLETED"]


def _ago(iso: str | None) -> str:
    if not iso:
        return ""
    try:
        d = datetime.now().astimezone() - datetime.fromisoformat(iso)
    except ValueError:
        return ""
    s = int(d.total_seconds())
    if s < 90:
        return f"{s}s"
    if s < 5400:
        return f"{s // 60}m"
    if s < 172800:
        return f"{s // 3600}h"
    return f"{s // 86400}d"


def _short_limit(t: str) -> str:
    t = t or ""
    return t[:-3] if t.endswith(":00") and t.count(":") == 2 else t


def _class_label(r: dict) -> str:
    k = r.get("klass") or ""
    if r.get("escalated") and not r.get("resolved"):
        return "NEEDS-YOU"
    if r.get("last_status") == "ACTED":
        return "FIXED"
    return {"OK": "", "PENDING": "", "COMPLETED": "", "CRASHED_TRIVIAL": "TRIVIAL"}.get(k, k)


def _note(r: dict) -> str:
    fixes = r.get("fixes") or []
    if fixes:
        f = fixes[-1]
        return f"{f.get('action')}->{f.get('new_id')} " + " ".join(f.get("args") or []) + (f" ({f.get('reason')})" if f.get("reason") else "")
    if r.get("parent"):
        return f"retry of {r['parent']}"
    if r.get("evidence") and r.get("klass") not in ("OK", "PENDING", "COMPLETED"):
        return r["evidence"]
    reason = r.get("reason") or ""
    return "" if reason == "None" or not is_live(r.get("state", "")) else reason


def header(state: State, target: str) -> str:
    m = state.meta
    live = [r for r in state.jobs.values() if is_live(r.get("state", ""))]
    cutoff = datetime.now().astimezone() - timedelta(hours=24)
    recent = [r for r in state.jobs.values() if _within(r, cutoff)]
    done = sum(1 for r in recent if norm_state(r.get("state")) == "COMPLETED")
    failed = sum(1 for r in recent if norm_state(r.get("state")) in ("FAILED", "OUT_OF_MEMORY", "TIMEOUT", "NODE_FAIL", "PREEMPTED"))
    waiting = [r["id"] for r in state.jobs.values() if r.get("escalated") and not r.get("resolved")]
    up = _ago(m.get("watcher_started")) if m.get("watcher_started") else "?"
    where = target.rstrip('/').rsplit('/', 1)[-1] + (f"@{m['cluster']}" if m.get("cluster") else "")
    l1 = (f"{where} — watcher {m.get('watcher_job', '?')} up {up}, "
          f"slack chat {'on' if m.get('slack_inbound') else 'off'}, last poll {_ago(m.get('last_poll')) or '?'} ago")
    l2 = (f"running {sum(1 for r in live if norm_state(r['state']) == 'RUNNING')} · pending "
          f"{sum(1 for r in live if norm_state(r['state']) != 'RUNNING')} · done(24h) {done} · failed(24h) {failed}"
          + (f" · NEEDS YOU: {', '.join(waiting)}" if waiting else ""))
    return l1 + "\n" + l2


def _within(r: dict, cutoff: datetime | None) -> bool:
    if cutoff is None or is_live(r.get("state", "")):
        return True
    seen = r.get("last_change") or r.get("first_seen") or ""
    try:
        return bool(seen) and datetime.fromisoformat(seen) >= cutoff
    except ValueError:
        return True


def table(state: State, since_hours: float | None = 24, max_rows: int = 40) -> str:
    cutoff = datetime.now().astimezone() - timedelta(hours=since_hours) if since_hours else None
    rows = [r for r in state.jobs.values() if _within(r, cutoff)]
    rows.sort(key=lambda r: ((0 if (r.get("escalated") and not r.get("resolved")) else 1),
                             _ORDER.index(norm_state(r.get("state"))) if norm_state(r.get("state")) in _ORDER else 99, r["id"]))
    if not rows:
        return "(no jobs in the last %sh)" % (int(since_hours) if since_hours else "∞")
    hdr = f"{'job':<13} {'name':<30} {'state':<9} {'class':<10} {'when':>5} {'elapsed/limit':<19} {'node':<12} {'att':<3} note"
    lines = [hdr]
    for r in rows[:max_rows]:
        st = norm_state(r.get("state"))
        st = {"OUT_OF_MEMORY": "OOM", "COMPLETED": "DONE", "CANCELLED": "CANCEL", "PREEMPTED": "PREEMPT"}.get(st, st)[:9]
        att = f"{r.get('attempts')}" if r.get("attempts") else ""
        el = f"{(r.get('elapsed') or '')}/{_short_limit(r.get('timelimit'))}"
        lines.append(f"{r['id']:<13} {(r.get('name') or '')[:30]:<30} {st:<9} {_class_label(r)[:10]:<10} {_ago(r.get('last_change') or r.get('first_seen')):>5} "
                     f"{el:<19} {(r.get('nodes') or '')[:12]:<12} {att:<3} {_note(r)[:60]}")
    if len(rows) > max_rows:
        lines.append(f"... {len(rows) - max_rows} more")
    return "\n".join(lines)


def status(state: State, target: str, since_hours: float | None = 24) -> str:
    return header(state, target) + "\n```\n" + table(state, since_hours) + "\n```"


def digest(state: State, target: str) -> str:
    waiting = [r["id"] for r in state.jobs.values() if r.get("escalated") and not r.get("resolved")]
    tail = ("\nAwaiting you: " + ", ".join(waiting)) if waiting else "\nNothing awaiting you."
    return f"Daily digest\n{status(state, target, 24)}{tail}"
