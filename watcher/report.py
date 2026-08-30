"""Status table + daily digest."""
from __future__ import annotations

from datetime import datetime, timedelta

from .slurm import norm_state
from .state import State

_ORDER = ["RUNNING", "PENDING", "FAILED", "OUT_OF_MEMORY", "TIMEOUT", "PREEMPTED", "NODE_FAIL", "CANCELLED", "COMPLETED"]


def table(state: State, since_hours: float | None = None, max_rows: int = 40) -> str:
    cutoff = datetime.now().astimezone() - timedelta(hours=since_hours) if since_hours else None
    rows = []
    for rec in state.jobs.values():
        if cutoff:
            seen = rec.get("last_change") or rec.get("first_seen") or ""
            try:
                if seen and datetime.fromisoformat(seen) < cutoff and not (rec.get("state") in ("RUNNING", "PENDING")):
                    continue
            except ValueError:
                pass
        rows.append(rec)
    rows.sort(key=lambda r: (_ORDER.index(norm_state(r.get("state"))) if norm_state(r.get("state")) in _ORDER else 99, r["id"]))
    counts: dict[str, int] = {}
    for r in rows:
        counts[norm_state(r.get("state"))] = counts.get(norm_state(r.get("state")), 0) + 1
    lines = [f"{'job':<14} {'name':<28} {'state':<12} {'elapsed/limit':<20} {'att':<3} last action"]
    for r in rows[:max_rows]:
        last = (r.get("fixes") or [{}])[-1]
        act = last.get("action", "") + (f"->{last.get('new_id')}" if last.get("new_id") else "")
        if r.get("klass") in ("CRASHED", "STALLED", "UNKNOWN") and r.get("escalated"):
            act = "ESCALATED " + act
        lines.append(f"{r['id']:<14} {(r.get('name') or '')[:28]:<28} {norm_state(r.get('state')):<12} "
                     f"{(r.get('elapsed') or '')+'/'+(r.get('timelimit') or ''):<20} {str(r.get('attempts', '')) if r.get('attempts') else '':<3} {act}")
    if len(rows) > max_rows:
        lines.append(f"... {len(rows) - max_rows} more")
    lines.append("counts: " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))
    return "\n".join(lines)


def digest(state: State, target: str) -> str:
    t = table(state, since_hours=24)
    waiting = [r["id"] for r in state.jobs.values() if r.get("escalated") and not r.get("resolved")]
    head = f"Daily digest for {target}"
    tail = ("\nAwaiting you: " + ", ".join(waiting)) if waiting else "\nNothing awaiting you."
    return f"{head}\n```\n{t}\n```{tail}"
