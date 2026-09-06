"""Status table + daily digest. Rendered as monospace text for Slack code blocks / terminal."""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta
from typing import Callable

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


def _chat(m: dict) -> str:
    if not m.get("slack_inbound"):
        return "off"
    if m.get("slack_poll_error"):
        return f"ERROR ({m['slack_poll_error'][:60]})"
    return f"on (polled {_ago(m.get('slack_last_poll')) or '?'} ago)"


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
          f"slack chat {_chat(m)}, last poll {_ago(m.get('last_poll')) or '?'} ago")
    l2 = (f"running {sum(1 for r in live if norm_state(r['state']) == 'RUNNING')} · pending "
          f"{sum(1 for r in live if norm_state(r['state']) != 'RUNNING')} · done(24h) {done} · failed(24h) {failed}"
          + (f" · NEEDS YOU: {', '.join(waiting)}" if waiting else "")
          + (f" · QUARANTINED: {', '.join(m['quarantine'])}" if m.get("quarantine") else ""))
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


# ---------------------------------------------------------------- digest (grouped summary)
_FAILED = ("FAILED", "OUT_OF_MEMORY", "TIMEOUT", "NODE_FAIL", "PREEMPTED", "BOOT_FAIL")
_SHORT = {"OUT_OF_MEMORY": "OOM", "COMPLETED": "done", "CANCELLED": "cancelled", "PREEMPTED": "preempted", "FAILED": "failed",
          "TIMEOUT": "timed out", "NODE_FAIL": "node fail", "RUNNING": "running", "PENDING": "pending"}
_CAP = 6


def _name(r: dict) -> str:
    return r.get("name") or r["id"].split("_")[0]


def _suffix(r: dict) -> str:
    """'_22' for an array task, else the job id."""
    return "_" + r["id"].split("_", 1)[1] if "_" in r["id"] else r["id"]


def _ids(recs: list[dict], cap: int = 4) -> str:
    ids = [_suffix(r) for r in recs]
    return ", ".join(ids[:cap]) + (f" +{len(ids) - cap}" if len(ids) > cap else "")


def _latest(recs: list[dict]) -> str:
    return max((r.get("last_change") or r.get("first_seen") or "" for r in recs), default="")


def _error(r: dict) -> str:
    fp = r.get("fingerprint") or ""
    if ":" in fp:
        return fp.split(":", 1)[1]
    ev = r.get("evidence") or ""
    return "" if r.get("klass") in ("OK", "PENDING", "COMPLETED") else ev


def _thread(state: State, recs: list[dict]) -> str | None:
    for r in recs:
        for cand in (r, state.jobs.get(state.lineage_root(r["id"]), {}), state.jobs.get(r["id"].split("_")[0], {})):
            if cand.get("thread_ts"):
                return cand["thread_ts"]
    return None


def _states(recs: list[dict]) -> str:
    c = Counter(_SHORT.get(norm_state(r.get("state")), norm_state(r.get("state")).lower()) for r in recs)
    return ", ".join(f"{n} {st}" for st, n in c.most_common())


def _by_name(recs: list[dict]) -> dict[str, list[dict]]:
    g: dict[str, list[dict]] = {}
    for r in recs:
        g.setdefault(_name(r), []).append(r)
    return g


def _section(title: str, lines: list[str], more: int = 0) -> str:
    body = "\n".join(lines) if lines else "  (none)"
    if more > 0:
        body += f"\n  … +{more} more"
    return f"*{title}*\n{body}"


def _link(link: Callable[[str], str | None] | None, ts: str | None) -> str:
    if not (link and ts):
        return ""
    url = link(ts)
    return f" · <{url}|thread>" if url else ""


def summary(state: State, target: str, link: Callable[[str], str | None] | None = None,
            prev_needs: set[str] | None = None) -> str:
    """Scannable daily digest: one line per problem (job-name lineage), ordered by what the user must do.
    `link(thread_ts) -> permalink` decorates NEEDS-YOU items; `prev_needs` marks names that are new since the last digest."""
    m = state.meta
    jobs = list(state.jobs.values())
    cutoff = datetime.now().astimezone() - timedelta(hours=24)
    recent = [r for r in jobs if _within(r, cutoff)]
    live = [r for r in jobs if is_live(r.get("state", ""))]
    running = [r for r in live if norm_state(r["state"]) == "RUNNING"]
    pending = [r for r in live if norm_state(r["state"]) != "RUNNING"]
    finished = [r for r in recent if not is_live(r.get("state", ""))]
    done = [r for r in finished if norm_state(r["state"]) == "COMPLETED"]
    failed = [r for r in finished if norm_state(r["state"]) in _FAILED]
    cancelled = [r for r in finished if norm_state(r["state"]) == "CANCELLED"]

    up = _ago(m.get("watcher_started")) if m.get("watcher_started") else "?"
    where = target.rstrip("/").rsplit("/", 1)[-1] + (f"@{m['cluster']}" if m.get("cluster") else "")
    out = [f"Daily digest · {datetime.now().astimezone():%a %b %-d}\n"
           f"{where} · watcher {m.get('watcher_job', '?')} up {up} · chat {_chat(m)} · "
           f"{len(running)} running · {len(pending)} pending · 24h: {len(done)} done, {len(failed)} failed"
           + (f", {len(cancelled)} cancelled" if cancelled else "")]

    # -- needs you: open escalations, one line per job name, newest first
    open_ = _by_name([r for r in jobs if r.get("escalated") and not r.get("resolved")])
    items = sorted(open_.items(), key=lambda kv: _latest(kv[1]), reverse=True)
    lines = []
    for name, recs in items[:_CAP]:
        new = " (new)" if prev_needs is not None and name not in prev_needs else ""
        lines.append(f"• {name}{new}  {_states(recs)} · {_ids(recs)} · {_ago(_latest(recs))} ago{_link(link, _thread(state, recs))}")
        err = next((e for e in (_error(r) for r in recs) if e), "")
        if err:
            lines.append(f"    {err[:110]}")
    out.append(_section(f"NEEDS YOU ({len(open_)})" if open_ else "NEEDS YOU", lines, len(items) - _CAP))

    # -- fixed automatically in the last 24h: deterministic resubmits + Claude ACTED turns
    def _fixed_at(r: dict) -> str:
        return (r.get("fixes") or [{}])[-1].get("at", "") if r.get("fixes") else (r.get("last_change") or "")
    auto = [r for r in jobs if (r.get("fixes") or r.get("last_status") == "ACTED")
            and _fixed_at(r) and datetime.fromisoformat(_fixed_at(r)) >= cutoff]
    lines = []
    fixed = sorted(_by_name(auto).items(), key=lambda kv: _latest(kv[1]), reverse=True)
    for name, recs in fixed[:_CAP]:
        reasons = Counter()
        for r in recs:
            for f in r.get("fixes") or []:
                reasons[(f.get("reason") or f.get("action") or "?").split(":")[0]] += 1
            if r.get("last_status") == "ACTED":
                reasons["patched by Claude"] += 1
        why = ", ".join(f"{k}{f' ×{n}' if n > 1 else ''}" for k, n in reasons.most_common(3))
        succ = [f["new_id"] for r in recs for f in (r.get("fixes") or []) if f.get("new_id")]
        outcome = ""
        if succ:
            st = Counter(_SHORT.get(norm_state(state.jobs.get(i, {}).get("state", "")), "?") for i in succ)
            outcome = " → " + ", ".join(f"{n} {s}" for s, n in st.most_common())
        lines.append(f"• {name}  {len(recs)} job{'s' if len(recs) != 1 else ''}: {why}{outcome}")
    out.append(_section("FIXED AUTOMATICALLY (24h)", lines, len(fixed) - _CAP))

    # -- finished: per name, failures first
    fin = sorted(_by_name(finished).items(),
                 key=lambda kv: (-sum(norm_state(r["state"]) in _FAILED for r in kv[1]), -len(kv[1])))
    lines = [f"• {name}  {_states(recs)}" for name, recs in fin[:_CAP]]
    out.append(_section(f"FINISHED (24h): {len(done)} done · {len(failed)} failed · {len(cancelled)} cancelled", lines, len(fin) - _CAP))

    # -- running: per name with the longest elapsed/limit
    run = sorted(_by_name(running).items(), key=lambda kv: -len(kv[1]))
    lines = []
    for name, recs in run[:_CAP]:
        longest = max(recs, key=lambda r: r.get("elapsed") or "")
        lines.append(f"• {name}  {len(recs)} running · longest {longest.get('elapsed') or '?'}/{_short_limit(longest.get('timelimit'))}")
    out.append(_section(f"RUNNING ({len(running)})", lines, len(run) - _CAP))

    # -- pending: one line
    if pending:
        oldest = min(pending, key=lambda r: r.get("first_seen") or "")
        reasons = Counter((r.get("reason") or "").strip() for r in pending)
        reasons = Counter({k: v for k, v in reasons.items() if k and k not in ("None", "Priority", "Resources")})
        why = ("; " + ", ".join(f"{k} ×{n}" for k, n in reasons.most_common(2))) if reasons else ""
        out.append(f"*PENDING*: {len(pending)} across {len(_by_name(pending))} job names, oldest {_ago(oldest.get('first_seen'))} "
                   f"({_name(oldest)}){why}")

    tail = []
    if m.get("quarantine"):
        tail.append(f"*QUARANTINED*: {', '.join(m['quarantine'])} (reply `unquarantine <name>`)")
    try:
        n_orders = sum(1 for l in state.orders.read_text().splitlines() if l.startswith("- "))
    except OSError:
        n_orders = 0
    if n_orders:
        tail.append(f"Standing orders: {n_orders} (see standing_orders.md)")
    tail.append("Full table: reply `status` (or `status 48h`).")
    out.append("\n".join(tail))
    return "\n\n".join(out)


def open_needs(state: State) -> set[str]:
    return {_name(r) for r in state.jobs.values() if r.get("escalated") and not r.get("resolved")}


def digest(state: State, target: str, link: Callable[[str], str | None] | None = None) -> str:
    return summary(state, target, link=link, prev_needs=state.meta.get("digest_prev_needs") and set(state.meta["digest_prev_needs"]))
