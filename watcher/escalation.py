"""Failure fingerprints and the cross-job escalation map.

An array of 66 tasks dying the same way is one problem, not 66. Every failure gets a
fingerprint (klass + normalized last error line); the map in meta.json remembers, per
job name + fingerprint, whether Claude already escalated it and whether the user has
answered. Repeats skip the Claude turn. Past a threshold the job name is quarantined:
no resubmits, no turns, one Slack line, until the user lifts it.
"""
from __future__ import annotations

import re

from .state import State, now_iso

# klasses whose fingerprint is the klass alone: the log tail says nothing about *why*
_STATE_ONLY = {"PREEMPTED", "NODE_FAIL", "TIMEOUT", "BOOT_FAIL", "REQUEUE_HOLD", "TRANSIENT", "OOM", "UNKNOWN"}
# preferred first: a language-level exception line; then the scheduler/runtime trailers, which are generic
_ERR_LINES = [re.compile(r"^[\w.]*(?:Error|Exception|Exit)\b"),
              re.compile(r"^(?:slurmstepd:|srun:|Killed\b|Segmentation fault|Aborted|terminate called)")]
_NORM = [(re.compile(r"0x[0-9a-fA-F]+"), "0x#"), (re.compile(r"(?<![\w.])/[^\s'\"`,:;)]+"), "/…"), (re.compile(r"\d+"), "#"),
         (re.compile(r"\s+"), " ")]


def normalize(line: str) -> str:
    for rx, rep in _NORM:
        line = rx.sub(rep, line)
    return line.strip()[:120]


def fingerprint(klass: str, exit_code: str | None, tail: str) -> str:
    if klass in _STATE_ONLY:
        return klass
    lines = [l.strip() for l in (tail or "").splitlines() if l.strip()]
    pick = next((l for rx in _ERR_LINES for l in reversed(lines) if rx.match(l)), lines[-1] if lines else "")
    return f"{klass}:{normalize(pick)}" if pick else f"{klass}:exit={exit_code or '?'}"


def key(rec: dict, fp: str) -> str:
    return f"{rec.get('name') or rec['id'].split('_')[0]}|{fp}"


def is_failure(klass: str | None) -> bool:
    return klass not in (None, "OK", "PENDING", "COMPLETED", "CANCELLED", "STALLED")


class Escalations:
    """Thin view over state.meta['escalations'] and state.meta['quarantine']."""

    def __init__(self, state: State):
        self.state = state
        self.map: dict[str, dict] = state.meta.setdefault("escalations", {})
        self.quarantine: dict[str, dict] = state.meta.setdefault("quarantine", {})

    def get(self, k: str) -> dict:
        return self.map.setdefault(k, {"count": 0, "jobs": [], "nodes": [], "escalated": False, "resolved": False, "first": now_iso()})

    def count(self, rec: dict, k: str) -> dict:
        """Count this job once per key (handle() may see the same rec on several ticks)."""
        e = self.get(k)
        if rec.get("esc_key") != k:
            rec["esc_key"] = k
            e["count"] += 1
            e["last"] = now_iso()
            e["name"] = rec.get("name")
            if rec["id"] not in e["jobs"]:
                e["jobs"] = (e["jobs"] + [rec["id"]])[-50:]
            for n in (rec.get("nodes") or "").split(","):
                n = n.strip()
                if n and n not in ("None assigned", "") and n not in e["nodes"]:
                    e["nodes"].append(n)
        return e

    def mark(self, k: str, status: str, thread_ts: str | None = None) -> None:
        e = self.get(k)
        e["last_status"] = status
        if status == "ESCALATE":
            e["escalated"], e["resolved"] = True, False
        elif status in ("OK", "ACTED"):
            e["resolved"] = True
        if thread_ts:
            e["thread_ts"] = thread_ts

    def awaiting_user(self, k: str) -> bool:
        e = self.map.get(k)
        return bool(e and e.get("escalated") and not e.get("resolved"))

    def settled_for(self, k: str, rec: dict) -> str | None:
        """Claude already answered this fingerprint with OK/ACTED for a sibling of the same array (or the same
        lineage): the verdict carries over, no new turn. Returns the id of the job that got the turn, else None.
        Scoped to the same base job id so a *new* array failing the same way still gets a fresh look."""
        e = self.map.get(k)
        if not (e and e.get("resolved") and e.get("last_status") in ("OK", "ACTED")):
            return None
        base = rec["id"].split("_")[0]
        for j in e.get("jobs", []):
            if j.split("_")[0] == base and j != rec["id"]:
                return j
        return None

    def resolve_name(self, name: str | None) -> int:
        """The user replied about this job: every open escalation under its name is answered."""
        n = 0
        for e in self.map.values():
            if e.get("name") == name and e.get("escalated") and not e.get("resolved"):
                e["resolved"] = True
                n += 1
        return n

    def should_quarantine(self, e: dict, after: int, min_nodes: int) -> bool:
        return after > 0 and e["count"] >= after and len(e["nodes"]) >= min_nodes

    def put_quarantine(self, name: str, e: dict, k: str) -> None:
        self.quarantine[name] = {"since": now_iso(), "key": k, "count": e["count"], "nodes": list(e["nodes"])}

    def quarantined(self, name: str | None) -> dict | None:
        return self.quarantine.get(name or "")

    def lift(self, name: str) -> bool:
        if name in self.quarantine:
            del self.quarantine[name]
            self.resolve_name(name)
            return True
        return False
