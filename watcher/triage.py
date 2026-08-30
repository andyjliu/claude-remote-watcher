"""Deterministic classification of a job into (klass, tier, evidence). No LLM here."""
from __future__ import annotations

import re
import time

from . import logs
from .config import get
from .slurm import norm_state

# klass -> tier. Tier 0/1 are fixed deterministically, 2/3 go to Claude, None = nothing to do.
TIER = {
    "OK": None, "PENDING": None, "COMPLETED": None, "CANCELLED": None,
    "PREEMPTED": 0, "NODE_FAIL": 0, "TIMEOUT": 0, "BOOT_FAIL": 0, "REQUEUE_HOLD": 0, "TRANSIENT": 0,
    "OOM": 1,
    "CRASHED_TRIVIAL": 2,
    "CRASHED": 3, "STALLED": 3, "UNKNOWN": 3,
}


def _any(patterns: list[str], text: str) -> str | None:
    for pat in patterns or []:
        m = re.search(pat, text)
        if m:
            return m.group(0)
    return None


def classify(cfg: dict, rec: dict) -> tuple[str, int | None, str]:
    """rec is a jobs.json record (already refreshed from sacct this tick)."""
    st = norm_state(rec.get("state", ""))
    stdout = rec.get("stdout_path")
    tail = logs.tail(stdout, int(get(cfg, "watcher.log_tail_lines", 150)))

    if st in ("PENDING", "REQUEUED", "CONFIGURING", "SUSPENDED"):
        return "PENDING", None, rec.get("reason", "")
    if st == "RUNNING" or st == "COMPLETING":
        stall_min = float(rec.get("directives", {}).get("stall_min", get(cfg, "watcher.stall_min", 60)))
        m = logs.mtime(stdout)
        if m and stall_min > 0 and (time.time() - m) > stall_min * 60 and not rec.get("stall_reported"):
            # a still-running job whose log stopped growing
            pattern = rec.get("directives", {}).get("expect_pattern")
            if not pattern or not re.search(pattern, tail):
                return "STALLED", TIER["STALLED"], f"stdout unchanged for {int((time.time()-m)/60)} min"
        return "OK", None, ""
    if st == "COMPLETED":
        return "COMPLETED", None, ""
    if st == "CANCELLED":
        return "CANCELLED", None, rec.get("state", "")
    if st in ("PREEMPTED", "NODE_FAIL", "BOOT_FAIL", "TIMEOUT"):
        return st, TIER[st], f"sacct state {rec.get('state')}"
    if st == "OUT_OF_MEMORY":
        return "OOM", TIER["OOM"], "sacct state OUT_OF_MEMORY"
    if st == "FAILED":
        hit = _any(get(cfg, "watcher.oom_patterns", []), tail)
        if hit:
            return "OOM", TIER["OOM"], f"log matched {hit!r}"
        hit = _any(get(cfg, "watcher.trivial_patterns", []), tail)
        if hit:
            return "CRASHED_TRIVIAL", TIER["CRASHED_TRIVIAL"], f"log matched {hit!r}"
        return "CRASHED", TIER["CRASHED"], f"exit {rec.get('exit_code')}"
    return "UNKNOWN", TIER["UNKNOWN"], f"unhandled sacct state {rec.get('state')!r}"
