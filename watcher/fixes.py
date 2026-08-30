"""Tier-0/1 deterministic fixes: resubmit, OOM memory bump / batch halving. Records everything."""
from __future__ import annotations

import difflib
import re
from pathlib import Path

from . import slurm
from .config import get
from .state import State, now_iso


class FixError(Exception):
    pass


def _resubmit_args(rec: dict) -> list[str]:
    args = slurm.submit_line_args(rec.get("submit_line", ""))
    if not args:
        raise FixError("no SubmitLine recorded; cannot resubmit exactly")
    # a single failed array task is resubmitted as a 1-element array
    if "_" in rec["id"]:
        idx = rec["id"].split("_", 1)[1]
        if idx.isdigit():
            args = slurm.set_flag(args, "--array", idx)
    return args


def resubmit(state: State, rec: dict, extra_args: list[str] | None = None, reason: str = "") -> str:
    args = _resubmit_args(rec)
    for flag_val in extra_args or []:
        flag, _, val = flag_val.partition("=")
        args = slurm.set_flag(args, flag, val)
    new_id = slurm.sbatch(args, cwd=rec["workdir"])
    root = state.lineage_root(rec["id"])
    state.jobs[new_id] = {
        "id": new_id, "parent": rec["id"], "name": rec.get("name"), "workdir": rec["workdir"],
        "submit_line": "sbatch " + " ".join(args), "script": rec.get("script"), "directives": rec.get("directives", {}),
        "state": "PENDING", "first_seen": now_iso(), "resubmitted_by_watcher": True, "thread_ts": state.jobs.get(root, {}).get("thread_ts"),
    }
    rec["fixes"] = rec.get("fixes", []) + [{"at": now_iso(), "action": "resubmit", "new_id": new_id, "args": extra_args or [], "reason": reason}]
    rec["handled"] = True
    return new_id


def oom_fix(cfg: dict, state: State, rec: dict) -> tuple[str, str]:
    """Returns (new_job_id, description)."""
    d = rec.get("directives", {})
    batch_arg = d.get("batch_arg")
    if batch_arg and rec.get("script"):
        desc = _halve_batch(state, rec, batch_arg)
        new_id = resubmit(state, rec, reason=f"OOM: {desc}")
        return new_id, desc
    factor = float(d.get("mem_bump", get(cfg, "watcher.mem_bump", 1.5)))
    cap = float(get(cfg, "watcher.max_mem_gb", 480))
    cur = slurm.parse_mem_gb(rec.get("req_mem", "")) or 0
    if cur <= 0:
        raise FixError("cannot determine current --mem")
    new = min(cap, cur * factor)
    if new <= cur + 0.5:
        raise FixError(f"--mem already at cap ({cur:.0f}G)")
    new_id = resubmit(state, rec, extra_args=[f"--mem={int(round(new))}G"], reason="OOM: mem bump")
    return new_id, f"--mem {cur:.0f}G -> {int(round(new))}G"


def _halve_batch(state: State, rec: dict, batch_arg: str) -> str:
    script = Path(rec["script"])
    old = script.read_text()
    rx = re.compile(rf"({re.escape(batch_arg)}[=\s]+)(\d+)")
    m = rx.search(old)
    if not m:
        raise FixError(f"{batch_arg} not found in {script}")
    n = int(m.group(2))
    if n <= 1:
        raise FixError(f"{batch_arg} already 1")
    new = rx.sub(lambda mm: f"{mm.group(1)}{max(1, n // 2)}", old, count=1)
    record_patch(state, rec["id"], str(script), old, new, "OOM: halve batch")
    script.write_text(new)
    return f"{batch_arg} {n} -> {max(1, n // 2)} in {script.name}"


def record_patch(state: State, job_id: str, path: str, old: str, new: str, reason: str) -> Path:
    diff = "".join(difflib.unified_diff(old.splitlines(True), new.splitlines(True), f"a/{path}", f"b/{path}"))
    stamp = now_iso().replace(":", "").replace("+", "p")
    p = state.patches / f"{stamp}_{job_id}.diff"
    p.write_text(f"# job {job_id}: {reason}\n# {path}\n{diff}")
    return p
