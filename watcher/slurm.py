"""Thin wrappers over sacct/squeue/sbatch/scontrol. Everything returns plain dicts."""
from __future__ import annotations

import os
import re
import shlex
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

SACCT_FIELDS = [
    "JobID", "JobName", "State", "ExitCode", "WorkDir", "StdOut", "StdErr", "Submit", "Start",
    "End", "Elapsed", "Timelimit", "ReqMem", "MaxRSS", "NodeList", "Partition", "QOS",
    "SubmitLine", "Reason", "DerivedExitCode",
]
_LIVE = {"PENDING", "RUNNING", "REQUEUED", "SUSPENDED", "COMPLETING", "CONFIGURING", "RESIZING"}


def run(cmd: list[str], timeout: int = 60, **kw) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, **kw)


def is_live(state: str) -> bool:
    return state.split()[0] in _LIVE if state else False


def norm_state(state: str) -> str:
    """'CANCELLED by 123' -> 'CANCELLED'; 'OUT_OF_MEMORY' stays."""
    return (state or "UNKNOWN").split()[0]


def sacct(user: str, since: datetime) -> list[dict]:
    fmt = ",".join(f"{f}%400" if f in ("SubmitLine", "StdOut", "StdErr", "WorkDir") else f for f in SACCT_FIELDS)
    cp = run(["sacct", "-u", user, "-X", "--parsable2", "--noheader", "-S",
              since.strftime("%Y-%m-%dT%H:%M:%S"), "--format", fmt], timeout=120)
    if cp.returncode != 0:
        raise RuntimeError(f"sacct failed: {cp.stderr.strip()}")
    rows = []
    for line in cp.stdout.splitlines():
        parts = line.split("|")
        if len(parts) < len(SACCT_FIELDS):
            continue
        rows.append(dict(zip(SACCT_FIELDS, parts)))
    return rows


def squeue(user: str) -> dict[str, dict]:
    cp = run(["squeue", "-u", user, "-h", "-o", "%i|%j|%T|%r|%Z|%M|%l|%N"])
    out = {}
    for line in cp.stdout.splitlines():
        p = line.split("|")
        if len(p) >= 8:
            out[p[0]] = dict(id=p[0], name=p[1], state=p[2], reason=p[3], workdir=p[4],
                             elapsed=p[5], timelimit=p[6], nodes=p[7])
    return out


def scontrol_job(job_id: str) -> dict:
    cp = run(["scontrol", "show", "job", job_id])
    out = {}
    for m in re.finditer(r"(\w+)=(\S*)", cp.stdout):
        out.setdefault(m.group(1), m.group(2))
    return out


def sbatch(args: list[str], cwd: str) -> str:
    """Submit; return job id. Raises on failure."""
    cp = run(["sbatch", "--parsable", *args], cwd=cwd)
    if cp.returncode != 0:
        raise RuntimeError(f"sbatch failed: {cp.stderr.strip() or cp.stdout.strip()}")
    return cp.stdout.strip().split(";")[0]


def submit_line_args(submit_line: str) -> list[str]:
    """'sbatch --parsable foo.sbatch' -> ['--parsable', 'foo.sbatch'] (sbatch token dropped)."""
    try:
        toks = shlex.split(submit_line)
    except ValueError:
        toks = submit_line.split()
    if toks and Path(toks[0]).name == "sbatch":
        toks = toks[1:]
    return [t for t in toks if t != "--parsable"]


def set_flag(args: list[str], flag: str, value: str) -> list[str]:
    """Replace or append a long option (--mem=...) in an sbatch arg list."""
    out, done = [], False
    i = 0
    while i < len(args):
        a = args[i]
        if a == flag:
            i += 2
            if not done:
                out.append(f"{flag}={value}"); done = True
            continue
        if a.startswith(flag + "="):
            if not done:
                out.append(f"{flag}={value}"); done = True
            i += 1
            continue
        out.append(a); i += 1
    if not done:
        out.insert(0, f"{flag}={value}")
    return out


def script_from_args(args: list[str]) -> str | None:
    for a in args:
        if not a.startswith("-") and (a.endswith(".sbatch") or a.endswith(".sh") or a.endswith(".slurm") or os.path.isfile(a)):
            return a
    return None


def resolve_pattern(pattern: str, job_id: str, name: str, user: str, node: str = "") -> str:
    """Expand the common sbatch filename patterns."""
    base, _, arr = job_id.partition("_")
    rep = {"%j": job_id.replace("_", "_"), "%J": job_id, "%A": base, "%a": arr or "", "%x": name,
           "%u": user, "%N": node.split(",")[0] if node else "", "%%": "%"}
    # %j for array tasks is the task's own numeric id, which sacct does not give us; base is close enough.
    if arr:
        rep["%j"] = base
    out = pattern
    for k, v in rep.items():
        out = out.replace(k, v)
    return out


def parse_mem_gb(s: str) -> float | None:
    m = re.match(r"^\s*([\d.]+)\s*([KMGT]?)", s or "", re.I)
    if not m:
        return None
    n, u = float(m.group(1)), m.group(2).upper()
    return n * {"K": 1 / 1e6, "M": 1 / 1024, "G": 1, "T": 1024, "": 1 / 1024}[u]


def parse_slurm_time(s: str) -> timedelta | None:
    """'1-00:00:00' | '02:30:00' | '45:00' | '30'."""
    if not s or s in ("UNLIMITED", "Partition_Limit"):
        return None
    d = 0
    if "-" in s:
        d, s = s.split("-", 1)
        d = int(d)
    parts = [int(x) for x in s.split(":")]
    if len(parts) == 3:
        h, m, sec = parts
    elif len(parts) == 2:
        h, m, sec = (parts[0], parts[1], 0) if d else (0, parts[0], parts[1])
    else:
        h, m, sec = 0, parts[0], 0
    return timedelta(days=d, hours=h, minutes=m, seconds=sec)
