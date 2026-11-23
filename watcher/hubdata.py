"""Pure data collection and arithmetic for hub mode: cluster status from squeue, weekly utilization
from `sacct -D`, per-directory spec state from disk, job-to-spec mapping and time-until-dry. Nothing
here talks to Notion, so it is all testable offline."""
from __future__ import annotations

import json
import re
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

import yaml

from . import slurm

SQ_FIELDS = ["JobID", "Name", "State", "Partition", "tres-alloc", "tres-per-node", "tres-per-job", "NumCPUs",
             "MinMemory", "NumNodes", "Comment", "WorkDir", "StartTime", "SubmitTime", "TimeLimit", "TimeUsed",
             "NodeList", "Reason"]
TAG_RX = re.compile(r"\bcc:([A-Za-z0-9_.-]+)")
GPU_ALLOC_RX = re.compile(r"gres/gpu=(\d+)")
GPU_REQ_RX = re.compile(r"gres/gpu(?::[A-Za-z0-9_-]+)?:(\d+)")


def parse_time(s: str | None, tz) -> datetime | None:
    if not s or s in ("N/A", "Unknown", "None", "(null)"):
        return None
    try:
        d = datetime.fromisoformat(s)
    except ValueError:
        return None
    return d if d.tzinfo else d.replace(tzinfo=tz)


def gpus_of(tres_alloc: str, per_node: str, per_job: str, nodes: int) -> int:
    m = GPU_ALLOC_RX.search(tres_alloc or "")
    if m:
        return int(m.group(1))
    m = GPU_REQ_RX.search(per_node or "")
    if m:
        return int(m.group(1)) * max(1, nodes)
    m = GPU_REQ_RX.search(per_job or "")
    return int(m.group(1)) if m else 0


def mem_gb_of(tres_alloc: str, min_mem: str, nodes: int) -> float:
    m = re.search(r"mem=([\d.]+[KMGT]?)", tres_alloc or "")
    if m:
        return slurm.parse_mem_gb(m.group(1)) or 0.0
    return (slurm.parse_mem_gb(min_mem) or 0.0) * max(1, nodes)


@dataclass
class Job:
    id: str
    name: str
    state: str
    partition: str
    gpus: int
    cpus: int
    mem_gb: float
    comment: str
    workdir: str
    start: datetime | None
    submit: datetime | None
    limit: timedelta | None
    node: str
    reason: str = ""
    end: datetime | None = None
    elapsed_h: float | None = None
    spec: str | None = None        # spec slug
    mapped_by: str = "untracked"

    @property
    def tag(self) -> str | None:
        m = TAG_RX.search(self.comment or "")
        return m.group(1) if m else None

    @property
    def running(self) -> bool:
        return self.state == "RUNNING"

    @property
    def pending(self) -> bool:
        return self.state in ("PENDING", "REQUEUED", "CONFIGURING")

    def limit_end(self, now: datetime) -> tuple[datetime, bool]:
        """Upper-bound end time from the Slurm time limit, and whether it is only a lower bound
        (a pending job with no scheduler estimate: its start is unknown)."""
        lim = self.limit or timedelta(hours=6)
        if self.running and self.start:
            return self.start + lim, False
        if self.start and self.start > now:  # pending with a scheduler start estimate
            return self.start + lim, False
        return now + lim, True


def squeue_jobs(user: str, tz, runner=slurm.run) -> list[Job]:
    fmt = ",".join(f"{f}:|" for f in SQ_FIELDS)
    cp = runner(["squeue", "-u", user, "-h", "-O", fmt])
    if cp.returncode != 0:
        raise RuntimeError(f"squeue failed: {cp.stderr.strip()}")
    return [j for j in (parse_squeue_line(l, tz) for l in cp.stdout.splitlines()) if j]


def parse_squeue_line(line: str, tz) -> Job | None:
    p = [x.strip() for x in line.split("|")]
    if len(p) < len(SQ_FIELDS):
        return None
    d = dict(zip(SQ_FIELDS, p))
    nodes = int(d["NumNodes"] or 1) if d["NumNodes"].isdigit() else 1
    start = parse_time(d["StartTime"], tz)
    state = d["State"]
    return Job(id=d["JobID"], name=d["Name"], state=state, partition=d["Partition"],
               gpus=gpus_of(d["tres-alloc"], d["tres-per-node"], d["tres-per-job"], nodes),
               cpus=int(d["NumCPUs"]) if d["NumCPUs"].isdigit() else 0,
               mem_gb=mem_gb_of(d["tres-alloc"], d["MinMemory"], nodes),
               comment="" if d["Comment"] == "(null)" else d["Comment"], workdir=d["WorkDir"],
               start=start, submit=parse_time(d["SubmitTime"], tz),
               limit=slurm.parse_slurm_time(d["TimeLimit"]), node=d["NodeList"], reason=d["Reason"])


def sacct_rows(args: list[str], fields: list[str], runner=slurm.run) -> list[dict]:
    cp = runner(["sacct", "-X", "-n", "-P", "-o", ",".join(fields), *args], timeout=180)
    if cp.returncode != 0:
        raise RuntimeError(f"sacct failed: {cp.stderr.strip()}")
    return [dict(zip(fields, l.split("|"))) for l in cp.stdout.splitlines() if l.count("|") >= len(fields) - 1]


UTIL_FIELDS = ["JobID", "Partition", "State", "Start", "End", "AllocTRES"]


def weekly_gpu_hours(rows: list[dict], partitions: set[str], now: datetime, tz, hours: float = 168) -> float:
    """GPU-hours inside the trailing window on the counted partitions. `rows` come from `sacct -D`, so
    every requeued or preempted attempt is its own row and counts for the time it actually ran."""
    lo = now - timedelta(hours=hours)
    total = 0.0
    for r in rows:
        if r.get("Partition") not in partitions:
            continue
        st = parse_time(r.get("Start"), tz)
        if st is None:
            continue
        en = parse_time(r.get("End"), tz) or now
        a, b = max(st, lo), min(en, now)
        if b <= a:
            continue
        m = GPU_ALLOC_RX.search(r.get("AllocTRES") or "")
        if m:
            total += int(m.group(1)) * (b - a).total_seconds() / 3600
    return total


# ------------------------------------------------------------------ per-directory state
@dataclass
class DirInfo:
    path: Path
    has_state: bool
    watcher_live: bool
    stop: bool = False
    paused: bool = False
    done: bool = False
    blocked: str | None = None
    no_new_jobs: bool = False
    session: str | None = None
    status_line: str | None = None
    branch: str | None = None
    checkpoints: dict | None = None
    checkpoints_mtime: float = 0.0
    api_usd: float = 0.0
    tokens: float = 0.0
    gpu_hours: float = 0.0
    by_node: dict = field(default_factory=dict)   # checkpoint key -> {"usd", "tokens"}
    api_cap: float | None = None
    agent_usd: float = 0.0
    watcher_job: str | None = None


def read_ledger(path: Path) -> tuple[float, float, float, dict]:
    usd = tok = gpuh = 0.0
    by_node: dict[str, dict] = {}
    if not path.exists():
        return usd, tok, gpuh, by_node
    for line in path.read_text(errors="replace").splitlines():
        try:
            r = json.loads(line)
        except json.JSONDecodeError:
            continue
        u = float(r.get("usd") or 0)
        t = float(r.get("tokens_in") or 0) + float(r.get("tokens_out") or 0)
        usd, tok, gpuh = usd + u, tok + t, gpuh + float(r.get("gpu_hours") or 0)
        n = by_node.setdefault(str(r.get("node") or ""), {"usd": 0.0, "tokens": 0.0})
        n["usd"] += u
        n["tokens"] += t
    return usd, tok, gpuh, by_node


def api_cap_of(repo: Path) -> float | None:
    for rel in ("configs/defaults.yaml", "config/defaults.yaml"):
        p = repo / rel
        if p.exists():
            try:
                v = ((yaml.safe_load(p.read_text()) or {}).get("budget") or {}).get("api_usd")
                return float(v) if v is not None else None
            except (yaml.YAMLError, ValueError, TypeError, AttributeError):
                return None
    return None


def turn_costs(turns_dir: Path, cache: dict) -> float:
    """Sum of total_cost_usd over the watcher's claude -p outputs. `cache` maps filename -> cost so
    each file is parsed once."""
    if turns_dir.is_dir():
        for p in turns_dir.glob("*.json"):
            if p.name in cache:
                continue
            cost = 0.0
            try:
                for line in reversed(p.read_text(errors="replace").splitlines()):
                    if line.strip().startswith("{"):
                        cost = float(json.loads(line).get("total_cost_usd") or 0)
                        break
            except (json.JSONDecodeError, ValueError, OSError):
                pass
            cache[p.name] = cost
    return sum(cache.values())


def dir_info(repo: Path, live_watcher_dirs: dict[str, str], cost_cache: dict, ledger_rel: str = "results/spend.jsonl") -> DirInfo:
    sd = repo / ".watcher"
    info = DirInfo(path=repo, has_state=sd.is_dir(), watcher_live=str(sd) in live_watcher_dirs,
                   watcher_job=live_watcher_dirs.get(str(sd)))
    if info.has_state:
        info.stop = (sd / "STOP").exists()
        info.paused = (sd / "research" / "PAUSED").exists()
        info.done = (sd / "research" / "DONE").exists()
        b = sd / "research" / "BLOCKED"
        info.blocked = b.read_text().strip().splitlines()[0][:200] if b.exists() and b.read_text().strip() else None
        info.no_new_jobs = (sd / "NO_NEW_JOBS").exists()
        try:
            meta = json.loads((sd / "meta.json").read_text())
            r = meta.get("research") or {}
            info.session, info.status_line = r.get("session"), r.get("status_line")
        except (FileNotFoundError, json.JSONDecodeError):
            pass
        cp = sd / "checkpoints.json"
        if cp.exists():
            try:
                info.checkpoints = json.loads(cp.read_text())
                info.checkpoints_mtime = cp.stat().st_mtime
            except json.JSONDecodeError:
                info.checkpoints = None
        info.agent_usd = turn_costs(sd / "turns", cost_cache)
    info.api_usd, info.tokens, info.gpu_hours, info.by_node = read_ledger(repo / ledger_rel)
    info.api_cap = api_cap_of(repo)
    try:
        cp = subprocess.run(["git", "-C", str(repo), "rev-parse", "--abbrev-ref", "HEAD"], capture_output=True, text=True, timeout=10)
        info.branch = (cp.stdout.strip() or None) if cp.returncode == 0 else None
    except (OSError, subprocess.TimeoutExpired):
        pass
    return info


def derive_state(info: DirInfo | None, live_jobs: int, current: str | None) -> str:
    """Spec State from what is on disk. Done, dropped and queued are kept as set (by you or a command)."""
    if current in ("dropped", "queued"):
        return current
    if info is None or not info.has_state:
        return current or "not started"
    if info.done:
        return "done"
    if info.watcher_live:
        if info.paused or info.stop:
            return "paused"
        return "no new jobs" if info.no_new_jobs else "running"
    if live_jobs:
        return "running"   # watcher chain is down but its jobs still run; the hub restarts the chain
    return "done" if current == "done" else "paused"


# --------------------------------------------------------------------------- mapping
def map_jobs(jobs: list[Job], dir_to_spec: dict[str, str], known_tags: dict[str, str], valid_slugs: set[str],
             manual: dict[str, str]) -> None:
    """Fill job.spec / job.mapped_by. Order: manual correction, spec tag (seen live, cached because
    accounting does not keep comments), WorkDir under a mapped repo dir (longest match), else untracked."""
    dirs = sorted(dir_to_spec, key=len, reverse=True)
    for j in jobs:
        if j.id in manual:
            j.spec, j.mapped_by = manual[j.id] or None, "manual"
            continue
        tag = j.tag or known_tags.get(j.id)
        if tag:
            known_tags[j.id] = tag
        if tag and tag in valid_slugs:
            j.spec, j.mapped_by = tag, "tag"
            continue
        wd = (j.workdir or "").rstrip("/")
        hit = next((d for d in dirs if wd == d or wd.startswith(d + "/")), None)
        if hit:
            j.spec, j.mapped_by = dir_to_spec[hit], "workdir"
        else:
            j.spec, j.mapped_by = None, "untracked"


# ---------------------------------------------------------------------- time until dry
@dataclass
class Dry:
    hours: float
    lower_bound: bool
    detail: str


def time_until_dry(now: datetime, jobs: list[Job], counted: set[str], quota: int, spec_etas: dict[str, tuple[datetime | None, bool]],
                   queued: list[tuple[str, int, float | None]], active: set[str] = frozenset()) -> Dry:
    """Hours until no job of yours is running or pending on the counted partitions.

    jobs: live jobs. spec_etas: slug -> (end estimate from the loop's ETAs, overdue flag); a spec with an
    entry is assumed to keep the queue busy until then, whatever its individual jobs' limits say.
    queued: (slug, GPUs requested, estimated runtime h or None) for specs queued on this source; each
    starts once enough GPUs are free (by limit-based end times) and runs for its estimate.
    active: slugs of specs whose loop is running (State "running"). Their ETA counts even when they have
    no job on a counted partition right now, so a loop between jobs does not read as "dry in 0 h"."""
    ends: list[tuple[datetime, bool, str]] = []
    cjobs = [j for j in jobs if j.partition in counted and (j.running or j.pending)]
    seen_specs = set()
    for j in cjobs:
        if j.spec and j.spec in spec_etas and spec_etas[j.spec][0] is not None:
            if j.spec not in seen_specs:
                seen_specs.add(j.spec)
                eta, overdue = spec_etas[j.spec]
                end = max(eta, now)
                ends.append((end, False, f"{j.spec} ETA" + (" (overdue)" if overdue else "")))
            continue
        end, lb = j.limit_end(now)
        ends.append((end, lb, f"job {j.id} {j.name} ({'pending, start unknown' if lb else 'time limit'})"))
    for slug in sorted(active - seen_specs):
        eta, overdue = spec_etas.get(slug, (None, False))
        if eta is not None and eta > now:
            ends.append((eta, False, f"{slug} ETA (between jobs)"))
    # queued specs: simulate GPUs freeing up by limit-based end times of running counted jobs
    frees = sorted((j.limit_end(now)[0], j.gpus) for j in cjobs if j.running)
    used = sum(j.gpus for j in cjobs if j.running)
    t, i = now, 0
    for slug, need, est_h in queued:
        while used + need > quota and i < len(frees):
            t, used = max(t, frees[i][0]), used - frees[i][1]
            i += 1
        used += need
        if est_h is None:
            ends.append((t, True, f"queued {slug} (no runtime estimate)"))
        else:
            ends.append((t + timedelta(hours=est_h), False, f"queued {slug}"))
    if not ends:
        return Dry(0.0, False, "no running or pending jobs")
    end, lb, why = max(ends, key=lambda e: e[0])
    lower = lb or any(e[1] for e in ends)
    return Dry(max(0.0, (end - now).total_seconds() / 3600), lower, why)


def spec_eta(checkpoints: dict | None, tz, now: datetime) -> tuple[datetime | None, bool]:
    """End estimate for a spec from its checkpoints file: spec_eta if given, else the latest ETA of a
    running checkpoint. Returns (eta, overdue)."""
    if not checkpoints:
        return None, False
    cands = []
    se = parse_time(checkpoints.get("spec_eta"), tz)
    if se:
        cands.append(se)
    else:
        for c in checkpoints.get("checkpoints") or []:
            if c.get("status") == "running":
                e = parse_time(c.get("eta"), tz)
                if e:
                    cands.append(e)
    if not cands:
        return None, False
    eta = max(cands)
    return eta, eta < now
