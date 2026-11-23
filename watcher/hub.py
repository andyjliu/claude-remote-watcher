"""Hub mode: the per-cluster source agent for the Research Control Center.

One hub runs per cluster, as a self-chaining watcher job in its own directory (`watch start --hub DIR`).
Every `hub.status_interval_min` it reads Slurm and every mapped experiment dir's `.watcher/` (over the
shared home directory) and writes to Notion: the compute source's live status, a resource-history row
(plus hourly rollups), jobs and their spec mapping, spec rows (state, spend, ETA), the checkpoint
lists, and the time until the queue on the counted partitions runs dry. Schema: docs/control-center.md.

The hub never submits or cancels anything unless `hub.manage_watchers` is on; with it off it only logs
what it would restart. With `hub.dry_run` it records Notion writes to <state>/hub_dry_run.jsonl."""
from __future__ import annotations

import hashlib
import json
import os
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from . import hubdata as hd
from .config import cluster_name, get
from .notion import (Notion, NotionError, P_check, P_date, P_num, P_rel, P_select, P_text, P_title, P_url, norm_id,
                     read)
from .state import State, atomic_write, now_iso

USER = os.environ.get("USER") or os.environ.get("LOGNAME") or "unknown"
CP_STATUSES = {"not started", "running", "done", "blocked", "failed"}
CP_OUTCOMES = {"positive", "negative", "null", "mixed", "n/a"}
JOB_STATES = {"PENDING", "RUNNING", "COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "PREEMPTED", "OUT_OF_MEMORY",
              "NODE_FAIL", "REQUEUED"}


def iso(d: datetime | None) -> str | None:
    return d.isoformat(timespec="seconds") if d else None


def phash(props: dict) -> str:
    return hashlib.sha1(json.dumps(props, sort_keys=True, default=str).encode()).hexdigest()[:16]


class Hub:
    def __init__(self, cfg: dict, state: State, slack=None, dry_run: bool | None = None):
        self.cfg, self.state, self.slack = cfg, state, slack
        self.tz = ZoneInfo(get(cfg, "watcher.timezone", "UTC"))
        self.ids = {k: norm_id(v) for k, v in (get(cfg, "hub.notion", {}) or {}).items() if v}
        missing = [k for k in ("sources", "specs", "checkpoints", "history", "mappings", "jobs") if k not in self.ids]
        if missing:
            raise RuntimeError(f"hub.notion is missing database ids: {', '.join(missing)}")
        token_file = Path(get(cfg, "hub.notion_token_file", "~/.config/claude-watcher/notion_token")).expanduser()
        self.dry = bool(get(cfg, "hub.dry_run", False)) if dry_run is None else dry_run
        self.notion = Notion(token_file.read_text().strip(), log=state.logline,
                             dry_run_log=(state.dir / "hub_dry_run.jsonl") if self.dry else None)
        self.source_name = get(cfg, "hub.source_name") or cluster_name(cfg)
        self.counted: dict[str, int] = {str(k): int(v) for k, v in (get(cfg, "hub.counted", {"general": 8}) or {}).items()}
        self.quota = sum(self.counted.values())
        self.interval = float(get(cfg, "hub.status_interval_min", 10)) * 60
        self.prefix = get(cfg, "slurm.job_name_prefix", "cw") + ":"
        self._bot: str | None = None
        self._reads: dict[str, dict] = {}         # dir -> in-flight read {"thread", "box", "since"}
        self._good: dict[str, hd.DirInfo] = {}    # dir -> last complete read
        # model for the hub's own Claude turns (progress summaries); claude.tiers.hub, default opus
        t = get(cfg, "claude.tiers.hub", {}) or {}
        self.model, self.budget = str(t.get("model", "opus")), float(t.get("budget_usd", 5))
        from .commands import Commands
        self.commands = Commands(self)

    @property
    def m(self) -> dict:
        return self.state.meta.setdefault("hub", {})

    # ------------------------------------------------------------------ scheduling
    def due(self, now: float) -> bool:
        return now - float(self.m.get("last_cycle_epoch", 0)) >= self.interval

    def tick(self, force: bool = False) -> bool:
        try:
            self.commands.poll()
        except NotionError as e:
            self.state.logline(f"hub: notion error polling commands: {e}")
        except Exception as e:  # noqa: BLE001
            self.state.logline(f"hub: command poll error: {type(e).__name__}: {e}")
        now = datetime.now(timezone.utc).timestamp()
        if not (force or self.due(now)):
            return False
        self.m["last_cycle_epoch"] = now
        try:
            self.cycle()
        except NotionError as e:
            self.state.logline(f"hub: notion error: {e}")
        except Exception as e:  # noqa: BLE001  (a bad cycle must not stop the next one)
            self.state.logline(f"hub: cycle error: {type(e).__name__}: {e}")
        return True

    def bot_id(self) -> str | None:
        if self._bot is None and not self.dry:
            try:
                self._bot = self.notion.me().get("id")
            except NotionError:
                self._bot = None
        return self._bot

    # --------------------------------------------------------------------- cycle
    def cycle(self) -> dict:
        now = datetime.now(self.tz)
        calls0 = self.notion.calls
        jobs = hd.squeue_jobs(USER, self.tz)
        live_watchers = {j.workdir.rstrip("/"): j.id for j in jobs if j.name.startswith(self.prefix) and j.running}

        source = self.source_row()
        specs = self.notion.query(self.ids["specs"])
        specs += self.discover_specs(specs, now)
        by_page = {norm_id(p["id"]): p for p in specs}
        slug_of = {pid: (read(p, "Spec ID") or pid.replace("-", "")[:8]) for pid, p in by_page.items()}
        page_of = {s: pid for pid, s in slug_of.items()}
        mappings = [r for r in self.notion.query(self.ids["mappings"]) if source["id"] in (read(r, "Source") or [source["id"]])]

        # experiment dirs: every mapping row's repo dir plus every dir with a live watcher (except the hub's own)
        dirs: dict[str, dict] = {}
        for r in mappings:
            d = (read(r, "Repo dir") or "").rstrip("/")
            if d:
                spec_ids = read(r, "Spec") or []
                dirs[d] = {"row": r, "spec": slug_of.get(norm_id(spec_ids[0])) if spec_ids else None}
        hub_dir = str(Path(self.cfg["_target"]))
        for wd in live_watchers:
            d = str(Path(wd).parent) if wd.endswith("/.watcher") else wd
            if d != hub_dir and d not in dirs:
                dirs[d] = {"row": None, "spec": None}

        cost_cache = self.m.setdefault("turn_costs", {})
        infos: dict[str, hd.DirInfo] = {}
        for d in dirs:
            info = self.read_dir(d, live_watchers, cost_cache.setdefault(d, {}))
            if info is not None:
                infos[d] = info
        # a dir that can't be read (hung NFS) and has no earlier snapshot still maps its jobs,
        # but its spec, mapping and checkpoint rows are left as they are this cycle
        map_specs = {d: v["spec"] for d, v in dirs.items() if v["spec"]}
        skip_specs = {map_specs[d] for d in dirs if d not in infos and d in map_specs}
        dirs = {d: v for d, v in dirs.items() if d in infos}

        self.sync_mappings(dirs, infos, source["id"], page_of, now)
        dir_to_spec = {d: v["spec"] for d, v in dirs.items() if v["spec"]}
        for d, slug in dir_to_spec.items():
            self.write_dir_config(infos[d], slug, page_of.get(slug))

        manual = self.manual_job_specs(slug_of)
        tags = self.m.setdefault("tags", {})
        hd.map_jobs(jobs, map_specs, tags, set(page_of), manual)
        live_ids = {j.id for j in jobs}
        for jid in [k for k in tags if k not in live_ids and k not in self.m.get("job_pages", {})]:
            tags.pop(jid, None)
        self.sync_jobs(jobs, source["id"], page_of, now)

        spec_dirs = {slug: infos[d] for d, slug in dir_to_spec.items()}
        gpu_h = self.spec_gpu_hours(map_specs, tags, now)
        self.sync_checkpoints(spec_dirs, page_of, now)
        spec_etas = {slug: hd.spec_eta(i.checkpoints, self.tz, now) for slug, i in spec_dirs.items()}

        counted = set(self.counted)
        live_by_spec: dict[str, int] = {}
        for j in jobs:
            if j.spec and (j.running or j.pending) and not j.name.startswith(self.prefix):
                live_by_spec[j.spec] = live_by_spec.get(j.spec, 0) + 1
        queued, active = [], set()
        for pid, p in by_page.items():
            s = slug_of[pid]
            if s in skip_specs:
                continue
            new_state = hd.derive_state(spec_dirs.get(s), live_by_spec.get(s, 0), read(p, "State"))
            self.sync_spec(p, s, spec_dirs.get(s), new_state, spec_etas.get(s), now, gpu_h.get(s))
            if new_state == "running":
                active.add(s)
            if new_state == "queued":
                queued.append((s, int(read(p, "GPU request") or 0), read(p, "Est. runtime h")))
            self.check_budget(s, read(p, "Name") or s, spec_dirs.get(s), read(p, "API cap $"))

        dry = hd.time_until_dry(now, jobs, counted, self.quota, spec_etas, queued, active)
        status = self.status(jobs, counted, now, dry)
        self.write_source(source, status, now)
        self.write_history(source["id"], status, now)
        self.maintenance(now)
        self.commands.start_queued(by_page, slug_of, status["gpus_used"])
        self.manage_watchers(by_page, slug_of, dirs, infos, live_by_spec)
        self.m["last_cycle"] = iso(now)
        self.state.logline(f"hub cycle: {status['gpus_used']}/{self.quota} GPUs on {','.join(counted)}, "
                           f"{status['running']} running / {status['pending']} pending, dry in "
                           f"{'≥' if dry.lower_bound else ''}{dry.hours:.1f}h ({dry.detail}); "
                           f"{len(dirs)} dirs, {self.notion.calls - calls0} notion calls"
                           + (" [dry run]" if self.dry else ""))
        return status

    READ_TIMEOUT_SEC = 45

    def read_dir(self, d: str, live_watchers: dict, cost_cache: dict) -> hd.DirInfo | None:
        """dir_info() in a daemon thread with a timeout. A read stuck in the kernel (an NFS open that never
        returns, seen on babel-z9-24) can't be interrupted, so the thread is left behind and the dir's last good
        snapshot is used until the read finishes. Daemon threads don't hold up the job's exit."""
        r = self._reads.get(d)
        fresh = r is None
        if fresh:
            box: dict = {}

            def run():
                try:
                    box["info"] = hd.dir_info(Path(d), live_watchers, cost_cache)
                except Exception as e:  # noqa: BLE001
                    box["error"] = e
            t = threading.Thread(target=run, name=f"hub-read:{d}", daemon=True)
            r = self._reads[d] = {"thread": t, "box": box, "since": datetime.now(timezone.utc)}
            t.start()
        r["thread"].join(self.READ_TIMEOUT_SEC if fresh else 0)   # an earlier hung read gets no extra wait
        if r["thread"].is_alive():
            mins = (datetime.now(timezone.utc) - r["since"]).total_seconds() / 60
            self.state.logline(f"hub: reading {d}/.watcher has hung for {mins:.0f} min (NFS?); "
                               + ("using the last good snapshot" if d in self._good else "skipping its spec rows this cycle"))
            return self._good.get(d)
        self._reads.pop(d, None)
        if "error" in r["box"]:
            e = r["box"]["error"]
            self.state.logline(f"hub: reading {d}/.watcher failed: {type(e).__name__}: {e}")
            return self._good.get(d)
        self._good[d] = r["box"]["info"]
        return self._good[d]

    def discover_specs(self, specs: list[dict], now: datetime) -> list[dict]:
        """Hourly: a "SPEC:" child page of the backlog with no Specs row gets one (state not started)."""
        backlog = self.ids.get("backlog_page")
        last = self.m.get("spec_scan_at")
        if not backlog or (last and (now - datetime.fromisoformat(last)).total_seconds() < 3600):
            return []
        self.m["spec_scan_at"] = iso(now)
        known = {norm_id((read(p, "Spec page") or "").rstrip("/").split("/")[-1].split("?")[0].split("-")[-1]) for p in specs}
        new = []
        for b in self.notion.child_pages(backlog):
            title = (b.get("child_page") or {}).get("title", "")
            pid = norm_id(b["id"])
            if not title.upper().startswith("SPEC:") or pid in known:
                continue
            name = title.split(":", 1)[1].strip()
            row = self.notion.create(self.ids["specs"], {
                "Name": P_title(name), "Spec page": P_url(f"https://www.notion.so/{pid.replace('-', '')}"),
                "Spec ID": P_text(pid.replace("-", "")[:8]), "State": P_select("not started")})
            self.state.logline(f"hub: new spec row for '{name}'")
            if not row.get("dry_run"):
                new.append(row)
        return new

    # ------------------------------------------------------------------- source
    def source_row(self) -> dict:
        rows = self.notion.query(self.ids["sources"], {"property": "Name", "title": {"equals": self.source_name}})
        if rows:
            return rows[0]
        r = self.notion.create(self.ids["sources"], {"Name": P_title(self.source_name), "GPU quota": P_num(self.quota),
                                                      "Counted quotas": P_text(", ".join(f"{k}: {v} GPU" for k, v in self.counted.items())),
                                                      "Status interval min": P_num(self.interval / 60)})
        return r

    def status(self, jobs: list[hd.Job], counted: set[str], now: datetime, dry: hd.Dry) -> dict:
        c_run = [j for j in jobs if j.partition in counted and j.running]
        c_pen = [j for j in jobs if j.partition in counted and j.pending]
        other: dict[str, list[int]] = {}
        for j in jobs:
            if j.partition not in counted and (j.running or j.pending):
                o = other.setdefault(j.partition, [0, 0])
                o[0] += j.gpus if j.running else 0
                o[1] += 1
        util = self.weekly_util(now, counted)
        return {
            "gpus_used": sum(j.gpus for j in c_run), "gpus_pending": sum(j.gpus for j in c_pen),
            "running": len(c_run), "pending": len(c_pen),
            "cpus": sum(j.cpus for j in jobs if j.running), "mem_gb": round(sum(j.mem_gb for j in jobs if j.running), 1),
            "other": "; ".join(f"{p}: {g} GPU, {n} job{'s' * (n != 1)}" for p, (g, n) in sorted(other.items())) or "none",
            "util": util, "dry_h": round(dry.hours, 2), "dry_lb": dry.lower_bound, "dry_detail": dry.detail,
        }

    def weekly_util(self, now: datetime, counted: set[str]) -> float | None:
        """Recomputed at most every 30 min (sacct over 7 days is the slowest call in the cycle)."""
        last = self.m.get("util_at")
        if last and (now - datetime.fromisoformat(last)).total_seconds() < 1800:
            return self.m.get("util")
        try:
            rows = hd.sacct_rows(["-u", USER, "-D", "-S", (now - timedelta(days=7, hours=2)).strftime("%Y-%m-%dT%H:%M:%S"),
                                  "-r", ",".join(sorted(counted))], hd.UTIL_FIELDS)
        except RuntimeError as e:
            self.state.logline(f"hub: {e}")
            return self.m.get("util")
        gh = hd.weekly_gpu_hours(rows, counted, now, self.tz)
        self.m["util"], self.m["util_at"], self.m["util_gpu_h"] = round(gh / (self.quota * 168), 4), iso(now), round(gh, 1)
        return self.m["util"]

    def write_source(self, source: dict, s: dict, now: datetime) -> None:
        self.notion.update(source["id"], {
            "Last report": P_date(iso(now)), "Hub job": P_text(os.environ.get("CW_JOB_ID", "local")),
            "GPUs used": P_num(s["gpus_used"]), "GPUs pending": P_num(s["gpus_pending"]),
            "Running jobs": P_num(s["running"]), "Pending jobs": P_num(s["pending"]),
            "CPUs used": P_num(s["cpus"]), "Mem GB used": P_num(s["mem_gb"]), "Other partitions": P_text(s["other"]),
            "Weekly util": P_num(s["util"]), "Hours until dry": P_num(s["dry_h"]),
            "Dry is lower bound": P_check(s["dry_lb"]), "Dry detail": P_text(s["dry_detail"]),
        })

    # ------------------------------------------------------------------ history
    def write_history(self, source_id: str, s: dict, now: datetime) -> None:
        row = {"gpus_used": s["gpus_used"], "gpus_pending": s["gpus_pending"], "running": s["running"],
               "pending": s["pending"], "cpus": s["cpus"], "mem_gb": s["mem_gb"], "dry_h": s["dry_h"], "util": s["util"]}
        self.notion.create(self.ids["history"], self.history_props(source_id, now, "raw", row))
        hour = now.replace(minute=0, second=0, microsecond=0)
        buf = self.m.setdefault("hour_buf", {"hour": iso(hour), "rows": []})
        if buf["hour"] != iso(hour) and buf["rows"]:
            rows = buf["rows"]
            agg = {k: round(sum(r[k] or 0 for r in rows) / len(rows), 2) for k in row}
            agg["gpus_used_max"] = max(r["gpus_used"] for r in rows)
            self.notion.create(self.ids["history"], self.history_props(source_id, datetime.fromisoformat(buf["hour"]), "hour", agg))
            buf.update(hour=iso(hour), rows=[])
        buf["hour"] = iso(hour)
        buf["rows"].append(row)

    def history_props(self, source_id: str, at: datetime, grain: str, r: dict) -> dict:
        p = {"Time": P_title(at.strftime("%Y-%m-%d %H:%M") + ("" if grain == "raw" else " (hour)")), "Source": P_rel(source_id),
             "At": P_date(iso(at)), "Grain": P_select(grain), "GPUs used": P_num(r["gpus_used"]),
             "GPUs pending": P_num(r["gpus_pending"]), "Running jobs": P_num(r["running"]), "Pending jobs": P_num(r["pending"]),
             "CPUs used": P_num(r["cpus"]), "Mem GB used": P_num(r["mem_gb"]), "Hours until dry": P_num(r["dry_h"]),
             "Weekly util": P_num(r["util"])}
        if "gpus_used_max" in r:
            p["GPUs used max"] = P_num(r["gpus_used_max"])
        return p

    def maintenance(self, now: datetime) -> None:
        """Hourly: archive raw history older than hub.raw_history_days and job rows that ended more than
        hub.jobs_keep_days ago, at most 40 archives per run so a backlog cannot stall a cycle."""
        last = self.m.get("maint_at")
        if last and (now - datetime.fromisoformat(last)).total_seconds() < 3600:
            return
        self.m["maint_at"] = iso(now)
        budget = 40
        cut = now - timedelta(days=float(get(self.cfg, "hub.raw_history_days", 14)))
        old = self.notion.query(self.ids["history"], {"and": [
            {"property": "Grain", "select": {"equals": "raw"}},
            {"property": "At", "date": {"before": iso(cut)}}]}, limit=budget)
        for r in old:
            self.notion.archive(r["id"])
        budget -= len(old)
        cut = now - timedelta(days=float(get(self.cfg, "hub.jobs_keep_days", 30)))
        if budget > 0:
            pages = self.m.setdefault("job_pages", {})
            for r in self.notion.query(self.ids["jobs"], {"property": "Ended", "date": {"before": iso(cut)}}, limit=budget):
                self.notion.archive(r["id"])
                pages.pop(read(r, "Job ID"), None)

    # ----------------------------------------------------------------- mappings
    def sync_mappings(self, dirs: dict, infos: dict, source_id: str, page_of: dict, now: datetime) -> None:
        for d, v in dirs.items():
            info, row = infos[d], v["row"]
            props = {"Branch / worktree": P_text(info.branch), "Watcher job": P_text(
                         f"{info.watcher_job} ({self.prefix}{Path(d).name})" if info.watcher_live else
                         ("none (STOP)" if info.stop else "none")),
                     "Session": P_text(info.session), "Last seen": P_date(iso(now))}
            if row is None:
                props.update({"Name": P_title(f"{Path(d).name} @ {self.source_name}"), "Source": P_rel(source_id),
                              "Repo dir": P_text(d), "Manual": P_check(False)})
                self.notion.create(self.ids["mappings"], props)
                self.state.logline(f"hub: new mapping row for {d} (no spec yet; set Spec in Notion)")
                continue
            if read(row, "Manual"):
                props = {"Last seen": props["Last seen"], "Session": props["Session"], "Watcher job": props["Watcher job"]}
            key = f"map:{row['id']}"
            h = phash({k: v for k, v in props.items() if k != "Last seen"})
            stale = (now - datetime.fromisoformat(self.m.get(key + ":at", iso(now - timedelta(days=1))))).total_seconds() > 3600
            if self.m.get(key) != h or stale:
                self.notion.update(row["id"], props)
                self.m[key], self.m[key + ":at"] = h, iso(now)

    def write_dir_config(self, info: hd.DirInfo, slug: str, page_id: str | None) -> None:
        """<dir>/.watcher/control_center.json tells the dir's watcher which spec it runs (for the sbatch tag)."""
        if not info.has_state:
            return
        p = info.path / ".watcher" / "control_center.json"
        want = {"spec_id": slug, "spec_page": page_id, "source": self.source_name}
        try:
            cur = json.loads(p.read_text())
        except (FileNotFoundError, json.JSONDecodeError):
            cur = None
        if cur != want and not self.dry:
            atomic_write(p, json.dumps(want, indent=1) + "\n")

    # --------------------------------------------------------------------- jobs
    def manual_job_specs(self, slug_of: dict) -> dict[str, str]:
        rows = self.notion.query(self.ids["jobs"], {"property": "Mapped by", "select": {"equals": "manual"}})
        out = {}
        for r in rows:
            spec = read(r, "Spec") or []
            out[read(r, "Job ID")] = slug_of.get(norm_id(spec[0])) if spec else ""
        return out

    def sync_jobs(self, jobs: list[hd.Job], source_id: str, page_of: dict, now: datetime) -> None:
        pages = self.m.setdefault("job_pages", {})       # job id -> {"page", "hash", "live"}
        live = {j.id: j for j in jobs}
        gone = [jid for jid, v in pages.items() if v.get("live") and jid not in live]
        finals = {}
        if gone:
            try:
                for r in hd.sacct_rows(["-j", ",".join(gone)], ["JobID", "JobName", "Partition", "State", "Start", "End",
                                                                "AllocTRES", "ElapsedRaw", "Timelimit", "NodeList", "WorkDir", "Submit"]):
                    finals[r["JobID"]] = r
            except RuntimeError as e:
                self.state.logline(f"hub: {e}")
        for j in jobs:
            self.upsert_job(j, source_id, page_of, live=True)
        for jid in gone:
            r = finals.get(jid)
            if not r:
                continue
            st = hd.parse_time(r["Start"], self.tz)
            en = hd.parse_time(r["End"], self.tz)
            m = hd.GPU_ALLOC_RX.search(r.get("AllocTRES") or "")
            gpus = int(m.group(1)) if m else int(pages[jid].get("gpus", 0))
            prev = pages[jid]
            j = hd.Job(id=jid, name=r["JobName"], state=r["State"].split()[0], partition=r["Partition"], gpus=gpus,
                       cpus=0, mem_gb=0, comment="", workdir=r["WorkDir"], start=st, submit=hd.parse_time(r["Submit"], self.tz),
                       limit=hd.slurm.parse_slurm_time(r["Timelimit"]), node=r["NodeList"], end=en,
                       elapsed_h=int(r["ElapsedRaw"] or 0) / 3600 if (r["ElapsedRaw"] or "").isdigit() else None,
                       spec=prev.get("spec"), mapped_by=prev.get("mapped_by", "untracked"))
            self.upsert_job(j, source_id, page_of, live=False)

    def upsert_job(self, j: hd.Job, source_id: str, page_of: dict, live: bool) -> None:
        pages = self.m.setdefault("job_pages", {})
        state = j.state if j.state in JOB_STATES else "OTHER"
        elapsed = j.elapsed_h
        if elapsed is None and j.start and j.running:
            elapsed = (datetime.now(self.tz) - j.start).total_seconds() / 3600
        props = {"Job ID": P_title(j.id), "Source": P_rel(source_id), "Spec": P_rel(page_of.get(j.spec) if j.spec else None),
                 "Mapped by": P_select(j.mapped_by), "Job name": P_text(j.name), "Partition": P_text(j.partition),
                 "State": P_select(state), "GPUs": P_num(j.gpus), "Submitted": P_date(iso(j.submit)),
                 "Started": P_date(iso(j.start) if (j.running or not live) and j.start else None), "Ended": P_date(iso(j.end)),
                 "Time limit h": P_num(j.limit.total_seconds() / 3600 if j.limit else None),
                 "GPU-h": P_num(round(j.gpus * elapsed, 2) if elapsed is not None else None),
                 "Node": P_text(j.node if j.node and j.node != "None assigned" else ""), "WorkDir": P_text(j.workdir)}
        # GPU-h of a running job changes every cycle; hash it at whole hours so a quiet job is not rewritten every 10 min
        h = phash({**props, "GPU-h": round(j.gpus * (elapsed or 0))})
        rec = pages.get(j.id)
        if rec and rec.get("hash") == h:
            rec["live"] = live
            return
        if j.mapped_by == "manual":
            props.pop("Spec")
            props.pop("Mapped by")
        if rec and rec.get("page"):
            self.notion.update(rec["page"], props)
        else:
            rec = {"page": self.notion.create(self.ids["jobs"], props).get("id")}
        rec.update(hash=h, live=live, spec=j.spec, mapped_by=j.mapped_by, gpus=j.gpus)
        pages[j.id] = rec

    # -------------------------------------------------------------- checkpoints
    def sync_checkpoints(self, spec_dirs: dict[str, hd.DirInfo], page_of: dict, now: datetime) -> None:
        bot = self.bot_id()
        for slug, info in spec_dirs.items():
            spec_page = page_of.get(slug)
            data = info.checkpoints
            if not spec_page or data is None:
                continue
            rows = self.notion.query(self.ids["checkpoints"], {"property": "Spec", "relation": {"contains": spec_page}})
            by_key = {read(r, "Key"): r for r in rows if read(r, "Key")}
            cps = data.setdefault("checkpoints", [])
            by_cp = {c.get("key"): c for c in cps if c.get("key")}
            written = self.m.setdefault("cp_written", {})
            changed_file = False
            # 1. pull back edits you made in Notion since the hub last wrote that row
            for key, r in by_key.items():
                if bot and r.get("last_edited_by", {}).get("id") in (None, bot):
                    continue
                last_w = written.get(f"{slug}|{key}")
                if last_w and r.get("last_edited_time", "") <= last_w:
                    continue
                c = by_cp.get(key)
                if c is None:
                    c = {"key": key}
                    cps.append(c)
                    by_cp[key] = c
                for prop, fld in (("Name", "name"), ("Status", "status"), ("Summary", "summary"), ("Result", "result"),
                                  ("Outcome", "outcome"), ("Order", "order")):
                    v = read(r, prop)
                    if v not in (None, ""):
                        c[fld] = v
                c["edited_by"], c["user_edited_at"] = "user", r.get("last_edited_time")
                changed_file = True
                self.state.logline(f"hub: {slug} checkpoint {key} edited in Notion; passed back to the loop")
            if changed_file and not self.dry:
                atomic_write(info.path / ".watcher" / "checkpoints.json", json.dumps(data, indent=1) + "\n")
            # 2. push the file to Notion
            for c in cps:
                key = c.get("key")
                if not key:
                    continue
                status = c.get("status") if c.get("status") in CP_STATUSES else None
                cost = info.by_node.get(key, {})
                props = {"Name": P_title(c.get("name") or key), "Spec": P_rel(spec_page), "Key": P_text(key),
                         "Order": P_num(c.get("order")), "Status": P_select(status), "Loop state": P_text(c.get("loop_state")),
                         "Summary": P_text((c.get("summary") or "")[:300]), "Result": P_text((c.get("result") or "")[:500]),
                         "Outcome": P_select(c.get("outcome") if c.get("outcome") in CP_OUTCOMES else None),
                         "ETA": P_date(c.get("eta") if status == "running" else None),
                         "ETA as of": P_date(c.get("eta_as_of") if status == "running" else None),
                         "Updated": P_date(c.get("updated")), "Tokens": P_num(cost.get("tokens")),
                         "Cost $": P_num(round(cost["usd"], 4) if cost else None),
                         "Edited by": P_select("you" if c.get("edited_by") == "user" else "loop")}
                h = phash(props)
                hk = f"cp:{slug}|{key}"
                if self.m.get(hk) == h and key in by_key:
                    continue
                if key in by_key:
                    self.notion.update(by_key[key]["id"], props)
                else:
                    self.notion.create(self.ids["checkpoints"], props)
                self.m[hk] = h
                written[f"{slug}|{key}"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    # -------------------------------------------------------------------- specs
    def spec_gpu_hours(self, dir_to_spec: dict[str, str], tags: dict[str, str], now: datetime) -> dict[str, float]:
        """Hourly: actual GPU-hours per spec from `sacct -D` over hub.gpu_hours_days (default 90): jobs whose
        WorkDir is under the spec's repo dir, or that carried its tag. Ledger GPU-hours are submit-time
        estimates, so they are not used for this."""
        last = self.m.get("gpu_h_at")
        if last and (now - datetime.fromisoformat(last)).total_seconds() < 3600:
            return self.m.get("gpu_h", {})
        try:
            rows = hd.sacct_rows(["-u", USER, "-D", "-S", (now - timedelta(days=float(get(self.cfg, "hub.gpu_hours_days", 90))))
                                  .strftime("%Y-%m-%dT%H:%M:%S")], ["JobID", "WorkDir", "AllocTRES", "ElapsedRaw"])
        except RuntimeError as e:
            self.state.logline(f"hub: {e}")
            return self.m.get("gpu_h", {})
        dirs = sorted(dir_to_spec, key=len, reverse=True)
        out: dict[str, float] = {}
        for r in rows:
            m = hd.GPU_ALLOC_RX.search(r.get("AllocTRES") or "")
            if not m or not (r.get("ElapsedRaw") or "").isdigit():
                continue
            wd = (r.get("WorkDir") or "").rstrip("/")
            slug = tags.get(r["JobID"].split("_")[0]) or tags.get(r["JobID"]) or next(
                (dir_to_spec[d] for d in dirs if wd == d or wd.startswith(d + "/")), None)
            if slug:
                out[slug] = out.get(slug, 0.0) + int(m.group(1)) * int(r["ElapsedRaw"]) / 3600
        self.m["gpu_h"], self.m["gpu_h_at"] = {k: round(v, 1) for k, v in out.items()}, iso(now)
        return self.m["gpu_h"]

    def sync_spec(self, page: dict, slug: str, info: hd.DirInfo | None, state: str,
                  eta: tuple[datetime | None, bool] | None, now: datetime, gpu_h: float | None = None) -> None:
        props = {"State": P_select(state)}
        if not read(page, "Spec ID"):
            props["Spec ID"] = P_text(slug)
        if info is not None:
            cps = (info.checkpoints or {}).get("checkpoints") or []
            ups = [hd.parse_time(c.get("updated"), self.tz) for c in cps]
            ups = [u for u in ups if u]
            props.update({"API $": P_num(round(info.api_usd, 2)), "Agent $ (notional)": P_num(round(info.agent_usd, 2)),
                          "Tokens": P_num(info.tokens), "GPU-h": P_num(gpu_h)})
            if ups:
                props["Last progress"] = P_date(iso(max(ups)))
            if info.api_cap is not None and read(page, "API cap $") is None:
                props["API cap $"] = P_num(info.api_cap)
            if info.checkpoints and info.checkpoints.get("spec_eta"):
                props["Spec ETA"] = P_date(info.checkpoints.get("spec_eta"))
                props["Spec ETA as of"] = P_date(info.checkpoints.get("spec_eta_as_of") or
                                                 datetime.fromtimestamp(info.checkpoints_mtime, self.tz).isoformat(timespec="seconds"))
        h = phash(props)
        if self.m.get(f"spec:{slug}") != h:
            self.notion.update(page["id"], props)
            self.m[f"spec:{slug}"] = h

    def check_budget(self, slug: str, name: str, info: hd.DirInfo | None, cap_notion: float | None) -> None:
        cap = cap_notion if cap_notion is not None else (info.api_cap if info else None)
        if not info or not cap:
            return
        sent = self.m.setdefault("budget_alerts", {}).setdefault(slug, [])
        frac = info.api_usd / cap
        for level in (100, 80):
            if frac * 100 >= level and level not in sent:
                sent += [lv for lv in (80, 100) if lv <= level and lv not in sent]
                msg = (f"API spend for spec `{slug}` ({name}) is ${info.api_usd:.2f} of its ${cap:.0f} cap "
                       f"({frac:.0%}). Nothing was paused.")
                self.state.logline(f"hub: budget alert {level}% for {slug}")
                if self.slack is not None and not self.dry:
                    self.slack.post(f"[control center] {msg}")
                break

    # ------------------------------------------------------------- orchestration
    def manage_watchers(self, by_page: dict, slug_of: dict, dirs: dict, infos: dict, live_by_spec: dict) -> None:
        """Restart a spec's watcher chain that died without STOP. Logs only unless hub.manage_watchers."""
        act = bool(get(self.cfg, "hub.manage_watchers", False)) and not self.dry
        for d, v in dirs.items():
            info = infos[d]
            if not v["spec"] or not info.has_state or info.watcher_live or info.stop or info.done:
                continue
            page = next((p for pid, p in by_page.items() if slug_of[pid] == v["spec"]), None)
            if page is None or read(page, "State") not in ("running", "paused", "no new jobs"):
                continue
            note = self.m.setdefault("restart_notes", {})
            if not act:
                if note.get(d) != "logged":
                    self.state.logline(f"hub: watcher for {d} ({v['spec']}) is down without STOP; would restart "
                                       "(hub.manage_watchers is off)")
                    note[d] = "logged"
                continue
            from .cli import start_dir
            try:
                jid = start_dir(Path(d))
                self.state.logline(f"hub: restarted watcher for {d} ({v['spec']}): job {jid}")
                note[d] = f"restarted {jid} {now_iso()}"
            except Exception as e:  # noqa: BLE001
                self.state.logline(f"hub: could not restart watcher for {d}: {e}")
