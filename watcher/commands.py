"""Control commands for the Research Control Center (docs/control-center.md §2.8).

The dashboard writes a row to the Notion Commands table (Status pending). The hub of the row's compute source
polls the table every `hub.commands_poll_sec` (90 s), claims the row, carries the action out on this cluster
and sets Status done or failed with a Message. Rows are never deleted: the table is the action log.

Dry run is the default. A command is carried out only when both its own "Dry run" box is off and
`hub.commands_live` is true in the config; otherwise the hub writes what it would have done and changes nothing.

Actions, per spec (the spec's repo dir comes from its Spec mappings row on this source):
  pause_loop      research/PAUSED: the agent loop takes no actions; running jobs keep running
  resume_loop     removes research/PAUSED
  stop_new_jobs   NO_NEW_JOBS: the sbatch shim and the watcher's own resubmits refuse new jobs
  allow_new_jobs  removes NO_NEW_JOBS
  stop_and_kill   PAUSED + NO_NEW_JOBS, scancel the spec's running and pending jobs, then STOP the watcher
  start           clears PAUSED / NO_NEW_JOBS / STOP and submits the spec's watcher if none is live
  queue           State queued (Args {"gpus": N} sets GPU request); started once enough counted GPUs are free
  unqueue         drops a queued spec back to paused
  summarize       not built yet (step 6); fails with a message
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from . import hubdata as hd
from . import slurm
from .config import get
from .notion import P_date, P_num, P_select, P_text, norm_id, read
from .state import now_iso

ACTIONS = {"start", "queue", "unqueue", "pause_loop", "resume_loop", "stop_new_jobs", "allow_new_jobs",
           "stop_and_kill", "summarize"}
USER = os.environ.get("USER") or os.environ.get("LOGNAME") or "unknown"


class CommandError(Exception):
    pass


class Commands:
    def __init__(self, hub):
        self.hub = hub
        self.db = hub.ids.get("commands")
        self.poll_sec = float(get(hub.cfg, "hub.commands_poll_sec", 90))
        self.live = bool(get(hub.cfg, "hub.commands_live", False))

    @property
    def m(self) -> dict:
        return self.hub.m.setdefault("commands", {})

    def log(self, msg: str) -> None:
        self.hub.state.logline(f"command: {msg}")

    # ------------------------------------------------------------------ polling
    def due(self, now: float) -> bool:
        return bool(self.db) and now - float(self.m.get("last_poll_epoch", 0)) >= self.poll_sec

    def poll(self, force: bool = False) -> int:
        """Claim and run this source's pending commands, oldest first. Returns how many ran."""
        now = datetime.now(timezone.utc).timestamp()
        if not (force or self.due(now)):
            return 0
        self.m["last_poll_epoch"] = now
        rows = self.hub.notion.query(self.db, {"property": "Status", "select": {"equals": "pending"}},
                                     sorts=[{"timestamp": "created_time", "direction": "ascending"}])
        if not rows:
            return 0
        source_id = self.source_id()
        specs = self.spec_index()
        ran = 0
        for row in rows:
            src = [norm_id(x) for x in (read(row, "Source") or [])]
            spec_ids = [norm_id(x) for x in (read(row, "Spec") or [])]
            spec = specs.get(spec_ids[0]) if spec_ids else None
            # a row names its source; one with no source is ours when its spec is mapped on this cluster
            if src and source_id not in src:
                continue
            if not src and not (spec and spec.get("dir")):
                continue
            self.run_row(row, spec)
            ran += 1
        return ran

    def source_id(self) -> str:
        if not self.m.get("source_id"):
            self.m["source_id"] = norm_id(self.hub.source_row()["id"])
        return self.m["source_id"]

    def spec_index(self) -> dict[str, dict]:
        """page id -> {"page", "slug", "dir"}, the dir from this source's Spec mappings rows."""
        source_id = self.source_id()
        out: dict[str, dict] = {}
        for p in self.hub.notion.query(self.hub.ids["specs"]):
            pid = norm_id(p["id"])
            out[pid] = {"page": p, "slug": read(p, "Spec ID") or pid.replace("-", "")[:8], "dir": None}
        for r in self.hub.notion.query(self.hub.ids["mappings"]):
            if source_id not in [norm_id(x) for x in (read(r, "Source") or [source_id])]:
                continue
            for sid in read(r, "Spec") or []:
                d = (read(r, "Repo dir") or "").rstrip("/")
                if norm_id(sid) in out and d:
                    out[norm_id(sid)]["dir"] = d
        return out

    def run_row(self, row: dict, spec: dict | None) -> None:
        action = read(row, "Action") or ""
        dry = bool(read(row, "Dry run")) or not self.live or self.hub.dry
        label = f"{action} {spec['slug'] if spec else '(no spec)'}"
        self.hub.notion.update(row["id"], {"Status": P_select("claimed")})
        try:
            args = json.loads(read(row, "Args") or "{}") or {}
            if not isinstance(args, dict):
                raise CommandError("Args must be a JSON object")
            if action not in ACTIONS:
                raise CommandError(f"unknown action {action!r}")
            if action != "summarize" and spec is None:
                raise CommandError("the command names no spec, or its spec is not in the Specs table")
            msg = getattr(self, f"do_{action}")(spec, args, dry)
            status = "done"
        except (CommandError, json.JSONDecodeError, RuntimeError, OSError) as e:
            msg, status = f"{type(e).__name__ if not isinstance(e, CommandError) else 'failed'}: {e}", "failed"
        if dry and status == "done":
            why = "the row's Dry run box is on" if read(row, "Dry run") else "hub.commands_live is off"
            msg = f"[dry run: {why}] would {msg}. Nothing was changed."
        self.hub.notion.update(row["id"], {"Status": P_select(status), "Message": P_text(msg[:1900]),
                                           "Finished": P_date(datetime.now(self.hub.tz).isoformat(timespec="seconds"))})
        self.log(f"{label} -> {status}: {msg}")
        if status == "done" and not dry:
            self.hub.m["last_cycle_epoch"] = 0   # refresh spec states in Notion on the next tick

    # ------------------------------------------------------------------ helpers
    def state_dir(self, spec: dict) -> Path:
        if not spec.get("dir"):
            raise CommandError(f"spec {spec['slug']} has no repo dir on this source; set Repo dir in its Spec mappings row")
        d = Path(spec["dir"])
        if not d.is_dir():
            raise CommandError(f"repo dir {d} does not exist")
        from .config import load
        return Path(load(d)["_state"])

    @staticmethod
    def touch(p: Path, dry: bool, why: str) -> bool:
        if p.exists():
            return False
        if not dry:
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(f"{now_iso()}: {why}\n")
        return True

    @staticmethod
    def remove(p: Path, dry: bool) -> bool:
        if not p.exists():
            return False
        if not dry:
            p.unlink(missing_ok=True)
        return True

    def set_state(self, spec: dict, state: str, dry: bool, extra: dict | None = None) -> None:
        if dry:
            return
        props = {"State": P_select(state), **(extra or {})}
        self.hub.notion.update(spec["page"]["id"], props)
        self.hub.m.pop(f"spec:{spec['slug']}", None)   # let the next cycle rewrite the row from disk

    def spec_jobs(self, spec: dict) -> list[hd.Job]:
        """The spec's running and pending jobs: tagged cc:<slug> or submitted under its repo dir. Watcher jobs excluded."""
        d = (spec.get("dir") or "").rstrip("/")
        out = []
        for j in hd.squeue_jobs(USER, self.hub.tz):
            if j.name.startswith(self.hub.prefix) or not (j.running or j.pending):
                continue
            wd = (j.workdir or "").rstrip("/")
            if j.tag == spec["slug"] or (d and (wd == d or wd.startswith(d + "/"))):
                out.append(j)
        return out

    # ------------------------------------------------------------------ actions
    def do_pause_loop(self, spec, args, dry) -> str:
        sd = self.state_dir(spec)
        if not self.touch(sd / "research" / "PAUSED", dry, "paused from the control center"):
            return f"leave {spec['slug']}'s agent loop paused (it already was)"
        self.set_state(spec, "paused", dry)
        return f"pause {spec['slug']}'s agent loop ({sd}/research/PAUSED); its running jobs keep running"

    def do_resume_loop(self, spec, args, dry) -> str:
        sd = self.state_dir(spec)
        if not self.remove(sd / "research" / "PAUSED", dry):
            return f"leave {spec['slug']}'s agent loop as it is (it was not paused)"
        return f"resume {spec['slug']}'s agent loop (removed {sd}/research/PAUSED)"

    def do_stop_new_jobs(self, spec, args, dry) -> str:
        sd = self.state_dir(spec)
        if not self.touch(sd / "NO_NEW_JOBS", dry, "stop new jobs from the control center"):
            return f"keep blocking new jobs for {spec['slug']} (already blocked)"
        self.set_state(spec, "no new jobs", dry)
        return f"block new jobs for {spec['slug']} ({sd}/NO_NEW_JOBS); the loop keeps working and running jobs keep running"

    def do_allow_new_jobs(self, spec, args, dry) -> str:
        sd = self.state_dir(spec)
        if not self.remove(sd / "NO_NEW_JOBS", dry):
            return f"leave {spec['slug']} as it is (new jobs were not blocked)"
        return f"allow new jobs for {spec['slug']} again (removed {sd}/NO_NEW_JOBS)"

    def do_stop_and_kill(self, spec, args, dry) -> str:
        sd = self.state_dir(spec)
        self.touch(sd / "research" / "PAUSED", dry, "stop and kill from the control center")
        self.touch(sd / "NO_NEW_JOBS", dry, "stop and kill from the control center")
        jobs = self.spec_jobs(spec)
        failed = []
        if not dry:
            for j in jobs:
                cp = slurm.run(["scancel", j.id])
                if cp.returncode != 0:
                    failed.append(f"{j.id} ({(cp.stderr or cp.stdout).strip()[:80]})")
        self.touch(sd / "STOP", dry, "stop and kill from the control center")
        self.set_state(spec, "paused", dry)
        listed = ", ".join(f"{j.id} {j.name}" for j in jobs) or "none"
        if failed:
            raise CommandError(f"paused the loop and blocked new jobs, but scancel failed for {', '.join(failed)}")
        return f"stop {spec['slug']}: pause its loop, block new jobs, cancel {len(jobs)} job(s) [{listed}] and stop its watcher"

    def do_start(self, spec, args, dry) -> str:
        sd = self.state_dir(spec)
        cleared = [n for n, p in (("STOP", sd / "STOP"), ("PAUSED", sd / "research" / "PAUSED"),
                                   ("NO_NEW_JOBS", sd / "NO_NEW_JOBS")) if self.remove(p, dry)]
        from .loop import watcher_jobs
        from .config import load
        live = [j for j in watcher_jobs(load(Path(spec["dir"]))) if j["state"] in ("RUNNING", "PENDING")]
        if live:
            what = f"keep its watcher {live[0]['id']} ({live[0]['state'].lower()})"
        elif dry:
            what = "submit its watcher job"
        else:
            from .cli import start_dir
            jid = start_dir(Path(spec["dir"]))
            what = f"submitted its watcher as job {jid}"
        self.m.setdefault("queued", {}).pop(spec["slug"], None)
        self.set_state(spec, "running", dry)
        return f"start {spec['slug']}: {'clear ' + ', '.join(cleared) + ', ' if cleared else ''}{what}"

    def do_queue(self, spec, args, dry) -> str:
        if spec.get("dir") is None:
            raise CommandError(f"spec {spec['slug']} has no repo dir on this source; set Repo dir in its Spec mappings row")
        gpus = args.get("gpus", read(spec["page"], "GPU request"))
        try:
            gpus = int(gpus)
        except (TypeError, ValueError):
            raise CommandError("queue needs a GPU request: Args {\"gpus\": N} or the spec's GPU request") from None
        if gpus < 1 or gpus > self.hub.quota:
            raise CommandError(f"GPU request {gpus} is outside 1..{self.hub.quota}")
        if not dry:
            self.m.setdefault("queued", {})[spec["slug"]] = {"gpus": gpus, "at": now_iso()}
        self.set_state(spec, "queued", dry, {"GPU request": P_num(gpus)})
        return f"queue {spec['slug']} for {gpus} GPU(s); the hub starts it once {gpus} counted GPU(s) are free"

    def do_unqueue(self, spec, args, dry) -> str:
        was = self.m.get("queued", {}).get(spec["slug"]) or read(spec["page"], "State") == "queued"
        if not dry:
            self.m.setdefault("queued", {}).pop(spec["slug"], None)
        self.set_state(spec, "paused", dry)
        return f"take {spec['slug']} off the queue" + ("" if was else " (it was not queued)")

    def do_summarize(self, spec, args, dry) -> str:
        raise CommandError("progress summaries are not built yet (step 6)")

    # ------------------------------------------------------------------ queue
    def start_queued(self, by_page: dict, slug_of: dict, gpus_used: int) -> None:
        """Called each hub cycle: start queued specs, oldest first, while enough counted GPUs are free."""
        queued = self.m.get("queued") or {}
        if not queued:
            return
        free = self.hub.quota - gpus_used
        specs = None
        for slug, q in sorted(queued.items(), key=lambda kv: kv[1].get("at", "")):
            if q["gpus"] > free:
                continue
            specs = specs or self.spec_index()
            spec = next((s for s in specs.values() if s["slug"] == slug), None)
            if spec is None:
                queued.pop(slug, None)
                continue
            try:
                msg = self.do_start(spec, {}, dry=not self.live or self.hub.dry)
                free -= q["gpus"]
                self.log(f"queued {slug} ({q['gpus']} GPU) -> {msg}")
            except (CommandError, RuntimeError, OSError) as e:
                self.log(f"queued {slug}: start failed: {e}")
