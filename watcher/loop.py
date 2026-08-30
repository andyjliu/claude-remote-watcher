"""The controller: runs inside the watcher's Slurm allocation. Polls, triages, fixes, escalates,
chats, reports, chains. Everything durable is in <dir>/.watcher/."""
from __future__ import annotations

import json
import os
import re
import signal
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from . import directives, fixes, logs, slurm, triage
from .claude_turn import REPO, render, run_turn
from .compact import maybe_compact
from .config import get, path as cfg_path
from .report import digest, status, table
from .slack import Slack
from .state import State, now_iso

USER = os.environ.get("USER") or os.environ.get("LOGNAME") or "unknown"
MAX_LLM_TURNS_PER_TICK = 3


def job_name(cfg: dict) -> str:
    return f"{get(cfg, 'slurm.job_name_prefix', 'cw')}:{Path(cfg['_target']).name}"


def sbatch_args(cfg: dict, extra: list[str] | None = None) -> list[str]:
    s = cfg["slurm"]
    args = [f"--job-name={job_name(cfg)}", f"--partition={s['partition']}", f"--cpus-per-task={s.get('cpus', 2)}",
            f"--mem={s.get('mem', '4G')}", f"--time={s['time']}", "--requeue",
            f"--output={cfg['_state']}/slurm/%j.out", "--open-mode=append"]
    if s.get("qos"):
        args.append(f"--qos={s['qos']}")
    if s.get("gres"):
        args.append(f"--gres={s['gres']}")
    args += list(s.get("extra_sbatch") or [])
    args += extra or []
    args.append(str(Path(cfg["_state"]) / "watcher.sbatch"))
    return args


def watcher_jobs(cfg: dict) -> list[dict]:
    name = job_name(cfg)
    return [j for j in slurm.squeue(USER).values() if j["name"] == name]


class Loop:
    def __init__(self, target: Path, cfg: dict):
        self.cfg = cfg
        self.target = Path(cfg["_target"])
        self.state = State(Path(cfg["_state"]))
        self.slack = Slack(cfg, self.state)
        # Only a Slurm job that *is* a watcher job takes part in chaining; a loop run by hand
        # inside some other allocation (an interactive job) must not.
        self.my_id = os.environ.get("SLURM_JOB_ID", "") if os.environ.get("SLURM_JOB_NAME") == job_name(cfg) else ""
        os.environ["CW_JOB_ID"] = self.my_id or "local"
        self.start = time.time()
        self.rc_proc: subprocess.Popen | None = None
        self.tz = ZoneInfo(get(cfg, "watcher.timezone", "UTC"))
        self.blocked_notified = False
        self.quota_until = 0.0
        self.stop = False
        signal.signal(signal.SIGTERM, self._on_term)

    def _on_term(self, *_):
        self.state.logline("SIGTERM (preemption/cancel); exiting, successor takes over")
        self.stop = True

    # ---------------------------------------------------------------- lifecycle
    def claim_or_exit(self) -> bool:
        """Lowest RUNNING watcher id wins; duplicates exit without chaining."""
        if not self.my_id:
            return True
        running = [j for j in watcher_jobs(self.cfg) if j["state"] == "RUNNING" and j["id"] != self.my_id]
        older = [j for j in running if int(j["id"]) < int(self.my_id)]
        if older:
            self.state.logline(f"watcher {older[0]['id']} already running; this one ({self.my_id}) exits")
            return False
        return True

    def ensure_successor(self) -> None:
        if not self.my_id:
            return
        pending = [j for j in watcher_jobs(self.cfg) if j["state"] == "PENDING" and j["id"] != self.my_id]
        if pending:
            return
        try:
            sid = slurm.sbatch(sbatch_args(self.cfg, [f"--dependency=afterany:{self.my_id}"]), cwd=str(self.state.dir))
            self.state.logline(f"queued successor {sid} (afterany:{self.my_id})")
        except RuntimeError as e:
            self.state.logline(f"could not queue successor: {e}")

    def cancel_successors(self) -> None:
        for j in watcher_jobs(self.cfg):
            if j["id"] != self.my_id:
                slurm.run(["scancel", j["id"]])
                self.state.logline(f"cancelled watcher job {j['id']}")

    def walltime_exceeded(self) -> bool:
        limit = slurm.parse_slurm_time(get(self.cfg, "slurm.time", "1-00:00:00"))
        if not limit:
            return False
        margin = timedelta(minutes=float(get(self.cfg, "slurm.chain_margin_min", 60)))
        return (time.time() - self.start) > (limit - margin).total_seconds()

    # ------------------------------------------------------------ remote control
    def rc_wanted(self) -> bool:
        return bool(get(self.cfg, "claude.remote_control.enabled", True))

    def rc_start(self) -> None:
        if not self.rc_wanted():
            return
        log = open(self.state.dir / "remote_control.log", "a")
        name = f"{job_name(self.cfg)}@{os.environ.get('SLURM_CLUSTER_NAME') or os.uname().nodename.split('-')[0]}"
        cmd = [get(self.cfg, "claude.bin", "claude"), "remote-control", "--name", name,
               "--permission-mode", get(self.cfg, "claude.remote_control.permission_mode", "acceptEdits")]
        env = dict(os.environ)
        env.pop("ANTHROPIC_API_KEY", None)
        self.rc_proc = subprocess.Popen(cmd, cwd=self.state.dir, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, env=env)
        self.state.meta["rc_started"] = now_iso()
        self.state.logline(f"remote-control started pid={self.rc_proc.pid} name={name}")

    def rc_tick(self) -> None:
        if not self.rc_wanted():
            return
        if self.rc_proc is None or self.rc_proc.poll() is not None:
            if self.rc_proc is not None:
                self.state.logline(f"remote-control exited rc={self.rc_proc.returncode}; restarting")
                time.sleep(30)
            self.rc_start()
            return
        idle_h = float(get(self.cfg, "claude.remote_control.recycle_idle_hours", 12))
        m = logs.mtime(str(self.state.dir / "remote_control.log")) or time.time()
        if idle_h > 0 and (time.time() - m) > idle_h * 3600:
            self.state.logline("recycling idle remote-control session")
            self.rc_stop()
            self.rc_start()

    def rc_stop(self) -> None:
        if self.rc_proc and self.rc_proc.poll() is None:
            self.rc_proc.terminate()
            try:
                self.rc_proc.wait(20)
            except subprocess.TimeoutExpired:
                self.rc_proc.kill()
        self.rc_proc = None

    # ---------------------------------------------------------------- discovery
    def discover(self) -> list[dict]:
        """Refresh jobs.json from sacct; return records whose class changed or need handling."""
        started = datetime.fromisoformat(self.state.meta["started_at"])
        since = started - timedelta(days=float(get(self.cfg, "watcher.lookback_days", 2)))
        since = max(since, datetime.now(timezone.utc) - timedelta(days=21))
        rows = slurm.sacct(USER, since.astimezone())
        prefix = str(self.target) + "/"
        own = job_name(self.cfg)
        events = []
        for r in rows:
            wd = r["WorkDir"].rstrip("/") + "/"
            if not (wd == prefix or wd.startswith(prefix)) or r["JobName"] == own:
                continue
            jid = r["JobID"]
            rec = self.state.jobs.get(jid)
            new = rec is None
            if new:
                args = slurm.submit_line_args(r["SubmitLine"])
                script = slurm.script_from_args(args)
                if script and not os.path.isabs(script):
                    script = os.path.normpath(os.path.join(r["WorkDir"], script))
                rec = self.state.jobs[jid] = {"id": jid, "first_seen": now_iso(), "script": script,
                                              "directives": directives.parse(script), "fixes": [], "klass": None}
            prev_state = rec.get("state")
            rec.update(name=r["JobName"], state=r["State"], exit_code=r["ExitCode"], workdir=r["WorkDir"],
                       submit=r["Submit"], start=r["Start"], end=r["End"], elapsed=r["Elapsed"], timelimit=r["Timelimit"],
                       req_mem=r["ReqMem"], max_rss=r["MaxRSS"], nodes=r["NodeList"], partition=r["Partition"],
                       submit_line=r["SubmitLine"], reason=r["Reason"])
            if not rec.get("submit_line_recorded") and r["SubmitLine"]:
                rec["submit_line_recorded"] = True
            if not rec.get("stdout_path") or slurm.norm_state(prev_state or "") == "PENDING":
                pat = r["StdOut"] or ""
                rec["stdout_path"] = slurm.resolve_pattern(pat, jid, r["JobName"], USER, r["NodeList"]) if pat else None
            if r["State"] != prev_state:
                rec["last_change"] = now_iso()
                if slurm.norm_state(r["State"]) == "RUNNING":
                    rec["stall_reported"] = False
            klass, tier, evidence = triage.classify(self.cfg, rec)
            changed = klass != rec.get("klass")
            rec["klass"], rec["tier"], rec["evidence"] = klass, tier, evidence
            if changed:
                rec["handled"] = False
                self.state.logline(f"job {jid} {rec['name']}: {prev_state} -> {r['State']} => {klass}")
            if tier is not None and not rec.get("handled"):
                events.append(rec)
        return events

    # ----------------------------------------------------------------- handling
    def thread_for(self, rec: dict) -> str | None:
        if not get(self.cfg, "slack.thread_per_job", True):
            return None
        root = self.state.jobs.get(self.state.lineage_root(rec["id"]), rec)
        base = root["id"].split("_")[0]
        for cand in (root, self.state.jobs.get(base, {})):
            if cand.get("thread_ts"):
                return cand["thread_ts"]
        return None

    def post_job(self, rec: dict, text: str) -> None:
        ts = self.thread_for(rec)
        root = self.state.jobs.get(self.state.lineage_root(rec["id"]), rec)
        head = f"[{rec['id']} {rec.get('name','')}] "
        new_ts = self.slack.post(head + text, thread_ts=ts)
        if ts is None and new_ts:
            root["thread_ts"] = new_ts
            base = self.state.jobs.get(root["id"].split("_")[0])
            if base is not None and not base.get("thread_ts"):
                base["thread_ts"] = new_ts

    def classify_with_claude(self, rec: dict) -> None:
        """Cheap Claude triage for a CRASHED job the regexes did not recognize; rewrites klass/tier."""
        prompt = render("turn_classify.md", JOB_ID=rec["id"], JOB_NAME=rec.get("name", ""), JOB_STATE=rec.get("state", ""),
                        EXIT=rec.get("exit_code", ""), ELAPSED=rec.get("elapsed", ""), TIMELIMIT=rec.get("timelimit", ""),
                        MEM=rec.get("req_mem", ""), SCRIPT=rec.get("script") or "?", STDOUT=rec.get("stdout_path") or "?",
                        LOG_TAIL=logs.tail(rec.get("stdout_path"), int(get(self.cfg, "watcher.log_tail_lines", 150))))
        res = run_turn(self.cfg, self.state, "classify", prompt, tag=rec["id"])
        rec["classified"] = True
        if res["quota"]:
            self.after_turn(res, None); return
        m = re.search(r"^CLASS:\s*(\w+)", res["result"], re.M)
        cls = m.group(1).upper() if m else "UNSURE"
        remap = {"TRIVIAL": ("CRASHED_TRIVIAL", 2), "OOM": ("OOM", 1), "TRANSIENT": ("TRANSIENT", 0)}
        if cls in remap:
            rec["klass"], rec["tier"] = remap[cls]
        rec["evidence"] = f"claude classify: {cls}. " + rec.get("evidence", "")
        self.state.logline(f"job {rec['id']}: classify -> {cls} => {rec['klass']} (tier {rec['tier']})")
        self.state.note(f"job {rec['id']} ({rec.get('name')}): Claude classified the crash as {cls}")

    def handle(self, rec: dict, llm_budget: list[int]) -> None:
        d = directives.effective(self.state.dir, rec)
        rec["directives_effective"] = d
        if (rec["klass"] == "CRASHED" and get(self.cfg, "claude.classify_unknown", True) and not rec.get("classified")
                and not self.state.sentinel("BLOCKED") and time.time() > self.quota_until and llm_budget[0] > 0):
            self.classify_with_claude(rec)
        klass, tier = rec["klass"], rec["tier"]
        max_att = int(d.get("max_attempts", get(self.cfg, "watcher.max_attempts", 3)))
        attempts = self.state.attempts(rec["id"])
        noauto = bool(d.get("noauto")) or d.get("resume", "resubmit") == "none"

        if tier in (0, 1) and not noauto and attempts < max_att:
            try:
                self.state.bump_attempts(rec["id"])
                if tier == 0:
                    new_id = fixes.resubmit(self.state, rec, reason=klass)
                    msg = f"{klass}: resubmitted as {new_id} (attempt {attempts+1}/{max_att})"
                else:
                    new_id, desc = fixes.oom_fix(self.cfg, self.state, rec)
                    msg = f"OOM: {desc}; resubmitted as {new_id} (attempt {attempts+1}/{max_att})"
                self.state.logline(f"job {rec['id']}: {msg}")
                self.state.note(f"job {rec['id']} ({rec.get('name')}): {msg}")
                self.post_job(rec, msg)
                return
            except (fixes.FixError, RuntimeError) as e:
                self.state.logline(f"job {rec['id']}: deterministic fix failed: {e}")
                rec["fix_error"] = str(e)
                # fall through to a Claude turn

        if self.state.sentinel("BLOCKED") or time.time() < self.quota_until or llm_budget[0] <= 0:
            return  # leave unhandled; retried next tick
        llm_budget[0] -= 1

        if noauto:
            instr, kind = "instr_noauto.md", "tier3"
        elif tier in (0, 1):
            instr, kind = "instr_exhausted.md", "tier3"
        elif tier == 2:
            instr, kind = "instr_tier2.md", "tier2"
        else:
            instr, kind = "instr_tier3.md", "tier3"
        extra = f" Deterministic fix failed: {rec['fix_error']}." if rec.get("fix_error") else ""
        instructions = (REPO / "prompts" / instr).read_text().replace("{{ATTEMPTS}}", str(attempts)) + extra
        prompt = render("turn_event.md", TARGET=str(self.target), STATE=str(self.state.dir), KLASS=klass, TIER=str(tier),
                        JOB_ID=rec["id"], JOB_NAME=rec.get("name", ""), JOB_STATE=rec.get("state", ""), EXIT=rec.get("exit_code", ""),
                        ELAPSED=rec.get("elapsed", ""), TIMELIMIT=rec.get("timelimit", ""), MEM=rec.get("req_mem", ""),
                        NODES=rec.get("nodes", ""), EVIDENCE=rec.get("evidence", ""), ATTEMPTS=str(attempts), MAX_ATTEMPTS=str(max_att),
                        FIXES=json.dumps(rec.get("fixes", [])), SCRIPT=rec.get("script") or "?", SUBMIT_LINE=rec.get("submit_line", ""),
                        DIRECTIVES=json.dumps(d), STDOUT=rec.get("stdout_path") or "?",
                        LOG_TAIL=logs.tail(rec.get("stdout_path"), int(get(self.cfg, "watcher.log_tail_lines", 150))),
                        INSTRUCTIONS=instructions)
        res = run_turn(self.cfg, self.state, kind, prompt, tag=rec["id"])
        self.after_turn(res, rec)

    def after_turn(self, res: dict, rec: dict | None) -> None:
        if res["quota"]:
            mins = float(get(self.cfg, "claude.quota_backoff_min", 30))
            self.quota_until = time.time() + mins * 60
            self.state.logline(f"quota/rate-limit shaped failure; backing off {mins:.0f} min")
            return
        if not res["ok"]:
            self.state.meta["turn_failures"] = self.state.meta.get("turn_failures", 0) + 1
            if self.state.meta["turn_failures"] >= 6:
                (self.state.dir / "BLOCKED").write_text(f"{now_iso()}: 6 consecutive failed Claude turns; see turns/\n")
                self.state.logline("6 consecutive failed turns -> BLOCKED")
            return
        self.state.meta["turn_failures"] = 0
        status = res["status"]
        text = res["slack"] or res["result"][-1500:]
        if rec is not None:
            rec["handled"] = True
            rec["last_status"] = status
            if rec.get("klass") == "STALLED":
                rec["stall_reported"] = True
            if status == "ESCALATE":
                rec["escalated"], rec["resolved"] = True, False
            if status in ("OK", "ACTED"):
                rec["resolved"] = True
            self.post_job(rec, f"{status}: {text}")
        else:
            self.slack.post(text)
        if status == "BLOCKED":
            (self.state.dir / "BLOCKED").write_text(f"{now_iso()}: agent reported BLOCKED\n")

    # -------------------------------------------------------------------- inbox
    def process_inbox(self, llm_budget: list[int]) -> None:
        for p in sorted(self.state.inbox.glob("*.json")):
            if time.time() < self.quota_until or llm_budget[0] <= 0:
                return
            try:
                msg = json.loads(p.read_text())
            except json.JSONDecodeError:
                p.unlink(); continue
            text = (msg.get("text") or "").strip()
            thread = msg.get("thread_ts")
            scope_rec = None
            if thread:
                scope_rec = next((r for r in self.state.jobs.values() if r.get("thread_ts") == thread), None)
            done = p.with_suffix(".done")
            p.rename(done)
            if self.shortcut(text, thread, scope_rec):
                continue
            with open(self.state.orders, "a") as f:
                f.write(f"- {msg.get('received', now_iso())} via {msg.get('source')}"
                        f"{' (re job ' + scope_rec['id'] + ')' if scope_rec else ''}: {text}\n")
            if self.state.sentinel("BLOCKED"):
                (self.state.dir / "BLOCKED").unlink(missing_ok=True)
                self.state.logline("user replied; clearing BLOCKED")
                self.state.meta["turn_failures"] = 0
            llm_budget[0] -= 1
            scope = f" (in the Slack thread of job {scope_rec['id']}, {scope_rec.get('name')})" if scope_rec else ""
            prompt = render("turn_chat.md", TARGET=str(self.target), STATE=str(self.state.dir), RECEIVED=msg.get("received", ""),
                            SOURCE=msg.get("source", "?"), SCOPE=scope, TEXT=text, SUMMARY=table(self.state))
            res = run_turn(self.cfg, self.state, "chat", prompt, tag="chat")
            if res["quota"] or not res["ok"]:
                self.after_turn(res, None)
                if not res["quota"]:
                    self.slack.post("(watcher) my reply turn failed; see turns/ in the state dir", thread_ts=thread)
                continue
            if scope_rec is not None:
                scope_rec["resolved"] = True
                scope_rec["handled"] = True
            self.slack.post(res["slack"] or res["result"][-1500:], thread_ts=thread)
            if res["status"] == "BLOCKED":
                (self.state.dir / "BLOCKED").write_text(f"{now_iso()}: agent reported BLOCKED\n")

    def shortcut(self, text: str, thread: str | None, rec: dict | None) -> bool:
        """Commands answered without a Claude turn."""
        t = text.lower().strip()
        if t in ("status", "report"):
            body = digest(self.state, str(self.target)) if t == "report" else status(self.state, str(self.target))
            self.slack.post(body, thread_ts=thread); return True
        m = re.match(r"^status\s+(\d+)h$", t)
        if m:
            self.slack.post(status(self.state, str(self.target), float(m.group(1))), thread_ts=thread); return True
        if t == "stop":
            (self.state.dir / "STOP").write_text(f"{now_iso()}: stop via slack\n")
            self.slack.post("stopping watcher (touch-remove STOP or `watch start` to resume)", thread_ts=thread); return True
        if t in ("resume", "unblock"):
            (self.state.dir / "BLOCKED").unlink(missing_ok=True)
            self.state.meta["turn_failures"] = 0
            self.slack.post("BLOCKED cleared", thread_ts=thread); return True
        m = re.match(r"^(pause|resume)\s+(\S+)$", t)
        if m and m.group(2) in self.state.jobs:
            self.state.jobs[m.group(2)].setdefault("directive_overrides", {})["noauto"] = (m.group(1) == "pause")
            self.slack.post(f"{m.group(1)}d auto-fixes for {m.group(2)}", thread_ts=thread); return True
        return False

    # ------------------------------------------------------------------- digest
    def maybe_digest(self) -> None:
        now = datetime.now(self.tz)
        hour = int(get(self.cfg, "watcher.report_hour", 9))
        today = now.strftime("%Y-%m-%d")
        if now.hour < hour or self.state.meta.get("last_report_date") == today:
            return
        self.state.meta["last_report_date"] = today
        body = digest(self.state, str(self.target))
        if get(self.cfg, "watcher.report_llm_summary", False) and time.time() > self.quota_until:
            res = run_turn(self.cfg, self.state, "report", render("turn_report.md", TARGET=str(self.target), STATE=str(self.state.dir), TABLE=table(self.state, 24)))
            if res["ok"] and res["slack"]:
                body += "\n" + res["slack"]
        self.slack.post(body)
        self.state.logline("posted daily digest")

    # ---------------------------------------------------------------------- run
    def run(self) -> int:
        st = self.state
        st.meta.setdefault("started_at", now_iso())
        if "last_report_date" not in st.meta:  # no digest on the day the watcher starts
            st.meta["last_report_date"] = datetime.now(self.tz).strftime("%Y-%m-%d")
        st.meta["watcher_job"] = self.my_id or "local"
        st.meta["watcher_started"] = now_iso()
        st.logline(f"watcher loop up (job {self.my_id or 'local'}, target {self.target})")
        if st.sentinel("STOP"):
            st.logline("STOP present; exiting without chaining"); self.cancel_successors(); return 0
        if not self.claim_or_exit():
            return 0
        self.ensure_successor()
        self.rc_start()
        inbound = self.slack.start_listener()
        st.meta["slack_inbound"] = inbound
        if not st.meta.get("announced"):
            self.slack.post(f"watcher up for {self.target} (job {self.my_id}); slack chat {'on' if inbound else 'off (no app token)'}")
            st.meta["announced"] = True
        st.save()

        while not self.stop:
            tick_start = time.time()
            try:
                if st.sentinel("STOP"):
                    st.logline("STOP present; shutting down"); self.cancel_successors(); self.rc_stop()
                    self.slack.post(f"watcher stopped for {self.target}"); st.meta["announced"] = False; st.save(); return 0
                if self.walltime_exceeded():
                    st.logline("walltime margin reached; exiting for successor"); self.ensure_successor(); self.rc_stop(); st.save(); return 0
                if self.my_id and int(time.time()) % 3600 < 600:
                    self.ensure_successor()
                events = self.discover()
                st.meta["last_poll"] = now_iso()
                budget = [MAX_LLM_TURNS_PER_TICK]
                for rec in events:
                    self.handle(rec, budget)
                if st.sentinel("BLOCKED") and not self.blocked_notified:
                    self.slack.post(f"watcher BLOCKED for {self.target}: {(st.dir / 'BLOCKED').read_text().strip()}. Reply here to unblock.")
                    self.blocked_notified = True
                elif not st.sentinel("BLOCKED"):
                    self.blocked_notified = False
                self.process_inbox(budget)
                self.maybe_digest()
                if time.time() > self.quota_until:
                    maybe_compact(self.cfg, st)
                self.rc_tick()
            except Exception as e:  # noqa: BLE001
                st.logline(f"tick error: {type(e).__name__}: {e}")
            st.save()
            live = any(slurm.is_live(r.get("state", "")) for r in st.jobs.values())
            interval = get(self.cfg, "watcher.poll_interval_sec", 300) if live else get(self.cfg, "watcher.idle_poll_interval_sec", 1800)
            if st.inbox.glob("*.json") and inbound:
                interval = min(interval, 20)  # responsive chat
            for _ in range(int(max(1, interval - (time.time() - tick_start)))):
                if self.stop or any(st.inbox.glob("*.json")) or st.sentinel("STOP"):
                    break
                time.sleep(1)
        self.rc_stop(); st.save()
        return 0
