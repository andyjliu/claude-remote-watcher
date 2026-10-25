"""Research mode: a goal-driven loop of bounded Claude turns run in the experiment dir itself, next to
the job watcher. A turn runs in the background so Slack chat and job triage keep ticking while it is in
flight. The goal lives in the experiment repo (research.prompt_files); the watcher only paces turns,
resumes one session per allocation, routes Slack messages and failed jobs to it, and stops it when a
control file changes.

Turn contract (the last lines of every research turn's final message):
    SLACK: <optional; posted now if it starts with MILESTONE:/BLOCKER:/QUESTION:>
    STATUS: WORKING | WAITING <ISO time and/or job ids> | BLOCKED | DONE
"""
from __future__ import annotations

import hashlib
import os
import re
import subprocess
import time
from datetime import datetime
from pathlib import Path

from .claude_turn import build_cmd, parse, render, turn_path
from .config import get, tier
from .state import State, now_iso

STATUSES = ("WORKING", "WAITING", "BLOCKED", "DONE")
SEND_NOW = re.compile(r"^\s*(MILESTONE|BLOCKER|QUESTION|COMPLETE):", re.M)
ISO_RX = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2})?(?:Z|[+-]\d{2}:?\d{2})?")
JOB_RX = re.compile(r"\b\d{4,}(?:_\d+)?\b")
CONTROL_PREFIX = "control:"


def parse_wait(line: str, now: float, default_min: float, max_min: float) -> tuple[float, list[str]]:
    """'WAITING 2026-10-25T03:00-04:00; jobs 123 456_7' -> (until epoch, [job ids]). With job ids and no
    time, wait the maximum (a job state change ends it sooner); with neither, the default."""
    rest = line.split(None, 1)[1] if len(line.split(None, 1)) > 1 else ""
    until = None
    m = ISO_RX.search(rest)
    if m:
        try:
            d = datetime.fromisoformat(m.group(0).replace("Z", "+00:00"))
            until = (d if d.tzinfo else d.astimezone()).timestamp()
        except ValueError:
            until = None
        rest = rest.replace(m.group(0), " ")
    jobs = JOB_RX.findall(rest)
    if until is None:
        until = now + 60 * (max_min if jobs else default_min)
    return min(until, now + 60 * max_min), jobs


class Research:
    def __init__(self, cfg: dict, state: State, slack, job_id: str):
        self.cfg, self.state, self.slack, self.job_id = cfg, state, slack, job_id or "local"
        self.target = Path(cfg["_target"])
        self.dir = state.dir / "research"
        self.enabled = bool(get(cfg, "research.enabled", False))
        if self.enabled:
            self.dir.mkdir(parents=True, exist_ok=True)
        self.proc: subprocess.Popen | None = None
        self.cur: dict = {}

    @property
    def m(self) -> dict:
        return self.state.meta.setdefault("research", {})

    # ------------------------------------------------------------------ sentinels
    def sentinel(self, name: str) -> Path:
        return self.dir / name

    def blocked(self) -> str | None:
        p = self.sentinel("BLOCKED")
        return p.read_text().strip() if p.exists() else None

    def block(self, why: str) -> None:
        self.sentinel("BLOCKED").write_text(f"{why}\n({now_iso()})\n")
        self.m["status"] = "BLOCKED"
        self.state.logline(f"research BLOCKED: {why}")

    def unblock(self, explicit: bool) -> bool:
        """Any user message clears an agent BLOCKED; a control-file block only clears on an explicit resume,
        which also accepts the current control files as the new baseline."""
        why = self.blocked()
        if why is None:
            return False
        if why.startswith(CONTROL_PREFIX):
            if not explicit:
                return False
            self.m["control"] = self.hash_control()
        self.sentinel("BLOCKED").unlink(missing_ok=True)
        self.m["status"], self.m["failures"] = "WORKING", 0
        self.state.logline("research unblocked")
        return True

    # ------------------------------------------------------------- control files
    def control_files(self) -> list[Path]:
        out: set[Path] = set()
        for pat in get(self.cfg, "research.control_files", []) or []:
            for p in self.target.glob(pat):  # pathlib's '**' yields directories: take every file under them
                out |= {f for f in p.rglob("*") if f.is_file()} if p.is_dir() else {p}
        return sorted(out)

    def hash_control(self) -> dict[str, str]:
        return {str(p.relative_to(self.target)): hashlib.sha256(p.read_bytes()).hexdigest() for p in self.control_files()}

    def control_changed(self) -> list[str]:
        cur = self.hash_control()
        base = self.m.get("control")
        if base is None:
            self.m["control"] = cur
            return []
        return sorted(k for k in set(cur) | set(base) if cur.get(k) != base.get(k))

    # ----------------------------------------------------------------- inputs
    def note_job(self, rec: dict, tail: str) -> None:
        """A failed or stalled job the deterministic fixer could not handle: the research turn owns it."""
        ev = self.m.setdefault("job_events", [])
        if any(e["id"] == rec["id"] and e["klass"] == rec.get("klass") for e in ev):
            return
        last = next((l for l in reversed(tail.splitlines()) if l.strip()), "")[:300]
        ev.append({"id": rec["id"], "name": rec.get("name", ""), "klass": rec.get("klass", ""), "state": rec.get("state", ""),
                   "exit": rec.get("exit_code", ""), "stdout": rec.get("stdout_path") or "?", "last_line": last,
                   "evidence": (rec.get("evidence") or "")[:300], "at": now_iso()})
        del ev[:-50]

    def new_orders(self) -> str:
        p = self.state.orders
        if not p.exists():
            return ""
        data = p.read_bytes()
        return data[int(self.m.get("orders_offset", 0)):].decode(errors="replace").strip()

    def _orders_size(self) -> int:
        try:
            return self.state.orders.stat().st_size
        except FileNotFoundError:
            return 0

    # ------------------------------------------------------------------ pacing
    def wake_reason(self, now: float | None = None) -> str | None:
        """Why a research turn should start now, or None."""
        if not self.enabled or self.proc is not None:
            return None
        if self.sentinel("DONE").exists() or self.sentinel("PAUSED").exists() or self.blocked() is not None:
            return None
        now = now or time.time()
        st = self.m.get("status")
        if not st:
            return "first turn"
        if self._orders_size() > int(self.m.get("orders_offset", 0)):
            return "new standing orders from the user"
        if self.m.get("job_events"):
            return "a job needs attention"
        if st == "WORKING" and now - float(self.m.get("last_end", 0)) >= float(get(self.cfg, "research.working_gap_sec", 60)):
            return "continuing (last turn said WORKING)"
        if st == "WAITING":
            if now >= float(self.m.get("wait_until", 0)):
                return "wait expired"
            seen = self.m.get("wait_states", {})
            for jid in self.m.get("wait_jobs", []):
                rec = self.state.jobs.get(jid)
                if rec and rec.get("state") != seen.get(jid):
                    return f"job {jid} is now {rec.get('state')}"
        return None

    def wants_tick(self) -> bool:
        return (self.proc is not None and self.proc.poll() is not None) or self.wake_reason() is not None

    def turns_today(self) -> int:
        day = datetime.now().strftime("%Y-%m-%d")
        if self.m.get("day") != day:
            self.m["day"], self.m["turns_day"] = day, 0
        return int(self.m.get("turns_day", 0))

    # -------------------------------------------------------------------- tick
    def tick(self, job_table: str, window_sec: float, quota_ok: bool) -> dict | None:
        """Finish a completed turn (returns its parsed result) and/or start a due one."""
        if not self.enabled:
            return None
        res = None
        if self.proc is not None:
            if self.proc.poll() is not None:
                res = self.finish(self.proc.returncode)
            elif time.time() > self.cur["deadline"]:
                self.state.logline("research turn exceeded its timeout; terminating")
                self.kill()
                res = self.finish(124)
            else:
                return None
        changed = self.control_changed()
        if changed and not (self.blocked() or "").startswith(CONTROL_PREFIX):
            why = f"{CONTROL_PREFIX} {', '.join(changed)} changed since the baseline"
            self.block(why)
            self.slack.post(f"[research] BLOCKER: control files changed ({', '.join(changed)}). "
                            "If you made the change, reply `research resume` to accept it as the new baseline.")
            return res
        if res is not None or not quota_ok:
            return res  # a finishing tick only collects; the next turn starts on a later tick
        reason = self.wake_reason()
        if reason is None:
            return res
        min_window = float(get(self.cfg, "research.min_window_min", 30)) * 60
        if window_sec < min_window:
            return res
        cap = int(get(self.cfg, "research.max_turns_per_day", 60))
        if cap > 0 and self.turns_today() >= cap:
            if not self.m.get("cap_noted"):
                self.m["cap_noted"] = True
                self.state.logline(f"research: daily turn cap ({cap}) reached")
            return res
        self.m["cap_noted"] = False
        self.start(reason, job_table, window_sec)
        return res

    def start(self, reason: str, job_table: str, window_sec: float) -> None:
        resume = self.m.get("session") if self.m.get("session_job") == self.job_id else None
        orders = self.new_orders() or "(none)"
        events = self.m.pop("job_events", [])
        ev_text = "\n".join(f"- job {e['id']} ({e['name']}): {e['klass']} state={e['state']} exit={e['exit']}; "
                            f"log {e['stdout']}; last line: {e['last_line']}" for e in events) or "(none)"
        common = dict(TARGET=str(self.target), STATE=str(self.state.dir), REASON=reason, ORDERS=orders, JOB_EVENTS=ev_text,
                      JOBS=job_table, NOW=now_iso(), JOB_ID=self.job_id,
                      PROMPT_FILES=", ".join(f"`{p}`" for p in get(self.cfg, "research.prompt_files", []) or []) or "(none)",
                      CONTROL_FILES=", ".join(f"`{k}`" for k in sorted(self.m.get("control", {}))) or "(none)")
        prompt = render("turn_research_resume.md" if resume else "turn_research.md", **common)
        timeout = min(float(get(self.cfg, "research.turn_timeout_min", 120)) * 60, window_sec)
        out = turn_path(self.state, "research", "resume" if resume else "fresh")
        out.with_suffix(".prompt.md").write_text(prompt)
        cmd = build_cmd(self.cfg, self.state, "research", settings=self.state.dir / "settings.research.json", resume=resume)
        env = dict(os.environ)
        env.pop("ANTHROPIC_API_KEY", None)
        stdin = open(out.with_suffix(".prompt.md"))
        stdout = open(out, "w")
        stderr = open(out.with_suffix(".err"), "w")
        self.proc = subprocess.Popen(cmd, cwd=self.target, stdin=stdin, stdout=stdout, stderr=stderr, env=env,
                                     start_new_session=True)
        for f in (stdin, stdout, stderr):
            f.close()
        self.cur = {"out": out, "resume": resume, "deadline": time.time() + timeout, "events": events, "reason": reason}
        self.m["orders_offset"] = self._orders_size()
        self.m["last_start"], self.m["last_reason"] = now_iso(), reason
        self.m["in_flight"] = True
        self.m["turns_day"] = self.turns_today() + 1
        self.m["turns_total"] = int(self.m.get("turns_total", 0)) + 1
        model, budget = tier(self.cfg, "research")
        self.state.logline(f"research turn start ({reason}) {'resume ' + resume[:8] if resume else 'fresh session'} "
                           f"model={model} budget=${budget} timeout={timeout / 60:.0f}m -> {out.name}")

    def finish(self, rc: int, count_failure: bool = True) -> dict:
        out = self.cur["out"]
        stdout = out.read_text() if out.exists() else ""
        err = out.with_suffix(".err")
        stderr = err.read_text() if err.exists() else ""
        res = parse(rc, stdout, stderr)
        res["path"] = str(out)
        self.proc = None
        self.m["in_flight"] = False
        self.m["last_end"] = time.time()
        self.state.logline(f"research turn done rc={rc} status={res['status_line'] or '<none>'} ok={res['ok']}")
        if res["session"]:
            self.m["session"], self.m["session_job"] = res["session"], self.job_id
        if res["quota"] or not res["ok"]:
            if self.cur.get("resume"):
                self.m.pop("session", None)  # a broken session fails the same way every time; start fresh
            self.m.setdefault("job_events", [])[:0] = self.cur.get("events", [])  # not seen: hand them to the next turn
            if not res["quota"] and count_failure:
                self.m["failures"] = int(self.m.get("failures", 0)) + 1
                n, cap = self.m["failures"], int(get(self.cfg, "research.max_turn_failures", 3))
                if n >= cap:
                    self.block(f"{n} consecutive failed research turns; see {out.name}")
                    self.slack.post(f"[research] BLOCKER: {n} consecutive research turns failed (last: {out.name}). "
                                    "Reply `research resume` after a look.")
            self.cur = {}
            return res
        self.m["failures"] = 0
        st = res["status"] if res["status"] in STATUSES else "WORKING"
        if st != res["status"]:
            self.state.logline(f"research: unknown STATUS {res['status']!r}; treating as WORKING")
        self.m["status"], self.m["status_line"] = st, res["status_line"]
        if st == "WAITING":
            until, jobs = parse_wait(res["status_line"], time.time(), float(get(self.cfg, "research.wait_default_min", 15)),
                                     float(get(self.cfg, "research.wait_max_min", 120)))
            self.m["wait_until"], self.m["wait_jobs"] = until, jobs
            self.m["wait_states"] = {j: self.state.jobs.get(j, {}).get("state") for j in jobs}
        text = res["slack"].strip()
        if st == "BLOCKED":
            self.block(f"agent: {text.splitlines()[0][:300] if text else 'no reason given'}")
        elif st == "DONE":
            self.sentinel("DONE").write_text(f"{text or 'done'}\n({now_iso()})\n")
            self.state.logline("research DONE")
        if text and (SEND_NOW.search(text) or st in ("BLOCKED", "DONE")):
            self.slack.post_long(f"[research] {text}")
        self.state.note(f"research turn ({self.cur.get('reason')}): {res['status_line']}"
                        + (f"\nslack: {text[:400]}" if text else ""))
        self.cur = {}
        return res

    def kill(self) -> None:
        if self.proc and self.proc.poll() is None:
            try:
                os.killpg(self.proc.pid, 15)
                self.proc.wait(30)
            except (ProcessLookupError, subprocess.TimeoutExpired):
                try:
                    os.killpg(self.proc.pid, 9)
                except ProcessLookupError:
                    pass

    # ----------------------------------------------------------------- reporting
    def summary(self) -> str:
        if not self.enabled:
            return ""
        m = self.m
        if self.proc is not None or m.get("in_flight"):  # in_flight: seen from another process (`watch status`)
            state = f"turn in flight since {m.get('last_start', '?')} ({m.get('last_reason', '')})"
        elif self.sentinel("DONE").exists():
            state = "DONE: " + self.sentinel("DONE").read_text().splitlines()[0][:200]
        elif self.sentinel("PAUSED").exists():
            state = "PAUSED (reply `research resume`)"
        elif self.blocked():
            state = "BLOCKED: " + self.blocked().splitlines()[0][:200]
        else:
            state = m.get("status_line") or m.get("status") or "not started"
        return f"research: {state} · turns today {m.get('turns_day', 0)}, total {m.get('turns_total', 0)}"

    def chat_context(self) -> str:
        if not self.enabled:
            return ""
        return ("\n## Research loop\n"
                f"This directory also runs an unattended research loop ({self.summary()}). Its durable state is in the "
                f"experiment dir (see {', '.join(get(self.cfg, 'research.prompt_files', []) or [])}); its turn log is "
                "`research` entries in `agent_notes.md`. The user's message is already in `standing_orders.md`, and the "
                "research loop reads new standing orders at the start of its next turn, so research instructions and "
                "decisions need no action from you beyond acknowledging them. Answer questions about research progress "
                "from the experiment dir's status files and results, read-only. Do not edit files in the experiment dir "
                "while the research loop is running; a research turn may be in flight.\n")
