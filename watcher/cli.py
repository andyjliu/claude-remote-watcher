"""`watch` command line."""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

from . import config, slurm
from .config import CONF_DIR, REPO, get
from .report import digest, table
from .state import State, now_iso

SBATCH_BODY = """#!/bin/bash
# Rendered by `watch start`; the controller's sbatch args (partition, qos, ...) come from
# cluster.yaml at submit time. Re-run `watch start` after changing config.
set -u
unset ANTHROPIC_API_KEY          # subscription billing, never API billing
export PATH="$HOME/.local/bin:$PATH"
export CLAUDE_WATCHER_REPO="{repo}"
cd "{state}"
exec "{repo}/bin/watch" _loop "{target}"
"""


def _prepare(target: Path) -> dict:
    cfg = config.load(target)
    sd = Path(cfg["_state"])
    sd.mkdir(parents=True, exist_ok=True)
    (sd / "slurm").mkdir(exist_ok=True)
    for f in ("agent_notes.md", "standing_orders.md"):
        p = sd / f
        if not p.exists():
            p.write_text(f"# {f[:-3].replace('_', ' ')} for {target}\n\n(created {now_iso()})\n")
    (sd / "CLAUDE.md").write_text((REPO / "prompts" / "CLAUDE.md").read_text())
    tmpl = (REPO / "claude" / "settings.template.json").read_text()
    (sd / "settings.json").write_text(tmpl.replace("{{STATE}}", str(sd)).replace("{{CONF_DIR}}", str(CONF_DIR)).replace("{{REPO}}", str(REPO)))
    (sd / "watcher.sbatch").write_text(SBATCH_BODY.format(repo=REPO, state=sd, target=cfg["_target"]))
    if not (sd / "watcher.yaml").exists():
        (sd / "watcher.yaml").write_text("# per-directory overrides (see config/watcher.example.yaml)\n{}\n")
    _trust_workspace(sd)
    # keep .watcher/ out of the experiment repo's git status
    git_excl = Path(cfg["_target"]) / ".git" / "info" / "exclude"
    if git_excl.parent.is_dir():
        cur = git_excl.read_text() if git_excl.exists() else ""
        if ".watcher/" not in cur:
            git_excl.write_text(cur.rstrip("\n") + "\n.watcher/\n")
    return cfg


def _trust_workspace(sd: Path) -> None:
    """Pre-accept Claude Code's workspace-trust dialog for the state dir (remote-control refuses
    to start otherwise, and there is no terminal in a Slurm job to accept it)."""
    cj = Path("~/.claude.json").expanduser()
    try:
        data = json.loads(cj.read_text()) if cj.exists() else {}
    except json.JSONDecodeError:
        return
    proj = data.setdefault("projects", {}).setdefault(str(sd), {})
    if proj.get("hasTrustDialogAccepted"):
        return
    proj["hasTrustDialogAccepted"] = True
    tmp = cj.with_suffix(".json.cwtmp")
    tmp.write_text(json.dumps(data, indent=2))
    os.replace(tmp, cj)


def cmd_start(a):
    from .loop import job_name, sbatch_args, watcher_jobs
    target = Path(a.dir).resolve()
    if not target.is_dir():
        sys.exit(f"not a directory: {target}")
    cfg = _prepare(target)
    sd = Path(cfg["_state"])
    (sd / "STOP").unlink(missing_ok=True)
    live = [j for j in watcher_jobs(cfg) if j["state"] in ("RUNNING", "PENDING")]
    if live and not a.force:
        print(f"watcher already queued/running for {target}: " + ", ".join(f"{j['id']}({j['state']})" for j in live))
        return
    st = State(sd)
    st.meta.setdefault("started_at", now_iso())
    st.save()
    jid = slurm.sbatch(sbatch_args(cfg), cwd=str(sd))
    print(f"submitted watcher {jid} ({job_name(cfg)}) on {get(cfg, 'slurm.partition')} for {target}\nstate: {sd}")


def cmd_stop(a):
    cfg = config.load(Path(a.dir).resolve())
    sd = Path(cfg["_state"])
    sd.mkdir(parents=True, exist_ok=True)
    (sd / "STOP").write_text(f"{now_iso()}: watch stop\n")
    from .loop import watcher_jobs
    jobs = watcher_jobs(cfg)
    if a.now:
        for j in jobs:
            slurm.run(["scancel", j["id"]])
        print("cancelled " + ", ".join(j["id"] for j in jobs) if jobs else "no watcher jobs")
    else:
        print(f"STOP written; running watcher will exit within a poll interval and cancel its successor. ({len(jobs)} watcher job(s) queued)")


def cmd_status(a):
    cfg = config.load(Path(a.dir).resolve())
    from .loop import watcher_jobs
    st = State(Path(cfg["_state"]))
    ws = watcher_jobs(cfg)
    print("watcher jobs: " + (", ".join(f"{j['id']}({j['state']},{j['elapsed']})" for j in ws) or "none"))
    for s in ("STOP", "BLOCKED"):
        if st.sentinel(s):
            print(f"!! {s}: {(st.dir / s).read_text().strip()}")
    print(f"slack inbound: {st.meta.get('slack_inbound')}  started: {st.meta.get('started_at')}")
    print(table(st))


def cmd_say(a):
    cfg = config.load(Path(a.dir).resolve())
    st = State(Path(cfg["_state"]))
    msg = {"ts": now_iso(), "thread_ts": None, "text": " ".join(a.text), "received": now_iso(), "source": "cli"}
    p = st.inbox / f"cli_{now_iso().replace(':', '')}.json"
    p.write_text(json.dumps(msg))
    print(f"queued for the watcher's next tick: {p}")


def cmd_report(a):
    cfg = config.load(Path(a.dir).resolve())
    st = State(Path(cfg["_state"]))
    body = digest(st, cfg["_target"])
    print(body)
    if a.slack:
        from .slack import Slack
        Slack(cfg, st).post(body); st.save()


def cmd_render(a):
    cfg = _prepare(Path(a.dir).resolve())
    from .loop import sbatch_args
    print("sbatch " + " ".join(sbatch_args(cfg)))
    print(json.dumps({k: v for k, v in cfg.items() if not k.startswith("_")}, indent=1))


def cmd_loop(a):
    from .loop import Loop
    target = Path(a.dir).resolve()
    cfg = config.load(target)
    sys.exit(Loop(target, cfg).run())


def main(argv=None):
    ap = argparse.ArgumentParser(prog="watch", description="Claude-driven Slurm experiment watcher")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("start", help="start watching a directory (idempotent)"); p.add_argument("dir"); p.add_argument("--force", action="store_true"); p.set_defaults(f=cmd_start)
    p = sub.add_parser("stop", help="stop the watcher for a directory"); p.add_argument("dir"); p.add_argument("--now", action="store_true", help="scancel immediately"); p.set_defaults(f=cmd_stop)
    p = sub.add_parser("status", help="watcher + job table"); p.add_argument("dir"); p.set_defaults(f=cmd_status)
    p = sub.add_parser("say", help="send the watcher a message (like a Slack DM)"); p.add_argument("dir"); p.add_argument("text", nargs="+"); p.set_defaults(f=cmd_say)
    p = sub.add_parser("report", help="print the digest"); p.add_argument("dir"); p.add_argument("--slack", action="store_true"); p.set_defaults(f=cmd_report)
    p = sub.add_parser("render", help="show effective config and sbatch line"); p.add_argument("dir"); p.set_defaults(f=cmd_render)
    p = sub.add_parser("_loop", help=argparse.SUPPRESS); p.add_argument("dir"); p.set_defaults(f=cmd_loop)
    a = ap.parse_args(argv)
    a.f(a)


if __name__ == "__main__":
    main()
