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
from .report import digest, status, table
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
    (sd / "settings.json").write_text(tmpl.replace("{{STATE}}", _rule_path(sd)).replace("{{CONF_DIR}}", _rule_path(CONF_DIR))
                                      .replace("{{REPO}}", _rule_path(REPO)))
    if get(cfg, "research.enabled", False):
        _render_research_settings(cfg, sd)
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


def _rule_path(p: Path) -> str:
    """Absolute path for a permission rule written as `//{path}`: Claude Code reads `//x` as /x."""
    return str(p).lstrip("/")


def _render_research_settings(cfg: dict, sd: Path) -> None:
    """settings.research.json: the research deny-list plus Edit/Write denies on every control file."""
    target = Path(cfg["_target"])
    tmpl = (REPO / "claude" / "settings.research.template.json").read_text()
    for k, v in (("{{STATE}}", sd), ("{{CONF_DIR}}", CONF_DIR), ("{{REPO}}", REPO), ("{{TARGET}}", target)):
        tmpl = tmpl.replace(k, _rule_path(v))
    data = json.loads(tmpl)
    deny = data["permissions"]["deny"]
    for pat in get(cfg, "research.control_files", []) or []:
        deny += [f"Edit(//{_rule_path(target)}/{pat})", f"Write(//{_rule_path(target)}/{pat})"]
    env = get(cfg, "research.env", {}) or {}  # e.g. {CLAUDE_CODE_SUBAGENT_MODEL: opus}
    if env:
        data.setdefault("env", {}).update({str(k): str(v) for k, v in env.items()})
    (sd / "settings.research.json").write_text(json.dumps(data, indent=1))
    _trust_dir(target)


def _trust_dir(d: Path) -> None:
    """Pre-accept the workspace-trust dialog for a dir a headless turn runs in."""
    cj = Path("~/.claude.json").expanduser()
    try:
        data = json.loads(cj.read_text()) if cj.exists() else {}
    except json.JSONDecodeError:
        return
    proj = data.setdefault("projects", {}).setdefault(str(d), {})
    if proj.get("hasTrustDialogAccepted"):
        return
    proj["hasTrustDialogAccepted"] = True
    tmp = cj.with_suffix(".json.cwtmp")
    tmp.write_text(json.dumps(data, indent=2))
    os.replace(tmp, cj)


def _trust_workspace(sd: Path) -> None:
    """Pre-accept Claude Code's workspace-trust dialog for the state dir and the global
    remote-control consent (remote-control refuses to start without the former and blocks
    forever on an "Enable Remote Control? (y/n)" readline prompt without the latter; there is
    no terminal in a Slurm job to answer either)."""
    cj = Path("~/.claude.json").expanduser()
    try:
        data = json.loads(cj.read_text()) if cj.exists() else {}
    except json.JSONDecodeError:
        return
    proj = data.setdefault("projects", {}).setdefault(str(sd), {})
    if proj.get("hasTrustDialogAccepted") and data.get("remoteDialogSeen"):
        return
    proj["hasTrustDialogAccepted"] = True
    data["remoteDialogSeen"] = True
    tmp = cj.with_suffix(".json.cwtmp")
    tmp.write_text(json.dumps(data, indent=2))
    os.replace(tmp, cj)


HUB_YAML = """# Hub mode: this watcher is the cluster's Research Control Center source agent (docs/control-center.md).
# Notion database ids and the counted partitions live under `hub:` in ~/.config/claude-watcher/cluster.yaml.
hub:
  enabled: true
research:
  enabled: false
claude:
  remote_control:
    enabled: false
"""


def start_dir(target: Path, force: bool = False) -> str | None:
    """Submit the watcher job for a directory; returns the job id, or None if one is already live."""
    from .loop import sbatch_args, watcher_jobs
    cfg = _prepare(target)
    sd = Path(cfg["_state"])
    (sd / "STOP").unlink(missing_ok=True)
    live = [j for j in watcher_jobs(cfg) if j["state"] in ("RUNNING", "PENDING")]
    if live and not force:
        return None
    st = State(sd)
    st.meta.setdefault("started_at", now_iso())
    st.save()
    return slurm.sbatch(sbatch_args(cfg), cwd=str(sd))


def cmd_start(a):
    from .loop import job_name, watcher_jobs
    target = Path(a.dir).expanduser().resolve()
    if a.hub:
        target.mkdir(parents=True, exist_ok=True)
        (target / ".watcher").mkdir(exist_ok=True)
        wy = target / ".watcher" / "watcher.yaml"
        if not wy.exists() or "hub:" not in wy.read_text():
            wy.write_text(HUB_YAML)
    if not target.is_dir():
        sys.exit(f"not a directory: {target}")
    jid = start_dir(target, a.force)
    cfg = config.load(target)
    if jid is None:
        live = [j for j in watcher_jobs(cfg) if j["state"] in ("RUNNING", "PENDING")]
        print(f"watcher already queued/running for {target}: " + ", ".join(f"{j['id']}({j['state']})" for j in live))
        return
    print(f"submitted watcher {jid} ({job_name(cfg)}) on {get(cfg, 'slurm.partition')} for {target}\nstate: {cfg['_state']}"
          + ("\nmode: hub (Research Control Center source agent)" if get(cfg, "hub.enabled", False) else ""))


def cmd_hub_once(a):
    """One hub cycle in the foreground, for testing: `watch hub-once DIR [--dry-run]`."""
    from .hub import Hub
    target = Path(a.dir).expanduser().resolve()
    (target / ".watcher").mkdir(parents=True, exist_ok=True)
    cfg = config.load(target)
    st = State(Path(cfg["_state"]))
    hub = Hub(cfg, st, None, dry_run=a.dry_run or None)
    ran = hub.commands.poll(force=True)
    if ran:
        print(f"ran {ran} control command(s); see the Commands table or {st.dir / 'controller.log'}")
    status = hub.cycle()
    st.save()
    print(json.dumps(status, indent=1, default=str))
    if hub.dry:
        print(f"dry run: Notion writes recorded in {st.dir / 'hub_dry_run.jsonl'}")


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
    if get(cfg, "research.enabled", False):
        from .research import Research
        print(Research(cfg, st, None, "").summary())
    print(status(st, cfg["_target"], a.hours).replace("\n```", "").replace("```\n", ""))


def cmd_say(a):
    cfg = config.load(Path(a.dir).resolve())
    st = State(Path(cfg["_state"]))
    msg = {"ts": now_iso(), "thread_ts": None, "text": " ".join(a.text), "received": now_iso(), "source": "cli"}
    p = st.inbox / f"cli_{now_iso().replace(':', '')}_{os.getpid()}_{len(list(st.inbox.glob('*')))}.json"
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
    p = sub.add_parser("start", help="start watching a directory (idempotent)"); p.add_argument("dir"); p.add_argument("--force", action="store_true")
    p.add_argument("--hub", action="store_true", help="run this watcher as the cluster's Research Control Center hub"); p.set_defaults(f=cmd_start)
    p = sub.add_parser("hub-once", help="run one hub cycle in the foreground"); p.add_argument("dir"); p.add_argument("--dry-run", action="store_true"); p.set_defaults(f=cmd_hub_once)
    p = sub.add_parser("stop", help="stop the watcher for a directory"); p.add_argument("dir"); p.add_argument("--now", action="store_true", help="scancel immediately"); p.set_defaults(f=cmd_stop)
    p = sub.add_parser("status", help="watcher + job table"); p.add_argument("dir"); p.add_argument("--hours", type=float, default=24, help="show jobs changed within this window (live jobs always)"); p.set_defaults(f=cmd_status)
    p = sub.add_parser("say", help="send the watcher a message (like a Slack DM)"); p.add_argument("dir"); p.add_argument("text", nargs="+"); p.set_defaults(f=cmd_say)
    p = sub.add_parser("report", help="print the digest"); p.add_argument("dir"); p.add_argument("--slack", action="store_true"); p.set_defaults(f=cmd_report)
    p = sub.add_parser("render", help="show effective config and sbatch line"); p.add_argument("dir"); p.set_defaults(f=cmd_render)
    p = sub.add_parser("_loop", help=argparse.SUPPRESS); p.add_argument("dir"); p.set_defaults(f=cmd_loop)
    a = ap.parse_args(argv)
    a.f(a)


if __name__ == "__main__":
    main()
