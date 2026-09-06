"""Run one headless `claude -p` turn with the right model/budget for its tier, parse the result."""
from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path

from .config import REPO, get, tier
from .state import State, now_iso

QUOTA_RX = re.compile(r"usage limit|rate limit|session limit|quota|credit balance|overloaded|too many requests", re.I)
QUOTA_HTTP = {429, 529}  # api_error_status the CLI reports for rate/usage limits and overload


def render(template: str, **vars: str) -> str:
    text = (REPO / "prompts" / template).read_text()
    common = (REPO / "prompts" / "common.md").read_text()
    text = text.replace("{{COMMON}}", common)
    for k, v in vars.items():
        text = text.replace("{{" + k + "}}", str(v))
    return text


def run_turn(cfg: dict, state: State, kind: str, prompt: str, tag: str = "") -> dict:
    """kind = tier2 | tier3 | chat | compact | report. Returns dict(rc, result, status, slack, quota, path)."""
    model, budget = tier(cfg, kind)
    stamp = now_iso().replace(":", "").replace("+", "p")
    out = state.turns / f"{stamp}_{kind}{('_' + tag) if tag else ''}.json"
    cmd = [get(cfg, "claude.bin", "claude"), "-p", "--output-format", "json",
           "--permission-mode", "bypassPermissions", "--settings", str(state.dir / "settings.json"),
           "--model", model, "--max-budget-usd", str(budget)]
    timeout = int(get(cfg, "claude.turn_timeout_min", 40)) * 60
    state.logline(f"claude turn {kind} model={model} budget=${budget} -> {out.name}")
    try:
        cp = subprocess.run(cmd, input=prompt, capture_output=True, text=True, timeout=timeout, cwd=state.dir)
        rc, stdout, stderr = cp.returncode, cp.stdout, cp.stderr
    except subprocess.TimeoutExpired as e:
        rc, stdout, stderr = 124, (e.stdout or ""), (e.stderr or "") + "\n[timeout]"
    out.write_text(stdout)
    out.with_suffix(".err").write_text(stderr)
    (out.with_suffix(".prompt.md")).write_text(prompt)

    result, is_error, api_status = "", False, None
    for line in reversed(stdout.splitlines()):
        line = line.strip()
        if line.startswith("{"):
            try:
                obj = json.loads(line)
                result = obj.get("result", "") or ""
                is_error = bool(obj.get("is_error"))
                api_status = obj.get("api_error_status")
                if api_status is None and isinstance(obj.get("error"), dict):
                    api_status = obj["error"].get("status") or obj["error"].get("status_code")
                break
            except json.JSONDecodeError:
                continue
    status_m = [l for l in result.splitlines() if l.startswith("STATUS:")]
    status = status_m[-1].split(":", 1)[1].strip().split()[0].upper() if status_m else ""
    slack = ""
    if "SLACK:" in result:
        slack = result.rsplit("SLACK:", 1)[1]
        slack = slack.split("STATUS:", 1)[0].strip()
    quota = (api_status in QUOTA_HTTP or bool(QUOTA_RX.search(stdout + stderr))) and (rc != 0 or is_error)
    ok = rc == 0 and not is_error and bool(status)
    state.logline(f"claude turn done rc={rc} status={status or '<none>'} ok={ok}")
    return {"rc": rc, "ok": ok, "result": result, "status": status, "slack": slack, "quota": quota, "path": str(out)}
