"""`#WATCHER key=value` directives in sbatch scripts."""
from __future__ import annotations

import re
from pathlib import Path

_RX = re.compile(r"^\s*#\s*WATCHER\s+(.+?)\s*$", re.I)
BOOL_KEYS = {"noauto"}


def parse(script: str | None) -> dict:
    out: dict = {}
    if not script:
        return out
    try:
        text = Path(script).read_text(errors="replace")
    except OSError:
        return out
    for line in text.splitlines()[:200]:
        m = _RX.match(line)
        if not m:
            continue
        for tok in m.group(1).split():
            if "=" in tok:
                k, v = tok.split("=", 1)
                out[k.strip().lower()] = v.strip()
            elif tok.lower() in BOOL_KEYS:
                out[tok.lower()] = True
    return out


def overrides(state_dir: Path, job_name: str) -> dict:
    """Directives from <state>/overrides.yaml, keyed by job-name glob. Written by the agent or the user."""
    import fnmatch
    import yaml
    p = Path(state_dir) / "overrides.yaml"
    try:
        data = yaml.safe_load(p.read_text()) or {}
    except (OSError, yaml.YAMLError):
        return {}
    out: dict = {}
    for pattern, d in data.items():
        if isinstance(d, dict) and fnmatch.fnmatch(job_name, str(pattern)):
            out.update({str(k).lower(): v for k, v in d.items()})
    return out


def effective(state_dir: Path, rec: dict) -> dict:
    """Script directives overridden by overrides.yaml, overridden by per-job pause/resume."""
    d = dict(rec.get("directives") or {})
    d.update(overrides(state_dir, rec.get("name") or ""))
    d.update(rec.get("directive_overrides") or {})
    return d
