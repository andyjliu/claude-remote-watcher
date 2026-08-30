"""Config loading: cluster.yaml (per cluster) deep-merged with <dir>/.watcher/watcher.yaml."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

REPO = Path(os.environ.get("CLAUDE_WATCHER_REPO", Path(__file__).resolve().parent.parent))
CONF_DIR = Path(os.environ.get("CLAUDE_WATCHER_CONFIG_DIR", "~/.config/claude-watcher")).expanduser()
STATE_DIRNAME = ".watcher"


def _deep_merge(base: dict, over: dict) -> dict:
    out = dict(base)
    for k, v in (over or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def _load_yaml(p: Path) -> dict:
    if not p.exists():
        return {}
    with open(p) as f:
        return yaml.safe_load(f) or {}


def state_dir(target: Path) -> Path:
    return Path(target).resolve() / STATE_DIRNAME


def load(target: Path) -> dict:
    """Effective config for a watched directory."""
    cfg = _load_yaml(REPO / "config" / "cluster.example.yaml")  # defaults for every key
    cfg = _deep_merge(cfg, _load_yaml(CONF_DIR / "cluster.yaml"))
    cfg = _deep_merge(cfg, _load_yaml(state_dir(target) / "watcher.yaml"))
    cfg["_target"] = str(Path(target).resolve())
    cfg["_state"] = str(state_dir(target))
    cfg["_repo"] = str(REPO)
    return cfg


def get(cfg: dict, dotted: str, default: Any = None) -> Any:
    cur: Any = cfg
    for part in dotted.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur


def cluster_name(cfg: dict) -> str:
    """Human name of this cluster: slurm.cluster, else $SLURM_CLUSTER_NAME, else the hostname prefix.
    Shown as a "[name]" prefix on every Slack message, in the remote-control session name, and
    used to route DMs when several clusters share one Slack bot."""
    name = get(cfg, "slurm.cluster") or os.environ.get("SLURM_CLUSTER_NAME") or os.uname().nodename.split("-")[0]
    return str(name).strip()


def path(cfg: dict, dotted: str) -> Path | None:
    v = get(cfg, dotted)
    return Path(v).expanduser() if v else None


def tier(cfg: dict, name: str) -> tuple[str, float]:
    t = get(cfg, f"claude.tiers.{name}", {}) or {}
    return str(t.get("model", "sonnet")), float(t.get("budget_usd", 3))
