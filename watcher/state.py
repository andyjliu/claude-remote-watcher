"""On-disk state under <dir>/.watcher/. Everything the watcher knows survives a restart."""
from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    with os.fdopen(fd, "w") as f:
        f.write(text)
    os.replace(tmp, path)


class State:
    def __init__(self, state_dir: Path):
        self.dir = Path(state_dir)
        self.jobs_file = self.dir / "jobs.json"
        self.meta_file = self.dir / "meta.json"
        self.notes = self.dir / "agent_notes.md"
        self.orders = self.dir / "standing_orders.md"
        self.log = self.dir / "controller.log"
        self.inbox = self.dir / "inbox"
        self.patches = self.dir / "patches"
        self.turns = self.dir / "turns"
        for d in (self.inbox, self.patches, self.turns, self.dir / "notes_archive"):
            d.mkdir(parents=True, exist_ok=True)
        self.jobs: dict[str, dict] = self._load(self.jobs_file)
        self.meta: dict = self._load(self.meta_file)

    @staticmethod
    def _load(p: Path) -> dict:
        try:
            return json.loads(p.read_text())
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def save(self) -> None:
        atomic_write(self.jobs_file, json.dumps(self.jobs, indent=1, sort_keys=True))
        atomic_write(self.meta_file, json.dumps(self.meta, indent=1, sort_keys=True))

    def logline(self, msg: str) -> None:
        line = f"{now_iso()} [{os.environ.get('CW_JOB_ID', 'local')}] {msg}"
        print(line, flush=True)
        with open(self.log, "a") as f:
            f.write(line + "\n")

    def note(self, text: str) -> None:
        """Append a timestamped entry to agent_notes.md (the agent's cross-turn memory)."""
        with open(self.notes, "a") as f:
            f.write(f"\n### {now_iso()} (controller)\n{text.rstrip()}\n")

    def sentinel(self, name: str) -> bool:
        return (self.dir / name).exists()

    def lineage_root(self, job_id: str) -> str:
        seen = set()
        while job_id in self.jobs and self.jobs[job_id].get("parent") and job_id not in seen:
            seen.add(job_id)
            job_id = self.jobs[job_id]["parent"]
        return job_id

    def attempts(self, job_id: str) -> int:
        return int(self.jobs.get(self.lineage_root(job_id), {}).get("attempts", 0))

    def bump_attempts(self, job_id: str) -> int:
        root = self.lineage_root(job_id)
        rec = self.jobs.setdefault(root, {})
        rec["attempts"] = int(rec.get("attempts", 0)) + 1
        return rec["attempts"]
