"""Keep agent_notes.md bounded: archive the full file, ask Claude to rewrite, fall back to a tail."""
from __future__ import annotations

import re
import shutil

from .claude_turn import render, run_turn
from .config import get
from .state import State, now_iso


def maybe_compact(cfg: dict, state: State) -> bool:
    max_kb = float(get(cfg, "watcher.notes_max_kb", 24))
    try:
        size_kb = state.notes.stat().st_size / 1024
    except FileNotFoundError:
        return False
    if size_kb <= max_kb:
        return False
    archive = f"{now_iso()[:19].replace(':', '')}.md"
    shutil.copy(state.notes, state.dir / "notes_archive" / archive)
    prompt = render("turn_compact.md", COMMON="", TARGET=cfg["_target"], STATE=cfg["_state"],
                    SIZE_KB=f"{size_kb:.0f}", MAX_KB=f"{max_kb:.0f}", ARCHIVE=archive)
    res = run_turn(cfg, state, "compact", prompt)
    if res["ok"] and state.notes.stat().st_size / 1024 < size_kb:
        state.logline(f"notes compacted {size_kb:.0f}KB -> {state.notes.stat().st_size/1024:.0f}KB")
        return True
    # fallback: keep the last 30 entries
    text = state.notes.read_text()
    entries = re.split(r"(?m)^(?=### )", text)
    keep = "".join(entries[-30:])
    state.notes.write_text(f"# agent notes (auto-truncated {now_iso()}; full history in notes_archive/{archive})\n\n{keep}")
    state.logline("notes compacted by fallback truncation")
    return True
