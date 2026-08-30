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
