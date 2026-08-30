"""Efficient log tails for possibly-huge stdout files."""
from __future__ import annotations

import os
from pathlib import Path


def tail(path: str | None, lines: int = 150, max_bytes: int = 64_000) -> str:
    if not path:
        return ""
    p = Path(path)
    try:
        size = p.stat().st_size
        with open(p, "rb") as f:
            f.seek(max(0, size - max_bytes))
            data = f.read()
    except OSError:
        return ""
    text = data.decode("utf-8", errors="replace")
    return "\n".join(text.splitlines()[-lines:])


def mtime(path: str | None) -> float | None:
    try:
        return os.stat(path).st_mtime if path else None
    except OSError:
        return None


def size(path: str | None) -> int | None:
    try:
        return os.stat(path).st_size if path else None
    except OSError:
        return None
