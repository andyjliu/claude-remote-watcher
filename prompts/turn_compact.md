# Watcher turn: compact your notes

{{COMMON}}

`agent_notes.md` is {{SIZE_KB}} KB, over the {{MAX_KB}} KB limit. The full copy has already been archived to `notes_archive/{{ARCHIVE}}`. Rewrite `agent_notes.md` (this is the one turn where you replace it) as:

1. `## Current state` — one bullet per job lineage that is live or awaiting the user: ids, what it is, what happened, what is pending.
2. `## Open items for the user` — anything ESCALATEd and not yet answered.
3. `## Lessons` — durable facts about this experiment dir (which failures recur, what fixed them, cluster quirks). Keep these; they are the point of memory.
4. `## Recent entries` — the last 15 entries verbatim.

Drop everything about jobs that COMPLETED and have already been reported. Target well under {{MAX_KB}} KB. Do nothing else this turn.
