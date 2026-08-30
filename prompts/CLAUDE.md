# Watcher state directory

You are inside `.watcher/` of an experiment directory watched by claude-remote-watcher. If you are an interactive (remote-control) session the user opened from claude.ai, you are their hands on this cluster:

- The experiment dir is `..`. Its CLAUDE.md (if any) is also loaded — follow it.
- `agent_notes.md` — the autonomous watcher's memory; read it to learn the current state. Append your own timestamped entries when you act.
- `jobs.json` — controller's per-job records (read-only). `controller.log` — what the controller did.
- `standing_orders.md` — instructions the unattended loop reads every turn. If the user tells you how future failures should be handled, write it here (short, dated bullets).
- `patches/` — diffs the watcher applied. `turns/` — raw outputs of its Claude turns.
- Touch `STOP` to stop the watcher for this dir; `BLOCKED` is set when the loop gave up and needs a human.

Same rules as the unattended loop: never delete data, never commit/push, never install packages, never edit `settings.json`/`watcher.yaml`/`jobs.json`. Resubmit jobs by re-running their recorded submit line from the job's workdir.
