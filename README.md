# claude-remote-watcher

Drop a watcher into any directory you launch Slurm jobs from. It runs as its own
cheap Slurm job, tracks every job submitted from that directory, fixes the easy
failures itself, DMs you on Slack about the rest (and answers when you reply),
sends a daily digest, and exposes a Claude Code session you can open from
claude.ai/code on any device. Design notes: `PLAN.md`.

## Install (once per cluster)

```bash
git clone <this repo> ~/claude-remote-watcher && ~/claude-remote-watcher/install.sh
$EDITOR ~/.config/claude-watcher/cluster.yaml     # partition / qos / gres / time / models per tier
```

`install.sh` makes a venv, symlinks `~/.local/bin/watch`, seeds the cluster
config, and links existing valuegen Slack credentials if it finds them.
Requirements: `sbatch/sacct/squeue/scontrol`, `python3 >= 3.9`, `claude` CLI
logged in (subscription; the watcher unsets `ANTHROPIC_API_KEY`).

## Use

```bash
watch start  /path/to/exp      # submit the watcher job; idempotent
watch status /path/to/exp      # watcher jobs + job table
watch say    /path/to/exp "ignore t_oom failures, I'm testing"   # same as a Slack DM
watch report /path/to/exp [--slack]
watch stop   /path/to/exp [--now]
watch render /path/to/exp      # effective config + sbatch line
```

Then just `sbatch` from that directory as usual. Optional per-script directives:

```
#WATCHER max_attempts=3           # tier-0/1 resubmits per job lineage
#WATCHER mem=64G                  # OOM: resubmit with exactly this --mem
#WATCHER mem_bump=1.5             # OOM: multiply --mem (capped by watcher.max_mem_gb)
#WATCHER batch_arg=--batch_size   # OOM alternative: halve this value in the script instead
#WATCHER stall_min=45             # RUNNING with no stdout growth for N min -> escalate
#WATCHER expect_pattern=loss=     # ...unless this regex is in the log tail
#WATCHER resume=none              # never resubmit
#WATCHER noauto                   # never act; always ask
#WATCHER ignore                   # track only: no fixes, no Claude turns, no pings (another controller owns it)
```

## What it does

| failure | detection | action |
|---|---|---|
| PREEMPTED / NODE_FAIL / TIMEOUT | sacct | re-run the recorded submit line (tier 0) |
| OOM | sacct state or log pattern | bump `--mem` or halve `batch_arg`, resubmit (tier 1) |
| trivial crash (import/path/name/arg) | log pattern | Claude turn: patch once, save diff to `patches/`, resubmit (tier 2) |
| anything else, stalls, exhausted retries | — | Claude turn: diagnose, propose, ask you on Slack (tier 3) |

Tier-0/1 need no LLM. A healthy directory costs zero tokens. Each Claude turn
is a fresh `claude -p` session with the chosen tier's model and budget
(`claude.tiers` in `cluster.yaml`); its memory is `.watcher/agent_notes.md`,
auto-compacted past `notes_max_kb`.

## Talking to it

- **Slack**: one DM thread per job lineage. Reply in a thread to scope your
  message to that job ("approve", "just resubmit", "leave it"). Top-level
  `status`, `report`, `stop`, `resume`, `pause <job>` are answered without a
  Claude turn; anything else becomes a Claude chat turn and a standing order.
  Two-way needs a Socket Mode token — see Slack setup.
  Every message is prefixed with the cluster it came from (`[orchard] ...`,
  from `slurm.cluster`). When several clusters share the bot, address one with
  `orchard status`, `orchard: report`, or `@orchard leave t_oom alone`
  (`@all ...` / `all: ...` for everyone); an unaddressed message is handled by
  every cluster, and a reply inside a job thread only by the cluster that owns
  the thread.
- **claude.ai/code**: the watcher runs `claude remote-control --name cw:<dir>@<cluster>`;
  pick that session on claude.ai/code or the mobile app. It starts in
  `.watcher/`, reads the notes, and can leave standing orders for the loop.
- **shell**: `watch say`.

## Slack setup

Outbound needs `~/.config/claude-watcher/slack_bot_token` (`xoxb-`, scopes
`chat:write`, `im:write`) and `slack_user_id` (`U...`). Two-way chat needs, in
api.slack.com for the same app: Socket Mode **on** (gives an `xapp-` token with
`connections:write`) → Event Subscriptions → bot event `message.im` → scope
`im:history` → reinstall to workspace → save the token to
`~/.config/claude-watcher/slack_app_token` (mode 600). No public URL needed;
the listener runs inside the watcher job.

## Lifecycle

Launch once. The watcher job queues its own successor (`--dependency=afterany`)
as soon as it starts, so a cancelled, crashed, preempted, or timed-out watcher is
always followed by a fresh one that resumes from `.watcher/`; if two ever run,
the lower job id wins. With no live jobs it polls every `idle_poll_interval_sec`
and does nothing. `STOP` (via `watch stop` or Slack `stop`) ends the chain;
`BLOCKED` means it needs you — reply on Slack or `watch say resume`.

## State (`<dir>/.watcher/`, gitignored via `.git/info/exclude`)

`jobs.json` controller records · `agent_notes.md` agent memory ·
`standing_orders.md` your instructions · `patches/` every diff applied ·
`overrides.yaml` job-name-glob → directives (agent- or user-written; steers the deterministic fixer) · `turns/` raw Claude outputs · `controller.log` · `remote_control.log` ·
`slurm/` watcher job logs · `settings.json` rendered deny-list ·
`watcher.yaml` per-dir overrides.
