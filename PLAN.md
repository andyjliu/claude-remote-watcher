# claude-remote-watcher — design

Drop a watcher into any directory you launch Slurm experiments from. It runs as
its own Slurm job, tracks every job submitted from that directory, fixes the
easy failures itself, DMs you on Slack for the rest, sends status digests, and
exposes a Claude Code session you can talk to from claude.ai or Slack.

Generalizes the machinery already in `~/value-generalization`
(`slurm_jobs/monitor_dpo_aft.sbatch`, `scripts/autoresearch_notify_slack.sh`,
`.claude/settings.monitor.json`) into something cluster-agnostic and
directory-agnostic.

## Principles

1. **Zero tokens when healthy.** A deterministic Python triage layer polls
   Slurm and classifies every job. Claude is invoked only when there is an
   event (failure, stall, unknown state) or a report is due. This is the main
   structural difference from `monitor_dpo_aft`, which spent a `claude -p` turn
   every 15 min regardless.
2. **Launch once, forget.** A Slurm `scrontab` entry runs `watch ensure`
   every 15 min: resubmits the watcher if it died, wakes it when new jobs
   appear, lets it idle out when nothing is running. Preemption and walltime
   are handled inside the allocation (`--requeue`, chaining); scrontab is the
   backstop for everything else. All state is on disk, so any restart resumes.
3. **Two processes, one allocation.** The autonomous loop (`claude -p` per
   event, fresh session, file-based memory — proven robust) and an
   interactive `claude remote-control` server share the same `.watcher/` state
   dir. You talk to the interactive one; it leaves standing orders the loop
   reads on its next turn. Neither depends on the other staying alive.
4. **Portable = config, not code.** Only `sacct`/`squeue`/`sbatch`, `python3`
   (stdlib), `curl`, and the `claude` CLI. Every cluster fact (partition, QOS,
   gres, time cap, node excludes) lives in `~/.config/claude-watcher/cluster.yaml`.
5. **Bounded autonomy.** Fix tiers with attempt caps; a Claude Code settings
   deny-list; every patch recorded as a diff and reported; never commits.
6. **Bounded memory.** `claude -p` turns are fresh sessions; the only
   cross-turn memory is `agent_notes.md`, which the loop compacts itself.
   The remote-control session is recycled when idle so it never bloats.

## Repo layout

```
claude-remote-watcher/
  bin/watch                     # CLI: start <dir> | stop <dir> | ensure <dir> | status <dir> | say <dir> "msg" | report <dir>
  watcher/                      # python package, stdlib only
    discover.py                 # sacct/squeue → job records for jobs whose WorkDir is under <dir>
    triage.py                   # classify (OK/PENDING/PREEMPTED/TIMEOUT/OOM/NODE_FAIL/STALLED/CRASHED/UNKNOWN)
    fixes.py                    # tier-0/1 deterministic fixes (requeue, bump mem) + attempt bookkeeping
    state.py                    # .watcher/jobs.json, attempts, last-report time; atomic writes
    turn.py                     # render prompt for a claude -p turn, run it, parse STATUS line
    slack.py                    # outbound DM (port of autoresearch_notify_slack.sh); inbound Socket-Mode listener
    report.py                   # digest table + optional LLM summary
    directives.py               # parse `#WATCHER key=value` comments in sbatch scripts
    ensure.py                   # scrontab entrypoint: (re)submit / wake / idle-out logic
    compact.py                  # roll agent_notes.md into a summary + recent tail
  slurm/watcher.sbatch.tmpl     # rendered per-launch from cluster.yaml + watcher.yaml; self-chains
  prompts/
    turn.md                     # per-event prompt (templated: event, job record, log tail, notes, standing orders)
    CLAUDE.md                   # rules the agent obeys (copied into <dir>/.watcher/ as the agent's project CLAUDE.md)
  claude/settings.json          # permission deny-list (rm, git commit/push/reset, pip install, scancel of others, etc.)
  config/
    cluster.example.yaml        # per-cluster: partition/qos/gres/time/excludes, sacct quirks
    watcher.example.yaml        # per-dir defaults: intervals, policy tiers, report hour, model, budgets
  install.sh                    # symlinks bin/watch, creates ~/.config/claude-watcher/, checks deps + claude login
  README.md
```

State lives in the watched directory, gitignored:

```
<dir>/.watcher/
  config.yaml          # merged effective config (cluster + dir overrides)
  jobs.json            # job_id → {script, state, attempts, last_log_mtime, fixes[], ...}
  agent_notes.md       # cross-turn memory for the autonomous loop (append-only, timestamped)
  standing_orders.md   # written by you (remote-control session / Slack / `watch say`); read every turn
  inbox/               # raw inbound Slack messages, consumed into standing_orders.md
  patches/             # every source/sbatch diff the agent applied, with job id + reason
  turns/               # claude -p JSON outputs
  controller.log
  STOP | BLOCKED       # sentinels (same protocol as monitor_dpo_aft)
```

## Runtime

`watch start /path/to/exp` →
1. Creates `.watcher/`, merges config, renders `watcher.sbatch` with the
   cluster's partition/QOS/gres/time, submits it. Prints job id + Slack DM
   "watcher up for /path/to/exp".
2. Inside the allocation (`set -u`, `unset ANTHROPIC_API_KEY` for subscription
   billing, `--requeue`):
   - **Loop** (`python -m watcher.loop`): every `poll_interval` (default 5 min)
     run `discover` + `triage`. For each event: apply tier-0/1 fix
     deterministically, or spawn a `claude -p --settings claude/settings.json
     --max-budget-usd N` turn for tier-2/3. Post to Slack per policy. Chain a
     fresh allocation before the walltime cap (`--dependency=afterany`).
   - **Remote control** (`claude remote-control --name watcher-<dirname>
     --permission-mode acceptEdits`), cwd = `<dir>`, restarted by the loop if
     it dies. Shows up in claude.ai/code → sessions on any device.
   - **Slack inbound** (optional, Socket Mode): DMs to the bot land in
     `.watcher/inbox/` and are appended to `standing_orders.md`; the next
     loop tick runs a Claude turn immediately with that message as the event,
     and replies in Slack.
3. `watch stop` touches `STOP`; loop exits without chaining; remote-control
   server shut down; scrontab entry removed.

### Launch-once lifecycle (`watch ensure`)

`watch start <dir>` also installs a scrontab line
(`*/15 * * * * watch ensure <dir>`; falls back to user crontab on the login
node if `scrontab` is unavailable). Each tick, `ensure`:

- If a watcher job for `<dir>` is PENDING/RUNNING → nothing.
- Else if `STOP` exists → nothing.
- Else if any job from `<dir>` is PENDING/RUNNING, or was submitted in the
  last `wake_window` (default 2h) → submit the watcher; DM "watcher resumed".
- Else → nothing (idle; costs no allocation).

Inside the allocation the loop exits (without chaining) after `idle_hours`
(default 6) with no live jobs, DMing "watcher idle, will wake on new jobs".
So a directory you keep launching from is watched continuously; one you've
finished with quietly releases its slot. `--requeue` handles preemption;
chaining handles walltime; `ensure` catches cancelled/crashed/lost watchers.
Every path resumes from `.watcher/jobs.json` + `agent_notes.md` — nothing
in memory is load-bearing.

### Memory compaction

- **Loop turns**: each `claude -p` is a fresh session with `agent_notes.md`
  as memory. When notes exceed `notes_max_kb` (default 24) the loop runs a
  compaction turn: rewrite to a short "current state" summary + open
  items + last 20 entries; the full history is archived to
  `notes_archive/<date>.md`. Per-job closed items are dropped once the job
  is COMPLETED and reported in a digest.
- **Remote-control session**: restarted (`--continue` off, fresh session)
  when idle for `rc_recycle_hours` (default 12) or after every chained
  allocation, so it never accumulates unbounded context. The `.watcher/`
  files are its memory, not the transcript; CLAUDE.md tells it to read them
  first.
- **Slack threads**: one thread per job id (thread_ts stored in jobs.json)
  so a job's history is collapsed in Slack; digests are top-level.

### Discovery

Primary: `sacct -u $USER -S <watcher_start - 1d> -X --parsable2
--format=JobID,JobName,State,ExitCode,WorkDir,StdOut,StdErr,Submit,Start,End,Elapsed,Timelimit,ReqMem,MaxRSS,NodeList,Partition`
filtered on `WorkDir` under `<dir>` (arrays collapsed by parent id, per-task
state kept). `squeue` for reasons (e.g. `QOSMaxSubmitJobPerUserLimit`,
`launch failed requeued held`).

Optional per-script directives, parsed from the sbatch file at submit time:

```
#WATCHER resume=resubmit          # how to restart: resubmit (default) | none
#WATCHER max_attempts=3
#WATCHER mem_bump=1.5             # OOM: multiply --mem, cap at cluster.max_mem
#WATCHER batch_arg=--batch_size   # OOM alt: halve this arg instead of bumping mem
#WATCHER stall_min=45             # no stdout growth for N min while RUNNING → STALLED
#WATCHER expect_pattern=loss=     # log line pattern proving progress (optional)
#WATCHER noauto                   # never auto-fix, always escalate
```

### Triage → policy tiers

| class | detection | tier | action |
|---|---|---|---|
| PREEMPTED / NODE_FAIL / TIMEOUT | sacct State | 0 | requeue/resubmit same script, attempts++ (cap) |
| OOM | State=OUT_OF_MEMORY, or `oom-kill`/`CUDA out of memory` in log | 1 | bump `--mem` or halve `batch_arg`, resubmit; DM the change |
| CRASHED, trivial | nonzero exit + traceback matching ImportError/ModuleNotFound/FileNotFound/NameError/SyntaxError/typo-shaped | 2 | **Claude turn**: diagnose, patch (diff → `patches/`), resubmit once; DM diff + summary |
| CRASHED, other / STALLED / UNKNOWN / attempts exhausted | everything else | 3 | **Claude turn** diagnoses, writes proposal to notes, DMs you; no action until standing order |
| COMPLETED | State | – | log; included in next digest |

Tier 2 is where your risk lives. Guardrails: one attempt per job; only edits
files under `<dir>` (settings deny outside); never edits `.watcher/`,
`cluster.yaml`, or the watcher repo; the full diff goes to Slack so you can
`watch say "revert 1234"`.

### Slack

Outbound: port `autoresearch_notify_slack.sh` to `watcher/slack.py`
(`conversations.open` + `chat.postMessage`, xoxb token + user id; webhook
fallback). Reuse the existing `~/.config/valuegen/slack_autoresearch_*` files
via config path. Message policy: immediate for tier ≥1 actions, escalations,
watcher start/stop/chain/BLOCKED; throttled heartbeat (default off); digest at
`report_hour` (default 09:00 local) plus `watch report` on demand.

Digest format: one table (job, state, elapsed/limit, attempts, last action) +
counts + anything awaiting you. Optional 3-line Claude summary if any job
changed class since last digest.

Inbound: **the existing DM is one-way** — the autoresearch app only has
`chat:write`/`im:write` and nothing listens. The watcher adds a Socket Mode
listener (`slack_sdk`, the one non-stdlib dep; it needs no inbound port, so
it works from a compute node) that receives your DMs and thread replies.
Messages become standing orders and trigger a Claude turn whose reply is
posted back in the same thread. Commands handled without a Claude turn:
`status`, `report`, `stop`, `pause <job>`, `resume <job>`, `revert <patch>`,
`approve <patch>`. Replying inside a job's thread scopes the order to that
job.

### Interactive chat from claude.ai

`claude remote-control --name watcher-<dir>` in the allocation. From
claude.ai/code (or the mobile app) pick that session; you're in `<dir>` with
`.watcher/` at hand. The prompts/CLAUDE.md copied into `.watcher/` tells that
session how to read state and how to leave standing orders. Remote-control
availability from a compute node needs one smoke test (Slack egress from a CPU
job was verified 2026-07-13; the claude.ai websocket should be similar).

## Setup on your end (one-time per cluster)

1. `git clone` this repo; `./install.sh` (symlinks `watch`, writes
   `~/.config/claude-watcher/cluster.yaml` from the example — fill partition/
   qos/gres/time; on Babel: `partition: preempt`, `qos: preempt_qos`,
   `gres: gpu:1` because 0-GPU jobs are rejected).
2. `claude` logged in on that cluster (NFS `~/.claude` already is here).
3. Slack — outbound already works (`~/.config/valuegen/slack_autoresearch_*`);
   copy/point to `~/.config/claude-watcher/`. Inbound (required for two-way
   chat): in api.slack.com → your existing app → **Socket Mode: on** (creates
   an `xapp-` app-level token with `connections:write`) → Event Subscriptions
   → bot events `message.im` → OAuth scopes add `im:history` → reinstall to
   workspace → save the xapp token to `~/.config/claude-watcher/slack_app_token`
   (mode 600). ~5 minutes; no server or public URL needed.
4. Optional: `#WATCHER` directives in your sbatch scripts.

## Phases

- **P0 (skeleton, no LLM)**: `watch start/stop/ensure/status`, scrontab,
  discover + triage, tier-0/1 fixes, Slack outbound, self-chaining sbatch,
  idle-out, digest. Already useful.
- **P1 (agent + two-way chat)**: `claude -p` turns for tier-2/3 with notes
  memory + compaction, `settings.json`, patches dir, STATUS protocol, BLOCKED
  handling; Slack Socket-Mode inbound with per-job threads and shortcuts.
- **P2 (claude.ai chat)**: remote-control co-process, recycling, standing
  orders shared with the loop.
- **P3 (nice-to-have)**: W&B run-state check, multi-directory watchers under
  one allocation, `watch adopt <jobid>` for jobs launched elsewhere.

## Known risks / decisions to revisit

- Watcher on a preemptible partition gets preempted itself: fine (`--requeue`,
  state is on disk), but a preempted watcher can't requeue *your* jobs until
  it's back. Mitigation: `general` partition if you have the job-cap headroom.
- Subscription rate limits are shared with your interactive use; the loop
  backs off 30 min on quota-shaped errors (as monitor_dpo_aft does).
- Tier-2 code patches on a live research repo: the deny-list plus
  one-attempt cap plus diff-to-Slack is the guardrail; `noauto` opts a script out.
- Claude in Slack (`/install-slack-app`) might replace the Socket-Mode
  listener entirely if it can target a remote-control session; unverified —
  Socket Mode is the portable fallback.
