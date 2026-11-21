# Research Control Center: Notion schema and loop contract (proposal, v0)

Status: **approved 2026-11-20** (with the checkpoint Result/Outcome fields added). The Notion
databases exist (IDs in §6). Step 2 (hub source agent, sbatch shim, loop instructions, ETA turn) is
on the `control-center` branch; running loops pick up the new instructions after the merge and a
watcher restart.

## 0. What discovery found, and what it changes

| Fact on Babel | Consequence for the design |
| --- | --- |
| `claude-remote-watcher` runs **one watcher job per experiment dir** (`cw:<dir>` on `fnsw`), not one per cluster. Today only `cw:adverse-selection` is live; `rlvr-values/task-vectors` is stopped (`STOP`, 11/11). | The per-cluster duties get their own watcher instance, the **hub**: `watch start --hub ~/.control-center`, on `fnsw`, self-chaining like every watcher. It reads every watched dir's `.watcher/` over shared `$HOME`. The per-dir watchers only gain the small changes in §3. |
| Quota: QOS `normal` on `general` has `GrpTRES gres/gpu=8`. | Babel registry row: `general: 8 GPUs (QOS normal)`. `preempt` (24) and `fnsw` are recorded but not counted. |
| Pings are **DMs** from one bot to you (DM channel id in each watcher's `meta.json` → `slack_channel`), prefixed `[babel]`. Research turns send lines tagged `MILESTONE:`/`BLOCKER:`/`QUESTION:`/`COMPLETE:`. The bot token's scopes are `chat:write, im:write, im:history`. | The hub reads that DM channel with `conversations.history` and the existing token. No new Slack scopes are needed. |
| Research sessions: `meta.json → research.session` names the live Claude Code session, stored at `~/.claude/projects/<dir with / → ->/<session>.jsonl`. | Transcript mirror = the last 10 user/assistant turns of that file. |
| Both loops already keep `results/spend.jsonl` (`node`, `tokens_in`, `tokens_out`, `usd`, `gpu_hours`). Watcher turns record their cost in `.watcher/turns/*.json`. | Spend per spec and per checkpoint comes from the ledger. Claude Code turns run on the subscription, so their cost is shown separately as "agent $ (notional)". |
| Slurm here stores **no job comments in accounting** (`AccountingStoreFlags=(null)`), and `sbatch` has no `SBATCH_COMMENT` env var. | Spec tag = `--comment=cc:<spec_id>`, added by an `sbatch` shim on the research turns' `PATH` and by the watcher's own resubmits. The hub records the tag the first time it sees a job in `squeue`. Fallback mapping is the job's `WorkDir` (which accounting does store) under a mapped repo dir. Anything else is `untracked`. |
| `docs/loop.md` and `AGENTS.md` are **control files** in each repo: hashed, and any edit blocks the loop until `research resume`. | The new loop instructions go into the watcher's own `prompts/turn_research*.md`, so they reach every loop at once and no repo control file changes. |
| Loops already track a work graph in `STATUS.md` with node states `todo/active/waiting/done/gated/stuck` and per-env nodes such as `O2[swe-bench]`. | Checkpoints = work-graph nodes, keyed by node id. The loop's own state is mapped to the five statuses in §2.2 and also kept verbatim. |
| No Notion integration token on Babel. | Done: `~/.config/claude-watcher/notion_token`. |
| No Anthropic/OpenAI admin keys. Each loop already has its own API cap (`budget.api_usd` in `configs/defaults.yaml`: advsel $50, taskvec $100). | Org-level provider spend and the GitHub watchdog are dropped. Budgets are per spec: the hub compares each spec's ledger with its cap and sends a Slack alert at 80% and 100%. |
| `fnsw` has two 32-CPU nodes (v9-20 often drained); `fnsw_qos` allows 10 running jobs. | Every watcher holds an `fnsw` slot, so the hub starts per-spec watchers only for specs that need one (§3). |

## 1. Where it lives in Notion

A new page **Research Control Center** under *Q4 Experiment Spec Backlog (interim)*, holding the 11
databases below as full-page child databases. Each spec page also gets a linked view of
**Checkpoints** filtered to that spec, so its checkpoint list appears on the spec page itself.

Conventions: times are Notion dates with times, in ISO with zone. `spec_id` is a short slug
(`advsel`, `taskvec`). "Hub" = the source agent. **W** = who writes the field: H hub, L loop (through
the hub), U you (UI or Notion).

## 2. Databases

### 2.1 Specs (one row per spec page)

| Property | Type | W | Notes |
| --- | --- | --- | --- |
| Name | title | H | Spec page title, minus the `SPEC:` prefix |
| Spec page | url | H | Child page of the backlog |
| Spec ID | text | H/U | Slug used in job tags; set once, unique |
| State | select: not started · running · paused · no new jobs · queued · done · dropped | H | Derived from the watcher sentinels and checkpoints |
| Last progress | date | H | Latest checkpoint `Updated`. Drives the 7-day filter |
| Spec ETA | date | L | When the spec stops needing GPUs (loop estimate) |
| Spec ETA as of | date | L | |
| GPU request | number | U/L | GPUs the spec needs to start (for Queue) |
| Est. runtime h | number | U/L | For queued specs, used by time-until-dry |
| API $ | number | H | Sum of `results/spend.jsonl` `usd` |
| API cap $ | number | H/U | Read from the repo's `budget.api_usd`; you can override it here. Slack alert at 80% and 100%; nothing is paused automatically |
| Agent $ (notional) | number | H | Sum of watcher turn costs (subscription, not billed) |
| Tokens | number | H | Ledger `tokens_in + tokens_out` |
| GPU-h | number | H | `sacct` actuals, last 90 days, jobs mapped to the spec (by WorkDir or tag); refreshed hourly. The ledger's `gpu_hours` are submit-time estimates (advsel: 5741 in the ledger vs about 999 actual), so they are not used |
| Checkpoints | relation ↔ Checkpoints.Spec | H | Two-way, set from the Checkpoints side |
| Summaries | relation ↔ Summaries.Spec | H | Two-way; the latest is the highest Version |

Discovery: child pages of the backlog whose title starts with `SPEC:`. You can add a row by hand for
a spec page that lives elsewhere (e.g. the values-under-RLVR agent spec).

### 2.2 Checkpoints (the canonical checkpoint list)

| Property | Type | W | Notes |
| --- | --- | --- | --- |
| Name | title | L | Short label, e.g. "Organism eval" |
| Spec | relation → Specs | H | |
| Key | text | L | Work-graph node id, e.g. `O2[swe-bench]`. Unique within a spec |
| Order | number | L | Display order |
| Status | select: not started · running · done · blocked · failed | L | Mapping below |
| Loop state | text | L | Native state, verbatim (`waiting: jobs 11229992`) |
| Summary | text | L | One line, ≤ 140 chars |
| ETA | date | L | Running checkpoints only |
| ETA as of | date | L | Stale if > 2 h old (the UI flags it) |
| Result | text | L | Done or failed checkpoints only: one line on what was found, including negative or null results (e.g. "H1 not supported: flawed and scrubbed cheat rates within seed noise") |
| Outcome | select: positive · negative · null · mixed · n/a | L | How the result reads against the checkpoint's hypothesis; `n/a` for infra checkpoints |
| Updated | date | L | Last change to Status, Summary or Result |
| Tokens | number | H | Ledger rows with `node == Key` |
| Cost $ | number | H | Same, `usd` |
| Edited by | select: loop · you | H | Set to `you` when a non-integration user edits the row; the hub then pulls the edit back to the loop |

State mapping: `todo` → not started; `active`, `waiting` → running; `done` → done; `gated`,
`stuck` → blocked; **failed** = the node ended without its result and the loop has moved on (new
explicit state).

### 2.3 Compute sources (registry plus live status)

| Property | Type | W | Notes |
| --- | --- | --- | --- |
| Name | title | U | `babel` |
| Counted quotas | text | U | `general: 8 GPU (QOS normal)` |
| GPU quota | number | U | 8 |
| Last report | date | H | The UI shows its age; no report means nothing is assumed to run |
| Hub job | text | H | Slurm id of the hub watcher |
| Status interval min | number | U | Default 10 |
| GPUs used | number | H | Counted partitions only |
| GPUs pending | number | H | GPUs requested by pending jobs |
| Running jobs · Pending jobs | number | H | |
| CPUs used · Mem GB used | number | H | All of your jobs on the cluster |
| Other partitions | text | H | e.g. `preempt: 5 GPU, 3 jobs; fnsw: 2 jobs` |
| Weekly util | number (%) | H | Trailing 168 h: GPU-h on `general` ÷ (8 × 168). Uses `sacct -D`, so preempted and requeued attempts count. `preempt` and `fnsw` are excluded |
| Hours until dry | number | H | Spec §8: hours until zero jobs remain running or pending on `general`. Uses checkpoint ETAs and `Spec ETA`, the Slurm time limit where no ETA exists, and `Est. runtime h` for queued specs |
| Dry is lower bound | checkbox | H | Shown as "≥ N h" |
| Dry detail | text | H | Which job or spec sets the number |
| Notes | text | U | |

### 2.4 Resource history

One wide row per status write: Source (relation), Time (title is the ISO time), Grain (raw · hour),
GPUs used, GPUs pending, Running jobs, Pending jobs, CPUs used, Mem GB used, Hours until dry, Weekly
util. Raw rows are kept 14 days; hourly rollups (mean and max of GPUs used) are kept indefinitely.
That's about 2,000 raw rows per source at any time, plus about 8,800 rollups a year.

### 2.5 Spec mappings

| Property | Type | W | Notes |
| --- | --- | --- | --- |
| Name | title | H | `advsel @ babel` |
| Spec · Source | relations | H/U | |
| Repo dir | text | H/U | `/home/andyliu/adverse-selection` |
| Branch / worktree | text | H | Current checkout, e.g. `as-t0-swebench` |
| Watcher job | text | H | `cw:adverse-selection` job id |
| Session | text | H | Research session id |
| Manual | checkbox | U | Rows you corrected; the hub never overwrites them |
| Last seen | date | H | |

How the hub discovers a mapping: a watched dir whose `research.prompt_files` include a `SPEC.md`
naming the spec page is mapped automatically. Otherwise the dir's Notion mirror callout is matched.
Otherwise the row is left with no spec until you set one.

### 2.6 Jobs

Job ID (title), Source, Spec (relation; empty = untracked), Mapped by (tag · workdir · manual ·
untracked), Name, Partition, State, GPUs, Submitted, Started, Ended, Time limit, GPU-h, Requeues,
Node, WorkDir. Rows are written for live jobs and kept for 30 days after they end. Only rows whose
state changed are written.

### 2.7 Errors

One row per failure fingerprint. This reuses the watcher's existing fingerprint (job name plus the
normalized last error line). Properties: Fingerprint (title), Source, Spec, Cause (OOM · TIMEOUT ·
PREEMPTED · NODE_FAIL · STALL · CRASH · recurring exception · agent-reported), Count, First seen,
Last seen, Jobs (text), Nodes (text), Example (≤ 500 chars, no secrets), Resolved (checkbox). The UI
groups the rows by spec and by cause.

### 2.8 Commands (the action log)

| Property | Type | W | Notes |
| --- | --- | --- | --- |
| Name | title | U | `pause_loop advsel` |
| Spec · Source | relations | U | |
| Action | select: start · queue · unqueue · pause_loop · resume_loop · stop_new_jobs · allow_new_jobs · stop_and_kill · summarize | U | `resume_loop` and `allow_new_jobs` undo the matching controls |
| Args | text (JSON) | U | e.g. `{"gpus":4}` for queue, `{"regenerate":true}` |
| Dry run | checkbox | U | Default **on** until the controls are verified; the hub logs what it would do and does nothing |
| Requested | date | U | |
| Status | select: pending · claimed · done · failed | H | |
| Message | text | H | What the hub did, or why it failed |
| Finished | date | H | |

The hub polls for `pending` rows for its source every 90 s (`hub.commands_poll_sec`) and claims a row
before acting on it. A row with no Source is taken by the hub whose Spec mappings place its spec on that
cluster. Rows are never deleted. Code: `watcher/commands.py`.

**Two dry-run gates.** A command runs only when its own Dry run box is off **and** `hub.commands_live: true`
is set in `cluster.yaml` (default false). Otherwise the hub sets Status done with a Message that starts
`[dry run: …] would …` and changes nothing. Turn `commands_live` on once the dry-run messages look right.

| Action | What the hub does (in the spec's repo dir from Spec mappings) |
| --- | --- |
| `pause_loop` / `resume_loop` | creates / removes `.watcher/research/PAUSED` (same as `research pause` in Slack) |
| `stop_new_jobs` / `allow_new_jobs` | creates / removes `.watcher/NO_NEW_JOBS`; the sbatch shim and the watcher's own resubmits refuse while it exists |
| `stop_and_kill` | PAUSED + NO_NEW_JOBS, `scancel` of the spec's running and pending jobs (tagged `cc:<spec>` or submitted under its repo dir; watcher jobs excluded), then `.watcher/STOP` |
| `start` | removes STOP / PAUSED / NO_NEW_JOBS and submits the spec's watcher if none is live |
| `queue` | State queued with `GPU request` from Args `{"gpus": N}`; each cycle the hub starts queued specs, oldest first, once that many counted GPUs are free |
| `unqueue` | State back to paused |
| `summarize` | not built yet (step 6): fails with a message |

The dashboard writes these rows itself (Notion connector, `notion-create-pages`) and reads them back
through the `Recent (dashboard)` view.

### 2.9 Pings

Slack ts (title; dedupe key), Time, Source, Spec, Kind (milestone · blocker · question · complete ·
escalation · digest · other), Message (≤ 2,000 chars), Slack link (permalink). The hub mirrors the
bot's top-level DM posts for its own cluster tag.

### 2.10 Transcripts

One row per mapped spec: Spec (title / relation), Session, Updated, Turns (rich text: the last 10
user/assistant turns rendered as markdown, tool calls shown as one-line stubs, up to about 60k
characters). Rewritten on every status write only when the session file has changed.

### 2.11 Summaries

One row per generated summary. The page body is the summary, with figures uploaded as Notion
files. Properties: Name, Spec, Version, Status (requested · running · done · failed), Created, Model,
Previous (relation → Summaries), Command (relation → Commands).

## 3. Watcher changes these tables need (for the PR, steps 2 and 4)

- **Hub mode** (`watch start --hub DIR`): status, history, jobs, mapping, errors, transcripts and
  pings writes on a `status_interval_min` cadence, plus the command poll. Notion writes are batched,
  only changed rows are written, and requests are capped at about 2 per second, below Notion's limit
  of about 3.
- **The hub is the only job you launch.** It owns the per-spec watchers. On Start or Queue it runs
  `watch start <repo dir>`. On Stop and kill, or when a spec reaches `done`, it runs `watch stop`.
  Every cycle it checks that each spec in state running, paused or no-new-jobs has a live watcher
  chain. If the chain died without a `STOP` sentinel, it starts it again and logs that in Errors.
  Specs that are not started, queued, done or dropped hold no `fnsw` slot. The hub also chains
  itself, so after a one-time `watch start --hub ~/.control-center` you launch nothing else, on this
  cluster or on any later one.
- **Spec tag:** the research turns' `PATH` gets `<watcher repo>/shim/sbatch`, which adds
  `--comment=cc:<spec_id>` when no comment is set. It also refuses to submit, with a clear message,
  while `.watcher/NO_NEW_JOBS` exists. The watcher's own resubmits (tier-0/1 requeue
  continuations of jobs already running) are allowed under stop-new-jobs: they reuse the original
  `SubmitLine`, which already carries the tag. Stop and kill is the control that ends those too.
- **Controls** map to existing machinery. Pause = the existing `research pause`. Stop new jobs = the
  `NO_NEW_JOBS` sentinel. Stop and kill = research pause, then `scancel` of that dir's tracked jobs
  (refused under dry run). Start = `watch start <dir>`, or `research resume`. Queue = the hub starts
  the spec once `8 − GPUs used ≥ GPU request`.
- **ETA refresh:** when a spec has a running checkpoint whose `ETA as of` is over 1 h old and no
  research turn is due within the next 10 min, the dir's watcher runs a small `eta` turn. It is a
  fresh `claude -p` session on Sonnet with a $0.50 cap, reads the files listed in §4, and may change
  only the ETA fields. A normal research turn refreshes the ETAs too, so busy loops never need the
  extra turn.

## 4. Proposed loop instruction text

This goes into `prompts/turn_research.md` and `turn_research_resume.md` (owned by the watcher, not
by the repos), as a new section before **End of turn**.

> ## Checkpoints (Research Control Center)
> Your spec's checkpoints are shown on a dashboard the user reads from their phone. Keep them
> current in `{{STATE}}/checkpoints.json`. The watcher syncs that file to Notion every few minutes,
> and edits the user makes in Notion come back into it, marked `"edited_by": "user"`. Treat a user
> edit like a standing order.
>
> - A checkpoint is a node of your work graph. Use the node id from your goal documents as `key`
>   (e.g. `O2[swe-bench]`), and add one entry per node, including nodes that have not started. Do
>   not invent checkpoints the goal documents don't have. If the graph changes, add the new nodes and
>   mark dropped ones `failed` with a reason. Never delete an entry.
> - `status` is one of `not started`, `running`, `done`, `blocked`, `failed`. Map your work-graph
>   states as follows: todo → not started, active/waiting → running, done → done, gated/stuck →
>   blocked. Use `failed` when the node ended without its result and you have moved on. Put your
>   native state, with job ids, in `loop_state`.
> - `summary` is one plain line, at most 140 characters, saying what is true now, with the number
>   that matters if there is one (e.g. "seed 0 flawed arm at step 512; cheat rate 0.31 vs 0.12").
>   Cite no files here.
> - Give every `running` checkpoint an `eta` (ISO-8601 with zone). Base it on measured progress:
>   steps per hour from logs, episodes done out of total, time left on queued jobs. Do not use the
>   Slurm time limit unless nothing else exists. Set `eta_as_of` to now whenever you recompute it,
>   which is at the end of every turn in which a running checkpoint exists. Also set top-level
>   `spec_eta`: when this spec will stop needing GPUs if the remaining plan goes as expected.
> - When a checkpoint becomes `done` or `failed`, add `result`: one plain line on what was found,
>   with the number behind it. Negative and null results are results, so say them plainly (e.g. "H1
>   not supported: flawed vs scrubbed cheat rate 0.14 vs 0.12, CIs overlap"). Set `outcome` to
>   `positive`, `negative`, `null` or `mixed` against the checkpoint's hypothesis, or `n/a` for
>   infrastructure steps.
> - Set `updated` to now whenever you change a checkpoint's `status`, `summary` or `result`.
> - **Spend:** route every paid API call through your ledger (`results/spend.jsonl`, or the ledger
>   your goal documents name) with `node` set to the checkpoint key it serves, and with
>   `tokens_in`, `tokens_out` and `usd` filled in. The dashboard sums the ledger, so a call outside
>   it is spend the user can't see.
> - Every job you `sbatch` is tagged with this spec automatically. Do not set `--comment` yourself.
>   If `sbatch` refuses with "new jobs paused by the user", that is a standing order: keep
>   monitoring and analysing, submit nothing, and say so in your SLACK line only if it blocks the
>   critical path.
>
> Format:
> ```json
> {"spec_id": "advsel", "spec_eta": "2026-11-23T18:00-05:00", "checkpoints": [
>   {"key": "O2[swe-bench]", "name": "Organism eval", "order": 7, "status": "running",
>    "loop_state": "waiting: jobs 11229992 11229036", "summary": "...",
>    "eta": "2026-11-21T14:00-05:00", "eta_as_of": "2026-11-21T00:10-05:00",
>    "updated": "2026-11-21T00:10-05:00"},
>   {"key": "I0", "name": "ImpossibleBench floor check", "order": 2, "status": "done",
>    "loop_state": "done", "summary": "...", "result": "...", "outcome": "n/a",
>    "updated": "2026-11-17T09:00-05:00"}]}
> ```

The `eta` turn gets a short prompt of its own: read `checkpoints.json`, the Loop state block of the
handoff file, `squeue`/`sacct` for the listed jobs, and their log tails, then rewrite only `eta`,
`eta_as_of` and `spec_eta`.

**Seeding:** the first time the hub sees a mapped dir with no `checkpoints.json`, a one-off `eta`-tier
turn builds it from the work-graph table in `STATUS.md` / `docs/loop.md`, for you to check in Notion.

## 5. What I needed from you (done)

1. **OK on this schema** (§2), the checkpoint format and the instruction text (§4), or edits to any
   of them.
2. **A Notion internal integration token** for the hub. Create it at notion.so/profile/integrations
   (capabilities: read, update and insert content), write it to Babel at
   `~/.config/claude-watcher/notion_token` (mode 600), and share the *Q4 Experiment Spec Backlog
   (interim)* page with the integration. Without it the cluster cannot write to Notion. The claude.ai
   connector belongs to your account and cannot be used from Babel.

Both are done (schema approved 2026-11-20, token saved).

## 5a. UI constraint: the claude.ai Notion connector

The dashboard is a claude.ai artifact that reads Notion through your claude.ai connector.
Its multi-source SQL mode (joins across data sources) needs a Notion Business plan and fails here
with `entitlement_required`. Single-source **rows mode** (up to 100 rows, filters and sorts) and
**view mode** work on the current plan. So the UI makes one query per table (Specs, Checkpoints,
Compute sources, a recent window of Jobs and Resource history) and joins them in the page. The hub
keeps tables small enough for that: raw history older than 14 days and jobs that ended more than
30 days ago are archived.

## 6. Notion IDs (created 2026-11-20)

Parent page **Research Control Center**: `41f95cc0-892f-8133-befa-f111cd2ec759`
(child of the backlog page `3fe95cc0-892f-817b-88e0-e3de41c5f32a`).

| Database | Database id | Data source id |
| --- | --- | --- |
| Compute sources | `41f95cc0892f81299f12eb76d8ee0d83` | `41f95cc0-892f-8123-836c-000b656dd77d` |
| Specs | `41f95cc0892f8171b613e34bad14570d` | `41f95cc0-892f-8187-82db-000b26b5a95c` |
| Checkpoints | `41f95cc0892f819a8737f98a32689012` | `41f95cc0-892f-8119-bfcf-000b6796c118` |
| Resource history | `41f95cc0892f8143b0f3c2454ad7bc3e` | `41f95cc0-892f-81f8-9ff5-000b2098c23e` |
| Spec mappings | `41f95cc0892f81f5a35ef4830be51750` | `41f95cc0-892f-8158-a785-000bbdcc1919` |
| Jobs | `41f95cc0892f81d6acfae45ecaafcb52` | `41f95cc0-892f-81b4-a0b3-000b9d23b0ed` |
| Errors | `41f95cc0892f8142a685de010702562d` | `41f95cc0-892f-8189-81c5-000bd545021c` |
| Commands | `41f95cc0892f81ab97abc423e7bbb18c` | `41f95cc0-892f-814b-a7a6-000b025729dd` |
| Pings | `41f95cc0892f816c8f04d4aa0818c5ec` | `41f95cc0-892f-810e-82f5-000bb069bfc0` |
| Transcripts | `41f95cc0892f810284c8d9ff678ae005` | `41f95cc0-892f-8171-ab18-000bb588a79d` |
| Summaries | `41f95cc0892f81b28344d88d891c0374` | `41f95cc0-892f-813e-8ad1-000b473c9b5c` |

Seed rows: Compute sources `babel` (`41f95cc0-892f-8196-bc6d-c17433cb8cc2`); Specs `advsel`
(`41f95cc0-892f-8169-9827-cdabdd2185a6`, running), `taskvec` (`41f95cc0-892f-81f7-a301-d6ec656a85bb`,
paused), `wsinit` (`41f95cc0-892f-81be-a51e-cab24df8743c`, done); Spec mappings for advsel and taskvec.
Each of the three spec pages has an inline Checkpoints view filtered to its spec (appended at the end
of the page).

Saved views the dashboard reads (view-mode queries don't count against the Notion plan's hourly
query allowance; rows and SQL mode do):

| Table | View | View id |
| --- | --- | --- |
| Compute sources | Default view | `41f95cc0892f81e2857f000c2877b280` |
| Specs | Default view | `41f95cc0892f81f3b7d2000c4da72efe` |
| Checkpoints | By order (dashboard) | `41f95cc0892f819f89c8000c89c97bae` |
| Jobs | Active (dashboard): RUNNING or PENDING | `41f95cc0892f8180aa81000cacf0ce90` |
| Resource history | Every cycle, newest first (dashboard) | `41f95cc0892f8139ab75000c6e9efe52` |
| Resource history | Hourly, newest first (dashboard) | `41f95cc0892f81d4b9a8000c283720c5` |
| Commands | Recent (dashboard) | `41f95cc0892f813e81da000c11193141` |

Renaming a view is fine; changing its filter or sort changes what the dashboard shows.

Notes for the hub: relation values are page ids. The Notion REST API (the hub's integration) uses
database ids for queries on API version 2022-06-28, or data source ids on 2025-09-03; the claude.ai
connector (the dashboard) uses the `collection://` data source ids.
