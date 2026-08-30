# Setup, step by step

Everything below runs from a compute node (an `fnsw` interactive job is fine);
nothing needs the login node. Shared home (`~`) is assumed.

## 1. Install the repo (once per cluster, ~1 min)

```bash
git clone <repo-url> ~/claude-remote-watcher     # or rsync it over
~/claude-remote-watcher/install.sh
```

What it does: creates `.venv` (pyyaml + slack_sdk), symlinks
`~/.local/bin/watch`, seeds `~/.config/claude-watcher/cluster.yaml`, links
existing valuegen Slack credentials, and prints a dependency check. Rerun it
after `git pull`.

## 2. Cluster config (once per cluster, ~2 min)

`$EDITOR ~/.config/claude-watcher/cluster.yaml`. The seeded file is Babel's:

```yaml
slurm:
  partition: fnsw          # cheap, long-lived, CPU-only OK
  qos: fnsw_qos
  gres: "gpu:0"            # set "gpu:1" on partitions that reject 0-GPU jobs
  time: "1-00:00:00"       # watcher chains a successor; this is just the slice length
claude:
  tiers:                   # model + $ cap for each kind of Claude turn
    classify: {model: sonnet,          budget_usd: 1}   # menial: triage unrecognized crashes
    tier2:    {model: claude-opus-4-8, budget_usd: 6}   # code changes
    tier3:    {model: fable,           budget_usd: 8}   # fix-related diagnosis / proposals
    chat:     {model: claude-opus-4-8, budget_usd: 4}   # your Slack / standing-order replies
    compact:  {model: sonnet,          budget_usd: 1}   # menial: notes compaction
    report:   {model: sonnet,          budget_usd: 1}   # menial: digest prose
  classify_unknown: true   # ask `classify` before spending a tier-3 turn on unrecognized crashes
watcher:
  report_hour: 9
  timezone: America/New_York
```

For another cluster, change `slurm:` and (if partitions differ) `extra_sbatch`.
Verify with `watch render <dir>` — it prints the exact `sbatch` line.

## 3. Claude Code login (already done on Babel)

`claude` must be logged in with your subscription in `~/.claude`. The watcher
`unset`s `ANTHROPIC_API_KEY` so it can never bill the API. Test:
`echo hi | claude -p --model haiku`.

## 4. Slack — outbound (already done on Babel)

`~/.config/claude-watcher/slack_bot_token` (`xoxb-…`, scopes `chat:write`,
`im:write`) and `slack_user_id` (`U…`). `install.sh` linked these to the
valuegen files. Test: `watch report <dir> --slack` after step 6.

## 5. Slack — inbound / two-way chat (one-time, ~5 min, needs you)

The existing app only *sends*. To let you reply:

1. https://api.slack.com/apps → open the autoresearch app.
2. **Socket Mode** (left sidebar) → Enable → name the token → it shows an
   `xapp-…` token with scope `connections:write`. Copy it.
3. **OAuth & Permissions** → Bot Token Scopes → add `im:history` (keep
   `chat:write`, `im:write`).
4. **Event Subscriptions** → Enable → Subscribe to bot events → add
   `message.im` → Save.
5. Top banner: **Reinstall to workspace** (scopes changed).
6. On the cluster:
   ```bash
   install -m 600 /dev/null ~/.config/claude-watcher/slack_app_token
   $EDITOR ~/.config/claude-watcher/slack_app_token      # paste the xapp- token
   ```
7. Restart any running watcher (`watch stop --now <dir> && watch start <dir>`).
   Its "watcher up" DM should now say `slack chat on`.

Then DM the bot: `status` (instant), or anything else (Claude turn, reply in
thread). Reply *inside a job's thread* to scope it to that job.

## 6. Start watching a directory

```bash
watch start ~/value-generalization
```

Creates `~/value-generalization/.watcher/` (gitignored via `.git/info/exclude`),
pre-trusts it for Claude Code, submits `cw:value-generalization` on `fnsw`, and
DMs you. Every job whose Slurm `WorkDir` is under that directory (submitted up
to `lookback_days` ago or later) is tracked. Nothing else changes about how
you launch: keep using `sbatch`.

Optional per-directory tuning: `~/value-generalization/.watcher/watcher.yaml`
(same keys as cluster.yaml). Optional per-script `#WATCHER …` directives
(README).

## 7. claude.ai chat

Open https://claude.ai/code (or the mobile app) → the session
`cw:<dirname>@<cluster>` appears under your sessions while the watcher job is
running. It starts in `.watcher/`, reads the notes, and can act on the
experiment dir. Ask it to "leave a standing order" to change how the loop
handles things.

## 8. Day to day

- `watch status <dir>` — watcher jobs, sentinels, job table.
- Slack: per-job threads for fixes/escalations; a digest at `report_hour`.
- `watch stop <dir>` (graceful) / `--now`.
- If you see `BLOCKED` in a DM: reply anything, or `watch say <dir> resume`.
- After changing the cluster config or pulling repo updates:
  `watch stop --now <dir> && watch start <dir>` (the running job has the old
  code loaded).

## Porting to a new cluster checklist

1. rsync/clone the repo; `install.sh`.
2. Edit `cluster.yaml` `slurm:` block (`watch render` to verify).
3. `claude` login; copy the three Slack credential files (mode 600).
4. Confirm outbound HTTPS from compute nodes (`curl -sI https://slack.com`).
5. `watch start <dir>`; check `.watcher/slurm/<jobid>.out` and
   `.watcher/remote_control.log`.
