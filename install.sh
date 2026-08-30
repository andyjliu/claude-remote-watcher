#!/bin/bash
# One-time setup per cluster. Creates a venv, installs the package, seeds
# ~/.config/claude-watcher/cluster.yaml, and checks deps. Safe to rerun.
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONF_DIR="${CLAUDE_WATCHER_CONFIG_DIR:-$HOME/.config/claude-watcher}"

cd "$REPO"
if command -v uv >/dev/null 2>&1; then
  [ -d .venv ] || uv venv -q .venv
  uv pip install -q --python .venv/bin/python -e .
else
  [ -d .venv ] || python3 -m venv .venv
  .venv/bin/pip install -q -e .
fi

mkdir -p "$CONF_DIR"
if [ ! -f "$CONF_DIR/cluster.yaml" ]; then
  cp config/cluster.example.yaml "$CONF_DIR/cluster.yaml"
  echo "seeded $CONF_DIR/cluster.yaml -- edit partition/qos/gres/time before 'watch start'"
fi

# Reuse the valuegen Slack bot credentials if present and ours are not.
for pair in "slack_autoresearch_bot_token:slack_bot_token" "slack_autoresearch_user_id:slack_user_id"; do
  src="$HOME/.config/valuegen/${pair%%:*}"; dst="$CONF_DIR/${pair##*:}"
  if [ -s "$src" ] && [ ! -e "$dst" ]; then ln -s "$src" "$dst"; echo "linked $dst -> $src"; fi
done

mkdir -p "$HOME/.local/bin"
ln -sf "$REPO/bin/watch" "$HOME/.local/bin/watch"

echo "--- checks"
for c in sbatch sacct squeue scontrol claude; do
  if command -v "$c" >/dev/null 2>&1; then echo "ok   $c"; else echo "MISSING $c"; fi
done
[ -s "$CONF_DIR/slack_bot_token" ] && echo "ok   slack outbound creds" || echo "note slack outbound not configured ($CONF_DIR/slack_bot_token + slack_user_id)"
[ -s "$CONF_DIR/slack_app_token" ] && echo "ok   slack inbound (socket mode) token" || echo "note slack inbound not configured ($CONF_DIR/slack_app_token) -- two-way chat disabled until then"
echo "done. usage: watch start /path/to/experiment/dir"
