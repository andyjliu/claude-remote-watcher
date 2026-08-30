# Watcher turn: message from the user

{{COMMON}}

## Message
Received {{RECEIVED}} via {{SOURCE}} on cluster `{{CLUSTER}}`{{SCOPE}}:

> {{TEXT}}

## Context
Current jobs (last 24h + live, from jobs.json):
```
{{SUMMARY}}
```

## Your job this turn
Do what the user asks. Their message is also appended to `standing_orders.md` — if it changes how future jobs should be handled, keep it there; if it was a one-off request, you may leave it. If they approved a proposal from your notes, carry it out now (edit + diff to `patches/` + resubmit as appropriate). Answer their question in the SLACK footer; that text is posted back to them in the same thread.
