# Watcher turn: classify a failure

You are a triage step for an unattended Slurm watcher. Do not fix anything, do not resubmit. You may read the script and log if the tail is not enough (`{{SCRIPT}}`, `{{STDOUT}}`) but keep this quick.

Job `{{JOB_ID}}` (`{{JOB_NAME}}`) ended in sacct state `{{JOB_STATE}}`, exit `{{EXIT}}`, after {{ELAPSED}} (limit {{TIMELIMIT}}, mem {{MEM}}). The deterministic patterns did not match.

## Log tail
```
{{LOG_TAIL}}
```

Pick exactly one class:
- TRIVIAL — a small, unambiguous code/config slip (bad import, typo, wrong path or flag name, missing dir) that a one-line patch fixes.
- OOM — ran out of host or GPU memory.
- TRANSIENT — infrastructure hiccup unrelated to the code: NFS/IO error, network/download failure, CUDA init/driver or NCCL error at startup, node problem, `Killed` with no stack, hub rate limit. A plain resubmit is the right response.
- REAL — a genuine bug, bad hyperparameters, data problem, or anything needing judgment.
- UNSURE — cannot tell from what is available.

Reply with one line of reasoning, then exactly:

CLASS: <TRIVIAL|OOM|TRANSIENT|REAL|UNSURE>
STATUS: OK
