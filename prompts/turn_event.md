# Watcher turn: {{KLASS}} on job {{JOB_ID}}

{{COMMON}}

## Event
- Job `{{JOB_ID}}` (`{{JOB_NAME}}`), sacct state `{{JOB_STATE}}`, exit `{{EXIT}}`, elapsed {{ELAPSED}} / limit {{TIMELIMIT}}, mem {{MEM}}, node {{NODES}}.
- Classified as **{{KLASS}}** (tier {{TIER}}): {{EVIDENCE}}
- Attempts so far in this job's lineage: {{ATTEMPTS}} of {{MAX_ATTEMPTS}}. Prior fixes: {{FIXES}}
- Script: `{{SCRIPT}}`  Submit line: `{{SUBMIT_LINE}}`  Directives: {{DIRECTIVES}}
- Stdout: `{{STDOUT}}`

## Log tail
```
{{LOG_TAIL}}
```

## Your job this turn
{{INSTRUCTIONS}}
