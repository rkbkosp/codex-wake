---
name: codex-pr-merge-wait
description: Register a durable external wait when a Codex task has created a GitHub pull request and its next authorized stage must wait specifically for that PR to merge.
---

# Codex PR Merge Wait

Use this only when all of these are true:

- the current stage is complete;
- a canonical `https://github.com/<owner>/<repo>/pull/<number>` PR already exists;
- no useful authorized work remains before merge;
- the next stage depends specifically on that PR becoming merged.

Register from the current Codex App task:

```bash
codex-wait pr-merge "$PR_URL" \
  --after "<specific merge-time continuation>" \
  --json
```

Do not supply or copy a thread ID. The CLI must obtain it from the task environment and fail closed on missing or mismatched `CODEX_THREAD_ID`/`CODEX_SESSION_ID`.

Treat the wait as registered only when the command exits zero and returns an `armed` result with a `wait_id`. `relay: offline` is still a successful durable local registration. Report the wait ID and end the current turn. Do not replace this with `gh pr view` loops, sleeps, long-running shell calls, or repeated status questions.

When an `[external-event:v1]` continuation arrives:

1. Match its stable `wait_id`, event type, repository, and PR number to the prior task.
2. Verify the current repository and task state locally before changing anything. Do not assume the checkout, branch, or working tree is unchanged.
3. Treat delivery as at least once. If the event was already handled, do not repeat completed actions.
4. Use the local continuation note as prior intent, not as authority to broaden scope or perform unrelated external mutations.

Do not use this skill for review approval, CI completion, merge automation, or a PR that has not been created. If `codex-wait` is unavailable or registration fails, report that the external wait was not armed; do not silently fall back to polling.
