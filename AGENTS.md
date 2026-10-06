## Agent skills

### Issue tracker

Issues are tracked in GitHub Issues for `skt-shinyruo/Yierdis`. See `docs/agents/issue-tracker.md`.

### Triage labels

Triage uses the five default canonical label names. See `docs/agents/triage-labels.md`.

### Domain docs

This repository uses a single-context layout. See `docs/agents/domain.md`.

### Git worktrees

Create every linked worktree inside this repository:

```bash
git worktree add -b <branch> .worktrees/<name> HEAD
```

Absolute path: `/home/feng/code/project/Yierdis/.worktrees/<name>`. `.gitignore` already ignores `.worktrees/`, so the main checkout stays clean. Edits there stay inside the workspace sandbox.

Point the implementer's working directory and searches at that worktree path. Remove it with `git worktree remove .worktrees/<name>` when its branch is merged.
