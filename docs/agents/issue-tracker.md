# Issue tracker: GitHub

Issues and specs for this repo live as GitHub issues. Use the `gh` CLI for all operations.

## Conventions

- Create: `gh issue create --title "..." --body "..."`
- Read: `gh issue view <number> --comments`
- List: `gh issue list --state open --json number,title,body,labels,comments`
- Comment: `gh issue comment <number> --body "..."`
- Label: `gh issue edit <number> --add-label "..."` or `--remove-label "..."`
- Close: `gh issue close <number> --comment "..."`

Infer the repository from `git remote -v`; `gh` does this automatically inside the clone.

## Pull requests as a triage surface

**PRs as a request surface: no.**

GitHub shares one number space across issues and pull requests. Resolve an
ambiguous `#42` with `gh pr view 42`, falling back to `gh issue view 42`.

## Skill operations

- "Publish to the issue tracker" means create a GitHub issue.
- "Fetch the relevant ticket" means run `gh issue view <number> --comments`.

## Wayfinding operations

- The map is one issue labelled `wayfinder:map`.
- Child tickets are GitHub sub-issues, falling back to a task list when needed.
- Child labels use `wayfinder:<type>`: `research`, `prototype`, `grilling`, or `task`.
- Represent blocking relationships with native issue dependencies, falling back
  to a `Blocked by: #<n>` line.
- Claim work with `gh issue edit <number> --add-assignee @me`.
- Resolve work by commenting with the answer, closing the child, and adding its
  context pointer to the map.
