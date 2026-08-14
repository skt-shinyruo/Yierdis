# Domain Docs

This repository uses a multi-context domain documentation layout.

## Before exploring

- Read `CONTEXT-MAP.md` at the repository root.
- Follow it to each `CONTEXT.md` relevant to the work.
- Read relevant system-wide decisions under `docs/adr/`.
- Read context-specific decisions under `<context-root>/docs/adr/`.

If a file does not exist, proceed silently. Do not create it preemptively.
`/domain-modeling`, `/grill-with-docs`, and
`/improve-codebase-architecture` create domain docs lazily when decisions
or terminology are resolved.

## Layout

```text
/
|-- CONTEXT-MAP.md
|-- docs/adr/
`-- <context-root>/
    |-- CONTEXT.md
    `-- docs/adr/
```

A context root may be a Maven module group such as `yierdis-command/`,
`yierdis-db/`, or `yierdis-networking/`. `CONTEXT-MAP.md` is authoritative;
do not assume every Maven module needs an independent context.

## Vocabulary

Use terms exactly as defined by the relevant `CONTEXT.md`. If a needed concept
is absent, reconsider the term or record the gap for `/domain-modeling`.

## ADR conflicts

Explicitly flag output that contradicts an existing ADR rather than silently
overriding the decision.
