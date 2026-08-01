# Module And Test Infrastructure Simplification Plan

> Implement each task in a compiling commit. Use built-in subagents for
> independent review; do not invoke `codex exec` workers.

**Goal:** flatten the Maven graph, make DB test helpers local to the DB test
suite, and merge architecture/integration tests without changing production
contracts.

**Baseline:** stage 3 commit `34bb621c` (with the preceding DB/FFM commits),
31 POM files including six lane aggregators, two testkit modules, and two
cross-module test modules.

**JDK:** Every Java and Maven command uses:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn ...
```

## Global Constraints

- Preserve production artifact IDs, Java packages, public APIs, runtime
  behavior, owner-thread/lifecycle rules, and dependency direction.
- Keep all retained tests enabled; do not ignore, quarantine, or weaken a
  behavior assertion to make a move compile.
- Use `git mv` for moves and `apply_patch` for manual content edits.
- Do not modify the unrelated root-worktree command-pipeline plan.
- Historical superpowers records are not active module-path documentation and
  are left unchanged.

## Task 1: Record Stage 4 Baseline And Add Design

- [x] Confirm the stage 3 worktree is clean and review the target topology.
- [x] Add `docs/superpowers/specs/2026-08-01-module-test-infrastructure-simplification-design.md`.
- [x] Add this implementation plan.
- [ ] Commit the design and plan before source moves.

## Task 2: Flatten Lane Aggregators

Modify the root module list to enumerate every leaf exactly once. Delete the
six lane POMs and change each affected leaf parent to `yierdis-parent` with a
root relative path. Remove dependency-management entries for retired testkit
artifacts only after their source consumers have moved.

Verification:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -N validate
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -DskipTests compile
```

Commit the graph flattening separately from test-source moves.

## Task 3: Merge DB And Memory Testkits

- Move `TestBytes.java` and `package-info.java` into the DB-memory test tree.
- Move both stable backend helpers and their tests into the same test tree,
  retaining package names.
- Remove the two testkit POMs and their root dependency-management entries.
- Remove the two testkit dependencies from `yierdis-db-memory/pom.xml`.
- Confirm no production source imports a testkit package.

Verification:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am test
```

Commit the local test ownership migration.

## Task 4: Merge The Cross-Module Test Module

- Create `yierdis-tests/pom.xml` with the deduplicated union of both old test
  POM dependency sets.
- Move both test source trees and resources into `yierdis-tests/src/test`.
- Delete the two old test POMs and remove their root module entries.
- Delete `ArchitectureBoundaryTest`, `ArchitecturePolicyResourceTest`, and
  `architecture-policy.yml`.
- Remove SnakeYAML from root dependency management and the old architecture
  test dependency list.
- Audit `YierdisDbArchitectureGuardTest` and retain only current boundary,
  lifecycle, owner, and factory assertions.

Verification:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests -am test
```

Commit the unified test module and guard reduction.

## Task 5: Update Active Paths And Validate Contracts

Update `README.md`, active `docs/project-docs/**`, scripts, and any current
Maven command examples to use leaf modules or `yierdis-tests`. Do not rewrite
historical plans. Search for removed module/artifact names and stale parent
paths. Compare production package/API inventories before and after the move.

Run the focused DB, FFM, architecture/integration, and full reactor suites:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/yierdis-memory-ffm -am test
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am test
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests -am test
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn test
```

## Task 6: Independent Final Review

- [ ] Obtain one built-in review for Maven resolution, test ownership, and
  package/API preservation.
- [ ] Obtain a second built-in review for stale paths, dead dependencies, and
  architecture-guard coverage.
- [ ] Fix every confirmed finding and rerun the narrowest affected tests.
- [ ] Run `git diff --check`, verify the worktree is clean, and commit the
  final review fixes.
