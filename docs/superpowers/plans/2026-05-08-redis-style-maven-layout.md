# Redis-Style Maven Layout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move Yierdis from `libs/apps/tests` physical Maven paths to a Dubbo-style top-level module layout using Redis-oriented module names.

**Architecture:** This migration changes physical Maven paths and artifact IDs together, while keeping Java package names stable in this pass. Top-level aggregators become product-domain families: common, memory, networking, server, command, db, cli, benchmark, and tests. Architecture guards and documentation are updated to reference the new Maven artifact names and paths.

**Tech Stack:** Java 25, Maven multi-module reactor, JUnit 4, ArchUnit.

---

## File Structure Map

Target module paths:

```text
yierdis-common/
  yierdis-common-bytes/

yierdis-memory/
  yierdis-memory-api/
  yierdis-memory-ffm/

yierdis-networking/
  yierdis-networking-custom-v1/
  yierdis-networking-custom-v1-execution/
  yierdis-networking-netty/

yierdis-server/
  yierdis-server-api/
  yierdis-server-core/
  yierdis-server-executor/
  yierdis-server-runtime/
  yierdis-server-main/

yierdis-command/
  yierdis-command-api/
  yierdis-command-core/
  yierdis-command-builtin/

yierdis-db/
  yierdis-db-api/
  yierdis-db-testkit/
  yierdis-db-memory/

yierdis-cli/
yierdis-benchmark/

yierdis-tests/
  yierdis-architecture-tests/
  yierdis-integration-tests/
```

Artifact rename map:

```text
yierdis-bytes-lib                       -> yierdis-common-bytes
yierdis-bytes-netty                     -> removed as standalone artifact; Netty sink source moves to yierdis-networking-netty
yierdis-memory-foreign                  -> yierdis-memory-ffm
yierdis-custom-v1-wire                  -> yierdis-networking-custom-v1
yierdis-custom-v1-execution-adapter     -> yierdis-networking-custom-v1-execution
yierdis-custom-v1-netty                 -> yierdis-networking-netty
yierdis-execution-api                   -> yierdis-server-api
yierdis-engine                          -> yierdis-server-core
yierdis-executor-core                   -> yierdis-server-executor
yierdis-runtime-api                     -> yierdis-server-runtime-api
yierdis-runtime-embedded                -> yierdis-server-runtime
yierdis-command-kernel                  -> yierdis-command-core
yierdis-command-defaults                -> yierdis-command-builtin
yierdis-storage-api                     -> yierdis-db-api
yierdis-storage-testkit                 -> yierdis-db-testkit
yierdis-storage-memory                  -> yierdis-db-memory
yierdis-server-app                      -> yierdis-server-main
yierdis-client                          -> yierdis-cli
yierdis-bench                           -> yierdis-benchmark
```

Java package rename is explicitly out of scope for this pass.

## Tasks

### Task 1: Move directories with Git

**Files:** all Maven module directories.

- [ ] Move `libs/bytes/yierdis-bytes-lib` to `yierdis-common/yierdis-common-bytes`.
- [ ] Move `libs/memory` to `yierdis-memory`, then rename `yierdis-memory-foreign` to `yierdis-memory-ffm`.
- [ ] Move `libs/protocol` to `yierdis-networking`, then rename its children to the networking artifacts.
- [ ] Move `libs/execution/yierdis-execution-api`, `libs/execution/yierdis-engine`, `libs/executor/yierdis-executor-core`, `libs/runtime/yierdis-runtime-api`, `libs/runtime/yierdis-runtime-embedded`, and `apps/yierdis-server-app` under `yierdis-server`.
- [ ] Move `libs/command` to `yierdis-command`, then rename `kernel` to `core` and `defaults` to `builtin`.
- [ ] Move `libs/storage` to `yierdis-db`, then rename children to `db-*`.
- [ ] Move `apps/yierdis-client` to `yierdis-cli`.
- [ ] Move `apps/yierdis-bench` to `yierdis-benchmark`.
- [ ] Move `tests` to `yierdis-tests`.
- [ ] Move `NettyByteBufSink` source and tests from removed `yierdis-bytes-netty` into `yierdis-networking-netty`.

### Task 2: Update POMs

**Files:** root `pom.xml` and every moved module `pom.xml`.

- [ ] Update root `<modules>` to list target top-level modules.
- [ ] Update root dependency management artifact IDs to target names.
- [ ] Update aggregator artifact IDs, child `<module>` entries, and `<relativePath>` values.
- [ ] Update every dependency artifact ID according to the rename map.
- [ ] Update shade target paths only by module relocation; main class names remain unchanged.

### Task 3: Update architecture guards

**Files:** `yierdis-tests/yierdis-architecture-tests/**`.

- [ ] Update `architecture-policy.yml` module keys and dependency artifact names.
- [ ] Update hardcoded path assertions from `libs/apps/tests` to target paths.
- [ ] Keep Java package boundary rules unchanged unless they name artifact ownership text.

### Task 4: Update docs and scripts

**Files:** `README.md`, `docs/**/*.md`, `scripts/*.sh`.

- [ ] Update current documentation path references to target module paths.
- [ ] Update current artifact names in architecture/module docs to target names.
- [ ] Update smoke and bench scripts to find jars in `yierdis-server/yierdis-server-main`, `yierdis-cli`, and `yierdis-benchmark`.
- [ ] Leave historical plan/spec files readable but add a note where current structure supersedes older `libs/apps/tests` paths.

### Task 5: Verify

**Commands:**

```bash
mvn -q -DskipTests validate
mvn -q -pl yierdis-tests/yierdis-architecture-tests -am test
```

Expected: both commands exit 0.
