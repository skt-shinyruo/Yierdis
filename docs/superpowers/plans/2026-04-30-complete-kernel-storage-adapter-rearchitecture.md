# Complete Kernel Storage Adapter Rearchitecture Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Finish the remaining roadmap items from `docs/superpowers/specs/2026-04-28-kernel-storage-adapter-rearchitecture-design.md`.

**Architecture:** Keep Phase 6 conservative: do not promote command-family, storage-internal, or transport modules until a narrow seam exists. Complete Phase 7 by retiring active `yierdis-core-*` artifact names and moving engine/runtime implementation into their target module families. Remove the shared `yierdis-args` bucket by moving server runtime CLI config to server-app and keeping bench options in bench.

**Tech Stack:** Maven multi-module Java 25, JUnit 4, architecture guard tests, existing package names retained as migration-compatible Java packages.

---

### Task 1: Add Completion Guards

**Files:**
- Modify: `yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`
- Modify: `yierdis-architecture-tests/src/test/resources/architecture-policy.yml`

- [x] **Step 1: Add RED architecture assertions**

Add guard coverage for:
- root POM no longer aggregates `yierdis-core` or exposes `yierdis-core-api`, `yierdis-core-contract`, `yierdis-core-engine`, `yierdis-core-runtime`, `yierdis-core-db`;
- `yierdis-execution` aggregates `yierdis-engine`;
- `yierdis-runtime` aggregates `yierdis-runtime-embedded`;
- active POMs do not have production dependencies on retired `yierdis-core-*` artifacts;
- architecture policy names `yierdis-engine` and `yierdis-runtime-embedded`, not retired core sections.

- [x] **Step 2: Verify RED**

Run:

```bash
env JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -q -pl yierdis-architecture-tests -am test
```

Expected: fail on the new retired-core assertions before module moves.

### Task 2: Retire Core Artifact Names

**Files:**
- Move: `yierdis-core/yierdis-core-engine` to `yierdis-execution/yierdis-engine`
- Move: `yierdis-core/yierdis-core-runtime` to `yierdis-runtime/yierdis-runtime-embedded`
- Delete active sources/POMs for `yierdis-core/yierdis-core-api`, `yierdis-core/yierdis-core-contract`, and `yierdis-core/pom.xml`
- Modify: root `pom.xml`
- Modify: affected module POMs under `yierdis-app`, `yierdis-client`, `yierdis-integration-tests`, `yierdis-architecture-tests`
- Modify: architecture tests path helpers and policy sections

- [x] **Step 1: Move engine and runtime modules with `git mv`**

Run:

```bash
git mv yierdis-core/yierdis-core-engine yierdis-execution/yierdis-engine
git mv yierdis-core/yierdis-core-runtime yierdis-runtime/yierdis-runtime-embedded
```

- [x] **Step 2: Update artifact names and dependencies**

Replace production dependency artifacts:
- `yierdis-core-engine` -> `yierdis-engine`
- `yierdis-core-runtime` -> `yierdis-runtime-embedded`

Remove dependency-management entries and active modules for:
- `yierdis-core-api`
- `yierdis-core-contract`
- `yierdis-core-engine`
- `yierdis-core-runtime`
- `yierdis-core-db`

- [x] **Step 3: Remove retired bridge modules**

Delete active tracked files under:
- `yierdis-core/yierdis-core-api`
- `yierdis-core/yierdis-core-contract`
- `yierdis-core/pom.xml`

- [x] **Step 4: Verify renamed modules**

Run:

```bash
env JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -q -pl yierdis-execution/yierdis-engine -am test
env JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -q -pl yierdis-runtime/yierdis-runtime-embedded -am test
env JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -q -pl yierdis-architecture-tests -am test
```

Expected: all pass.

### Task 3: Retire Shared Args Bucket

**Files:**
- Move: `yierdis-args/src/main/java/yier/bubu/redis/args/*` to `yierdis-app/yierdis-server-app/src/main/java/yier/bubu/redis/args/`
- Move: `yierdis-args/src/test/java/yier/bubu/redis/args/YierdisServerArgsTest.java` to `yierdis-app/yierdis-server-app/src/test/java/yier/bubu/redis/args/YierdisServerArgsTest.java`
- Delete: `yierdis-args/pom.xml`
- Modify: root `pom.xml`
- Modify: `yierdis-app/yierdis-server-app/pom.xml`
- Modify: `yierdis-bench/pom.xml`

- [x] **Step 1: Move server runtime config into server-app**

Use `git mv` for the four server args/config Java files and their tests.

- [x] **Step 2: Remove `yierdis-args` artifact**

Remove the root module and dependency-management entry. Drop server-app's dependency on `yierdis-args`. Let bench depend on `yierdis-server-app` only if its reuse of server launch args remains necessary.

- [x] **Step 3: Verify args ownership**

Run:

```bash
env JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -q -pl yierdis-app/yierdis-server-app,yierdis-bench -am test
```

Expected: pass.

### Task 4: Finish Docs And Full Verification

**Files:**
- Modify: `README.md`
- Modify: `docs/module-architecture.md`
- Modify: `docs/request-execution-flow.md`
- Modify: `docs/development-navigation.md`
- Modify: `docs/testing-and-debugging.md`
- Modify: `docs/superpowers/specs/2026-04-28-kernel-storage-adapter-rearchitecture-design.md`

- [x] **Step 1: Update docs from roadmap to completed state**

Replace remaining target-model references to old core artifacts with `yierdis-engine`, `yierdis-runtime-embedded`, and server-app-owned args/config. Mark Phase 6 as evaluated with no additional promotions because candidates do not yet meet the promotion gate.

- [x] **Step 2: Run docs scans**

Run:

```bash
rg -n "yierdis-core-(api|contract|command|db|engine|runtime)|core-api|core-command|core-db|core-engine|core-runtime|yierdis-args" README.md docs --glob '!docs/superpowers/plans/**'
```

Expected: only historical/superseded spec context remains, not current architecture guidance.

- [x] **Step 3: Full verification and commit**

Run:

```bash
env JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -q test
git diff --check
git status --short
```

Expected: Maven and diff check pass; only intended staged changes remain before commit.
