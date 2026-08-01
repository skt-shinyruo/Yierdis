# Module And Test Infrastructure Simplification Design

## Status

Approved as stage 4 of the four-stage project simplification program. This
stage changes Maven aggregation and test ownership only; production artifact
IDs, Java packages, public APIs, runtime behavior, and the FFM/DB ownership
boundaries remain unchanged. Performance and allocation counts are not
acceptance criteria.

## Context And Baseline

The current reactor uses six parent/aggregator POMs (`yierdis-common`,
`yierdis-memory`, `yierdis-db`, `yierdis-command`, `yierdis-networking`, and
`yierdis-server`). Each repeats a leaf list already known by the root POM and
each leaf inherits configuration through an intermediate parent.

Test support is split across two standalone modules even though the helpers
are consumed only by `yierdis-db-memory` tests. Cross-module tests are split
into an architecture JAR and an integration JAR. The architecture JAR also
contains a 4,168-line source/POM-shape suite and a YAML policy parser. Those
tests freeze historical file shapes rather than protecting a runtime or
public-module contract.

## Goals

- Make the root POM the sole reactor aggregator and list every leaf module
  directly.
- Make every leaf inherit `yierdis-parent` directly, preserving all artifact
  IDs, dependency coordinates, compiler settings, and plugin behavior.
- Move DB-only test helpers into `yierdis-db-memory/src/test/java` without
  changing their Java package names or behavior.
- Merge architecture and integration tests into one `yierdis-tests` module,
  preserving all behavior tests and the focused architecture guards.
- Remove source-shape policy machinery whose assertions duplicate implementation
  details and whose dependencies have no production role.
- Update active documentation, scripts, and Maven examples to the resulting
  module paths.

## Non-Goals And Frozen Contracts

- Do not change production Java sources, public classes, Java packages,
  supported commands, wire replies, error behavior, DB owner-thread rules,
  stable handles, FFM lifecycle, or maxmemory semantics.
- Do not rename a published artifact. `yierdis-tests` is a test-only artifact;
  the two old test artifact IDs are intentionally retired.
- Do not add a compatibility parent, duplicate test module, or dependency on a
  testkit from production code.
- Do not preserve a module merely to retain a path that has no independent
  lifecycle or published API.

## Target Maven Topology

The root `pom.xml` directly aggregates these leaf modules:

```text
yierdis-common-bytes
yierdis-common-memory
yierdis-common-command
yierdis-memory-api
yierdis-memory-ffm
yierdis-db-api
yierdis-db-memory
yierdis-command-api
yierdis-command-core
yierdis-command-builtin
yierdis-networking-resp
yierdis-networking-netty
yierdis-server-api
yierdis-server-core
yierdis-server-executor
yierdis-server-runtime-api
yierdis-server-runtime
yierdis-server-main
yierdis-cli
yierdis-benchmark
yierdis-tests
```

Each leaf POM has `yierdis-parent` as its parent and uses the root relative
path (`../../pom.xml` for modules one directory below a lane, or the existing
root-relative path for top-level modules). The six lane POMs are deleted.
Dependency management remains in the root, so dependency versions and direct
dependency declarations do not change as a side effect of flattening.

## Target Test Ownership

### DB Test Helpers

Move the two DB testkit source files (`TestBytes` and its package descriptor)
and the memory testkit backend helpers plus their tests into
`yierdis-db-memory/src/test/java`, retaining these packages:

- `yier.bubu.redis.storage.testkit`;
- `yier.bubu.redis.memory.testkit`.

The `yierdis-db-memory` POM keeps the test-scope dependencies it actually
needs (`junit`, DB/common/memory APIs) and drops both testkit dependencies.
The `yierdis-db-testkit` and `yierdis-memory-testkit` modules and their root
dependency-management entries are deleted. No test helper is placed under
`src/main`.

### Unified Cross-Module Tests

Create `yierdis-tests/pom.xml` as a test-only JAR inheriting the root parent.
Move both existing `src/test/java` trees under the single module, preserving
all Java package names and resources needed by behavior tests. The dependency
set is the union of the two old POMs, deduplicated and kept test-scoped.

Retain ArchUnit and the focused guards:

- `ArchitectureDependencyRuleTest`;
- `RespBoundaryGuardTest`;
- `YierdisDbArchitectureGuardTest` after removing assertions tied only to
  deleted private shapes.

Delete `ArchitectureBoundaryTest`, `ArchitecturePolicyResourceTest`, and
`architecture-policy.yml`. Remove the SnakeYAML version and dependency from
the root and test POMs. Integration tests remain behavior-oriented and keep
their current support classes and package names.

## Architecture Guard Scope

The unified module must continue to enforce dependency direction through
ArchUnit, the RESP retired-protocol scan, and the DB/FFM isolation and
factory-only composition checks. Guards may inspect a current boundary or
public contract, but must not assert a particular private field, helper class,
source line, lane parent, or deleted path. Maven model validity is verified by
the reactor itself rather than by a second hand-maintained YAML policy.

## Documentation And Tooling

Update active project documentation, scripts, and command examples that name
the removed aggregators, testkit modules, or old test module paths. Historical
design/plan records remain historical records and are not rewritten. All
verification commands use the repository's explicit JDK 25 prefix.

## Acceptance Criteria

1. `mvn -N validate` and the full JDK 25 reactor resolve the flattened graph
   with no duplicate project or missing parent.
2. No deleted module path, old test artifact, SnakeYAML policy dependency, or
   stale testkit import remains in active source, POM, script, or project
   documentation.
3. The unified test module passes all retained architecture and integration
   tests; DB-memory tests pass with helpers in the local test source tree.
4. Published production artifacts and Java package/API inventories are
   unchanged from the stage 3 baseline.
5. The final worktree is clean and the narrowest affected tests plus the full
   JDK 25 reactor have passed.
