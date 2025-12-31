## 1. Maven Modules
- [x] Create `yierdis-offheap-api` module (jar).
- [x] Rename/move the existing `yierdis-offheap` module to `yierdis-offheap-netty` (jar).
- [x] Update root `pom.xml` module list and build order.

## 2. Code Moves
- [x] Move `yier.bubu.redis.db.offheap.api.*` sources + tests into `yierdis-offheap-api`.
- [x] Move `yier.bubu.redis.db.offheap.netty.*` sources + tests into `yierdis-offheap-netty`.
- [x] Update `YierdisOffHeapAllocators` to avoid compile-time dependency on implementation modules (reflection-based).

## 3. Dependency Updates
- [x] Update `yierdis-offheap-foreign` to depend on `yierdis-offheap-api` (+ api test-jar for contract tests).
- [x] Update `yierdis-server` to depend on `yierdis-offheap-api` and `yierdis-offheap-netty`.

## 4. Documentation
- [x] Update `AGENTS.md` to reflect the new module split.

## 5. Validation
- [x] Run `mvn test` (default).
- [x] Run `mvn -Pforeign-memory test`.

## 6. Module Grouping
- [x] Add a parent/aggregator module `yierdis-offheap` (packaging `pom`).
- [x] Move `yierdis-offheap-api`, `yierdis-offheap-netty`, and `yierdis-offheap-foreign` under `yierdis-offheap/`.
- [x] Make off-heap submodules inherit from the `yierdis-offheap` parent POM.
