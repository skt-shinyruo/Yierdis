## 1. Maven Structure
- [x] Add parent aggregator `pom.xml` (packaging `pom`) with module list.
- [x] Create server module directory and move existing code/pom into it.
- [x] Ensure `mvn test` at repo root still runs server tests by default.

## 2. Off-heap API/Netty Module
- [x] Create `yierdis-offheap` module (jar).
- [x] Move `yier.bubu.redis.db.offheap.*` (API + Netty backend + factory) and its tests into this module.
- [x] Make the server module depend on `yierdis-offheap`.

## 3. Foreign Backend Module
- [x] Create `yierdis-offheap-foreign` module (jar).
- [x] Move Foreign backend sources/tests from `src/*/java-foreign` into the module’s standard `src/main/java` and `src/test/java`.
- [x] Configure compiler/surefire flags for incubator module in that module (Java 17):
  - `--add-modules jdk.incubator.foreign`

## 4. Profiles and Optionality
- [x] Make the foreign module build-only under profile `foreign-memory` (default build does not touch it).
- [x] Keep `YierdisOffHeapAllocators` reflection-based loading behavior unchanged.
- [x] (Optional) Under the `foreign-memory` profile, make the server module include the foreign module on the runtime/shaded classpath.

## 5. Documentation
- [x] Update `README.md` to reflect multi-module build paths and the profile commands.

## 6. Validation
- [x] Run `mvn test` (default).
- [x] Run `mvn -Pforeign-memory test`.
