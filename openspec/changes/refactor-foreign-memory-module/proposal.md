# Change: Refactor Foreign Memory backend into a Maven module/jar

## Why
The current Foreign Memory backend is implemented via a non-standard extra source root (`src/main/java-foreign` and `src/test/java-foreign`)
that is conditionally compiled using `build-helper-maven-plugin`.

While functional, this is surprising to new contributors and makes IDE/build tooling less predictable.
A more standard approach is to ship the Foreign Memory backend as a separate Maven module/jar that is built only when explicitly enabled.

## What Changes
- Convert the repository build into a multi-module Maven reactor:
  - **Off-heap module** provides the off-heap API + Netty backend (`yier.bubu.redis.db.offheap.*`) as a normal jar.
  - **Server module** (existing Yierdis server code) remains the default build target and depends on the off-heap module.
  - **Foreign backend module** provides `yier.bubu.redis.db.offheap.foreign.*` and its tests (profile-only).
- Remove the `src/main/java-foreign` / `src/test/java-foreign` directories from the server module.
- Keep the Foreign backend optional:
  - Default `mvn test` remains Java 17-only and does not require incubator module flags.
  - `-Pforeign-memory` builds and tests the foreign module, including the required `--add-modules jdk.incubator.foreign`.

## Impact
- Build structure changes (new parent `pom.xml` + module poms).
- File moves for sources and tests.
- README updates for new build/run paths.

## Compatibility
- External RESP2 behavior remains unchanged.
- Internal API remains in `yier.bubu.redis.db.offheap.*`; the foreign implementation remains in `yier.bubu.redis.db.offheap.foreign.*`.
