# Design: Split off-heap modules

## Module Layout

### `yierdis-offheap` (pom)
- Purpose: group all off-heap jars under a single parent directory/module.
- Contains:
  - `api/`
  - `netty/`
  - `foreign/` (profile-only)

### `yierdis-offheap-api` (jar)
- Purpose: define the off-heap contracts and configuration that other modules depend on.
- Contains:
  - `yier.bubu.redis.db.offheap.api.*`
  - API contract tests (as a test-jar) consumed by backend modules.

### `yierdis-offheap-netty` (jar)
- Purpose: Netty direct `ByteBuf` based implementation.
- Contains:
  - `yier.bubu.redis.db.offheap.netty.*`

### `yierdis-offheap-foreign` (jar, profile-only)
- Purpose: Java 17 incubator Foreign Memory API based implementation.
- Contains:
  - `yier.bubu.redis.db.offheap.foreign.*`
- Build constraints:
  - compiled/tested only under Maven profile `foreign-memory`
  - requires `--add-modules jdk.incubator.foreign`

## Factory Loading
`YierdisOffHeapAllocators` remains in the API module and uses reflection to load backend implementation classes so:
- the API module does not require implementation jars at compile time
- the server can include/exclude optional backends at build/runtime via Maven deps/profile
