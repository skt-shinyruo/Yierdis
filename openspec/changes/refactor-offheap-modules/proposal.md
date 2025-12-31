# Change: Split off-heap layer into api/netty/foreign Maven modules

## Why
The off-heap layer currently ships as a single module that contains both the API types and the Netty implementation.

For a more standard and reusable engineering structure, we want the off-heap layer to be packaged as three jars:
- an API jar that defines the public contracts
- a Netty implementation jar
- a Foreign Memory API implementation jar (profile-only)

This improves modularity and makes it clearer which code is “contract” vs “backend implementation”.

## What Changes
- Add an off-heap parent/aggregator module `yierdis-offheap/` (packaging `pom`) under the root reactor.
- Under `yierdis-offheap/`, package the off-heap layer as three jars:
  - `api/` → `yierdis-offheap-api` (jar): API types + factory (`yier.bubu.redis.db.offheap.api.*`)
  - `netty/` → `yierdis-offheap-netty` (jar): Netty backend (`yier.bubu.redis.db.offheap.netty.*`)
  - `foreign/` → `yierdis-offheap-foreign` (jar, profile-only): Foreign Memory backend (`yier.bubu.redis.db.offheap.foreign.*`)
- Update the server module to depend on `yierdis-offheap-api` (+ `yierdis-offheap-netty` by default).
- Keep backend selection behavior stable:
  - API factory uses reflection to load implementation classes so optional backends remain optional at build time.

## Impact
- Maven reactor module list changes (new parent module + module moves under `yierdis-offheap/`).
- Source/test file moves between modules and directory structure changes.
- POM dependency updates.
- Documentation updates for module names.

## Compatibility
- External RESP2 behavior remains unchanged.
- Off-heap API packages remain `yier.bubu.redis.db.offheap.api|netty|foreign` (no behavioral change intended).
