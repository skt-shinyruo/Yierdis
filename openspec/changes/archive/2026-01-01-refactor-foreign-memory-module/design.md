# Design: Foreign backend as separate Maven module

## Target Layout
Root becomes a parent/aggregator POM.

Modules:
- `yierdis-offheap/`:
  - Off-heap module group (parent/aggregator `pom`).
  - Contains:
    - `api/` (`yierdis-offheap-api`): off-heap API types + factory.
    - `netty/` (`yierdis-offheap-netty`): Netty off-heap backend (Java 17 compatible).
    - `foreign/` (`yierdis-offheap-foreign`, profile-only): Foreign Memory off-heap backend.
- `yierdis-server/`:
  - The existing server implementation (protocol/command/db/client).
  - Depends on `yierdis-offheap-api` and `yierdis-offheap-netty`.

## Profile Strategy
The parent POM defines the server module as default.
Under profile `foreign-memory`, the foreign module is included in the reactor build.

This ensures:
- Default builds do not require incubator modules.
- Foreign backend remains opt-in and explicit.

## Runtime Story
The server continues to load the foreign backend via reflection.
Users can enable it by:
- Building the foreign module jar (via profile) and putting it on the classpath, or
- Building the server with the `foreign-memory` profile so the shaded jar includes the foreign backend (enabled by a profile-only dependency).
