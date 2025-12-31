# memory-management Specification

## Purpose
TBD - created by archiving change add-maxmemory-eviction. Update Purpose after archive.
## Requirements
### Requirement: Configurable maxmemory
The system SHALL allow configuring a maximum memory budget for the in-memory dataset via server flags.

#### Scenario: Unlimited by default
- **GIVEN** the server starts with `--maxmemoryBytes 0` (or no flag)
- **WHEN** write commands are executed
- **THEN** the server SHALL NOT evict keys due to maxmemory

### Requirement: Maxmemory eviction policies
When maxmemory is enabled, the system SHALL support a minimal set of Redis-like eviction behaviors.

#### Scenario: allkeys-random evicts to stay within limit
- **GIVEN** maxmemory is enabled with policy `allkeys-random`
- **WHEN** writes cause used memory to exceed the limit
- **THEN** the system SHALL evict one or more keys until memory is within the limit (best-effort)

#### Scenario: allkeys-lru uses sampling-based eviction
- **GIVEN** maxmemory is enabled with policy `allkeys-lru`
- **AND** multiple keys exist with different recent-access times
- **WHEN** writes cause used memory to exceed the limit
- **THEN** the system SHALL evict a least-recently-used key among a bounded random sample

#### Scenario: noeviction rejects writes
- **GIVEN** maxmemory is enabled with policy `noeviction`
- **WHEN** a write would exceed the configured memory budget
- **THEN** the write SHALL fail with an OOM-style error

### Requirement: Approximate memory accounting
The system SHALL provide a documented, stable approximation for in-memory usage that is suitable for demos.

#### Scenario: MEMORY USAGE is stable and non-negative
- **GIVEN** a key exists
- **WHEN** `MEMORY USAGE <key>` is executed
- **THEN** the reply SHALL be a non-negative integer representing approximate bytes

### Requirement: Encoding introspection
The system SHALL expose internal encodings for teaching purposes.

#### Scenario: OBJECT ENCODING returns the internal encoding name
- **GIVEN** a key exists
- **WHEN** `OBJECT ENCODING <key>` is executed
- **THEN** the reply SHALL be a simple string describing the current encoding (e.g., `raw`, `int`, `listpack`, `hashtable`)

### Requirement: Prefer expiration cleanup before eviction
When maxmemory is enabled, the system SHALL attempt to reclaim expired keys before evicting non-expired keys.

#### Scenario: Expired keys are removed first
- **GIVEN** an expired key exists and maxmemory is exceeded
- **WHEN** a write triggers memory enforcement
- **THEN** the system SHALL attempt to remove the expired key before evicting other keys

