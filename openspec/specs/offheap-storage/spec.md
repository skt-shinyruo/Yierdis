# offheap-storage Specification

## Purpose
TBD - created by archiving change refactor-full-offheap-storage. Update Purpose after archive.
## Requirements
### Requirement: Off-heap storage engine
The system SHALL store all user data and index structures off-heap using an Unsafe-based allocator, with only minimal metadata objects
on the JVM heap.

#### Scenario: Basic set/get uses off-heap
- **WHEN** the client executes `SET k v` then `GET k`
- **THEN** the value bytes are stored off-heap
- **AND** the reply does not allocate a new heap `byte[]` for the stored value

### Requirement: Deterministic shutdown frees native memory
The system SHALL deterministically free all off-heap allocations when the DB/server is shut down.

#### Scenario: No leaks after shutdown
- **WHEN** the server stores keys and values and then shuts down
- **THEN** allocator accounting returns to zero (no outstanding allocations)

### Requirement: Hard memory limit
The system SHALL enforce a configured maximum off-heap memory limit for all allocations that are derived from client inputs.

#### Scenario: Allocation is rejected when exceeding the limit
- **GIVEN** a max off-heap memory limit that is smaller than the requested value
- **WHEN** the client executes a command that would exceed the limit
- **THEN** the command fails with an `ERR` response
- **AND** the server remains healthy and continues to process subsequent commands

### Requirement: Use-after-free defenses in tests
The system SHALL provide test-mode defenses that detect common use-after-free patterns.

#### Scenario: Freed block access is detected in tests
- **GIVEN** a debug/test mode allocator configuration
- **WHEN** code attempts to read from a freed block
- **THEN** the test fails with a clear diagnostic

