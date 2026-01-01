# offheap-memory-api Specification

## Purpose
TBD - created by archiving change add-offheap-memory-api. Update Purpose after archive.
## Requirements
### Requirement: Off-heap memory abstraction
The system SHALL provide an internal off-heap memory API for allocating, freeing, and accessing byte sequences without relying on heap `byte[]`.

#### Scenario: Allocate and round-trip bytes
- **WHEN** code allocates an off-heap buffer of size N and writes bytes to it
- **THEN** reading back the bytes returns the same content

### Requirement: Netty off-heap backend
The system SHALL provide a Netty-based implementation of the off-heap memory API using direct `ByteBuf` and deterministic release.

#### Scenario: Netty backend releases memory
- **GIVEN** a Netty off-heap allocation
- **WHEN** the owning component frees the buffer
- **THEN** the underlying `ByteBuf` reference count reaches zero

### Requirement: Foreign Memory API off-heap backend
The system SHALL provide a Foreign Memory API implementation of the off-heap memory API with deterministic close semantics.

#### Scenario: Foreign backend invalidates memory after close
- **GIVEN** a Foreign Memory allocation
- **WHEN** the owning component closes/frees it
- **THEN** subsequent access fails deterministically (throws) in tests

### Requirement: Backend selection
The system SHALL allow selecting the off-heap backend at runtime.

#### Scenario: Same behavior across backends
- **GIVEN** the Netty backend is selected
- **WHEN** a buffer is allocated and written
- **THEN** reads match expected bytes
- **GIVEN** the Foreign backend is selected
- **WHEN** the same operations are performed
- **THEN** reads match the same expected bytes

