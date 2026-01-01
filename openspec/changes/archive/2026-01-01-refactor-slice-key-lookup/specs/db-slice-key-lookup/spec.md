## ADDED Requirements

### Requirement: DB Keyspace Supports Slice-Based Lookup For Read Path
The DB layer MUST be able to lookup keys using a request-scoped slice/len key view (ptr+len style), without requiring
materializing a new `byte[]` for the lookup key.

#### Scenario: GET uses slice key lookup without key materialization
- **GIVEN** a client request decoded into argv slices (request-backed ptr+len)
- **WHEN** `GET` is executed in the fast pipeline
- **THEN** the DB keyspace lookup is performed using the slice key view
- **AND** the implementation does not allocate a `byte[]` for the lookup key
- **AND** the returned value bytes are unchanged

#### Scenario: TYPE uses slice key lookup without key materialization
- **GIVEN** a key exists in the DB with a canonical stored `byte[]` key
- **WHEN** `TYPE` is executed with an equal-by-contents request slice key
- **THEN** the DB resolves the canonical stored key and returns the correct type
- **AND** the implementation does not allocate a `byte[]` for the lookup key

#### Scenario: EXISTS supports multi-key slice lookup
- **GIVEN** a request with multiple keys (some present, some absent)
- **WHEN** `EXISTS k1 k2 ...` is executed
- **THEN** the returned count matches Redis semantics
- **AND** each key is probed using slice-based lookup

### Requirement: Slice Key Lookup Preserves Expiration Semantics
Slice-based key lookups MUST preserve existing expiration behavior and must continue to rely on canonical keys when
reading from secondary indexes like `expires`.

#### Scenario: Expired keys are treated as absent via slice lookup
- **GIVEN** a key has expired according to the expires index
- **WHEN** `GET` or `TYPE` is executed using a slice key
- **THEN** the key is treated as absent (same as the `byte[]` lookup path)
- **AND** the expires entry is cleaned up as needed (same as current behavior)

### Requirement: External Semantics Unchanged
All slice lookup refactors MUST preserve external RESP2 semantics and command behavior.

#### Scenario: Binary-safe key bytes remain exact
- **GIVEN** a key containing non-UTF8 bytes is stored
- **WHEN** it is looked up using a slice key in the fast pipeline
- **THEN** lookup correctness is based on raw bytes (not UTF-8 decoding)

