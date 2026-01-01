# db-expiration Specification

## Purpose
TBD - created by archiving change refactor-expire-key-sharing. Update Purpose after archive.
## Requirements
### Requirement: Expires Index Shares Canonical Keys
Yierdis MUST store expiration index entries using the canonical key reference from the main keyspace, so that the key
stored in `expires` is the same `byte[]` instance as the key stored in `store` for the same logical key.

#### Scenario: EXPIRE uses canonical key reference
- **GIVEN** a key exists in the main keyspace
- **WHEN** an expiration is set using a different-but-equal key byte array
- **THEN** the expires index stores the canonical key reference from the main keyspace
- **AND** external command results are unchanged

