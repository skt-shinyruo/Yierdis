## ADDED Requirements

### Requirement: Slice-Based Key Lookup Covers STRLEN/TTL/EXPIRE
For fast-path execution, `STRLEN`, `TTL`, and `EXPIRE` MUST probe the DB using slice/len key lookup so that the lookup key
does not need to be materialized as a new `byte[]` per request.

#### Scenario: STRLEN uses slice key lookup
- **GIVEN** a string key exists in the DB
- **WHEN** `STRLEN` is executed via the fast pipeline
- **THEN** the returned length is correct
- **AND** the lookup uses the request-backed slice key view

#### Scenario: TTL uses slice key lookup
- **GIVEN** a key exists in the DB with an expiration
- **WHEN** `TTL` is executed via the fast pipeline
- **THEN** the returned TTL follows existing semantics (seconds rounding and -1/-2 cases unchanged)
- **AND** the lookup uses the request-backed slice key view

#### Scenario: EXPIRE uses slice key lookup
- **GIVEN** a key exists in the DB
- **WHEN** `EXPIRE key seconds` is executed via the fast pipeline
- **THEN** the command returns the same 0/1 result as before
- **AND** the expires index continues to store canonical keys

