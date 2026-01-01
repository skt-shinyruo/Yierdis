# resp2-io Specification

## Purpose
TBD - created by archiving change refactor-resp-slice-io. Update Purpose after archive.
## Requirements
### Requirement: RESP2 Bulk Strings Support Slices
The RESP2 writer MUST support writing bulk strings from a heap byte slice (`byte[]` + offset + length) without requiring
callers to materialize a trimmed `byte[]`.

#### Scenario: Write a partial view of a byte array
- **GIVEN** a byte array with extra capacity bytes beyond the logical value length
- **WHEN** the server replies with a bulk string using the logical length
- **THEN** the reply contains exactly the logical bytes (no trailing garbage bytes)

### Requirement: RESP2 Bulk Strings Support Off-Heap Slices
The RESP2 writer MUST support writing bulk strings from `YierdisOffHeapSlice` directly to the Netty output buffer.

#### Scenario: Off-heap slice writes without heap materialization
- **GIVEN** an off-heap slice containing binary data
- **WHEN** the server replies with it as a bulk string
- **THEN** the reply bytes exactly match the slice bytes

### Requirement: RESP2 Bulk Strings Support Integer Encoding Without Temporary Byte Arrays
The RESP2 writer MUST provide a bulk-string write path for integer-encoded values without requiring a temporary `byte[]`.

#### Scenario: Integer reply uses ASCII digits in bulk string form
- **GIVEN** an integer-encoded string value
- **WHEN** the server replies with it as a bulk string
- **THEN** the reply contains the ASCII decimal representation of the integer

