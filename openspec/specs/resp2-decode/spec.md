# resp2-decode Specification

## Purpose
TBD - created by archiving change refactor-resp-decode-zero-copy. Update Purpose after archive.
## Requirements
### Requirement: RESP2 Request Decode Supports Zero-Copy Bulk Arguments
The fast-path RESP2 request decoder MUST be able to represent bulk string arguments as slice/len views over Netty’s input
buffer, without requiring per-argument `byte[]` materialization during decoding.

#### Scenario: Decode argv without copying payload bytes
- **GIVEN** a RESP2 request containing an array of bulk strings
- **WHEN** the server decodes the request into a command argv representation
- **THEN** the decoder does not allocate a `byte[]` per bulk argument during decoding
- **AND** argument bytes can be read via slice/len access immediately for routing and parsing

### Requirement: Request Argument Lifetime Is Explicit and Safe Under Netty refCnt
Zero-copy argument slices MUST have an explicit ownership boundary and MUST be released exactly once when the command has
been processed or abandoned, preventing memory leaks and preventing access after release.

#### Scenario: Partial frames do not leak retained buffers
- **GIVEN** a RESP2 request that arrives in multiple TCP frames (partial decode)
- **WHEN** the decoder is invoked before the full command bytes are available
- **THEN** the decoder does not retain any input buffer slices
- **AND** once the full command is decoded, retained buffers are released when the command is recycled

### Requirement: External RESP2 Semantics Remain Unchanged
All request decode refactors MUST preserve external RESP2 semantics and command behavior.

#### Scenario: Binary-safe and null-vs-empty bulk behavior remains correct
- **GIVEN** a request containing binary (non-UTF8) bytes and both `$-1` and `$0` bulk strings
- **WHEN** the command is decoded and executed
- **THEN** binary bytes are preserved exactly
- **AND** null bulk and empty bulk remain distinguishable

