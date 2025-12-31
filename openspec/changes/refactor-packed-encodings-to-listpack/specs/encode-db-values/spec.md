## ADDED Requirements

### Requirement: Listpack-Like Packed Encodings
Yierdis MUST represent packed encodings for small composite values using a single contiguous byte buffer (listpack-like),
so that packed values do not allocate one Java object per element.

#### Scenario: Hash packed values avoid per-field objects
- **GIVEN** a small hash stored in packed form
- **WHEN** fields and values are added/updated
- **THEN** the packed representation remains contiguous and binary-safe
- **AND** external command results remain unchanged

### Requirement: Quicklist Nodes Use Packed Buffers
Yierdis lists MUST store quicklist nodes as packed buffers (listpack-like), not as `byte[][]` element arrays, and MUST
upgrade from a single packed buffer to multiple nodes when thresholds are exceeded.

#### Scenario: Large lists split into bounded nodes
- **GIVEN** a list grown beyond the packed thresholds
- **WHEN** new elements are pushed
- **THEN** the list upgrades to quicklist form with bounded node sizes
- **AND** `LRANGE`, `LPOP`, and `RPOP` continue to behave identically to before

### Requirement: Packed-to-Upgraded Transitions
Packed encodings MUST upgrade to their corresponding “large” encodings (HT/skiplist/quicklist) when configured thresholds
are exceeded, and MUST preserve the value’s logical content across upgrades.

#### Scenario: Packed ZSET upgrades without changing ordering
- **GIVEN** a small zset stored in packed form
- **WHEN** it grows beyond packed thresholds
- **THEN** it upgrades to dict+skiplist
- **AND** the resulting order for range queries remains consistent with score then member lexicographic ordering

### Requirement: Binary-Safe Semantics
All packed encodings MUST treat values as binary-safe `byte[]` and MUST NOT apply character encoding conversions when
comparing, storing, or retrieving elements.

#### Scenario: Non-UTF8 bytes remain round-trippable
- **GIVEN** a value containing non-UTF8 bytes
- **WHEN** it is stored and retrieved through supported commands
- **THEN** the returned bytes exactly match the stored bytes

