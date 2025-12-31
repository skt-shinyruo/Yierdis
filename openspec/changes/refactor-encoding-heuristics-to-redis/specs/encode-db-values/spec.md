## ADDED Requirements

### Requirement: Redis-Aligned Encoding Thresholds
Yierdis MUST apply Redis-style threshold rules for upgrading packed encodings to their large encodings, using defaults
that mirror Redis’ typical configuration values.

#### Scenario: Hash upgrades when size/value thresholds are exceeded
- **GIVEN** a hash stored in packed form
- **WHEN** the number of field/value pairs grows beyond the packed entry threshold OR a field/value exceeds the packed value-size threshold
- **THEN** the hash upgrades to the hashtable encoding
- **AND** all existing fields and values remain accessible with binary-safe semantics

#### Scenario: ZSet upgrades when size/value thresholds are exceeded
- **GIVEN** a zset stored in packed form
- **WHEN** the number of members grows beyond the packed entry threshold OR a member exceeds the packed value-size threshold
- **THEN** the zset upgrades to dict+skiplist encoding
- **AND** range ordering remains score ascending, then member lexicographic

#### Scenario: Set upgrades from intset to hashtable on non-integer members
- **GIVEN** a set stored as an intset (canonical integer members)
- **WHEN** a non-integer (binary-safe) member is added OR the entry threshold is exceeded
- **THEN** the set upgrades to hashtable encoding
- **AND** previously stored members are preserved exactly

### Requirement: List Node Sizing Mirrors Quicklist Semantics
Yierdis list quicklist nodes MUST be bounded by a max packed-node size in bytes (quicklist listpack sizing), and MUST
split/merge nodes according to this bound when elements are pushed/popped.

#### Scenario: List nodes split on growth and merge on shrink
- **GIVEN** a list grown large enough to require multiple nodes
- **WHEN** elements are pushed until a node exceeds the configured max size
- **THEN** the list splits into additional nodes without changing element order
- **AND** after pops shrink the list, adjacent nodes may merge when they fit within the bound

### Requirement: Packed Iteration Is Sequential
When executing range-style operations against packed encodings (e.g., list or packed zset ranges), Yierdis MUST iterate
sequentially over packed entries starting from the first returned element, rather than re-scanning from the beginning
for each element.

#### Scenario: Packed ZSet range replies do not require conversion
- **GIVEN** a packed zset
- **WHEN** a range query is executed (by rank or by score)
- **THEN** results are returned in the correct Redis ordering rules
- **AND** the packed representation does not need to upgrade solely to serve the range reply

