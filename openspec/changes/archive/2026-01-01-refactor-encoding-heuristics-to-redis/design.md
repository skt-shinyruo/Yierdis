# Design Notes: Redis-aligned encoding heuristics

This change is about making Yierdis’ *on-heap* encoding selection and packed iteration closer to Redis, without chasing
byte-identical internal formats.

## Redis knobs to mirror (defaults)
The goal is to mirror the *rules and default values* that Redis uses to decide when to keep a value “packed” vs upgrade
to a larger structure. Redis exposes these as configuration options; Yierdis can keep them as internal defaults and
optionally expose them later.

Target defaults (aligned with typical Redis defaults):
- Hash:
  - `hash-max-listpack-entries = 512`
  - `hash-max-listpack-value = 64`
- ZSet:
  - `zset-max-listpack-entries = 128`
  - `zset-max-listpack-value = 64`
- Set:
  - `set-max-intset-entries = 512`
- List / quicklist:
  - node max listpack size roughly ~8 KB (commonly `list-max-listpack-size = -2`)

## List: closer to quicklist-as-the-real-structure
Redis lists are conceptually “quicklist of listpacks”. In Yierdis we currently model:
1) a single packed listpack-like buffer, then
2) upgrade to a deque of listpack nodes (“quicklist-like”).

To get closer to Redis behavior without adding complexity:
- Prefer **byte-based** thresholds (encoded/raw bytes) over “max entries” thresholds.
- Ensure node split/merge decisions are driven by the same bound used for node sizing.

## Set: remove non-Redis intermediate encoding
Redis set encodings are effectively:
- `intset` for small canonical-integer-only sets,
- hashtable otherwise.

Yierdis currently adds a `listpack` stage for small non-integer sets. This is useful in Java, but it diverges from Redis.
This change proposes removing it so the upgrade path matches Redis.

## Packed ZSet iteration
Packed ZSet currently stores entries contiguously but may implement range replies by:
- converting indexes to offsets via repeated scans.

Even if packed ZSets remain small, we want the behavior to match Redis’ “contiguous scan” intuition:
- compute the starting offset once,
- then iterate sequentially over entries.

This is a local algorithmic refactor; external ordering and results remain unchanged.

