# Design: Zero-copy request decode (RESP2) with explicit lifecycle

## Goals
- Avoid per-argument `byte[]` allocation during request decode where possible.
- Align request parsing with Redis’ “ptr + len” style by keeping argument bytes in the inbound `ByteBuf` until the command
  is processed.
- Make lifetime explicit and safe under Netty ref counting.
- Keep changes minimal and isolated to the fast pipeline.

## Current state (baseline)
`RespCommandDecoder` parses `*<argc>` and `$<len>` frames and then copies each bulk string payload into a new `byte[]`.
The decoded `RespCommand` is recycled by `YierdisFastCommandHandler` after execution.

## Proposed representation

### Preferred approach: single retained frame + offset/len arrays
Instead of creating one retained slice object per argument, decode one command as:
- `ByteBuf frame`: a retained slice over the input buffer covering exactly the bytes consumed by the command
- `int[] argOffsets`: offset (in bytes) from `frame` start to the bulk payload bytes
- `int[] argLengths`: payload length; `-1` for null bulk

This has better alignment with ptr+len and avoids:
- N separate `ByteBuf` slice objects
- N `retain()` operations per command

### Alternative (simpler, but higher overhead): retained slice per arg
Decode each bulk string payload as `in.readRetainedSlice(len)` and store `ByteBuf[] argvSlices`.
This is simpler to implement but can be expensive for large `argc` and doesn’t match Redis’ style as closely.

We choose the single-retained-frame approach for better scalability and conceptual alignment.

## API surface (RespCommand)
`RespCommand` becomes an argv view over a retained `ByteBuf` frame:
- `int argc()`
- `boolean isNull(int index)`
- `int len(int index)` (returns `-1` for null)
- `byte byteAt(int index, int offset)`
- `void copyToByteArray(int index, byte[] dst, int dstOff)` (bounds checked)
- `byte[] toByteArray(int index)` (allocates exactly `len` bytes; null → null)

Fast-path parsing can avoid copies by reading directly from `frame` via offsets.
DB boundaries can still materialize `byte[]` when required.

## Lifecycle / ownership

### Ownership boundary
The fast pipeline handler (`YierdisFastCommandHandler`) owns the decoded command object for the duration of command
execution and MUST ensure it is released/recycled in `finally`.

### Release mechanism
`RespCommand.recycle()` MUST:
- release the retained `frame` once (if present),
- clear arg metadata,
- return the object to Netty’s `Recycler`.

### Decoder safety rules
The decoder MUST:
- perform “enough bytes?” checks before retaining the frame,
- retain exactly once per fully decoded command,
- never retain anything for partial frames,
- on any runtime exception during decoding after a retain, release immediately before propagating.

This yields predictable lifetime behavior and makes leak detection straightforward.

## Handling pipelining and partial frames
Netty’s `ByteToMessageDecoder` may call `decode()` repeatedly with partial input. The decoder MUST:
- snapshot `startReaderIndex`,
- if a full command is not present, restore reader index and return without allocating/retaining.

For pipelined input containing multiple commands in a single buffer, each decoded command retains its own `frame` slice.
This can prolong the lifetime of the underlying buffer until all commands are processed, which is acceptable and mirrors
Redis’ “process and then release” approach.

## Compatibility constraints
- RESP2 external semantics remain unchanged.
- DoS bounds remain enforced (`maxBulkBytes`, `maxArgs`, `maxLineBytes`).
- Null bulk (`$-1`) must remain distinguishable from empty bulk (`$0\r\n\r\n`).

## Future follow-ups (not in this change)
- Slice-based DB lookup for keys (hash + compare without materializing `byte[]`) to unlock real copy reductions on read
  commands like GET/EXISTS/TYPE.
- Reusing the same slice abstraction for off-heap storage (`refactor-full-offheap-storage`).

