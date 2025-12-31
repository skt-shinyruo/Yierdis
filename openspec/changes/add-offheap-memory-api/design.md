# Design: Off-heap memory abstraction layer (Netty + Foreign Memory)

## Goals
- Provide a minimal, testable API that storage code can use without caring about the underlying off-heap mechanism.
- Support two interchangeable backends: Netty direct buffers and Java Foreign Memory API.
- Keep the default build/run path stable on Java 17.

## Proposed API Shape
### Core interfaces
- `YierdisOffHeapAllocator`:
  - `YierdisOffHeapBuf allocate(int capacity)`
  - `void free(YierdisOffHeapBuf buf)` (or `buf.close()` with owner semantics)
  - accounting: `long usedBytes()`, `long maxBytes()`
- `YierdisOffHeapBuf` (mutable):
  - `int capacity()`
  - `byte getByte(int index)` / `void setByte(int index, byte v)`
  - `void getBytes(int index, byte[] dst, int off, int len)`
  - `void setBytes(int index, byte[] src, int off, int len)`
  - `YierdisOffHeapSlice slice(int index, int len)` (read-only view)
- `YierdisOffHeapSlice` (read-only):
  - `(handle, off, len)` semantics; can be written to RESP without heap copies.

The API MUST make ownership explicit:
- A buffer is owned by exactly one component; it must be freed deterministically.
- Slices do not own memory; they must not outlive the owning buffer.

## Backend: Netty
- Implementation wraps direct `ByteBuf`.
- `allocate()` uses `PooledByteBufAllocator.DEFAULT.directBuffer(capacity)`.
- `free()` calls `release()` and updates accounting.
- Slice is `(ByteBuf, index, len)`; RESP writer can write from it directly.

## Backend: Foreign Memory API
### Java 17 reality
On Java 17 the API is incubator (`jdk.incubator.foreign`), requiring module flags.
To keep default build simple:
- Put the backend behind a Maven profile that adds `--add-modules jdk.incubator.foreign` for compile/test/run.

### Implementation shape
- `allocate()` creates an Arena + MemorySegment (or a long-lived Arena with explicit close).
- `free()` closes the owning Arena/segment and updates accounting.
- Slice is `(MemorySegment, off, len)`; RESP writer can write from it by copying into Netty `ByteBuf` without an intermediate heap array.

## Integration Strategy
This change only introduces the abstraction and selectable backends.
Actual migration of DB structures to off-heap can happen incrementally later:
- First: reply path for strings (avoid heap copies)
- Then: string RAW payloads
- Then: composite types and indexes if desired

