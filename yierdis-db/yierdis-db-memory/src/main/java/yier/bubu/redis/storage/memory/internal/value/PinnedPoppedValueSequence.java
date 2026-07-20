package yier.bubu.redis.storage.memory.internal.value;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.storage.api.result.BulkStringSink;
import yier.bubu.redis.storage.api.result.PoppedValueSequence;

/**
 * 只读 pop 预览；preflight 结束前按 native block 固定底层 payload。
 */
public final class PinnedPoppedValueSequence implements PoppedValueSequence {
    private static final PinnedPoppedValueSequence NULL_VALUE = new PinnedPoppedValueSequence(
            null,
            new NativeListEntryRef[0],
            new NativeRawHandleSet(0),
            true,
            0L,
            0L
    );
    private static final PinnedPoppedValueSequence EMPTY_VALUE = new PinnedPoppedValueSequence(
            null,
            new NativeListEntryRef[0],
            new NativeRawHandleSet(0),
            false,
            0L,
            0L
    );

    private final NativeAllocator allocator;
    private final NativeListEntryRef[] entries;
    private final NativeRawHandleSet pinnedRawHandles;
    private final boolean nullValue;
    private final long encodedElementBytes;
    private final long retainedMemoryBytes;
    private final AtomicBoolean closed = new AtomicBoolean();

    private PinnedPoppedValueSequence(
            NativeAllocator allocator,
            NativeListEntryRef[] entries,
            NativeRawHandleSet pinnedRawHandles,
            boolean nullValue,
            long encodedElementBytes,
            long retainedMemoryBytes
    ) {
        this.allocator = allocator;
        this.entries = Objects.requireNonNull(entries, "entries").clone();
        this.pinnedRawHandles = Objects.requireNonNull(pinnedRawHandles, "pinnedRawHandles");
        this.nullValue = nullValue;
        this.encodedElementBytes = encodedElementBytes;
        this.retainedMemoryBytes = retainedMemoryBytes;
    }

    public static PinnedPoppedValueSequence nullValue() {
        return NULL_VALUE;
    }

    public static PinnedPoppedValueSequence empty() {
        return EMPTY_VALUE;
    }

    public static PinnedPoppedValueSequence capture(NativeAllocator allocator, NativeListEntryRef[] entries) {
        Objects.requireNonNull(allocator, "allocator");
        Objects.requireNonNull(entries, "entries");
        long encoded = 0L;
        long retained = 0L;
        NativeRawHandleSet pinnedRawHandles = new NativeRawHandleSet(entries.length);
        for (NativeListEntryRef entry : entries) {
            Objects.requireNonNull(entry, "entry");
            NativeHandle handle = entry.handle();
            if (handle != null && pinnedRawHandles.add(handle.raw())) {
                retained = addSaturating(retained, entry.retainedBytes());
            }
            encoded = addSaturating(encoded, entry.encodedElementBytes());
        }
        pinnedRawHandles.pinAll(allocator);
        return new PinnedPoppedValueSequence(allocator, entries, pinnedRawHandles, false, encoded, retained);
    }

    @Override
    public boolean isNull() {
        return nullValue;
    }

    @Override
    public int count() {
        return entries.length;
    }

    @Override
    public long encodedElementBytes() {
        return encodedElementBytes;
    }

    @Override
    public long retainedMemoryBytes() {
        return retainedMemoryBytes;
    }

    @Override
    public void emitTo(BulkStringSink out) {
        Objects.requireNonNull(out, "out");
        for (NativeListEntryRef entry : entries) {
            NativeHandle handle = entry.handle();
            if (handle == null) {
                out.bulkStringNull();
                continue;
            }
            out.bulkString(NativeBytesSlice.retained(
                    allocator,
                    handle,
                    entry.payloadOffset(),
                    entry.payloadLength()
            ));
        }
    }

    @Override
    public void close() {
        if (allocator == null || !closed.compareAndSet(false, true)) {
            return;
        }
        pinnedRawHandles.unpinAll(allocator);
    }

    private static long addSaturating(long left, long right) {
        if (right <= 0L) {
            return left;
        }
        return Long.MAX_VALUE - left < right ? Long.MAX_VALUE : left + right;
    }
}
