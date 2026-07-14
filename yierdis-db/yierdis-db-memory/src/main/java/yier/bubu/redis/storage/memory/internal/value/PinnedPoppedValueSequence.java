package yier.bubu.redis.storage.memory.internal.value;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.storage.api.result.BulkStringSink;
import yier.bubu.redis.storage.api.result.PoppedValueSequence;

/**
 * A read-only pop preview that pins the current list entries until preflight completes.
 */
public final class PinnedPoppedValueSequence implements PoppedValueSequence {
    private static final PinnedPoppedValueSequence NULL_VALUE = new PinnedPoppedValueSequence(
            null,
            new NativeListEntryRef[0],
            true,
            0L,
            0L
    );
    private static final PinnedPoppedValueSequence EMPTY_VALUE = new PinnedPoppedValueSequence(
            null,
            new NativeListEntryRef[0],
            false,
            0L,
            0L
    );

    private final NativeAllocator allocator;
    private final NativeListEntryRef[] entries;
    private final boolean nullValue;
    private final long encodedElementBytes;
    private final long retainedMemoryBytes;
    private final AtomicBoolean closed = new AtomicBoolean();

    private PinnedPoppedValueSequence(
            NativeAllocator allocator,
            NativeListEntryRef[] entries,
            boolean nullValue,
            long encodedElementBytes,
            long retainedMemoryBytes
    ) {
        this.allocator = allocator;
        this.entries = Objects.requireNonNull(entries, "entries").clone();
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
        boolean[] pinned = new boolean[entries.length];
        try {
            for (int index = 0; index < entries.length; index++) {
                NativeListEntryRef entry = entries[index];
                Objects.requireNonNull(entry, "entry");
                NativeHandle handle = entry.handle();
                if (handle != null) {
                    allocator.pin(handle);
                    pinned[index] = true;
                }
                encoded = addSaturating(encoded, entry.encodedElementBytes());
                retained = addSaturating(retained, entry.retainedBytes());
            }
            return new PinnedPoppedValueSequence(allocator, entries, false, encoded, retained);
        } catch (RuntimeException | Error failure) {
            for (int index = 0; index < entries.length; index++) {
                if (!pinned[index]) {
                    continue;
                }
                NativeHandle handle = entries[index].handle();
                try {
                    allocator.unpin(handle);
                } catch (RuntimeException | Error unpinFailure) {
                    failure.addSuppressed(unpinFailure);
                }
            }
            throw failure;
        }
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
            out.bulkString(NativeBytesSlice.retained(allocator, handle, 0, entry.payloadLength()));
        }
    }

    @Override
    public void close() {
        if (allocator == null || !closed.compareAndSet(false, true)) {
            return;
        }
        Throwable failure = null;
        for (NativeListEntryRef entry : entries) {
            NativeHandle handle = entry.handle();
            if (handle == null) {
                continue;
            }
            try {
                allocator.unpin(handle);
            } catch (RuntimeException | Error next) {
                if (failure == null) {
                    failure = next;
                } else {
                    failure.addSuppressed(next);
                }
            }
        }
        if (failure != null) {
            if (failure instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (failure instanceof Error error) {
                throw error;
            }
            throw new AssertionError("unexpected unpin failure type", failure);
        }
    }

    private static long addSaturating(long left, long right) {
        if (right <= 0L) {
            return left;
        }
        return Long.MAX_VALUE - left < right ? Long.MAX_VALUE : left + right;
    }
}
