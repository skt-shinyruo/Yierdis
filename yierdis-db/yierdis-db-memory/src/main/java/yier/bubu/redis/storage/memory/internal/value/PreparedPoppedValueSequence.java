package yier.bubu.redis.storage.memory.internal.value;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.storage.api.result.BulkStringSink;
import yier.bubu.redis.storage.api.result.PoppedValueSequence;

public final class PreparedPoppedValueSequence implements PoppedValueSequence {
    private static final PreparedPoppedValueSequence NULL_VALUE = new PreparedPoppedValueSequence(
            null,
            new NativeListEntryRef[0],
            true,
            0L,
            0L
    );
    private static final PreparedPoppedValueSequence EMPTY_VALUE = new PreparedPoppedValueSequence(
            null,
            new NativeListEntryRef[0],
            false,
            0L,
            0L
    );

    private final NativeAllocator allocator;
    private final NativeListEntryRef[] entries;
    private final int count;
    private final boolean nullValue;
    private final long encodedElementBytes;
    private final long retainedMemoryBytes;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean handlesReleased = new AtomicBoolean();
    private volatile boolean ownsHandles;

    private PreparedPoppedValueSequence(
            NativeAllocator allocator,
            NativeListEntryRef[] entries,
            boolean nullValue,
            long encodedElementBytes,
            long retainedMemoryBytes
    ) {
        Objects.requireNonNull(entries, "entries");
        int count = entries.length;
        if (count < 0) {
            throw new IllegalArgumentException("count must be >= 0");
        }
        if (encodedElementBytes < 0L) {
            throw new IllegalArgumentException("encodedElementBytes must be >= 0");
        }
        if (retainedMemoryBytes < 0L) {
            throw new IllegalArgumentException("retainedMemoryBytes must be >= 0");
        }
        this.allocator = allocator;
        this.entries = entries.clone();
        this.count = count;
        this.nullValue = nullValue;
        this.encodedElementBytes = encodedElementBytes;
        this.retainedMemoryBytes = retainedMemoryBytes;
    }

    public static PreparedPoppedValueSequence nullValue() {
        return NULL_VALUE;
    }

    public static PreparedPoppedValueSequence empty() {
        return EMPTY_VALUE;
    }

    public static PreparedPoppedValueSequence owned(NativeAllocator allocator, NativeListEntryRef[] entries) {
        Objects.requireNonNull(allocator, "allocator");
        Objects.requireNonNull(entries, "entries");
        long encodedElementBytes = 0L;
        long retainedMemoryBytes = 0L;
        for (NativeListEntryRef entry : entries) {
            Objects.requireNonNull(entry, "entry");
            encodedElementBytes = addSaturating(encodedElementBytes, entry.encodedElementBytes());
            retainedMemoryBytes = addSaturating(retainedMemoryBytes, entry.retainedBytes());
        }
        return new PreparedPoppedValueSequence(
                allocator,
                entries,
                false,
                encodedElementBytes,
                retainedMemoryBytes
        );
    }

    public NativeHandle[] retainedHandles() {
        int handleCount = 0;
        for (NativeListEntryRef entry : entries) {
            if (entry.handle() != null) {
                handleCount++;
            }
        }
        NativeHandle[] handles = new NativeHandle[handleCount];
        int next = 0;
        for (NativeListEntryRef entry : entries) {
            if (entry.handle() != null) {
                handles[next++] = entry.handle();
            }
        }
        return handles;
    }

    public void activateOwnership() {
        ownsHandles = true;
        releaseIfClosed();
    }

    @Override
    public boolean isNull() {
        return nullValue;
    }

    @Override
    public int count() {
        return count;
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
        if (count == 0) {
            return;
        }
        for (NativeListEntryRef entry : entries) {
            NativeHandle handle = entry.handle();
            if (handle == null) {
                out.bulkStringNull();
                continue;
            }
            out.bulkString(new NativeBytesSlice(allocator, handle, 0, entry.payloadLength()));
        }
    }

    @Override
    public void close() {
        closed.compareAndSet(false, true);
        releaseIfClosed();
    }

    private void releaseIfClosed() {
        if (!ownsHandles || !closed.get() || !handlesReleased.compareAndSet(false, true)) {
            return;
        }
        RuntimeException failure = null;
        for (NativeListEntryRef entry : entries) {
            NativeHandle handle = entry.handle();
            if (handle == null) {
                continue;
            }
            try {
                allocator.free(handle);
            } catch (RuntimeException e) {
                failure = addFailure(failure, e);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static long addSaturating(long left, long right) {
        if (right <= 0L) {
            return left;
        }
        if (Long.MAX_VALUE - left < right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    private static RuntimeException addFailure(RuntimeException failure, RuntimeException next) {
        if (failure == null) {
            return next;
        }
        failure.addSuppressed(next);
        return failure;
    }
}
