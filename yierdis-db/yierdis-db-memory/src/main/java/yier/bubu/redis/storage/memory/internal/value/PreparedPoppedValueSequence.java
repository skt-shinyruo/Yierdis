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
            new NativeRawHandleSet(0),
            true,
            0L,
            0L
    );
    private static final PreparedPoppedValueSequence EMPTY_VALUE = new PreparedPoppedValueSequence(
            null,
            new NativeListEntryRef[0],
            new NativeRawHandleSet(0),
            false,
            0L,
            0L
    );

    private final NativeAllocator allocator;
    private final NativeListEntryRef[] entries;
    private final NativeRawHandleSet retainedRawHandles;
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
            NativeRawHandleSet retainedRawHandles,
            boolean nullValue,
            long encodedElementBytes,
            long retainedMemoryBytes
    ) {
        Objects.requireNonNull(entries, "entries");
        Objects.requireNonNull(retainedRawHandles, "retainedRawHandles");
        if (encodedElementBytes < 0L) {
            throw new IllegalArgumentException("encodedElementBytes must be >= 0");
        }
        if (retainedMemoryBytes < 0L) {
            throw new IllegalArgumentException("retainedMemoryBytes must be >= 0");
        }
        this.allocator = allocator;
        this.entries = entries.clone();
        this.retainedRawHandles = retainedRawHandles;
        this.count = entries.length;
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
        NativeRawHandleSet retainedRawHandles = new NativeRawHandleSet(entries.length);
        for (NativeListEntryRef entry : entries) {
            Objects.requireNonNull(entry, "entry");
            encodedElementBytes = addSaturating(encodedElementBytes, entry.encodedElementBytes());
            NativeHandle handle = entry.handle();
            if (handle == null) {
                continue;
            }
            if (retainedRawHandles.add(handle.raw())) {
                retainedMemoryBytes = addSaturating(retainedMemoryBytes, entry.retainedBytes());
            }
        }
        return new PreparedPoppedValueSequence(
                allocator,
                entries,
                retainedRawHandles,
                false,
                encodedElementBytes,
                retainedMemoryBytes
        );
    }

    public NativeHandle[] retainedHandles() {
        NativeHandle[] handles = new NativeHandle[retainedRawHandles.size()];
        int[] next = {0};
        retainedRawHandles.forEach(rawHandle -> handles[next[0]++] = NativeHandle.fromRaw(rawHandle));
        return handles;
    }

    public boolean retainsRawHandle(long rawHandle) {
        return retainedRawHandles.contains(rawHandle);
    }

    public void activateOwnership() {
        // commit 先让旧 listpack 放弃块计量，再由响应对象接管唯一一次 free。
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
        for (NativeListEntryRef entry : entries) {
            NativeHandle handle = entry.handle();
            if (handle == null) {
                out.bulkStringNull();
                continue;
            }
            out.bulkString(new NativeBytesSlice(
                    allocator,
                    handle,
                    entry.payloadOffset(),
                    entry.payloadLength()
            ));
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
        retainedRawHandles.freeAll(allocator);
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

}
