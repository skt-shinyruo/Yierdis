package yier.bubu.redis.storage.memory.internal.value;

import static yier.bubu.redis.common.memory.MemoryUsageSnapshot.addSaturating;

import java.util.function.IntConsumer;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.storage.api.result.ByteValueSink;
import yier.bubu.redis.storage.api.result.PoppedValueSequence;

public final class PreparedPoppedValueSequence implements PoppedValueSequence {
    private final StableMemoryBackend allocator;
    private final NativeListEntryRef[] entries;
    private final NativeHandleSet retainedHandles;
    private final int count;
    private final boolean nullValue;
    private final long retainedMemoryBytes;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean handlesReleased = new AtomicBoolean();
    private volatile boolean ownsHandles;

    private PreparedPoppedValueSequence(
            StableMemoryBackend allocator,
            NativeListEntryRef[] entries,
            NativeHandleSet retainedHandles,
            boolean nullValue,
            long retainedMemoryBytes
    ) {
        Objects.requireNonNull(entries, "entries");
        Objects.requireNonNull(retainedHandles, "retainedHandles");
        if (retainedMemoryBytes < 0L) {
            throw new IllegalArgumentException("retainedMemoryBytes must be >= 0");
        }
        this.allocator = allocator;
        this.entries = entries.clone();
        this.retainedHandles = retainedHandles;
        this.count = entries.length;
        this.nullValue = nullValue;
        this.retainedMemoryBytes = retainedMemoryBytes;
    }

    public static PreparedPoppedValueSequence nullValue() {
        return new PreparedPoppedValueSequence(
                null,
                new NativeListEntryRef[0],
                new NativeHandleSet(0),
                true,
                0L
        );
    }

    public static PreparedPoppedValueSequence empty() {
        return new PreparedPoppedValueSequence(
                null,
                new NativeListEntryRef[0],
                new NativeHandleSet(0),
                false,
                0L
        );
    }

    public static PreparedPoppedValueSequence owned(StableMemoryBackend allocator, NativeListEntryRef[] entries) {
        Objects.requireNonNull(allocator, "allocator");
        Objects.requireNonNull(entries, "entries");
        long retainedMemoryBytes = 0L;
        NativeHandleSet retainedHandles = new NativeHandleSet(entries.length);
        for (NativeListEntryRef entry : entries) {
            Objects.requireNonNull(entry, "entry");
            NativeHandle handle = entry.handle();
            if (handle == null) {
                continue;
            }
            if (retainedHandles.add(handle)) {
                retainedMemoryBytes = addSaturating(retainedMemoryBytes, entry.retainedBytes());
            }
        }
        return new PreparedPoppedValueSequence(
                allocator,
                entries,
                retainedHandles,
                false,
                retainedMemoryBytes
        );
    }

    public boolean retainsHandle(NativeHandle handle) {
        return retainedHandles.contains(handle);
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
    public int elementCount() {
        return count;
    }

    @Override
    public void visitElementLengths(IntConsumer out) {
        Objects.requireNonNull(out, "out");
        ensureOpen();
        for (NativeListEntryRef entry : entries) {
            out.accept(entry.handle() == null ? -1 : entry.payloadLength());
        }
    }

    @Override
    public long retainedMemoryBytes() {
        return retainedMemoryBytes;
    }

    @Override
    public void emitTo(ByteValueSink out) {
        Objects.requireNonNull(out, "out");
        ensureOpen();
        for (NativeListEntryRef entry : entries) {
            NativeHandle handle = entry.handle();
            if (handle == null) {
                out.nullValue();
                continue;
            }
            out.value(new NativeBytesSlice(
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
        retainedHandles.freeAll(allocator);
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("popped value sequence is closed");
        }
    }

}
