package yier.bubu.redis.storage.memory.internal.value;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.storage.api.result.ByteValueSink;
import yier.bubu.redis.storage.api.result.PayloadLengthSink;
import yier.bubu.redis.storage.api.result.PoppedValueSequence;

/**
 * 只读 pop 预览；preflight 结束前按 native block 固定底层 payload。
 */
public final class PinnedPoppedValueSequence implements PoppedValueSequence {
    private final StableMemoryBackend allocator;
    private final NativeListEntryRef[] entries;
    private final NativeHandleSet pinnedHandles;
    private final boolean nullValue;
    private final long retainedMemoryBytes;
    private final AtomicBoolean closed = new AtomicBoolean();

    private PinnedPoppedValueSequence(
            StableMemoryBackend allocator,
            NativeListEntryRef[] entries,
            NativeHandleSet pinnedHandles,
            boolean nullValue,
            long retainedMemoryBytes
    ) {
        this.allocator = allocator;
        this.entries = Objects.requireNonNull(entries, "entries").clone();
        this.pinnedHandles = Objects.requireNonNull(pinnedHandles, "pinnedHandles");
        this.nullValue = nullValue;
        this.retainedMemoryBytes = retainedMemoryBytes;
    }

    public static PinnedPoppedValueSequence nullValue() {
        return new PinnedPoppedValueSequence(
                null,
                new NativeListEntryRef[0],
                new NativeHandleSet(0),
                true,
                0L
        );
    }

    public static PinnedPoppedValueSequence empty() {
        return new PinnedPoppedValueSequence(
                null,
                new NativeListEntryRef[0],
                new NativeHandleSet(0),
                false,
                0L
        );
    }

    public static PinnedPoppedValueSequence capture(StableMemoryBackend allocator, NativeListEntryRef[] entries) {
        Objects.requireNonNull(allocator, "allocator");
        Objects.requireNonNull(entries, "entries");
        long retained = 0L;
        NativeHandleSet pinnedHandles = new NativeHandleSet(entries.length);
        for (NativeListEntryRef entry : entries) {
            Objects.requireNonNull(entry, "entry");
            NativeHandle handle = entry.handle();
            if (handle != null && pinnedHandles.add(handle)) {
                retained = addSaturating(retained, entry.retainedBytes());
            }
        }
        pinnedHandles.pinAll(allocator);
        return new PinnedPoppedValueSequence(allocator, entries, pinnedHandles, false, retained);
    }

    @Override
    public boolean isNull() {
        return nullValue;
    }

    @Override
    public int elementCount() {
        return entries.length;
    }

    @Override
    public void visitElementLengths(PayloadLengthSink out) {
        Objects.requireNonNull(out, "out");
        ensureOpen();
        for (NativeListEntryRef entry : entries) {
            out.payloadLength(entry.handle() == null ? -1 : entry.payloadLength());
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
            out.value(NativeBytesSlice.retained(
                    allocator,
                    handle,
                    entry.payloadOffset(),
                    entry.payloadLength()
            ));
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        if (allocator != null) {
            pinnedHandles.unpinAll(allocator);
        }
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("popped value sequence is closed");
        }
    }

    private static long addSaturating(long left, long right) {
        if (right <= 0L) {
            return left;
        }
        return Long.MAX_VALUE - left < right ? Long.MAX_VALUE : left + right;
    }
}
