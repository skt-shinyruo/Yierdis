package yier.bubu.redis.storage.memory.internal.value;

import static yier.bubu.redis.common.memory.MemoryUsageSnapshot.addSaturating;

import java.util.function.IntConsumer;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.storage.api.result.ByteValueSink;
import yier.bubu.redis.storage.api.result.PoppedValueSequence;

/**
 * pop 命中的只读元素序列；{@link ReleaseMode} 决定 close 时对 retained handle 的释放语义。
 */
public final class NativePoppedValueSequence implements PoppedValueSequence {
    /**
     * PIN：capture 前 pin 所有 unique handle，close 时 unpin；元素只读，不接管 free。
     * OWN：close 前须 {@link #activateOwnership()}，接管唯一一次 free。
     */
    public enum ReleaseMode {
        PIN,
        OWN
    }

    private final StableMemoryBackend allocator;
    private final NativeListEntryRef[] entries;
    private final NativeHandleSet retainedHandles;
    private final ReleaseMode releaseMode;
    private final boolean nullValue;
    private final long retainedMemoryBytes;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean handlesReleased = new AtomicBoolean();
    private volatile boolean ownsHandles;

    private NativePoppedValueSequence(
            StableMemoryBackend allocator,
            NativeListEntryRef[] entries,
            NativeHandleSet retainedHandles,
            ReleaseMode releaseMode,
            boolean nullValue,
            long retainedMemoryBytes
    ) {
        Objects.requireNonNull(entries, "entries");
        Objects.requireNonNull(retainedHandles, "retainedHandles");
        Objects.requireNonNull(releaseMode, "releaseMode");
        if (retainedMemoryBytes < 0L) {
            throw new IllegalArgumentException("retainedMemoryBytes must be >= 0");
        }
        this.allocator = allocator;
        this.entries = entries.clone();
        this.retainedHandles = retainedHandles;
        this.releaseMode = releaseMode;
        this.nullValue = nullValue;
        this.retainedMemoryBytes = retainedMemoryBytes;
    }

    public static NativePoppedValueSequence nullValue() {
        return new NativePoppedValueSequence(
                null,
                new NativeListEntryRef[0],
                new NativeHandleSet(0),
                ReleaseMode.PIN,
                true,
                0L
        );
    }

    public static NativePoppedValueSequence empty() {
        return new NativePoppedValueSequence(
                null,
                new NativeListEntryRef[0],
                new NativeHandleSet(0),
                ReleaseMode.PIN,
                false,
                0L
        );
    }

    /**
     * 只读 pop 预览；preflight 结束前按 native block 固定底层 payload。
     */
    public static NativePoppedValueSequence pinned(StableMemoryBackend allocator, NativeListEntryRef[] entries) {
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
        return new NativePoppedValueSequence(allocator, entries, pinnedHandles, ReleaseMode.PIN, false, retained);
    }

    /**
     * 接管所有权，等 {@link #activateOwnership()} 后 close 才释放唯一一次 free。
     */
    public static NativePoppedValueSequence owned(StableMemoryBackend allocator, NativeListEntryRef[] entries) {
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
        return new NativePoppedValueSequence(
                allocator,
                entries,
                retainedHandles,
                ReleaseMode.OWN,
                false,
                retainedMemoryBytes
        );
    }

    public boolean retainsHandle(NativeHandle handle) {
        return retainedHandles.contains(handle);
    }

    /**
     * 声明响应对象接管 handle 的唯一一次 free；仅 OWN 序列需要调用（PIN 序列 close 只 unpin，忽略该标记）。
     */
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
        return entries.length;
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
            out.value(releaseMode == ReleaseMode.PIN
                    ? NativeBytesSlice.retained(allocator, handle, entry.payloadOffset(), entry.payloadLength())
                    : new NativeBytesSlice(
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
        if (releaseMode == ReleaseMode.PIN) {
            if (allocator != null) {
                retainedHandles.unpinAll(allocator);
            }
            return;
        }
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
