package yier.bubu.redis.storage.memory.internal.entry;

import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.memory.api.OffHeapAllocator;
import yier.bubu.redis.memory.api.OffHeapBuf;
import yier.bubu.redis.memory.api.OffHeapSlice;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisForeignOffHeapAllocator;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class StringRoot implements TypeRoot {
    private static final int COPY_BUFFER_BYTES = 8 * 1024;
    private static final ThreadLocal<byte[]> TL_COPY_BUF = ThreadLocal.withInitial(() -> new byte[COPY_BUFFER_BYTES]);
    private static final byte[] ZERO_BUF = new byte[COPY_BUFFER_BYTES];
    private static final OffHeapSlice EMPTY_SLICE = new OffHeapSlice() {
        @Override
        public int length() {
            return 0;
        }

        @Override
        public byte getByte(int index) {
            throw new IndexOutOfBoundsException();
        }

        @Override
        public void getBytes(int index, byte[] dst, int dstOff, int len) {
            Objects.requireNonNull(dst, "dst");
            if (index != 0 || len != 0 || dstOff < 0 || dstOff > dst.length) {
                throw new IndexOutOfBoundsException();
            }
        }

        @Override
        public void writeTo(BytesSink out) {
            Objects.requireNonNull(out, "out");
        }
    };

    private final OffHeapAllocator allocator;
    private final boolean ownsAllocator;
    private final Map<Long, Slot> slots = new HashMap<>();
    private long nextHandle = 1L;
    private boolean closed;

    public StringRoot(YierdisFfmMemoryRuntime runtime) {
        this(new YierdisForeignOffHeapAllocator(Objects.requireNonNull(runtime, "runtime"), 0), true);
    }

    public StringRoot(OffHeapAllocator allocator) {
        this(allocator, false);
    }

    private StringRoot(OffHeapAllocator allocator, boolean ownsAllocator) {
        this.allocator = Objects.requireNonNull(allocator, "allocator");
        this.ownsAllocator = ownsAllocator;
    }

    @Override
    public ValueType type() {
        return ValueType.STRING;
    }

    @Override
    public ValueEncoding encoding() {
        return ValueEncoding.STRING_RAW;
    }

    public synchronized ValueEncoding encoding(ValueHandle handle) {
        requireSlot(handle);
        return ValueEncoding.STRING_RAW;
    }

    public synchronized boolean contains(ValueHandle handle) {
        ensureOpen();
        return handle != null && slots.containsKey(handle.raw());
    }

    public synchronized ValueHandle store(byte[] value) {
        ensureOpen();
        int len = value == null ? 0 : value.length;
        ValueHandle handle = new ValueHandle(nextHandle++);
        slots.put(handle.raw(), new Slot(allocate(value, len), len));
        return handle;
    }

    public synchronized ValueHandle store(BytesSlice value) {
        ensureOpen();
        int len = value == null ? 0 : value.length();
        ValueHandle handle = new ValueHandle(nextHandle++);
        slots.put(handle.raw(), new Slot(allocate(value, len), len));
        return handle;
    }

    public synchronized void overwrite(ValueHandle handle, byte[] value) {
        ensureOpen();
        Slot slot = requireSlot(handle);
        int len = value == null ? 0 : value.length;
        overwrite(slot, value, len);
    }

    public synchronized void overwrite(ValueHandle handle, BytesSlice value) {
        ensureOpen();
        Slot slot = requireSlot(handle);
        int len = value == null ? 0 : value.length();
        overwrite(slot, value, len);
    }

    public synchronized int append(ValueHandle handle, byte[] suffix) {
        ensureOpen();
        if (suffix == null || suffix.length == 0) {
            return length(handle);
        }
        Slot slot = requireSlot(handle);
        int oldLen = slot.length;
        int nextLen = Math.addExact(oldLen, suffix.length);
        ensureCapacity(slot, nextLen);
        slot.buffer.setBytes(oldLen, suffix, 0, suffix.length);
        slot.length = nextLen;
        return nextLen;
    }

    public synchronized int append(ValueHandle handle, BytesSlice suffix) {
        ensureOpen();
        if (suffix == null || suffix.length() == 0) {
            return length(handle);
        }
        Slot slot = requireSlot(handle);
        int oldLen = slot.length;
        int suffixLen = suffix.length();
        int nextLen = Math.addExact(oldLen, suffixLen);
        ensureCapacity(slot, nextLen);
        slot.buffer.setBytes(oldLen, suffix, 0, suffixLen);
        slot.length = nextLen;
        return nextLen;
    }

    public synchronized void ensureLength(ValueHandle handle, int requiredLen) {
        ensureOpen();
        if (requiredLen < 0) {
            throw new IllegalArgumentException("requiredLen must be >= 0");
        }
        Slot slot = requireSlot(handle);
        if (requiredLen <= slot.length) {
            return;
        }
        int oldLen = slot.length;
        ensureCapacity(slot, requiredLen);
        zeroFill(slot.buffer, oldLen, requiredLen);
        slot.length = requiredLen;
    }

    public synchronized byte byteAt(ValueHandle handle, int index) {
        ensureOpen();
        Slot slot = requireSlot(handle);
        if (index < 0 || index >= slot.length || slot.buffer == null) {
            throw new IndexOutOfBoundsException();
        }
        return slot.buffer.getByte(index);
    }

    public synchronized void setByteAt(ValueHandle handle, int index, byte value) {
        ensureOpen();
        Slot slot = requireSlot(handle);
        if (index < 0 || index >= slot.length || slot.buffer == null) {
            throw new IndexOutOfBoundsException();
        }
        slot.buffer.setByte(index, value);
    }

    public synchronized OffHeapSlice slice(ValueHandle handle) {
        ensureOpen();
        Slot slot = requireSlot(handle);
        if (slot.length == 0) {
            return EMPTY_SLICE;
        }
        return slot.buffer.slice(0, slot.length);
    }

    public synchronized byte[] copy(ValueHandle handle) {
        ensureOpen();
        Slot slot = requireSlot(handle);
        if (slot.length == 0) {
            return new byte[0];
        }
        byte[] out = new byte[slot.length];
        slot.buffer.getBytes(0, out, 0, slot.length);
        return out;
    }

    public synchronized int length(ValueHandle handle) {
        ensureOpen();
        return requireSlot(handle).length;
    }

    @Override
    public synchronized long estimatedBytes(ValueHandle handle) {
        ensureOpen();
        Slot slot = requireSlot(handle);
        return slot.buffer == null ? 0L : slot.buffer.capacity();
    }

    @Override
    public synchronized void release(ValueHandle handle) {
        if (handle == null) {
            return;
        }
        Slot removed = slots.remove(handle.raw());
        if (removed != null) {
            closeSlotBuffer(removed);
        }
    }

    @Override
    public synchronized void clear() {
        ensureOpen();
        RuntimeException failure = null;
        for (Slot slot : slots.values()) {
            try {
                closeSlotBuffer(slot);
            } catch (RuntimeException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        slots.clear();
        if (failure != null) {
            throw failure;
        }
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        RuntimeException failure = null;
        try {
            clear();
        } catch (RuntimeException e) {
            failure = e;
        }
        if (ownsAllocator) {
            try {
                allocator.close();
            } catch (RuntimeException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        closed = true;
        if (failure != null) {
            throw failure;
        }
    }

    private OffHeapBuf allocate(byte[] value, int len) {
        if (len <= 0) {
            return null;
        }
        OffHeapBuf buffer = allocator.allocate(len);
        boolean ok = false;
        try {
            buffer.setBytes(0, value, 0, len);
            ok = true;
            return buffer;
        } finally {
            if (!ok) {
                buffer.close();
            }
        }
    }

    private OffHeapBuf allocate(BytesSlice value, int len) {
        if (len <= 0) {
            return null;
        }
        OffHeapBuf buffer = allocator.allocate(len);
        boolean ok = false;
        try {
            buffer.setBytes(0, value, 0, len);
            ok = true;
            return buffer;
        } finally {
            if (!ok) {
                buffer.close();
            }
        }
    }

    private void overwrite(Slot slot, byte[] value, int len) {
        if (len <= 0) {
            closeSlotBuffer(slot);
            slot.length = 0;
            return;
        }
        if (slot.buffer != null && slot.buffer.capacity() >= len) {
            slot.buffer.setBytes(0, value, 0, len);
            slot.length = len;
            return;
        }
        OffHeapBuf next = allocate(value, len);
        closeSlotBuffer(slot);
        slot.buffer = next;
        slot.length = len;
    }

    private void overwrite(Slot slot, BytesSlice value, int len) {
        if (len <= 0) {
            closeSlotBuffer(slot);
            slot.length = 0;
            return;
        }
        if (slot.buffer != null && slot.buffer.capacity() >= len) {
            slot.buffer.setBytes(0, value, 0, len);
            slot.length = len;
            return;
        }
        OffHeapBuf next = allocate(value, len);
        closeSlotBuffer(slot);
        slot.buffer = next;
        slot.length = len;
    }

    private void ensureCapacity(Slot slot, int required) {
        if (required <= 0) {
            return;
        }
        if (slot.buffer != null && slot.buffer.capacity() >= required) {
            return;
        }
        int nextCapacity = nextCapacity(slot.buffer == null ? 0 : slot.buffer.capacity(), required);
        OffHeapBuf next = allocator.allocate(nextCapacity);
        boolean ok = false;
        try {
            if (slot.buffer != null && slot.length > 0) {
                copy(slot.buffer, next, slot.length);
            }
            ok = true;
        } finally {
            if (!ok) {
                next.close();
            }
        }
        closeSlotBuffer(slot);
        slot.buffer = next;
    }

    private static void copy(OffHeapBuf src, OffHeapBuf dst, int len) {
        byte[] scratch = TL_COPY_BUF.get();
        int remaining = len;
        int offset = 0;
        while (remaining > 0) {
            int chunk = Math.min(remaining, scratch.length);
            src.getBytes(offset, scratch, 0, chunk);
            dst.setBytes(offset, scratch, 0, chunk);
            offset += chunk;
            remaining -= chunk;
        }
    }

    private static void zeroFill(OffHeapBuf buffer, int from, int toExclusive) {
        int remaining = toExclusive - from;
        int offset = from;
        while (remaining > 0) {
            int chunk = Math.min(remaining, ZERO_BUF.length);
            buffer.setBytes(offset, ZERO_BUF, 0, chunk);
            offset += chunk;
            remaining -= chunk;
        }
    }

    private static int nextCapacity(int current, int required) {
        int cap = Math.max(16, current);
        while (cap < required) {
            int next = cap < 1024 * 1024 ? (cap << 1) : (cap + 1024 * 1024);
            if (next <= cap) {
                return required;
            }
            cap = next;
        }
        return cap;
    }

    private void closeSlotBuffer(Slot slot) {
        if (slot.buffer != null) {
            slot.buffer.close();
            slot.buffer = null;
        }
    }

    private Slot requireSlot(ValueHandle handle) {
        Objects.requireNonNull(handle, "handle");
        Slot slot = slots.get(handle.raw());
        if (slot == null) {
            throw new IllegalArgumentException("unknown string value handle: " + handle.raw());
        }
        return slot;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("string root is closed");
        }
    }

    private static final class Slot {
        private OffHeapBuf buffer;
        private int length;

        private Slot(OffHeapBuf buffer, int length) {
            this.buffer = buffer;
            this.length = length;
        }
    }
}
