package yier.bubu.redis.storage.memory.internal.entry;

import yier.bubu.redis.memory.api.OffHeapAllocator;
import yier.bubu.redis.memory.api.OffHeapBuf;
import yier.bubu.redis.memory.foreign.YierdisFfmAccess;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisFfmRegion;
import yier.bubu.redis.memory.foreign.YierdisFfmSlabAllocator;
import yier.bubu.redis.memory.foreign.YierdisForeignOffHeapAllocator;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class EntryTable implements AutoCloseable {
    private static final int KEY_HANDLE_OFFSET = 0;
    private static final int VALUE_HANDLE_OFFSET = 8;
    private static final int KEY_HASH_OFFSET = 16;
    private static final int TYPE_OFFSET = 20;
    private static final int ENCODING_OFFSET = 24;
    private static final int FLAGS_OFFSET = 28;
    private static final int EXPIRE_AT_MILLIS_OFFSET = 32;
    private static final int VERSION_OFFSET = 40;
    private static final int LRU_OR_LFU_OFFSET = 48;
    private static final int RECORD_BYTES = 56;

    private static final ValueType[] VALUE_TYPES = ValueType.values();
    private static final ValueEncoding[] VALUE_ENCODINGS = ValueEncoding.values();

    private final YierdisFfmMemoryRuntime runtime;
    private OffHeapAllocator allocator;
    private final boolean ownsAllocator;
    private final Map<Long, Slot> entries;
    private long nextHandle = 1L;
    private boolean closed;

    public EntryTable(YierdisFfmMemoryRuntime runtime, int initialCapacity) {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        this.allocator = null;
        this.ownsAllocator = false;
        this.entries = new HashMap<>(Math.max(16, initialCapacity));
    }

    public EntryTable(OffHeapAllocator allocator, int initialCapacity) {
        this.allocator = Objects.requireNonNull(allocator, "allocator");
        if (!(allocator instanceof YierdisForeignOffHeapAllocator foreignAllocator)) {
            throw new IllegalArgumentException("EntryTable requires a foreign off-heap allocator");
        }
        this.runtime = foreignAllocator.memoryRuntime();
        this.ownsAllocator = false;
        this.entries = new HashMap<>(Math.max(16, initialCapacity));
    }

    public EntryTable(YierdisFfmMemoryRuntime runtime, YierdisFfmSlabAllocator allocator, int initialCapacity) {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        this.allocator = Objects.requireNonNull(allocator, "allocator");
        this.ownsAllocator = true;
        this.entries = new HashMap<>(Math.max(16, initialCapacity));
    }

    public synchronized EntryHandle allocate(EntryRecord record) {
        Objects.requireNonNull(record, "record");
        ensureOpen();
        EntryHandle handle = new EntryHandle(nextHandle++);
        Slot slot = allocateSlot();
        boolean ok = false;
        try {
            write(slot, record);
            entries.put(handle.raw(), slot);
            ok = true;
            return handle;
        } finally {
            if (!ok) {
                slot.close();
            }
        }
    }

    public synchronized EntryRecord get(EntryHandle handle) {
        if (handle == null) {
            return null;
        }
        ensureOpen();
        Slot slot = entries.get(handle.raw());
        return slot == null ? null : read(slot);
    }

    public synchronized EntryRecord replace(EntryHandle handle, EntryRecord record) {
        Objects.requireNonNull(handle, "handle");
        Objects.requireNonNull(record, "record");
        ensureOpen();
        Slot slot = entries.get(handle.raw());
        if (slot == null) {
            throw new IllegalStateException("unknown entry handle: " + handle.raw());
        }
        EntryRecord previous = read(slot);
        write(slot, record);
        return previous;
    }

    public synchronized void release(EntryHandle handle) {
        Objects.requireNonNull(handle, "handle");
        ensureOpen();
        Slot slot = entries.get(handle.raw());
        if (slot == null) {
            throw new IllegalStateException("unknown entry handle: " + handle.raw());
        }
        slot.close();
        entries.remove(handle.raw());
        releaseOwnedAllocatorIfEmpty();
    }

    public synchronized int size() {
        ensureOpen();
        return entries.size();
    }

    public synchronized long nativeBytes() {
        ensureOpen();
        return (long) entries.size() * RECORD_BYTES;
    }

    public synchronized void clear() {
        ensureOpen();
        RuntimeException failure = null;
        for (Map.Entry<Long, Slot> entry : entries.entrySet()) {
            try {
                entry.getValue().close();
            } catch (RuntimeException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
                continue;
            }
            entry.setValue(null);
        }
        entries.values().removeIf(Objects::isNull);
        if (failure != null) {
            throw failure;
        }
        releaseOwnedAllocatorIfEmpty();
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        Throwable failure = null;
        try {
            clear();
        } catch (Throwable t) {
            failure = t;
        }
        if (ownsAllocator && allocator != null) {
            try {
                closeOwnedAllocator();
            } catch (Throwable t) {
                if (failure == null) {
                    failure = t;
                } else {
                    failure.addSuppressed(t);
                }
            }
        }
        closed = true;
        if (failure != null) {
            if (failure instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (failure instanceof Error error) {
                throw error;
            }
            throw new IllegalStateException("entry table close failed", failure);
        }
    }

    public YierdisFfmMemoryRuntime runtime() {
        return runtime;
    }

    private Slot allocateSlot() {
        if (allocator != null) {
            return new Slot(allocator.allocate(RECORD_BYTES));
        }
        return new Slot(runtime.allocateRegion("entry-record", RECORD_BYTES));
    }

    private void releaseOwnedAllocatorIfEmpty() {
        if (!ownsAllocator || allocator == null || !entries.isEmpty()) {
            return;
        }
        allocator.close();
        allocator = new YierdisFfmSlabAllocator(runtime);
    }

    private void closeOwnedAllocator() {
        allocator.close();
        allocator = null;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("entry table is closed");
        }
    }

    private static void write(Slot slot, EntryRecord record) {
        slot.setLong(KEY_HANDLE_OFFSET, record.keyHandle());
        slot.setLong(VALUE_HANDLE_OFFSET, record.valueHandle().raw());
        slot.setInt(KEY_HASH_OFFSET, record.keyHash());
        slot.setInt(TYPE_OFFSET, record.type().ordinal());
        slot.setInt(ENCODING_OFFSET, record.encoding().ordinal());
        slot.setInt(FLAGS_OFFSET, record.flags());
        slot.setLong(EXPIRE_AT_MILLIS_OFFSET, record.expireAtMillis());
        slot.setLong(VERSION_OFFSET, record.version());
        slot.setLong(LRU_OR_LFU_OFFSET, record.lruOrLfu());
    }

    private static EntryRecord read(Slot slot) {
        return new EntryRecord(
                slot.getLong(KEY_HANDLE_OFFSET),
                new ValueHandle(slot.getLong(VALUE_HANDLE_OFFSET)),
                slot.getInt(KEY_HASH_OFFSET),
                valueType(slot.getInt(TYPE_OFFSET)),
                valueEncoding(slot.getInt(ENCODING_OFFSET)),
                slot.getInt(FLAGS_OFFSET),
                slot.getLong(EXPIRE_AT_MILLIS_OFFSET),
                slot.getLong(VERSION_OFFSET),
                slot.getLong(LRU_OR_LFU_OFFSET)
        );
    }

    private static ValueType valueType(int ordinal) {
        if (ordinal < 0 || ordinal >= VALUE_TYPES.length) {
            throw new IllegalStateException("invalid value type ordinal: " + ordinal);
        }
        return VALUE_TYPES[ordinal];
    }

    private static ValueEncoding valueEncoding(int ordinal) {
        if (ordinal < 0 || ordinal >= VALUE_ENCODINGS.length) {
            throw new IllegalStateException("invalid value encoding ordinal: " + ordinal);
        }
        return VALUE_ENCODINGS[ordinal];
    }

    private static final class Slot {
        private final YierdisFfmRegion region;
        private final OffHeapBuf buffer;

        private Slot(YierdisFfmRegion region) {
            this.region = Objects.requireNonNull(region, "region");
            this.buffer = null;
        }

        private Slot(OffHeapBuf buffer) {
            this.region = null;
            this.buffer = Objects.requireNonNull(buffer, "buffer");
        }

        private long getLong(int offset) {
            if (region != null) {
                return YierdisFfmAccess.getLong(region.span(0, RECORD_BYTES), offset);
            }
            return ((long) buffer.getByte(offset) & 0xff)
                    | (((long) buffer.getByte(offset + 1) & 0xff) << 8)
                    | (((long) buffer.getByte(offset + 2) & 0xff) << 16)
                    | (((long) buffer.getByte(offset + 3) & 0xff) << 24)
                    | (((long) buffer.getByte(offset + 4) & 0xff) << 32)
                    | (((long) buffer.getByte(offset + 5) & 0xff) << 40)
                    | (((long) buffer.getByte(offset + 6) & 0xff) << 48)
                    | (((long) buffer.getByte(offset + 7) & 0xff) << 56);
        }

        private void setLong(int offset, long value) {
            if (region != null) {
                YierdisFfmAccess.setLong(region.span(0, RECORD_BYTES), offset, value);
                return;
            }
            for (int i = 0; i < Long.BYTES; i++) {
                buffer.setByte(offset + i, (byte) (value >>> (i * 8)));
            }
        }

        private int getInt(int offset) {
            if (region != null) {
                return YierdisFfmAccess.getInt(region.span(0, RECORD_BYTES), offset);
            }
            return (buffer.getByte(offset) & 0xff)
                    | ((buffer.getByte(offset + 1) & 0xff) << 8)
                    | ((buffer.getByte(offset + 2) & 0xff) << 16)
                    | ((buffer.getByte(offset + 3) & 0xff) << 24);
        }

        private void setInt(int offset, int value) {
            if (region != null) {
                YierdisFfmAccess.setInt(region.span(0, RECORD_BYTES), offset, value);
                return;
            }
            for (int i = 0; i < Integer.BYTES; i++) {
                buffer.setByte(offset + i, (byte) (value >>> (i * 8)));
            }
        }

        private void close() {
            if (region != null) {
                region.close();
            } else {
                buffer.close();
            }
        }
    }
}
