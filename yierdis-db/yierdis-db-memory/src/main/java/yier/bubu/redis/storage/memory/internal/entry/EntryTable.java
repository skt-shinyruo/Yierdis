package yier.bubu.redis.storage.memory.internal.entry;

import java.util.Objects;
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeMemoryException;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisStableNativeAllocator;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

public final class EntryTable implements AutoCloseable {
    // EntryRecord 在 native allocator 中使用固定 56-byte 小端布局；字段偏移一旦改变会影响已分配 entry 的解析。
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
    private final NativeAllocator allocator;
    private final boolean ownsAllocator;
    private boolean closed;

    public EntryTable(YierdisFfmMemoryRuntime runtime, int initialCapacity) {
        this(runtime, new YierdisStableNativeAllocator(
                Objects.requireNonNull(runtime, "runtime"),
                Math.max(4096, initialCapacity)
        ), true);
    }

    public EntryTable(YierdisFfmMemoryRuntime runtime, NativeAllocator allocator) {
        this(runtime, allocator, false);
    }

    private EntryTable(YierdisFfmMemoryRuntime runtime, NativeAllocator allocator, boolean ownsAllocator) {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        this.allocator = Objects.requireNonNull(allocator, "allocator");
        this.ownsAllocator = ownsAllocator;
    }

    public EntryHandle allocate(EntryRecord record) {
        Objects.requireNonNull(record, "record");
        ensureOpen();
        NativeHandle nativeHandle = allocator.allocate(NativeObjectKind.ENTRY_RECORD, RECORD_BYTES);
        boolean ok = false;
        try {
            EntryHandle handle = EntryHandle.fromNativeHandle(nativeHandle);
            write(nativeHandle, record);
            ok = true;
            return handle;
        } finally {
            if (!ok) {
                allocator.free(nativeHandle);
            }
        }
    }

    public EntryHandle reserve() {
        ensureOpen();
        NativeHandle nativeHandle = allocator.allocate(NativeObjectKind.ENTRY_RECORD, RECORD_BYTES);
        boolean ok = false;
        try {
            EntryHandle handle = EntryHandle.fromNativeHandle(nativeHandle);
            ok = true;
            return handle;
        } finally {
            if (!ok) {
                allocator.free(nativeHandle);
            }
        }
    }

    public void writeReserved(EntryHandle handle, EntryRecord record) {
        Objects.requireNonNull(handle, "handle");
        Objects.requireNonNull(record, "record");
        ensureOpen();
        write(handle.nativeHandle(), record);
    }

    public EntryRecord get(EntryHandle handle) {
        if (handle == null) {
            return null;
        }
        ensureOpen();
        try {
            return read(handle.nativeHandle());
        } catch (NativeMemoryException e) {
            throw e;
        } catch (RuntimeException e) {
            if (e.getMessage() != null && e.getMessage().contains("stale native handle")) {
                throw e;
            }
            throw e;
        }
    }

    public EntryRecord replace(EntryHandle handle, EntryRecord record) {
        Objects.requireNonNull(handle, "handle");
        Objects.requireNonNull(record, "record");
        ensureOpen();
        EntryRecord previous = read(handle.nativeHandle());
        write(handle.nativeHandle(), record);
        return previous;
    }

    public void release(EntryHandle handle) {
        Objects.requireNonNull(handle, "handle");
        ensureOpen();
        allocator.free(handle.nativeHandle());
    }

    public int size() {
        ensureOpen();
        return Math.toIntExact(allocator.stats().objectCount(NativeObjectKind.ENTRY_RECORD));
    }

    public long nativeBytes() {
        ensureOpen();
        return Math.multiplyExact(allocator.stats().objectCount(NativeObjectKind.ENTRY_RECORD), RECORD_BYTES);
    }

    public void clear() {
        ensureOpen();
        // EntryTable 不枚举共享 allocator；key directory 才拥有本 DB 的 entry graph。
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        Throwable failure = null;
        try {
            clear();
        } catch (RuntimeException | Error e) {
            failure = e;
        }
        if (ownsAllocator) {
            try {
                allocator.close();
            } catch (RuntimeException | Error e) {
                failure = addFailure(failure, e);
            }
        }
        closed = true;
        if (failure != null) {
            rethrow(failure);
        }
    }

    public YierdisFfmMemoryRuntime runtime() {
        return runtime;
    }

    NativeAllocator allocator() {
        return allocator;
    }

    private void write(NativeHandle handle, EntryRecord record) {
        try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_WRITE)) {
            setLong(view, KEY_HANDLE_OFFSET, record.keyHandle());
            setLong(view, VALUE_HANDLE_OFFSET, record.valueHandle().raw());
            setInt(view, KEY_HASH_OFFSET, record.keyHash());
            setInt(view, TYPE_OFFSET, record.type().ordinal());
            setInt(view, ENCODING_OFFSET, record.encoding().ordinal());
            setInt(view, FLAGS_OFFSET, record.flags());
            setLong(view, EXPIRE_AT_MILLIS_OFFSET, record.expireAtMillis());
            setLong(view, VERSION_OFFSET, record.version());
            setLong(view, LRU_OR_LFU_OFFSET, record.lruOrLfu());
        }
    }

    private EntryRecord read(NativeHandle handle) {
        try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
            return new EntryRecord(
                    getLong(view, KEY_HANDLE_OFFSET),
                    ValueHandle.fromRaw(getLong(view, VALUE_HANDLE_OFFSET)),
                    getInt(view, KEY_HASH_OFFSET),
                    valueType(getInt(view, TYPE_OFFSET)),
                    valueEncoding(getInt(view, ENCODING_OFFSET)),
                    getInt(view, FLAGS_OFFSET),
                    getLong(view, EXPIRE_AT_MILLIS_OFFSET),
                    getLong(view, VERSION_OFFSET),
                    getLong(view, LRU_OR_LFU_OFFSET)
            );
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("entry table is closed");
        }
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

    private static long getLong(NativeObjectView view, int offset) {
        return ((long) view.getByte(offset) & 0xff)
                | (((long) view.getByte(offset + 1) & 0xff) << 8)
                | (((long) view.getByte(offset + 2) & 0xff) << 16)
                | (((long) view.getByte(offset + 3) & 0xff) << 24)
                | (((long) view.getByte(offset + 4) & 0xff) << 32)
                | (((long) view.getByte(offset + 5) & 0xff) << 40)
                | (((long) view.getByte(offset + 6) & 0xff) << 48)
                | (((long) view.getByte(offset + 7) & 0xff) << 56);
    }

    private static void setLong(NativeObjectView view, int offset, long value) {
        for (int i = 0; i < Long.BYTES; i++) {
            view.setByte(offset + i, (byte) (value >>> (i * 8)));
        }
    }

    private static int getInt(NativeObjectView view, int offset) {
        return (view.getByte(offset) & 0xff)
                | ((view.getByte(offset + 1) & 0xff) << 8)
                | ((view.getByte(offset + 2) & 0xff) << 16)
                | ((view.getByte(offset + 3) & 0xff) << 24);
    }

    private static void setInt(NativeObjectView view, int offset, int value) {
        for (int i = 0; i < Integer.BYTES; i++) {
            view.setByte(offset + i, (byte) (value >>> (i * 8)));
        }
    }

    private static Throwable addFailure(Throwable failure, Throwable next) {
        if (failure == null) {
            return next;
        }
        failure.addSuppressed(next);
        return failure;
    }

    private static void rethrow(Throwable failure) {
        if (failure instanceof RuntimeException e) {
            throw e;
        }
        if (failure instanceof Error e) {
            throw e;
        }
        throw new IllegalStateException(failure);
    }
}
