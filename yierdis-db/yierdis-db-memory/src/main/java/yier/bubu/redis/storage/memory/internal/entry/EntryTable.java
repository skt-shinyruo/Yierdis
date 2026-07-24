package yier.bubu.redis.storage.memory.internal.entry;

import java.util.Objects;
import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.api.NativeObjectView;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

public final class EntryTable implements AutoCloseable {
    // 完整句柄不能在 DB 边界退化为 local raw；固定布局显式保存每个句柄的两个 long。
    private static final int KEY_HANDLE_OFFSET = 0;
    private static final int VALUE_HANDLE_OFFSET = 16;
    private static final int KEY_HASH_OFFSET = 32;
    private static final int TYPE_OFFSET = 36;
    private static final int ENCODING_OFFSET = 40;
    private static final int FLAGS_OFFSET = 44;
    private static final int EXPIRE_AT_MILLIS_OFFSET = 48;
    private static final int VERSION_OFFSET = 56;
    private static final int LRU_OR_LFU_OFFSET = 64;
    private static final ValueType[] VALUE_TYPES = ValueType.values();
    private static final ValueEncoding[] VALUE_ENCODINGS = ValueEncoding.values();

    private final StableMemoryBackend backend;
    private boolean closed;

    public EntryTable(StableMemoryBackend backend) {
        this.backend = Objects.requireNonNull(backend, "backend");
    }

    public EntryHandle allocate(EntryRecord record) {
        Objects.requireNonNull(record, "record");
        ensureOpen();
        NativeHandle nativeHandle = backend.allocate(
                NativeObjectKind.ENTRY_RECORD,
                NativeStorageLayout.ENTRY_RECORD_BYTES
        );
        boolean written = false;
        try {
            write(nativeHandle, record);
            written = true;
            return new EntryHandle(nativeHandle);
        } finally {
            if (!written) {
                backend.free(nativeHandle);
            }
        }
    }

    public EntryHandle reserve() {
        ensureOpen();
        return new EntryHandle(backend.allocate(
                NativeObjectKind.ENTRY_RECORD,
                NativeStorageLayout.ENTRY_RECORD_BYTES
        ));
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
        return read(handle.nativeHandle());
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
        backend.free(handle.nativeHandle());
    }

    public int size() {
        ensureOpen();
        return Math.toIntExact(backend.stats().objectCount(NativeObjectKind.ENTRY_RECORD));
    }

    public long nativeBytes() {
        ensureOpen();
        return Math.multiplyExact(
                backend.stats().objectCount(NativeObjectKind.ENTRY_RECORD),
                NativeStorageLayout.ENTRY_RECORD_BYTES
        );
    }

    public void clear() {
        ensureOpen();
    }

    @Override
    public void close() {
        closed = true;
    }

    StableMemoryBackend backend() {
        return backend;
    }

    private void write(NativeHandle handle, EntryRecord record) {
        try (NativeObjectView view = backend.resolve(handle, NativeAccessMode.READ_WRITE)) {
            writeHandle(view, KEY_HANDLE_OFFSET, record.keyHandle());
            writeHandle(view, VALUE_HANDLE_OFFSET, record.valueHandle().nativeHandle());
            view.setIntLittleEndian(KEY_HASH_OFFSET, record.keyHash());
            view.setIntLittleEndian(TYPE_OFFSET, record.type().ordinal());
            view.setIntLittleEndian(ENCODING_OFFSET, record.encoding().ordinal());
            view.setIntLittleEndian(FLAGS_OFFSET, record.flags());
            view.setLongLittleEndian(EXPIRE_AT_MILLIS_OFFSET, record.expireAtMillis());
            view.setLongLittleEndian(VERSION_OFFSET, record.version());
            view.setLongLittleEndian(LRU_OR_LFU_OFFSET, record.lruOrLfu());
        }
    }

    private EntryRecord read(NativeHandle handle) {
        try (NativeObjectView view = backend.resolve(handle, NativeAccessMode.READ_ONLY)) {
            return new EntryRecord(
                    readHandle(view, KEY_HANDLE_OFFSET),
                    new ValueHandle(readHandle(view, VALUE_HANDLE_OFFSET)),
                    view.getIntLittleEndian(KEY_HASH_OFFSET),
                    valueType(view.getIntLittleEndian(TYPE_OFFSET)),
                    valueEncoding(view.getIntLittleEndian(ENCODING_OFFSET)),
                    view.getIntLittleEndian(FLAGS_OFFSET),
                    view.getLongLittleEndian(EXPIRE_AT_MILLIS_OFFSET),
                    view.getLongLittleEndian(VERSION_OFFSET),
                    view.getLongLittleEndian(LRU_OR_LFU_OFFSET)
            );
        }
    }

    private static void writeHandle(NativeObjectView view, int offset, NativeHandle handle) {
        view.setLongLittleEndian(offset, handle.allocatorId());
        view.setLongLittleEndian(offset + Long.BYTES, handle.localRaw());
    }

    private static NativeHandle readHandle(NativeObjectView view, int offset) {
        return new NativeHandle(
                view.getLongLittleEndian(offset),
                view.getLongLittleEndian(offset + Long.BYTES)
        );
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
}
