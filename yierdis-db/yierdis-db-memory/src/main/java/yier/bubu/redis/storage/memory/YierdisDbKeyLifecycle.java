package yier.bubu.redis.storage.memory;

import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.OffHeapAllocator;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.storage.api.DbMemoryConstants;
import yier.bubu.redis.storage.api.DbChange;
import yier.bubu.redis.storage.api.DbChangeContext;
import yier.bubu.redis.storage.api.DbChangeKind;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.EntryTable;
import yier.bubu.redis.storage.memory.internal.entry.HashRoot;
import yier.bubu.redis.storage.memory.internal.entry.ListRoot;
import yier.bubu.redis.storage.memory.internal.entry.SetRoot;
import yier.bubu.redis.storage.memory.internal.entry.StringRoot;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;
import yier.bubu.redis.storage.memory.internal.entry.ZSetRoot;
import yier.bubu.redis.storage.memory.internal.expire.YierdisExpireIndex;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.key.KeyHandleAccess;
import yier.bubu.redis.storage.memory.internal.keyspace.NativeKeyDirectory;
import yier.bubu.redis.storage.memory.internal.keyspace.YierdisKeyspace;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

public final class YierdisDbKeyLifecycle {
    private final YierdisExpireIndex expires;
    private final OffHeapAllocator offHeapAllocator;
    private final NativeAllocator nativeAllocator;
    private final YierdisFfmMemoryRuntime memoryRuntime;
    private final EntryTable entryTable;
    private final NativeKeyDirectory keyDirectory;
    private final StringRoot stringRoot;
    private final ListRoot listRoot;
    private final HashRoot hashRoot;
    private final SetRoot setRoot;
    private final ZSetRoot zsetRoot;
    private final LongSupplier lruClockSupplier;
    private final LongConsumer adjustUsedBytesCallback;
    private final int dbIndex;

    YierdisDbKeyLifecycle(
            YierdisExpireIndex expires,
            OffHeapAllocator offHeapAllocator,
            NativeAllocator nativeAllocator,
            YierdisFfmMemoryRuntime memoryRuntime,
            EntryTable entryTable,
            NativeKeyDirectory keyDirectory,
            StringRoot stringRoot,
            ListRoot listRoot,
            HashRoot hashRoot,
            SetRoot setRoot,
            ZSetRoot zsetRoot,
            LongSupplier lruClockSupplier,
            LongConsumer adjustUsedBytesCallback
    ) {
        this(
                expires,
                offHeapAllocator,
                nativeAllocator,
                memoryRuntime,
                entryTable,
                keyDirectory,
                stringRoot,
                listRoot,
                hashRoot,
                setRoot,
                zsetRoot,
                lruClockSupplier,
                adjustUsedBytesCallback,
                0
        );
    }

    YierdisDbKeyLifecycle(
            YierdisExpireIndex expires,
            OffHeapAllocator offHeapAllocator,
            NativeAllocator nativeAllocator,
            YierdisFfmMemoryRuntime memoryRuntime,
            EntryTable entryTable,
            NativeKeyDirectory keyDirectory,
            StringRoot stringRoot,
            ListRoot listRoot,
            HashRoot hashRoot,
            SetRoot setRoot,
            ZSetRoot zsetRoot,
            LongSupplier lruClockSupplier,
            LongConsumer adjustUsedBytesCallback,
            int dbIndex
    ) {
        this.expires = Objects.requireNonNull(expires, "expires");
        this.offHeapAllocator = offHeapAllocator;
        this.nativeAllocator = java.util.Objects.requireNonNull(nativeAllocator, "nativeAllocator");
        this.memoryRuntime = memoryRuntime;
        this.entryTable = Objects.requireNonNull(entryTable, "entryTable");
        this.keyDirectory = Objects.requireNonNull(keyDirectory, "keyDirectory");
        this.stringRoot = Objects.requireNonNull(stringRoot, "stringRoot");
        this.listRoot = Objects.requireNonNull(listRoot, "listRoot");
        this.hashRoot = Objects.requireNonNull(hashRoot, "hashRoot");
        this.setRoot = Objects.requireNonNull(setRoot, "setRoot");
        this.zsetRoot = Objects.requireNonNull(zsetRoot, "zsetRoot");
        this.lruClockSupplier = Objects.requireNonNull(lruClockSupplier, "lruClockSupplier");
        this.adjustUsedBytesCallback = Objects.requireNonNull(adjustUsedBytesCallback, "adjustUsedBytesCallback");
        this.dbIndex = Math.max(0, dbIndex);
    }

    public OffHeapAllocator offHeapAllocator() {
        return offHeapAllocator;
    }

    NativeAllocator nativeAllocator() {
        return nativeAllocator;
    }

    public YierdisFfmMemoryRuntime memoryRuntime() {
        return memoryRuntime;
    }

    public EntryTable entryTable() {
        return entryTable;
    }

    public NativeKeyDirectory keyDirectory() {
        return keyDirectory;
    }

    public StringRoot stringRoot() {
        return stringRoot;
    }

    public ListRoot listRoot() {
        return listRoot;
    }

    public HashRoot hashRoot() {
        return hashRoot;
    }

    public SetRoot setRoot() {
        return setRoot;
    }

    public ZSetRoot zsetRoot() {
        return zsetRoot;
    }

    public KeyHandle keyHandle(byte[] keyBytes) {
        if (keyBytes == null) {
            return null;
        }
        return keyDirectory.getKeyHandle(keyBytes);
    }

    public KeyHandle keyHandle(BytesView keyView) {
        if (keyView == null) {
            return null;
        }
        return keyDirectory.getKeyHandle(YierdisDb.toByteArray(keyView));
    }

    public EntryHandle entryHandle(byte[] keyBytes) {
        if (keyBytes == null) {
            return null;
        }
        return keyDirectory.get(keyBytes);
    }

    public EntryRecord entryRecord(byte[] keyBytes) {
        EntryHandle handle = entryHandle(keyBytes);
        return handle == null ? null : entryRecord(handle);
    }

    public EntryRecord entryRecord(BytesView keyView) {
        if (keyView == null) {
            return null;
        }
        return entryRecord(YierdisDb.toByteArray(keyView));
    }

    public EntryRecord entryRecord(KeyHandle keyHandle) {
        if (keyHandle == null) {
            return null;
        }
        return entryRecord(keyBytes(keyHandle));
    }

    public EntryRecord entryRecord(EntryHandle handle) {
        if (handle == null) {
            return null;
        }
        return entryTable.get(handle);
    }

    public EntryRecord liveEntryRecord(byte[] keyBytes) {
        EntryHandle handle = entryHandle(keyBytes);
        if (handle == null) {
            return null;
        }
        EntryRecord record = entryRecord(handle);
        if (record == null) {
            unlinkEntry(keyBytes);
            return null;
        }
        KeyHandle keyHandle = keyHandle(keyBytes);
        return liveEntryRecord(keyHandle, record);
    }

    public EntryRecord liveEntryRecord(BytesView keyView) {
        if (keyView == null) {
            return null;
        }
        return liveEntryRecord(YierdisDb.toByteArray(keyView));
    }

    public EntryRecord liveEntryRecord(KeyHandle keyHandle) {
        if (keyHandle == null) {
            return null;
        }
        EntryRecord record = entryRecord(keyHandle);
        return liveEntryRecord(keyHandle, record);
    }

    private EntryRecord liveEntryRecord(KeyHandle keyHandle, EntryRecord record) {
        if (keyHandle == null || record == null) {
            return null;
        }
        long now = System.currentTimeMillis();
        if (removeIfExpired(keyHandle, record, now)) {
            return null;
        }
        if (isKeyExpired(keyHandle, now)) {
            return null;
        }
        return record;
    }

    public EntryRecord unlinkEntry(byte[] keyBytes) {
        if (keyBytes == null) {
            return null;
        }
        EntryHandle handle = keyDirectory.remove(keyBytes);
        if (handle == null) {
            return null;
        }
        EntryRecord record = entryTable.get(handle);
        if (record != null) {
            releaseValue(record);
            entryTable.release(handle);
        }
        return record;
    }

    public EntryRecord unlinkEntry(EntryHandle handle) {
        if (handle == null) {
            return null;
        }
        if (!keyDirectory.remove(handle)) {
            return null;
        }
        EntryRecord record = entryTable.get(handle);
        if (record != null) {
            releaseValue(record);
            entryTable.release(handle);
        }
        return record;
    }

    public int keyCount() {
        return keyDirectory.size();
    }

    public int expireCount() {
        return expires.size();
    }

    public KeyHandle randomKeyHandle() {
        return keyDirectory.randomKeyHandle();
    }

    public KeyHandle randomExpireKeyHandle() {
        return expires.randomKeyHandle();
    }

    public void forEachKeyHandle(BiConsumer<KeyHandle, EntryRecord> consumer) {
        Objects.requireNonNull(consumer, "consumer");
        keyDirectory.forEachEntry((keyHandle, entryHandle) -> consumer.accept(keyHandle, entryRecord(entryHandle)));
    }

    public Long expireAtMillis(byte[] keyBytes) {
        KeyHandle handle = keyHandle(keyBytes);
        return handle == null ? null : expires.get(handle);
    }

    public Long expireAtMillis(KeyHandle keyHandle) {
        return keyHandle == null ? null : expires.get(keyHandle);
    }

    public EntryRecord computeWithHandle(
            byte[] keyBytes,
            BiFunction<? super KeyHandle, ? super EntryRecord, ? extends EntryRecord> remappingFunction
    ) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        Objects.requireNonNull(remappingFunction, "remappingFunction");

        KeyHandle keyHandle = keyHandleForEntryRemapping(keyBytes);
        EntryHandle existingHandle = keyDirectory.get(keyBytes);
        EntryRecord oldRecord = existingHandle == null ? null : entryTable.get(existingHandle);
        EntryRecord newRecord = remappingFunction.apply(keyHandle, oldRecord);
        if (newRecord == null) {
            if (existingHandle != null) {
                keyDirectory.remove(keyBytes, existingHandle);
                entryTable.release(existingHandle);
                releaseValue(oldRecord);
            }
            return null;
        }
        if (existingHandle != null) {
            entryTable.replace(existingHandle, newRecord);
            releaseReplacedValue(oldRecord, newRecord);
            return newRecord;
        }

        EntryHandle created = null;
        boolean inserted = false;
        try {
            EntryHandle allocated = entryTable.allocate(newRecord);
            created = allocated;
            keyDirectory.compute(keyBytes, (key, oldHandle) -> {
                if (oldHandle != null) {
                    throw new IllegalStateException("native entry appeared during remapping");
                }
                return allocated;
            });
            inserted = true;
            return newRecord;
        } finally {
            if (!inserted) {
                if (created != null) {
                    entryTable.release(created);
                }
                releaseValue(newRecord);
            }
        }
    }

    public EntryRecord computeIfPresentWithHandle(
            byte[] keyBytes,
            BiFunction<? super KeyHandle, ? super EntryRecord, ? extends EntryRecord> remappingFunction
    ) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        Objects.requireNonNull(remappingFunction, "remappingFunction");

        EntryHandle existingHandle = keyDirectory.get(keyBytes);
        if (existingHandle == null) {
            return null;
        }
        EntryRecord oldRecord = entryTable.get(existingHandle);
        if (oldRecord == null) {
            keyDirectory.remove(keyBytes, existingHandle);
            return null;
        }

        EntryRecord newRecord = remappingFunction.apply(keyHandleForEntryRemapping(keyBytes), oldRecord);
        if (newRecord == null) {
            keyDirectory.remove(keyBytes, existingHandle);
            entryTable.release(existingHandle);
            releaseValue(oldRecord);
            return null;
        }
        entryTable.replace(existingHandle, newRecord);
        releaseReplacedValue(oldRecord, newRecord);
        return newRecord;
    }

    public long estimatedBytesForRemoval(KeyHandle keyHandle, EntryRecord record) {
        if (record != null && record.version() > 0) {
            return record.version();
        }
        if (keyHandle == null || record == null) {
            return 0L;
        }
        return DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE;
    }

    public ScanCursorV2 scan(ScanCursorV2 cursor, int maxSteps, YierdisKeyspace.ScanConsumer<EntryRecord> consumer) {
        return keyDirectory.scan(cursor, maxSteps, (keyHandle, entryHandle) -> consumer.accept(keyHandle, entryRecord(entryHandle)));
    }

    public boolean removeEntry(KeyHandle keyHandle, EntryRecord expectedRecord) {
        if (keyHandle == null) {
            return false;
        }
        byte[] keyBytes = keyBytes(keyHandle);
        EntryHandle handle = keyDirectory.get(keyBytes);
        if (handle == null) {
            return false;
        }
        EntryRecord current = entryTable.get(handle);
        if (current == null) {
            keyDirectory.remove(keyBytes, handle);
            return false;
        }
        if (expectedRecord != null && !current.equals(expectedRecord)) {
            return false;
        }
        keyDirectory.remove(keyBytes, handle);
        entryTable.release(handle);
        releaseValue(current);
        return true;
    }

    public boolean removeIfExpired(KeyHandle keyHandle, EntryRecord record, long nowMillis) {
        Long expireAtMillis = expireAtMillis(keyHandle);
        if (expireAtMillis == null || expireAtMillis > nowMillis) {
            return false;
        }
        byte[] keyBytes = keyBytes(keyHandle);
        long removalBytes = estimatedBytesForRemoval(keyHandle, record);
        removeExpireIndexOnly(keyHandle);
        if (removeEntry(keyHandle, record)) {
            adjustUsedBytesCallback.accept(-removalBytes);
            emitSyntheticDelete(keyBytes, DbChangeKind.EXPIRED);
            return true;
        }
        return false;
    }

    public void emitSyntheticDelete(KeyHandle keyHandle, DbChangeKind kind) {
        if (keyHandle == null || kind == null) {
            return;
        }
        emitSyntheticDelete(keyBytes(keyHandle), kind);
    }

    public void emitSyntheticDelete(byte[] keyBytes, DbChangeKind kind) {
        if (keyBytes == null || kind == null) {
            return;
        }
        DbChangeContext.emit(DbChange.syntheticDelete(dbIndex, kind, keyBytes));
    }

    public byte[] copyKeyBytes(KeyHandle keyHandle) {
        if (keyHandle == null) {
            return null;
        }
        return keyBytes(keyHandle);
    }

    public boolean isKeyExpired(KeyHandle keyHandle, long nowMillis) {
        Long expireAtMillis = expireAtMillis(keyHandle);
        return expireAtMillis != null && expireAtMillis <= nowMillis;
    }

    public void setExpireAtMillis(byte[] keyBytes, long expireAtMillis) {
        KeyHandle handle = keyHandle(keyBytes);
        if (handle != null) {
            setExpireAtMillis(handle, expireAtMillis);
        }
    }

    public void setExpireAtMillis(KeyHandle keyHandle, long expireAtMillis) {
        if (keyHandle == null) {
            return;
        }
        expires.setExpireAtMillis(keyHandle, expireAtMillis);
        replaceEntryExpire(keyHandle, expireAtMillis);
    }

    public void removeExpire(byte[] keyBytes) {
        KeyHandle handle = keyHandle(keyBytes);
        if (handle != null) {
            removeExpire(handle);
            return;
        }
        removeExpireByKeyBytes(keyBytes);
    }

    public void removeExpire(KeyHandle keyHandle) {
        if (keyHandle == null) {
            return;
        }
        removeExpireIndexOnly(keyHandle);
        replaceEntryExpire(keyHandle, -1L);
    }

    public void removeExpireIndexOnly(KeyHandle keyHandle) {
        if (keyHandle != null) {
            expires.removeExpire(keyHandle);
        }
    }

    public void removeExpireByKeyBytes(byte[] keyBytes) {
        if (keyBytes != null) {
            expires.removeExpire(keyBytes);
        }
    }

    public EntryRecord newRecord(
            KeyHandle keyHandle,
            ValueHandle valueHandle,
            ValueType type,
            ValueEncoding encoding,
            long expireAtMillis,
            long estimatedBytes,
            EntryRecord previous
    ) {
        return new EntryRecord(
                keyHandleIdentity(keyHandle),
                valueHandle,
                keyHandle == null ? 0 : keyHandle.dictHash(),
                type,
                encoding,
                0,
                expireAtMillis,
                estimatedBytes,
                accessClock(previous == null ? 0L : previous.lruOrLfu())
        );
    }

    public EntryRecord touchRecord(KeyHandle keyHandle, EntryRecord record) {
        if (keyHandle == null || record == null) {
            return record;
        }
        long nextClock = accessClock(record.lruOrLfu());
        if (nextClock == record.lruOrLfu()) {
            return record;
        }
        EntryRecord touched = new EntryRecord(
                record.keyHandle(),
                record.valueHandle(),
                record.keyHash(),
                record.type(),
                record.encoding(),
                record.flags(),
                record.expireAtMillis(),
                record.version(),
                nextClock
        );
        EntryHandle handle = keyDirectory.get(keyBytes(keyHandle));
        if (handle != null) {
            EntryRecord current = entryTable.get(handle);
            if (record.equals(current)) {
                entryTable.replace(handle, touched);
            }
        }
        return touched;
    }

    public void releaseValue(EntryRecord record) {
        if (record == null || record.valueHandle() == null || record.valueHandle().raw() == 0L) {
            return;
        }
        switch (record.type()) {
            case STRING:
                stringRoot.release(record.valueHandle());
                break;
            case LIST:
                listRoot.release(record.valueHandle());
                break;
            case HASH:
                hashRoot.release(record.valueHandle());
                break;
            case SET:
                setRoot.release(record.valueHandle());
                break;
            case ZSET:
                zsetRoot.release(record.valueHandle());
                break;
            default:
                break;
        }
    }

    public void clearValues() {
        stringRoot.clear();
        listRoot.clear();
        hashRoot.clear();
        setRoot.clear();
        zsetRoot.clear();
    }

    private void releaseReplacedValue(EntryRecord oldRecord, EntryRecord newRecord) {
        if (oldRecord == null || newRecord == null) {
            return;
        }
        if (oldRecord.type() == newRecord.type()
                && oldRecord.valueHandle() != null
                && oldRecord.valueHandle().equals(newRecord.valueHandle())) {
            return;
        }
        releaseValue(oldRecord);
    }

    private void replaceEntryExpire(KeyHandle keyHandle, long expireAtMillis) {
        if (keyHandle == null) {
            return;
        }
        byte[] keyBytes = keyBytes(keyHandle);
        EntryHandle handle = keyDirectory.get(keyBytes);
        if (handle == null) {
            return;
        }
        EntryRecord record = entryTable.get(handle);
        if (record == null) {
            keyDirectory.remove(keyBytes, handle);
            return;
        }
        entryTable.replace(handle, new EntryRecord(
                record.keyHandle(),
                record.valueHandle(),
                record.keyHash(),
                record.type(),
                record.encoding(),
                record.flags(),
                expireAtMillis,
                record.version(),
                record.lruOrLfu()
        ));
    }

    private long accessClock(long previous) {
        long next = lruClockSupplier.getAsLong();
        return next <= 0L ? previous : next;
    }

    private long expireAtMillisOrAbsent(KeyHandle keyHandle) {
        Long expireAtMillis = expireAtMillis(keyHandle);
        return expireAtMillis == null ? -1L : expireAtMillis;
    }

    private KeyHandle keyHandleForEntryRemapping(byte[] keyBytes) {
        KeyHandle keyHandle = keyHandle(keyBytes);
        if (keyHandle != null) {
            return keyHandle;
        }
        return KeyHandle.forHeap(keyBytes, hashBytes(keyBytes));
    }

    private static int hashBytes(byte[] keyBytes) {
        int h = 1;
        for (byte keyByte : keyBytes) {
            h = 31 * h + keyByte;
        }
        return h;
    }

    private static long keyHandleIdentity(KeyHandle keyHandle) {
        if (keyHandle == null) {
            return 0L;
        }
        var nativeHandle = KeyHandleAccess.allocatorNativeHandleOrNull(keyHandle);
        if (nativeHandle != null) {
            return nativeHandle.raw();
        }
        var ref = KeyHandleAccess.ffmBytesRefOrNull(keyHandle);
        if (ref != null) {
            return System.identityHashCode(ref.region());
        }
        return System.identityHashCode(keyHandle);
    }

    private static byte[] keyBytes(KeyHandle keyHandle) {
        Objects.requireNonNull(keyHandle, "keyHandle");
        int len = keyHandle.len();
        byte[] out = new byte[len];
        for (int i = 0; i < len; i++) {
            out[i] = keyHandle.byteAt(i);
        }
        return out;
    }
}
