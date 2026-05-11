package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.entry.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.api.OffHeapAllocator;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.api.ValueType;

import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

public final class YierdisDbKeyLifecycle {
    private final YierdisKeyspace<YierdisObject> store;
    private final YierdisExpireIndex expires;
    private final OffHeapAllocator offHeapAllocator;
    private final YierdisFfmMemoryRuntime memoryRuntime;
    private final EntryTable entryTable;
    private final NativeKeyDirectory keyDirectory;
    private final StringRoot stringRoot;
    private final ListRoot listRoot;
    private final HashRoot hashRoot;
    private final SetRoot setRoot;
    private final ZSetRoot zsetRoot;
    private final Consumer<YierdisObject> touchCallback;
    private final LongConsumer adjustUsedBytesCallback;

    YierdisDbKeyLifecycle(
            YierdisKeyspace<YierdisObject> store,
            YierdisExpireIndex expires,
            OffHeapAllocator offHeapAllocator,
            YierdisFfmMemoryRuntime memoryRuntime,
            Consumer<YierdisObject> touchCallback,
            LongConsumer adjustUsedBytesCallback
    ) {
        this(
                store,
                expires,
                offHeapAllocator,
                memoryRuntime,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                touchCallback,
                adjustUsedBytesCallback
        );
    }

    YierdisDbKeyLifecycle(
            YierdisKeyspace<YierdisObject> store,
            YierdisExpireIndex expires,
            OffHeapAllocator offHeapAllocator,
            YierdisFfmMemoryRuntime memoryRuntime,
            EntryTable entryTable,
            NativeKeyDirectory keyDirectory,
            StringRoot stringRoot,
            ListRoot listRoot,
            HashRoot hashRoot,
            SetRoot setRoot,
            ZSetRoot zsetRoot,
            Consumer<YierdisObject> touchCallback,
            LongConsumer adjustUsedBytesCallback
    ) {
        this.store = Objects.requireNonNull(store, "store");
        this.expires = Objects.requireNonNull(expires, "expires");
        this.offHeapAllocator = offHeapAllocator;
        this.memoryRuntime = memoryRuntime;
        this.entryTable = entryTable;
        this.keyDirectory = keyDirectory;
        this.stringRoot = stringRoot;
        this.listRoot = listRoot;
        this.hashRoot = hashRoot;
        this.setRoot = setRoot;
        this.zsetRoot = zsetRoot;
        this.touchCallback = Objects.requireNonNull(touchCallback, "touchCallback");
        this.adjustUsedBytesCallback = Objects.requireNonNull(adjustUsedBytesCallback, "adjustUsedBytesCallback");
    }

    public OffHeapAllocator offHeapAllocator() {
        return offHeapAllocator;
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
        return store.keyHandle(keyBytes);
    }

    public KeyHandle keyHandle(BytesView keyView) {
        return store.keyHandle(keyView);
    }

    public EntryHandle entryHandle(byte[] keyBytes) {
        if (keyDirectory == null) {
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
        if (entryTable == null || handle == null) {
            return null;
        }
        return entryTable.get(handle);
    }

    public EntryRecord liveEntryRecord(byte[] keyBytes) {
        KeyHandle keyHandle = keyHandle(keyBytes);
        if (keyHandle == null) {
            return null;
        }
        YierdisObject object = getStoredObject(keyHandle);
        if (object == null) {
            unlinkEntry(keyBytes);
            return null;
        }
        if (removeIfExpired(keyHandle, object, System.currentTimeMillis())) {
            return null;
        }
        EntryRecord record = entryRecord(keyBytes);
        if (record == null) {
            return null;
        }
        return record;
    }

    public EntryRecord liveEntryRecord(BytesView keyView) {
        if (keyView == null) {
            return null;
        }
        return liveEntryRecord(YierdisDb.toByteArray(keyView));
    }

    public EntryRecord unlinkEntry(byte[] keyBytes) {
        if (entryTable == null || keyDirectory == null) {
            return null;
        }
        EntryHandle handle = keyDirectory.remove(keyBytes);
        if (handle == null) {
            return null;
        }
        EntryRecord record = entryTable.get(handle);
        if (record != null) {
            entryTable.release(handle);
        }
        return record;
    }

    public EntryRecord unlinkEntry(EntryHandle handle) {
        if (entryTable == null || keyDirectory == null || handle == null) {
            return null;
        }
        if (!keyDirectory.remove(handle)) {
            return null;
        }
        EntryRecord record = entryTable.get(handle);
        if (record != null) {
            entryTable.release(handle);
        }
        return record;
    }

    public YierdisObject getStoredObject(KeyHandle keyHandle) {
        return store.get(keyHandle);
    }

    public int keyCount() {
        return store.size();
    }

    public int expireCount() {
        return expires.size();
    }

    public KeyHandle randomKeyHandle() {
        return store.randomKeyHandle();
    }

    public KeyHandle randomExpireKeyHandle() {
        return expires.randomKeyHandle();
    }

    public void forEachKeyHandle(java.util.function.BiConsumer<KeyHandle, YierdisObject> consumer) {
        Objects.requireNonNull(consumer, "consumer");
        store.forEachKeyHandle(consumer);
    }

    public Long expireAtMillis(byte[] keyBytes) {
        KeyHandle handle = keyHandle(keyBytes);
        return handle == null ? null : expires.get(handle);
    }

    public YierdisObject getLiveObject(byte[] keyBytes) {
        KeyHandle handle = keyHandle(keyBytes);
        if (handle == null) {
            return null;
        }
        return getLiveObject(handle);
    }

    public YierdisObject getLiveObject(BytesView keyView) {
        KeyHandle handle = keyHandle(keyView);
        if (handle == null) {
            return null;
        }
        return getLiveObject(handle);
    }

    public YierdisObject getLiveObject(KeyHandle keyHandle) {
        YierdisObject object = getStoredObject(keyHandle);
        if (object == null) {
            return null;
        }
        if (removeIfExpired(keyHandle, object, System.currentTimeMillis())) {
            return null;
        }
        touch(object);
        return object;
    }

    public EntryRecord computeWithHandle(
            byte[] keyBytes,
            BiFunction<? super KeyHandle, ? super EntryRecord, ? extends EntryRecord> remappingFunction
    ) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        Objects.requireNonNull(remappingFunction, "remappingFunction");
        ensureNativeEntryGraph();

        KeyHandle keyHandle = keyHandleForEntryRemapping(keyBytes);
        EntryHandle existingHandle = keyDirectory.get(keyBytes);
        EntryRecord oldRecord = existingHandle == null ? null : entryTable.get(existingHandle);
        EntryRecord newRecord = remappingFunction.apply(keyHandle, oldRecord);
        if (newRecord == null) {
            if (existingHandle != null) {
                keyDirectory.remove(keyBytes, existingHandle);
                entryTable.release(existingHandle);
            }
            return null;
        }
        if (existingHandle != null) {
            entryTable.replace(existingHandle, newRecord);
            return newRecord;
        }

        EntryHandle created = entryTable.allocate(newRecord);
        boolean inserted = false;
        try {
            keyDirectory.compute(keyBytes, (key, oldHandle) -> {
                if (oldHandle != null) {
                    throw new IllegalStateException("native entry appeared during remapping");
                }
                return created;
            });
            inserted = true;
            return newRecord;
        } finally {
            if (!inserted) {
                entryTable.release(created);
            }
        }
    }

    public EntryRecord computeIfPresentWithHandle(
            byte[] keyBytes,
            BiFunction<? super KeyHandle, ? super EntryRecord, ? extends EntryRecord> remappingFunction
    ) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        Objects.requireNonNull(remappingFunction, "remappingFunction");
        ensureNativeEntryGraph();

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
            return null;
        }
        entryTable.replace(existingHandle, newRecord);
        return newRecord;
    }

    public YierdisObject computeObjectWithHandle(
            byte[] keyBytes,
            BiFunction<? super KeyHandle, ? super YierdisObject, ? extends YierdisObject> remappingFunction
    ) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        Objects.requireNonNull(remappingFunction, "remappingFunction");
        final KeyHandle[] keyHandleRef = new KeyHandle[1];
        YierdisObject result = store.computeWithHandle(keyBytes, (keyHandle, oldObject) -> {
            keyHandleRef[0] = keyHandle;
            YierdisObject newObject = remappingFunction.apply(keyHandle, oldObject);
            return newObject;
        });
        if (keyHandleRef[0] != null) {
            syncEntry(keyBytes, keyHandleRef[0], result);
        }
        return result;
    }

    public YierdisObject computeObjectIfPresentWithHandle(
            byte[] keyBytes,
            BiFunction<? super KeyHandle, ? super YierdisObject, ? extends YierdisObject> remappingFunction
    ) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        Objects.requireNonNull(remappingFunction, "remappingFunction");
        final KeyHandle[] keyHandleRef = new KeyHandle[1];
        YierdisObject result = store.computeIfPresentWithHandle(keyBytes, (keyHandle, oldObject) -> {
            keyHandleRef[0] = keyHandle;
            YierdisObject newObject = remappingFunction.apply(keyHandle, oldObject);
            return newObject;
        });
        if (keyHandleRef[0] != null) {
            syncEntry(keyBytes, keyHandleRef[0], result);
        }
        return result;
    }

    public boolean remove(byte[] keyBytes, YierdisObject expectedValue) {
        boolean removed = store.remove(keyBytes, expectedValue);
        if (removed) {
            unlinkEntry(keyBytes);
        }
        return removed;
    }

    public boolean remove(KeyHandle keyHandle, YierdisObject expectedValue) {
        byte[] keyBytes = keyBytes(keyHandle);
        boolean removed = store.remove(keyHandle, expectedValue);
        if (removed) {
            unlinkEntry(keyBytes);
        }
        return removed;
    }

    public ScanCursorV2 scan(ScanCursorV2 cursor, int maxSteps, YierdisKeyspace.ScanConsumer<YierdisObject> consumer) {
        return store.scan(cursor, maxSteps, consumer);
    }

    public Long expireAtMillis(KeyHandle keyHandle) {
        return expires.get(keyHandle);
    }

    public boolean removeObject(KeyHandle keyHandle, YierdisObject object) {
        return remove(keyHandle, object);
    }

    public boolean removeIfExpired(byte[] keyBytes, YierdisObject object, long nowMillis) {
        KeyHandle handle = keyHandle(keyBytes);
        if (handle == null) {
            return false;
        }
        return removeIfExpired(handle, object, nowMillis);
    }

    public boolean removeIfExpired(KeyHandle keyHandle, YierdisObject object, long nowMillis) {
        Long expireAtMillis = expireAtMillis(keyHandle);
        if (expireAtMillis == null || expireAtMillis > nowMillis) {
            return false;
        }
        removeExpire(keyHandle);
        if (remove(keyHandle, object)) {
            object.releasePayloadIfAny();
            adjustUsedBytesCallback.accept(-object.estimatedBytes);
            return true;
        }
        return false;
    }

    public boolean isKeyExpired(KeyHandle keyHandle, long nowMillis) {
        Long expireAtMillis = expireAtMillis(keyHandle);
        return expireAtMillis != null && expireAtMillis <= nowMillis;
    }

    public void setExpireAtMillis(byte[] keyBytes, long expireAtMillis) {
        KeyHandle handle = keyHandle(keyBytes);
        if (handle != null) {
            setExpireAtMillis(handle, expireAtMillis);
            return;
        }
        expires.setExpireAtMillis(keyBytes, expireAtMillis, store);
    }

    public void setExpireAtMillis(KeyHandle keyHandle, long expireAtMillis) {
        expires.setExpireAtMillis(keyHandle, expireAtMillis);
        replaceEntryExpire(keyHandle, expireAtMillis);
    }

    public void removeExpire(byte[] keyBytes) {
        KeyHandle handle = keyHandle(keyBytes);
        if (handle != null) {
            removeExpire(handle);
            return;
        }
        expires.removeExpire(keyBytes);
    }

    public void removeExpire(KeyHandle keyHandle) {
        expires.removeExpire(keyHandle);
        replaceEntryExpire(keyHandle, -1L);
    }

    public void touch(YierdisObject object) {
        touchCallback.accept(object);
    }

    private void syncEntry(byte[] keyBytes, KeyHandle keyHandle, YierdisObject newObject) {
        if (entryTable == null || keyDirectory == null || keyBytes == null || keyHandle == null) {
            return;
        }
        EntryHandle existingHandle = keyDirectory.get(keyBytes);
        if (newObject == null) {
            if (existingHandle != null) {
                keyDirectory.remove(keyBytes, existingHandle);
                entryTable.release(existingHandle);
            }
            return;
        }

        EntryRecord record = toEntryRecord(keyHandle, newObject, expireAtMillisOrAbsent(keyHandle));
        if (existingHandle == null) {
            EntryHandle created = entryTable.allocate(record);
            boolean inserted = false;
            try {
                keyDirectory.compute(keyBytes, (key, oldHandle) -> {
                    if (oldHandle != null) {
                        throw new IllegalStateException("native entry appeared during sync");
                    }
                    return created;
                });
                inserted = true;
                return;
            } finally {
                if (!inserted) {
                    entryTable.release(created);
                }
            }
        }

        entryTable.replace(existingHandle, record);
    }

    private void ensureNativeEntryGraph() {
        if (entryTable == null || keyDirectory == null) {
            throw new IllegalStateException("native entry graph is not available");
        }
    }

    private void replaceEntryExpire(KeyHandle keyHandle, long expireAtMillis) {
        if (entryTable == null || keyDirectory == null || keyHandle == null) {
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

    private EntryRecord toEntryRecord(KeyHandle keyHandle, YierdisObject object, long expireAtMillis) {
        return new EntryRecord(
                keyHandleIdentity(keyHandle),
                valueHandle(object),
                keyHandle.dictHash(),
                object == null ? ValueType.STRING : object.type,
                object == null ? ValueEncoding.STRING_EMBSTR : object.encoding,
                0,
                expireAtMillis,
                object == null ? 0L : object.estimatedBytes,
                object == null ? 0L : object.lruClock
        );
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
        var ref = KeyHandleAccess.ffmBytesRefOrNull(keyHandle);
        if (ref != null) {
            return System.identityHashCode(ref.region());
        }
        return System.identityHashCode(keyHandle);
    }

    private static ValueHandle valueHandle(YierdisObject object) {
        if (object == null) {
            return new ValueHandle(0L);
        }
        if (object.valueHandle() != null) {
            return object.valueHandle();
        }
        return new ValueHandle(System.identityHashCode(object));
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
