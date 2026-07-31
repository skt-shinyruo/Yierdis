package yier.bubu.redis.storage.memory;

import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.StableMemoryBackend;
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
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.key.KeyHandleAccess;
import yier.bubu.redis.storage.memory.internal.keyspace.NativeKeyDirectory;
import yier.bubu.redis.storage.memory.internal.keyspace.YierdisKeyspace;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.LongSupplier;

public final class YierdisDbKeyLifecycle {
    private static final int EXPIRED_AWAITING_PHYSICAL_DELETION_FLAG = 1;

    public record EntryMutationResult<R>(EntryRecord record, R result, boolean releaseReplacedValue) {
        public EntryMutationResult(EntryRecord record, R result) {
            this(record, result, true);
        }

        public static <R> EntryMutationResult<R> of(EntryRecord record, R result) {
            return new EntryMutationResult<>(record, result, true);
        }

        public static <R> EntryMutationResult<R> of(EntryRecord record, R result, boolean releaseReplacedValue) {
            return new EntryMutationResult<>(record, result, releaseReplacedValue);
        }
    }

    private final StableMemoryBackend stableMemoryBackend;
    private final EntryTable entryTable;
    private final NativeKeyDirectory keyDirectory;
    private final StringRoot stringRoot;
    private final ListRoot listRoot;
    private final HashRoot hashRoot;
    private final SetRoot setRoot;
    private final ZSetRoot zsetRoot;
    private final LongSupplier lruClockSupplier;
    private int expireCount;
    private long expiredEntriesAwaitingPhysicalDeletion;

    YierdisDbKeyLifecycle(
            StableMemoryBackend stableMemoryBackend,
            EntryTable entryTable,
            NativeKeyDirectory keyDirectory,
            StringRoot stringRoot,
            ListRoot listRoot,
            HashRoot hashRoot,
            SetRoot setRoot,
            ZSetRoot zsetRoot,
            LongSupplier lruClockSupplier
    ) {
        this.stableMemoryBackend = java.util.Objects.requireNonNull(stableMemoryBackend, "stableMemoryBackend");
        this.entryTable = Objects.requireNonNull(entryTable, "entryTable");
        this.keyDirectory = Objects.requireNonNull(keyDirectory, "keyDirectory");
        this.stringRoot = Objects.requireNonNull(stringRoot, "stringRoot");
        this.listRoot = Objects.requireNonNull(listRoot, "listRoot");
        this.hashRoot = Objects.requireNonNull(hashRoot, "hashRoot");
        this.setRoot = Objects.requireNonNull(setRoot, "setRoot");
        this.zsetRoot = Objects.requireNonNull(zsetRoot, "zsetRoot");
        this.lruClockSupplier = Objects.requireNonNull(lruClockSupplier, "lruClockSupplier");
    }

    public StableMemoryBackend stableMemoryBackend() {
        return stableMemoryBackend;
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
            // keyDirectory 仍指向 entryTable 中已消失的 handle 时，顺手清理悬挂映射，避免读路径反复解析同一坏引用。
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
        if (isExpired(record, now)) {
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
            releaseEntry(handle, record);
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
            releaseEntry(handle, record);
        }
        return record;
    }

    public int keyCount() {
        return keyDirectory.size();
    }

    public int expireCount() {
        return expireCount;
    }

    public KeyHandle randomKeyHandle() {
        return keyDirectory.randomKeyHandle();
    }

    public void forEachKeyHandle(BiConsumer<KeyHandle, EntryRecord> consumer) {
        Objects.requireNonNull(consumer, "consumer");
        keyDirectory.forEachEntry((keyHandle, entryHandle) -> consumer.accept(keyHandle, entryRecord(entryHandle)));
    }

    public Long expireAtMillis(byte[] keyBytes) {
        return expireAtMillis(entryRecord(keyBytes));
    }

    public Long expireAtMillis(KeyHandle keyHandle) {
        return expireAtMillis(entryRecord(keyHandle));
    }

    public <R> R computeWithHandleResult(
            byte[] keyBytes,
            BiFunction<? super KeyHandle, ? super EntryRecord, EntryMutationResult<R>> remappingFunction
    ) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        Objects.requireNonNull(remappingFunction, "remappingFunction");

        EntryHandle existingHandle = keyDirectory.get(keyBytes);
        EntryRecord oldRecord = existingHandle == null ? null : entryTable.get(existingHandle);
        if (existingHandle == null) {
            return computeNewWithNativeKeyHandle(keyBytes, remappingFunction);
        }

        KeyHandle keyHandle = keyHandle(keyBytes);
        EntryMutationResult<R> mutation = Objects.requireNonNull(
                remappingFunction.apply(keyHandle, oldRecord),
                "entry mutation result"
        );
        EntryRecord newRecord = mutation.record();
        if (newRecord == null) {
            keyDirectory.remove(keyBytes, existingHandle);
            releaseEntry(existingHandle, oldRecord);
            releaseValue(oldRecord);
            return mutation.result();
        }
        replaceEntry(existingHandle, oldRecord, newRecord);
        if (mutation.releaseReplacedValue()) {
            releaseReplacedValue(oldRecord, newRecord);
        }
        return mutation.result();
    }

    public <R> R computeIfPresentWithHandleResult(
            byte[] keyBytes,
            BiFunction<? super KeyHandle, ? super EntryRecord, EntryMutationResult<R>> remappingFunction
    ) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        Objects.requireNonNull(remappingFunction, "remappingFunction");

        EntryHandle existingHandle = keyDirectory.get(keyBytes);
        if (existingHandle == null) {
            return null;
        }
        EntryRecord oldRecord = entryTable.get(existingHandle);
        if (oldRecord == null) {
            // directory 有 handle 但 entryTable 已无记录时，按悬挂映射处理，避免把它误当成可更新的旧值。
            keyDirectory.remove(keyBytes, existingHandle);
            return null;
        }

        EntryMutationResult<R> mutation = Objects.requireNonNull(
                remappingFunction.apply(keyHandle(keyBytes), oldRecord),
                "entry mutation result"
        );
        EntryRecord newRecord = mutation.record();
        if (newRecord == null) {
            keyDirectory.remove(keyBytes, existingHandle);
            releaseEntry(existingHandle, oldRecord);
            releaseValue(oldRecord);
            return mutation.result();
        }
        replaceEntry(existingHandle, oldRecord, newRecord);
        if (mutation.releaseReplacedValue()) {
            releaseReplacedValue(oldRecord, newRecord);
        }
        return mutation.result();
    }

    public long estimatedBytesForRemoval(KeyHandle keyHandle, EntryRecord record) {
        if (keyHandle == null || record == null) {
            return 0L;
        }
        return YierdisDbMemoryEstimator.entryMetadataBytes(record);
    }

    public ScanCursorV2 scan(ScanCursorV2 cursor, int maxSteps, YierdisKeyspace.ScanConsumer<EntryRecord> consumer) {
        return keyDirectory.scan(cursor, maxSteps, (keyHandle, entryHandle) -> consumer.accept(keyHandle, entryRecord(entryHandle)));
    }

    public NativeKeyDirectory.ScanResult scanWithWork(
            ScanCursorV2 cursor,
            long maxSteps,
            YierdisKeyspace.ScanConsumer<EntryRecord> consumer
    ) {
        return keyDirectory.scanWithWork(cursor, maxSteps, (keyHandle, entryHandle) ->
                consumer.accept(keyHandle, entryRecord(entryHandle)));
    }

    public boolean isCurrentExpiredCandidate(
            byte[] keyBytes,
            KeyHandle expectedKeyHandle,
            EntryRecord expectedRecord,
            long nowMillis
    ) {
        NativeHandle expectedIdentity = expectedKeyHandle == null
                ? null
                : KeyHandleAccess.allocatorNativeHandleOrNull(expectedKeyHandle);
        EntryRecord current = currentRecordForIdentity(keyBytes, expectedKeyHandle);
        return current != null
                && expectedRecord != null
                && expectedIdentity != null
                && expectedIdentity.equals(expectedRecord.keyHandle())
                && current.version() == expectedRecord.version()
                && current.expireAtMillis() == expectedRecord.expireAtMillis()
                && current.equals(expectedRecord)
                && isExpired(current, nowMillis);
    }

    public boolean hasCurrentExpiredEntry(
            byte[] keyBytes,
            KeyHandle expectedKeyHandle,
            long nowMillis
    ) {
        return isExpired(currentRecordForIdentity(keyBytes, expectedKeyHandle), nowMillis);
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
        releaseEntry(handle, current);
        releaseValue(current);
        return true;
    }

    public void replaceEntry(EntryHandle handle, EntryRecord oldRecord, EntryRecord newRecord) {
        Objects.requireNonNull(handle, "handle");
        Objects.requireNonNull(newRecord, "newRecord");
        entryTable.replace(handle, newRecord);
        reconcileDerivedEntryState(oldRecord, newRecord);
    }

    public void publishStagedEntry(
            EntryHandle handle,
            NativeKeyDirectory.StagedInsert stagedKey,
            EntryRecord newRecord
    ) {
        Objects.requireNonNull(handle, "handle");
        Objects.requireNonNull(stagedKey, "stagedKey");
        Objects.requireNonNull(newRecord, "newRecord");
        entryTable.writeReserved(handle, newRecord);
        keyDirectory.publishStagedInsert(stagedKey, handle);
        reconcileDerivedEntryState(null, newRecord);
    }

    public void releaseEntry(EntryHandle handle, EntryRecord record) {
        Objects.requireNonNull(handle, "handle");
        entryTable.release(handle);
        reconcileDerivedEntryState(record, null);
    }

    public boolean markExpiredEntryAwaitingPhysicalDeletion(
            KeyHandle keyHandle,
            EntryHandle entryHandle,
            EntryRecord expectedRecord,
            long nowMillis
    ) {
        if (keyHandle == null || entryHandle == null || expectedRecord == null
                || !isExpired(expectedRecord, nowMillis)) {
            return false;
        }
        EntryRecord current = entryTable.get(entryHandle);
        if (current == null || !current.equals(expectedRecord)
                || isExpiredEntryAwaitingPhysicalDeletion(current)) {
            return false;
        }
        replaceEntry(entryHandle, current, withFlags(
                current,
                current.flags() | EXPIRED_AWAITING_PHYSICAL_DELETION_FLAG
        ));
        return true;
    }

    public long expiredEntriesAwaitingPhysicalDeletion() {
        return expiredEntriesAwaitingPhysicalDeletion;
    }

    public void resetEntryStateCounters() {
        expireCount = 0;
        expiredEntriesAwaitingPhysicalDeletion = 0L;
    }

    public byte[] copyKeyBytes(KeyHandle keyHandle) {
        if (keyHandle == null) {
            return null;
        }
        return keyBytes(keyHandle);
    }

    public boolean isKeyExpired(KeyHandle keyHandle, long nowMillis) {
        return keyHandle != null && isExpired(entryRecord(keyHandle), nowMillis);
    }

    public boolean isKeyExpiredForScan(KeyHandle keyHandle, long nowMillis) {
        if (keyHandle == null) {
            return false;
        }
        return isExpired(entryRecord(keyHandle), nowMillis);
    }

    public EntryRecord newRecord(
            KeyHandle keyHandle,
            ValueHandle valueHandle,
            ValueType type,
            ValueEncoding encoding,
            long expireAtMillis,
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
                nextVersion(previous),
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
                replaceEntry(handle, current, touched);
            }
        }
        return touched;
    }

    public EntryRecord withExpireAtMillis(KeyHandle keyHandle, EntryRecord record, long expireAtMillis) {
        if (keyHandle == null || record == null) {
            return record;
        }
        return new EntryRecord(
                record.keyHandle(),
                record.valueHandle(),
                record.keyHash(),
                record.type(),
                record.encoding(),
                clearExpiredEntryAwaitingPhysicalDeletionFlag(record.flags()),
                expireAtMillis,
                nextVersion(record),
                accessClock(record.lruOrLfu())
        );
    }

    public void releaseValue(EntryRecord record) {
        if (record == null || record.valueHandle() == null || record.valueHandle().isNull()) {
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

    private long accessClock(long previous) {
        long next = lruClockSupplier.getAsLong();
        return next <= 0L ? previous : next;
    }

    private static long nextVersion(EntryRecord previous) {
        if (previous == null) {
            return 1L;
        }
        if (previous.version() == Long.MAX_VALUE) {
            throw new IllegalStateException("entry version is exhausted");
        }
        return previous.version() + 1L;
    }

    private <R> R computeNewWithNativeKeyHandle(
            byte[] keyBytes,
            BiFunction<? super KeyHandle, ? super EntryRecord, EntryMutationResult<R>> remappingFunction
    ) {
        EntryHandle created = null;
        NativeKeyDirectory.StagedInsert stagedKey = null;
        boolean published = false;
        boolean initialized = false;
        EntryRecord newRecord = null;
        try {
            created = entryTable.reserve();
            stagedKey = keyDirectory.stageInsert(keyBytes);
            KeyHandle keyHandle = stagedKey.keyHandle();
            EntryMutationResult<R> mutation = Objects.requireNonNull(
                    remappingFunction.apply(keyHandle, null),
                    "entry mutation result"
            );
            newRecord = mutation.record();
            if (newRecord == null) {
                return mutation.result();
            }

            entryTable.writeReserved(created, newRecord);
            keyDirectory.publishStagedInsert(stagedKey, created);
            published = true;
            reconcileDerivedEntryState(null, newRecord);
            initialized = true;
            return mutation.result();
        } finally {
            if (!initialized) {
                if (published) {
                    keyDirectory.remove(keyBytes, created);
                } else if (stagedKey != null) {
                    stagedKey.close();
                }
                if (created != null) {
                    entryTable.release(created);
                }
                if (newRecord != null) {
                    releaseValue(newRecord);
                }
            }
        }
    }

    private static NativeHandle keyHandleIdentity(KeyHandle keyHandle) {
        if (keyHandle == null) {
            return NativeHandle.NULL;
        }
        return KeyHandleAccess.allocatorNativeHandle(keyHandle);
    }

    private EntryRecord currentRecordForIdentity(byte[] keyBytes, KeyHandle expectedKeyHandle) {
        if (keyBytes == null || expectedKeyHandle == null) {
            return null;
        }
        NativeHandle expectedIdentity = KeyHandleAccess.allocatorNativeHandleOrNull(expectedKeyHandle);
        if (expectedIdentity == null) {
            return null;
        }
        KeyHandle currentKeyHandle = keyDirectory.getKeyHandle(keyBytes);
        if (currentKeyHandle == null
                || !expectedIdentity.equals(KeyHandleAccess.allocatorNativeHandleOrNull(currentKeyHandle))) {
            return null;
        }
        EntryHandle currentEntryHandle = keyDirectory.get(keyBytes);
        EntryRecord current = currentEntryHandle == null ? null : entryTable.get(currentEntryHandle);
        return current != null && expectedIdentity.equals(current.keyHandle()) ? current : null;
    }

    private static byte[] keyBytes(KeyHandle keyHandle) {
        Objects.requireNonNull(keyHandle, "keyHandle");
        int len = keyHandle.length();
        byte[] out = new byte[len];
        for (int i = 0; i < len; i++) {
            out[i] = keyHandle.getByte(i);
        }
        return out;
    }

    private void reconcileDerivedEntryState(EntryRecord oldRecord, EntryRecord newRecord) {
        boolean oldHasTtl = hasTtl(oldRecord);
        boolean newHasTtl = hasTtl(newRecord);
        if (oldHasTtl != newHasTtl) {
            int nextExpireCount = expireCount + (newHasTtl ? 1 : -1);
            if (nextExpireCount < 0) {
                throw new IllegalStateException("derived expire count underflow");
            }
            expireCount = nextExpireCount;
        }

        boolean oldAwaitingDeletion = isExpiredEntryAwaitingPhysicalDeletion(oldRecord);
        boolean newAwaitingDeletion = isExpiredEntryAwaitingPhysicalDeletion(newRecord);
        if (oldAwaitingDeletion == newAwaitingDeletion) {
            return;
        }
        if (newAwaitingDeletion) {
            if (expiredEntriesAwaitingPhysicalDeletion < Long.MAX_VALUE) {
                expiredEntriesAwaitingPhysicalDeletion++;
            }
            return;
        }
        if (expiredEntriesAwaitingPhysicalDeletion > 0L) {
            expiredEntriesAwaitingPhysicalDeletion--;
        }
    }

    private static boolean isExpiredEntryAwaitingPhysicalDeletion(EntryRecord record) {
        return record != null && (record.flags() & EXPIRED_AWAITING_PHYSICAL_DELETION_FLAG) != 0;
    }

    private static Long expireAtMillis(EntryRecord record) {
        return hasTtl(record) ? record.expireAtMillis() : null;
    }

    private static boolean hasTtl(EntryRecord record) {
        return record != null && record.expireAtMillis() >= 0L;
    }

    private static boolean isExpired(EntryRecord record, long nowMillis) {
        return hasTtl(record) && record.expireAtMillis() <= nowMillis;
    }

    private static int clearExpiredEntryAwaitingPhysicalDeletionFlag(int flags) {
        return flags & ~EXPIRED_AWAITING_PHYSICAL_DELETION_FLAG;
    }

    private static EntryRecord withFlags(EntryRecord record, int flags) {
        return new EntryRecord(
                record.keyHandle(),
                record.valueHandle(),
                record.keyHash(),
                record.type(),
                record.encoding(),
                flags,
                record.expireAtMillis(),
                nextVersion(record),
                record.lruOrLfu()
        );
    }
}
