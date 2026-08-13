package yier.bubu.redis.storage.memory;

import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.common.memory.MemoryReclaimResult;
import yier.bubu.redis.memory.api.NativeDefragOptions;
import yier.bubu.redis.memory.api.NativeDefragReport;
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
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;

public final class YierdisDbKeyLifecycle implements AutoCloseable {
    private static final int EXPIRED_AWAITING_PHYSICAL_DELETION_FLAG = 1;

    public record CurrentEntry(
            EntryHandle entryHandle,
            KeyHandle keyHandle,
            EntryRecord record
    ) {
    }

    @FunctionalInterface
    public interface EntryScanConsumer {
        boolean accept(KeyHandle keyHandle, EntryRecord record);
    }

    public record KeyScanResult(
            ScanCursorV2 startCursor,
            ScanCursorV2 nextCursor,
            long inspectedSlots,
            long tableGeneration
    ) {
    }

    public record DirectoryState(long tableGeneration, int activeCapacity, int oldCapacity) {
    }

    public final class StagedEntry implements AutoCloseable {
        private EntryHandle entryHandle;
        private NativeKeyDirectory.StagedInsert stagedKey;

        private StagedEntry(EntryHandle entryHandle, NativeKeyDirectory.StagedInsert stagedKey) {
            this.entryHandle = Objects.requireNonNull(entryHandle, "entryHandle");
            this.stagedKey = Objects.requireNonNull(stagedKey, "stagedKey");
        }

        public KeyHandle keyHandle() {
            return requireActiveStagedKey().keyHandle();
        }

        public long stagedHeapBytes() {
            return requireActiveStagedKey().stagedHeapBytes();
        }

        @Override
        public void close() {
            EntryHandle reservedEntry = entryHandle;
            NativeKeyDirectory.StagedInsert reservedKey = stagedKey;
            entryHandle = null;
            stagedKey = null;
            if (reservedEntry == null && reservedKey == null) {
                return;
            }

            Throwable failure = null;
            if (reservedKey != null) {
                try {
                    reservedKey.close();
                } catch (Throwable next) {
                    failure = recordFailure(failure, next);
                }
            }
            if (reservedEntry != null) {
                try {
                    entryTable.release(reservedEntry);
                } catch (Throwable next) {
                    failure = recordFailure(failure, next);
                }
            }
            throwIfFailure(failure);
        }

        private NativeKeyDirectory.StagedInsert requireActiveStagedKey() {
            if (stagedKey == null) {
                throw new IllegalStateException("staged entry is no longer active");
            }
            return stagedKey;
        }

        private EntryHandle requireActiveEntryHandle() {
            if (entryHandle == null) {
                throw new IllegalStateException("staged entry is no longer active");
            }
            return entryHandle;
        }

        private NativeKeyDirectory.StagedInsert consumeStagedKey() {
            return requireActiveStagedKey();
        }

        private void markPublished() {
            entryHandle = null;
            stagedKey = null;
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
    private boolean closeAttempted;

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

    void bindToCurrentThread() {
        stableMemoryBackend.bindToCurrentThread();
    }

    MemoryReclaimResult trimEmptyNativePages(MemoryPressureBudget budget) {
        return stableMemoryBackend.trimEmptyPages(Objects.requireNonNull(budget, "budget"));
    }

    NativeDefragReport defragCycle(NativeDefragOptions options) {
        return stableMemoryBackend.defragCycle(Objects.requireNonNull(options, "options"));
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

    public CurrentEntry currentEntry(byte[] keyBytes) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        EntryHandle handle = keyDirectory.get(keyBytes);
        EntryRecord record = handle == null ? null : entryTable.get(handle);
        KeyHandle keyHandle = handle == null ? null : keyDirectory.getKeyHandle(keyBytes);
        return new CurrentEntry(handle, keyHandle, record);
    }

    public StagedEntry stageEntry(byte[] keyBytes) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        EntryHandle handle = entryTable.reserve();
        try {
            return new StagedEntry(handle, keyDirectory.stageInsert(keyBytes));
        } catch (RuntimeException | Error failure) {
            try {
                entryTable.release(handle);
            } catch (RuntimeException | Error releaseFailure) {
                failure.addSuppressed(releaseFailure);
            }
            throw failure;
        }
    }

    public void abortStagedEntry(StagedEntry stagedEntry, Throwable failure) {
        Objects.requireNonNull(failure, "failure");
        if (stagedEntry == null) {
            return;
        }
        try {
            stagedEntry.close();
        } catch (RuntimeException | Error cleanupFailure) {
            failure.addSuppressed(cleanupFailure);
        }
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

    public long estimatedBytesForRemoval(KeyHandle keyHandle, EntryRecord record) {
        if (keyHandle == null || record == null) {
            return 0L;
        }
        return YierdisDbMemoryEstimator.entryMetadataBytes(record);
    }

    public ScanCursorV2 scan(ScanCursorV2 cursor, int maxSteps, EntryScanConsumer consumer) {
        Objects.requireNonNull(consumer, "consumer");
        return keyDirectory.scan(cursor, maxSteps, (keyHandle, entryHandle) -> consumer.accept(keyHandle, entryRecord(entryHandle)));
    }

    public KeyScanResult scanWithWork(
            ScanCursorV2 cursor,
            long maxSteps,
            EntryScanConsumer consumer
    ) {
        Objects.requireNonNull(consumer, "consumer");
        NativeKeyDirectory.ScanResult result = keyDirectory.scanWithWork(cursor, maxSteps, (keyHandle, entryHandle) ->
                consumer.accept(keyHandle, entryRecord(entryHandle)));
        return new KeyScanResult(
                result.startCursor(),
                result.nextCursor(),
                result.inspectedSlots(),
                result.tableGeneration()
        );
    }

    public DirectoryState directoryState() {
        var metrics = keyDirectory.metrics();
        return new DirectoryState(metrics.generation(), metrics.capacity(), metrics.oldCapacity());
    }

    public boolean directoryStateIsCurrent(long generation, int activeCapacity, int oldCapacity) {
        DirectoryState state = directoryState();
        return state.tableGeneration() == generation
                && state.activeCapacity() == activeCapacity
                && state.oldCapacity() == oldCapacity;
    }

    public long estimatedInsertHeapGrowthBytes() {
        return keyDirectory.estimatedInsertHeapGrowthBytes();
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

    public void publishStagedEntry(StagedEntry stagedEntry, EntryRecord newRecord) {
        Objects.requireNonNull(stagedEntry, "stagedEntry");
        Objects.requireNonNull(newRecord, "newRecord");
        EntryHandle handle = stagedEntry.requireActiveEntryHandle();
        NativeKeyDirectory.StagedInsert stagedKey = stagedEntry.consumeStagedKey();
        entryTable.writeReserved(handle, newRecord);
        keyDirectory.publishStagedInsert(stagedKey, handle);
        stagedEntry.markPublished();
        reconcileDerivedEntryState(null, newRecord);
    }

    public void deleteEntry(EntryHandle handle, EntryRecord record) {
        Objects.requireNonNull(handle, "handle");
        keyDirectory.remove(handle);
        releaseEntry(handle, record);
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

    public long estimatedValueBytes(EntryRecord record) {
        if (record == null || record.valueHandle() == null || record.valueHandle().isNull()) {
            return 0L;
        }
        return switch (record.type()) {
            case STRING -> stringRoot.estimatedBytes(record.valueHandle());
            case LIST -> listRoot.estimatedBytes(record.valueHandle());
            case HASH -> hashRoot.estimatedBytes(record.valueHandle());
            case SET -> setRoot.estimatedBytes(record.valueHandle());
            case ZSET -> zsetRoot.estimatedBytes(record.valueHandle());
        };
    }

    public byte[] copyStringValue(EntryRecord record) {
        if (record == null || record.type() != ValueType.STRING || record.valueHandle() == null) {
            return null;
        }
        return stringRoot.copy(record.valueHandle());
    }

    long componentRetainedHeapBytes() {
        return yier.bubu.redis.common.memory.MemoryUsageSnapshot.addSaturating(
                keyDirectory.heapBytes(),
                yier.bubu.redis.common.memory.MemoryUsageSnapshot.addSaturating(
                        listRoot.heapBytes(),
                        yier.bubu.redis.common.memory.MemoryUsageSnapshot.addSaturating(
                                hashRoot.heapBytes(),
                                yier.bubu.redis.common.memory.MemoryUsageSnapshot.addSaturating(
                                        setRoot.heapBytes(),
                                        zsetRoot.heapBytes()
                                )
                        )
                )
        );
    }

    YierdisStringOps createStringOps(YierdisDbRuntimeInternals internals) {
        return new YierdisStringOps(internals, stringRoot);
    }

    YierdisListOps createListOps(YierdisDbRuntimeInternals internals) {
        return new YierdisListOps(internals, listRoot);
    }

    YierdisHashOps createHashOps(YierdisDbRuntimeInternals internals) {
        return new YierdisHashOps(internals, hashRoot);
    }

    YierdisSetOps createSetOps(YierdisDbRuntimeInternals internals) {
        return new YierdisSetOps(internals, setRoot);
    }

    YierdisZSetOps createZSetOps(YierdisDbRuntimeInternals internals) {
        return new YierdisZSetOps(internals, zsetRoot);
    }

    YierdisHllOps createHllOps(YierdisDbRuntimeInternals internals) {
        return new YierdisHllOps(internals, stringRoot);
    }

    void clearData() {
        throwIfFailure(clearData(
                entryTable,
                keyDirectory,
                stringRoot,
                listRoot,
                hashRoot,
                setRoot,
                zsetRoot
        ));
    }

    void detachEntries() {
        keyDirectory.detachEntries();
    }

    long detachedEntryCount() {
        return keyDirectory.detachedEntryCount();
    }

    void reclaimDetachedEntry() {
        keyDirectory.reclaimDetachedEntry((ignoredKey, entryHandle) -> releaseOwnedEntry(entryHandle));
    }

    void armMemoryUsageIterationTrapsForTesting() {
        keyDirectory.armIterationTrapForTesting();
        listRoot.armIterationTrapForTesting();
        hashRoot.armIterationTrapForTesting();
        setRoot.armIterationTrapForTesting();
        zsetRoot.armIterationTrapForTesting();
    }

    void disarmMemoryUsageIterationTrapsForTesting() {
        keyDirectory.disarmIterationTrapForTesting();
        listRoot.disarmIterationTrapForTesting();
        hashRoot.disarmIterationTrapForTesting();
        setRoot.disarmIterationTrapForTesting();
        zsetRoot.disarmIterationTrapForTesting();
    }

    @Override
    public void close() {
        if (closeAttempted) {
            return;
        }
        closeAttempted = true;
        throwIfFailure(releaseAll(
                stableMemoryBackend,
                entryTable,
                keyDirectory,
                stringRoot,
                listRoot,
                hashRoot,
                setRoot,
                zsetRoot
        ));
    }

    static void closePartiallyConstructed(
            StableMemoryBackend backend,
            EntryTable entries,
            NativeKeyDirectory directory,
            StringRoot strings,
            ListRoot lists,
            HashRoot hashes,
            SetRoot sets,
            ZSetRoot zsets
    ) {
        throwIfFailure(releaseAll(backend, entries, directory, strings, lists, hashes, sets, zsets));
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

    private static Throwable recordFailure(Throwable current, Throwable next) {
        if (next == null) {
            return current;
        }
        if (current == null) {
            return next;
        }
        current.addSuppressed(next);
        return current;
    }

    private void releaseOwnedEntry(EntryHandle entryHandle) {
        Objects.requireNonNull(entryHandle, "entryHandle");
        Throwable failure = null;
        EntryRecord record = null;
        try {
            record = entryTable.get(entryHandle);
        } catch (Throwable next) {
            failure = recordFailure(failure, next);
        }
        try {
            releaseValue(record);
        } catch (Throwable next) {
            failure = recordFailure(failure, next);
        }
        try {
            entryTable.release(entryHandle);
        } catch (Throwable next) {
            failure = recordFailure(failure, next);
        }
        throwIfFailure(failure);
    }

    private static Throwable releaseAll(
            StableMemoryBackend backend,
            EntryTable entries,
            NativeKeyDirectory directory,
            StringRoot strings,
            ListRoot lists,
            HashRoot hashes,
            SetRoot sets,
            ZSetRoot zsets
    ) {
        Throwable failure = clearData(entries, directory, strings, lists, hashes, sets, zsets);
        failure = closeResource(entries, failure);
        failure = closeResource(directory, failure);
        failure = closeResource(strings, failure);
        failure = closeResource(lists, failure);
        failure = closeResource(hashes, failure);
        failure = closeResource(sets, failure);
        failure = closeResource(zsets, failure);
        return closeResource(backend, failure);
    }

    private static Throwable clearData(
            EntryTable entries,
            NativeKeyDirectory directory,
            StringRoot strings,
            ListRoot lists,
            HashRoot hashes,
            SetRoot sets,
            ZSetRoot zsets
    ) {
        Throwable failure = null;
        if (entries != null && directory != null) {
            Throwable[] entryFailure = new Throwable[1];
            try {
                directory.forEachEntry((ignoredKey, entryHandle) -> {
                    EntryRecord record = null;
                    try {
                        record = entries.get(entryHandle);
                    } catch (Throwable next) {
                        entryFailure[0] = recordFailure(entryFailure[0], next);
                    }
                    try {
                        releaseValue(record, strings, lists, hashes, sets, zsets);
                    } catch (Throwable next) {
                        entryFailure[0] = recordFailure(entryFailure[0], next);
                    }
                    try {
                        entries.release(entryHandle);
                    } catch (Throwable next) {
                        entryFailure[0] = recordFailure(entryFailure[0], next);
                    }
                });
            } catch (Throwable next) {
                entryFailure[0] = recordFailure(entryFailure[0], next);
            }
            failure = recordFailure(failure, entryFailure[0]);
        }
        if (directory != null) {
            try {
                directory.clear();
            } catch (Throwable next) {
                failure = recordFailure(failure, next);
            }
        }
        return failure;
    }

    private static void releaseValue(
            EntryRecord record,
            StringRoot strings,
            ListRoot lists,
            HashRoot hashes,
            SetRoot sets,
            ZSetRoot zsets
    ) {
        if (record == null || record.valueHandle() == null || record.valueHandle().isNull()) {
            return;
        }
        switch (record.type()) {
            case STRING -> strings.release(record.valueHandle());
            case LIST -> lists.release(record.valueHandle());
            case HASH -> hashes.release(record.valueHandle());
            case SET -> sets.release(record.valueHandle());
            case ZSET -> zsets.release(record.valueHandle());
        }
    }

    private static Throwable closeResource(AutoCloseable resource, Throwable failure) {
        if (resource == null) {
            return failure;
        }
        try {
            resource.close();
        } catch (Throwable next) {
            return recordFailure(failure, next);
        }
        return failure;
    }

    private static void throwIfFailure(Throwable failure) {
        if (failure == null) {
            return;
        }
        if (failure instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        throw new IllegalStateException("db resource cleanup failed", failure);
    }
}
