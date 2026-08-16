package yier.bubu.redis.storage.memory;

import java.util.Objects;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.memory.api.NativeAllocatorStats;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.ledger.AbstractPreparedMutation;
import yier.bubu.redis.storage.memory.internal.ledger.PreparedDbMutation;

interface MutationUse<R> {
    long upperBoundBytes();

    PreparedDbMutation<R> prepare(YierdisDbKernel kernel);

    default Admission admission() {
        return Admission.NORMAL;
    }

}

enum Admission {
    NORMAL,
    RECLAMATION
}

final class InspectionScope {
    private final YierdisDbKernel kernel;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final YierdisDbMemoryContext memoryContext;

    InspectionScope(
            YierdisDbKernel kernel,
            YierdisDbKeyLifecycle keyLifecycle,
            YierdisDbMemoryContext memoryContext
    ) {
        this.kernel = Objects.requireNonNull(kernel, "kernel");
        this.keyLifecycle = Objects.requireNonNull(keyLifecycle, "keyLifecycle");
        this.memoryContext = Objects.requireNonNull(memoryContext, "memoryContext");
    }

    String objectEncoding(BytesView keyView) {
        EntryRecord record = liveEntryRecord(keyLifecycle.keyHandle(keyView));
        return record == null ? null : encodingName(record.encoding());
    }

    String objectEncoding(byte[] keyBytes) {
        if (keyBytes == null) {
            return null;
        }
        EntryRecord record = liveEntryRecord(keyLifecycle.keyHandle(keyBytes));
        return record == null ? null : encodingName(record.encoding());
    }

    EntryRecord liveEntryRecord(yier.bubu.redis.storage.memory.internal.key.KeyHandle keyHandle) {
        return kernel.liveEntryRecord(keyHandle);
    }

    yier.bubu.redis.storage.memory.internal.key.KeyHandle keyHandle(BytesView keyView) {
        return keyLifecycle.keyHandle(keyView);
    }

    yier.bubu.redis.storage.memory.internal.key.KeyHandle keyHandle(byte[] keyBytes) {
        return keyLifecycle.keyHandle(keyBytes);
    }

    long estimatedValueBytes(EntryRecord record) {
        return keyLifecycle.estimatedValueBytes(record);
    }

    int keyCount() {
        return keyLifecycle.keyCount();
    }

    int expireCount() {
        return keyLifecycle.expireCount();
    }

    NativeAllocatorStats nativeAllocatorStats() {
        return memoryContext.nativeAllocatorStats();
    }

    long nativeLiveRegionCount() {
        return memoryContext.nativeLiveRegionCount();
    }

    private static String encodingName(yier.bubu.redis.storage.memory.internal.value.ValueEncoding encoding) {
        if (encoding == null) {
            return "unknown";
        }
        return switch (encoding) {
            case STRING_INT -> "int";
            case STRING_EMBSTR -> "embstr";
            case STRING_RAW -> "raw";
            case HASH_PACKED, LIST_PACKED, ZSET_PACKED -> "listpack";
            case HASH_HT, SET_HT -> "hashtable";
            case SET_INTSET -> "intset";
            case LIST_QUICKLIST -> "quicklist";
            case ZSET_SKIPLIST -> "skiplist";
        };
    }

}

final class MaintenanceScope {
    private final YierdisDbKernel kernel;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final YierdisDbMemoryContext memoryContext;

    MaintenanceScope(
            YierdisDbKernel kernel,
            YierdisDbKeyLifecycle keyLifecycle,
            YierdisDbMemoryContext memoryContext
    ) {
        this.kernel = Objects.requireNonNull(kernel, "kernel");
        this.keyLifecycle = Objects.requireNonNull(keyLifecycle, "keyLifecycle");
        this.memoryContext = Objects.requireNonNull(memoryContext, "memoryContext");
    }

    int keyCount() {
        return keyLifecycle.keyCount();
    }

    yier.bubu.redis.storage.memory.internal.key.KeyHandle randomKeyHandle() {
        return keyLifecycle.randomKeyHandle();
    }

    EntryRecord entryRecord(yier.bubu.redis.storage.memory.internal.key.KeyHandle keyHandle) {
        return keyLifecycle.entryRecord(keyHandle);
    }

    boolean isKeyExpired(
            yier.bubu.redis.storage.memory.internal.key.KeyHandle keyHandle,
            long nowMillis
    ) {
        return keyLifecycle.isKeyExpired(keyHandle, nowMillis);
    }

    void forEachKeyHandle(java.util.function.BiConsumer<
            yier.bubu.redis.storage.memory.internal.key.KeyHandle,
            EntryRecord
    > consumer) {
        keyLifecycle.forEachKeyHandle(consumer);
    }

    boolean reclaimExpired(
            yier.bubu.redis.storage.memory.internal.key.KeyHandle keyHandle,
            EntryRecord expectedRecord,
            long nowMillis
    ) {
        return kernel.reclaimExpired(keyHandle, expectedRecord, nowMillis);
    }

    boolean evict(
            yier.bubu.redis.storage.memory.internal.key.KeyHandle keyHandle,
            EntryRecord expectedRecord
    ) {
        return kernel.evict(keyHandle, expectedRecord);
    }

    void trimEmptyNativePages() {
        memoryContext.trimEmptyNativePages(
                yier.bubu.redis.common.memory.MemoryPressureBudget.unlimited()
        );
    }

    void trimAfterPreparedPreviewClose() {
        memoryContext.trimEmptyNativePagesAfterPreparedPreviewClose();
    }
}

final class PreparedBatchMutation<T> extends AbstractPreparedMutation<T> {
    private final PreparedDbMutation<?>[] changes;
    private final T result;

    PreparedBatchMutation(
            PreparedDbMutation<?>[] changes,
            int count,
            T result,
            long actualDeltaBytes,
            MutationOutcome outcome
    ) {
        super(actualDeltaBytes, 0L, outcome);
        Objects.requireNonNull(changes, "changes");
        if (count < 0 || count > changes.length) {
            throw new IllegalArgumentException("invalid prepared change count");
        }
        this.changes = new PreparedDbMutation<?>[count];
        for (int index = 0; index < count; index++) {
            this.changes[index] = Objects.requireNonNull(changes[index], "change");
        }
        this.result = result;
    }

    @Override
    protected T commitPrepared() {
        for (PreparedDbMutation<?> change : changes) {
            change.commit();
        }
        return result;
    }

    @Override
    protected void releaseSupersededPrepared() {
        for (PreparedDbMutation<?> change : changes) {
            change.releaseSuperseded();
        }
    }

    @Override
    protected void abortPrepared() {
        for (PreparedDbMutation<?> change : changes) {
            change.abort();
        }
    }
}
