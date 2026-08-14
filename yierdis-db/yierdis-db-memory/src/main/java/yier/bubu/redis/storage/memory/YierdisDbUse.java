package yier.bubu.redis.storage.memory;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.common.command.ImmutableCommandRecord;
import yier.bubu.redis.common.command.MutationContext;
import yier.bubu.redis.memory.api.NativeAllocatorStats;
import yier.bubu.redis.memory.api.NativeEpochScope;
import yier.bubu.redis.storage.api.DbCommitKind;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.memory.YierdisDbKeyLifecycle.CurrentEntry;
import yier.bubu.redis.storage.memory.YierdisDbKeyLifecycle.StagedEntry;
import yier.bubu.redis.storage.memory.internal.entry.EntryHandle;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.ledger.AbstractPreparedMutation;
import yier.bubu.redis.storage.memory.internal.ledger.PreparedCallbackMutation;
import yier.bubu.redis.storage.memory.internal.ledger.PreparedDbMutation;

sealed interface DbUse<R> permits ReadUse, MutationUse, InspectionUse, MaintenanceUse {
    static ReadUse<Void> ownerCheck() {
        return ignored -> null;
    }

    static <R> ReadUse<R> read(Function<ReadScope, R> action) {
        Objects.requireNonNull(action, "action");
        return action::apply;
    }

    static <R> InspectionUse<R> inspect(Function<InspectionScope, R> action) {
        Objects.requireNonNull(action, "action");
        return action::apply;
    }

    static <R> MaintenanceUse<R> maintain(Function<MaintenanceScope, R> action) {
        Objects.requireNonNull(action, "action");
        return action::apply;
    }
}

@FunctionalInterface
non-sealed interface ReadUse<R> extends DbUse<R> {
    R execute(ReadScope scope);
}

non-sealed interface MutationUse<R> extends DbUse<R> {
    long upperBoundBytes();

    PreparedChange<R> prepare(MutationScope scope);

    default MutationContext context() {
        return MutationContext.none();
    }

    default Admission admission() {
        return Admission.NORMAL;
    }

    default CommitSpec commit() {
        return CommitSpec.user();
    }
}

@FunctionalInterface
non-sealed interface InspectionUse<R> extends DbUse<R> {
    R execute(InspectionScope scope);
}

@FunctionalInterface
non-sealed interface MaintenanceUse<R> extends DbUse<R> {
    R execute(MaintenanceScope scope);
}

enum Admission {
    NORMAL,
    RECLAMATION
}

final class CommitSpec {
    private static final CommitSpec USER = new CommitSpec(
            DbCommitKind.USER,
            true,
            context -> {
                ImmutableCommandRecord record = context.retainCommandRecord();
                if (record == null) {
                    throw new yier.bubu.redis.storage.api.DbCommitStreamUnavailableException();
                }
                return record;
            }
    );
    private static final CommitSpec NONE = new CommitSpec(DbCommitKind.USER, false, ignored -> null);

    private final DbCommitKind kind;
    private final boolean required;
    private final Function<MutationContext, ImmutableCommandRecord> recordFactory;

    private CommitSpec(
            DbCommitKind kind,
            boolean required,
            Function<MutationContext, ImmutableCommandRecord> recordFactory
    ) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.required = required;
        this.recordFactory = Objects.requireNonNull(recordFactory, "recordFactory");
    }

    static CommitSpec user() {
        return USER;
    }

    static CommitSpec none() {
        return NONE;
    }

    static CommitSpec synthetic(DbCommitKind kind, Supplier<ImmutableCommandRecord> recordFactory) {
        Objects.requireNonNull(recordFactory, "recordFactory");
        return new CommitSpec(kind, true, ignored -> recordFactory.get());
    }

    DbCommitKind kind() {
        return kind;
    }

    boolean required() {
        return required;
    }

    ImmutableCommandRecord retainRecord(MutationContext context) {
        return recordFactory.apply(Objects.requireNonNull(context, "context"));
    }
}

final class ReadScope {
    private final YierdisDbKernel kernel;

    ReadScope(YierdisDbKernel kernel) {
        this.kernel = Objects.requireNonNull(kernel, "kernel");
    }

    EntryRecord liveEntryRecord(yier.bubu.redis.storage.memory.internal.key.KeyHandle keyHandle) {
        return kernel.liveEntryRecord(keyHandle);
    }

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

    ScanCursorV2 snapshot(ScanCursorV2 cursor, int count, List<YierdisSnapshotEntry> out) {
        Objects.requireNonNull(out, "out");
        if (count <= 0) {
            throw new IllegalArgumentException("count must be > 0");
        }
        try (NativeEpochScope ignored = memoryContext.beginSnapshotEpoch()) {
            long now = System.currentTimeMillis();
            int maxSteps = Math.max(64, count * 10);
            RemainingLimit remaining = new RemainingLimit(count);
            return keyLifecycle.scan(cursor == null ? ScanCursorV2.start() : cursor, maxSteps, (key, record) -> {
                if (key == null || record == null || keyLifecycle.isKeyExpired(key, now)) {
                    return true;
                }
                ValueType type = record.type();
                byte[] stringValue = type == ValueType.STRING ? keyLifecycle.copyStringValue(record) : null;
                Long expireAtMillis = record.expireAtMillis() < 0L ? null : record.expireAtMillis();
                out.add(new YierdisSnapshotEntry(
                        YierdisDb.toByteArray(key),
                        type,
                        stringValue,
                        expireAtMillis
                ));
                return remaining.consume();
            });
        }
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

    long expiredEntriesAwaitingPhysicalDeletion() {
        return keyLifecycle.expiredEntriesAwaitingPhysicalDeletion();
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

    private static final class RemainingLimit {
        private int remaining;

        private RemainingLimit(int remaining) {
            this.remaining = remaining;
        }

        private boolean consume() {
            remaining--;
            return remaining > 0;
        }
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

final class MutationScope {
    private final YierdisDbKeyLifecycle keyLifecycle;

    MutationScope(YierdisDbKeyLifecycle keyLifecycle) {
        this.keyLifecycle = Objects.requireNonNull(keyLifecycle, "keyLifecycle");
    }

    <T> PreparedChange<T> unchanged(T result, MutationOutcome outcome) {
        return new PreparedChange<>(PreparedEntryMutation.unchanged(keyLifecycle, result, outcome));
    }

    <T> PreparedChange<T> insert(
            T result,
            long actualDeltaBytes,
            long stagedNonNativeGrowthBytes,
            MutationOutcome outcome,
            StagedEntry stagedEntry,
            EntryRecord newRecord
    ) {
        return new PreparedChange<>(PreparedEntryMutation.insert(
                keyLifecycle,
                result,
                actualDeltaBytes,
                stagedNonNativeGrowthBytes,
                outcome,
                stagedEntry,
                newRecord
        ));
    }

    <T> PreparedChange<T> replace(
            T result,
            long actualDeltaBytes,
            long stagedNonNativeGrowthBytes,
            MutationOutcome outcome,
            EntryHandle existingEntryHandle,
            EntryRecord oldRecord,
            EntryRecord newRecord,
            boolean releaseReplacedValue
    ) {
        return new PreparedChange<>(PreparedEntryMutation.replace(
                keyLifecycle,
                result,
                actualDeltaBytes,
                stagedNonNativeGrowthBytes,
                outcome,
                existingEntryHandle,
                oldRecord,
                newRecord,
                releaseReplacedValue
        ));
    }

    <T> PreparedChange<T> delete(
            T result,
            long actualDeltaBytes,
            MutationOutcome outcome,
            EntryHandle existingEntryHandle,
            EntryRecord oldRecord,
            boolean releaseReplacedValue
    ) {
        return new PreparedChange<>(PreparedEntryMutation.delete(
                keyLifecycle,
                result,
                actualDeltaBytes,
                outcome,
                existingEntryHandle,
                oldRecord,
                releaseReplacedValue
        ));
    }

    <T> PreparedChange<T> upsert(
            T result,
            long actualDeltaBytes,
            long stagedNonNativeGrowthBytes,
            MutationOutcome outcome,
            CurrentEntry current,
            StagedEntry staged,
            EntryRecord newRecord,
            boolean releaseReplacedValue
    ) {
        return new PreparedChange<>(PreparedEntryMutation.upsert(
                keyLifecycle,
                result,
                actualDeltaBytes,
                stagedNonNativeGrowthBytes,
                outcome,
                current,
                staged,
                newRecord,
                releaseReplacedValue
        ));
    }

    <T> PreparedChange<T> callback(
            T result,
            long actualDeltaBytes,
            long stagedNonNativeGrowthBytes,
            MutationOutcome outcome,
            Runnable commit,
            Runnable releaseSuperseded,
            Runnable abort,
            boolean trimNativePagesAfterCommit
    ) {
        return new PreparedChange<>(new PreparedCallbackMutation<>(
                result,
                actualDeltaBytes,
                stagedNonNativeGrowthBytes,
                outcome,
                commit,
                releaseSuperseded,
                abort,
                trimNativePagesAfterCommit
        ));
    }

    <T> PreparedChange<T> batch(
            PreparedChange<?>[] changes,
            int count,
            T result,
            long actualDeltaBytes,
            MutationOutcome outcome
    ) {
        return new PreparedChange<>(new PreparedBatchMutation<>(
                changes,
                count,
                result,
                actualDeltaBytes,
                outcome
        ));
    }
}

final class PreparedChange<T> {
    private final PreparedDbMutation<T> prepared;

    PreparedChange(PreparedDbMutation<T> prepared) {
        this.prepared = Objects.requireNonNull(prepared, "prepared");
    }

    PreparedChange<T> releaseReplacedValueWith(Runnable hook) {
        entryMutation().releaseReplacedValueWith(hook);
        return this;
    }

    PreparedChange<T> closeOnAbort(AutoCloseable resource) {
        entryMutation().closeOnAbort(resource);
        return this;
    }

    PreparedChange<T> releaseNewValueOnAbortWith(Runnable hook) {
        entryMutation().releaseNewValueOnAbortWith(hook);
        return this;
    }

    PreparedChange<T> beforeEntryPublish(Runnable hook) {
        entryMutation().beforeEntryPublish(hook);
        return this;
    }

    PreparedChange<T> requestNativePageTrimAfterCommit() {
        entryMutation().requestNativePageTrimAfterCommit();
        return this;
    }

    void abort() {
        prepared.abort();
    }

    PreparedDbMutation<T> unwrap() {
        return prepared;
    }

    @SuppressWarnings("unchecked")
    private PreparedEntryMutation<T> entryMutation() {
        if (prepared instanceof PreparedEntryMutation<?> entryMutation) {
            return (PreparedEntryMutation<T>) entryMutation;
        }
        throw new IllegalStateException("prepared change is not an entry mutation");
    }
}

final class PreparedBatchMutation<T> extends AbstractPreparedMutation<T> {
    private final PreparedDbMutation<?>[] changes;
    private final T result;

    PreparedBatchMutation(
            PreparedChange<?>[] changes,
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
            this.changes[index] = Objects.requireNonNull(changes[index], "change").unwrap();
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
