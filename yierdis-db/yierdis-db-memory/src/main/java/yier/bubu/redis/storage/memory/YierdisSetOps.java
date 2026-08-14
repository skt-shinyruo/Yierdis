package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.YierdisDbKeyLifecycle.CurrentEntry;
import yier.bubu.redis.storage.memory.YierdisDbKeyLifecycle.StagedEntry;
import yier.bubu.redis.common.command.MutationContext;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.storage.api.DbMemoryConstants;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.api.SetReadOps;
import yier.bubu.redis.storage.api.SetWriteOps;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.storage.api.WriteResult;
import yier.bubu.redis.storage.api.result.ByteSequenceSource;
import yier.bubu.redis.storage.api.result.ByteSequenceSources;
import yier.bubu.redis.storage.api.result.CollectionScanWindow;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.NativeStorageLayout;
import yier.bubu.redis.storage.memory.internal.entry.SetRoot;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.value.SemanticResultSupport;

import java.util.List;
import java.util.Objects;

final class YierdisSetOps implements SetReadOps, SetWriteOps {
    private final YierdisDbKernel kernel;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final YierdisDbMemoryContext memoryContext;
    private final SetRoot setRoot;

    YierdisSetOps(
            YierdisDbKernel kernel,
            YierdisDbKeyLifecycle keyLifecycle,
            YierdisDbMemoryContext memoryContext,
            SetRoot setRoot
    ) {
        this.kernel = Objects.requireNonNull(kernel, "kernel");
        this.keyLifecycle = Objects.requireNonNull(keyLifecycle, "keyLifecycle");
        this.memoryContext = Objects.requireNonNull(memoryContext, "memoryContext");
        this.setRoot = Objects.requireNonNull(setRoot, "setRoot");
    }

    @Override
    public WriteResult<Long> sadd(byte[] keyBytes, List<byte[]> members) {
        return sadd(MutationContext.none(), keyBytes, members);
    }

    WriteResult<Long> sadd(MutationContext context, byte[] keyBytes, List<byte[]> members) {
        kernel.execute(DbUse.ownerCheck());
        Objects.requireNonNull(keyBytes, "keyBytes");
        long now = System.currentTimeMillis();
        kernel.reclaimExpiredBeforeMutation(keyBytes, now);
        return kernel.execute(new MutationUse<WriteResult<Long>>() {
            @Override
            public MutationContext context() {
                return context;
            }

            @Override
            public long upperBoundBytes() {
                return estimateSetAddUpperBound(keyBytes, members, now);
            }

            @Override
            public PreparedChange<WriteResult<Long>> prepare(MutationScope scope) {
                CurrentEntry currentEntry = keyLifecycle.currentEntry(keyBytes);
                EntryRecord current = currentEntry.record();
                if (current != null) {
                    requireSet(current);
                    int expectedAdditions = setRoot.countAdditions(requireSetHandle(current), members);
                    if (expectedAdditions == 0) {
                        return preparedNoEntry(scope, WriteResult.of(0L, MutationOutcome.NONE), MutationOutcome.NONE);
                    }
                }

                StagedEntry staged = null;
                ValueHandle replacement = null;
                long rootHeapBefore = setRoot.retainedHeapBytes();
                try {
                    KeyHandle targetKey = currentEntry.keyHandle();
                    if (current == null) {
                        staged = keyLifecycle.stageEntry(keyBytes);
                        targetKey = staged.keyHandle();
                    }

                    replacement = setRoot.create();
                    if (current != null) {
                        setRoot.sadd(replacement, setRoot.members(requireSetHandle(current)));
                    }
                    int added = setRoot.sadd(replacement, members);
                    MutationOutcome outcome = added > 0 ? MutationOutcome.VALUE_CHANGED : MutationOutcome.NONE;
                    if (added == 0) {
                        WriteResult<Long> result = WriteResult.of(0L, outcome);
                        if (replacement != null) {
                            setRoot.release(replacement);
                            replacement = null;
                        }
                        if (staged != null) {
                            staged.close();
                            staged = null;
                        }
                        return preparedNoEntry(scope, result, outcome);
                    }
                    EntryRecord next = setRecord(
                            targetKey,
                            replacement,
                            current == null ? -1L : current.expireAtMillis(),
                            current
                    );
                    WriteResult<Long> result = WriteResult.of((long) added, outcome);
                    long deltaBytes = estimateRecordBytes(targetKey, next)
                            - estimateRecordBytes(targetKey, current);
                    PreparedChange<WriteResult<Long>> prepared = scope.upsert(
                            result,
                            deltaBytes,
                            MemoryUsageSnapshot.addSaturating(
                                    staged == null ? 0L : staged.stagedHeapBytes(),
                                    setRoot.positiveRetainedHeapGrowthBytes(rootHeapBefore)
                            ),
                            outcome,
                            currentEntry,
                            staged,
                            next,
                            true
                    );
                    staged = null;
                    replacement = null;
                    return prepared;
                } catch (RuntimeException | Error failure) {
                    abortStaged(staged, replacement, failure);
                    throw failure;
                }
            }
        });
    }

    @Override
    public WriteResult<Long> srem(byte[] keyBytes, List<byte[]> members) {
        return srem(MutationContext.none(), keyBytes, members);
    }

    WriteResult<Long> srem(MutationContext context, byte[] keyBytes, List<byte[]> members) {
        kernel.execute(DbUse.ownerCheck());
        Objects.requireNonNull(keyBytes, "keyBytes");
        long now = System.currentTimeMillis();
        kernel.reclaimExpiredBeforeMutation(keyBytes, now);
        return kernel.execute(new MutationUse<WriteResult<Long>>() {
            @Override
            public MutationContext context() {
                return context;
            }

            @Override
            public long upperBoundBytes() {
                return 0;
            }

            @Override
            public Admission admission() {
                return Admission.RECLAMATION;
            }

            @Override
            public PreparedChange<WriteResult<Long>> prepare(MutationScope scope) {
                CurrentEntry currentEntry = keyLifecycle.currentEntry(keyBytes);
                EntryRecord current = currentEntry.record();
                if (current == null) {
                    return preparedNoEntry(scope, WriteResult.of(0L, MutationOutcome.NONE), MutationOutcome.NONE);
                }
                requireSet(current);
                ValueHandle handle = requireSetHandle(current);
                int removed = setRoot.countExistingMembers(handle, members);
                if (removed == 0) {
                    return preparedNoEntry(scope, WriteResult.of(0L, MutationOutcome.NONE), MutationOutcome.NONE);
                }

                MutationOutcome outcome = MutationOutcome.VALUE_CHANGED;
                WriteResult<Long> result = WriteResult.of((long) removed, outcome);
                if (removed >= setRoot.size(handle)) {
                    return preparedDelete(scope, currentEntry, current, result, outcome, true);
                }

                EntryRecord next = setRecord(currentEntry.keyHandle(), handle, current.expireAtMillis(), current);
                long deltaBytes = estimateRecordBytes(currentEntry.keyHandle(), next)
                        - estimateRecordBytes(currentEntry.keyHandle(), current);
                return scope.callback(
                        result,
                        deltaBytes,
                        0L,
                        outcome,
                        () -> {
                            int actualRemoved = setRoot.srem(handle, members);
                            if (actualRemoved != removed) {
                                throw new IllegalStateException("prepared SREM removed " + actualRemoved
                                        + " members instead of " + removed);
                            }
                            keyLifecycle.replaceEntry(currentEntry.entryHandle(), current, next);
                        },
                        null,
                        null,
                        deltaBytes < 0L
                );
            }
        });
    }

    @Override
    public ByteSequenceSource smembers(byte[] keyBytes) {
        return kernel.execute(DbUse.read(scope -> {
            EntryRecord record = liveSetRecord(scope, keyBytes);
            if (record == null) {
                return ByteSequenceSources.empty();
            }
            ValueHandle handle = requireSetHandle(record);
            return ByteSequenceSources.of(
                    setRoot.size(handle),
                    0L,
                    out -> setRoot.membersInto(handle, SemanticResultSupport.lengthSink(out)),
                    out -> setRoot.membersInto(handle, out)
            );
        }));
    }

    @Override
    public boolean sismember(byte[] keyBytes, byte[] member) {
        return kernel.execute(DbUse.read(scope -> {
            EntryRecord record = liveSetRecord(scope, keyBytes);
            if (record == null) {
                return false;
            }
            return setRoot.contains(requireSetHandle(record), member);
        }));
    }

    @Override
    public long scard(byte[] keyBytes) {
        return kernel.execute(DbUse.read(scope -> {
            EntryRecord record = liveSetRecord(scope, keyBytes);
            if (record == null) {
                return 0L;
            }
            return (long) setRoot.size(requireSetHandle(record));
        }));
    }

    @Override
    public CollectionScanWindow sscan(byte[] keyBytes, ScanCursorV2 cursor, byte[] globPattern, int count) {
        return kernel.execute(DbUse.read(scope -> {
            if (count <= 0) {
                throw new IllegalArgumentException("count must be > 0");
            }
            EntryRecord record = liveSetRecord(scope, keyBytes);
            if (record == null) {
                return new MaterializedCollectionScanWindow(ScanCursorV2.start(), List.of());
            }
            return setRoot.sscan(
                    requireSetHandle(record),
                    cursor == null ? ScanCursorV2.start() : cursor,
                    globPattern,
                    count
            );
        }));
    }

    private long estimateSetAddUpperBound(byte[] keyBytes, List<byte[]> members, long nowMillis) {
        EntryRecord existing = keyLifecycle.entryRecord(keyBytes);
        if (existing == null) {
            return newSetUpperBound(keyBytes, members);
        }

        KeyHandle keyHandle = keyLifecycle.keyHandle(keyBytes);
        if (keyLifecycle.isKeyExpired(keyHandle, nowMillis)) {
            return newSetUpperBound(keyBytes, members);
        }
        if (existing.type() != ValueType.SET) {
            return withScopeBookkeeping(0L);
        }

        ValueHandle handle = requireSetHandle(existing);
        int[] allocationSizes = setAllocationSizes(
                false,
                keyBytes,
                setRoot.nativePayloadSizes(handle),
                members
        );
        long stagedHeapBytes = setRoot.estimatedPreparedAddHeapGrowthBytes(
                handle,
                members,
                allocationSizes.length
        );
        long nativeUpperBound = nativePeak(stagedHeapBytes, allocationSizes);
        long logicalUpperBound = MemoryUsageSnapshot.addSaturating(
                YierdisDbMemoryEstimator.estimateSetWriteUpperBound(0, members),
                setRoot.estimatedBytes(handle)
        );
        return withScopeBookkeeping(Math.max(logicalUpperBound, nativeUpperBound));
    }

    private long newSetUpperBound(byte[] keyBytes, List<byte[]> members) {
        int[] allocationSizes = setAllocationSizes(
                true,
                keyBytes,
                new int[0],
                members
        );
        long stagedHeapBytes = MemoryUsageSnapshot.addSaturating(
                keyLifecycle.estimatedInsertHeapGrowthBytes(),
                setRoot.estimatedPreparedAddHeapGrowthBytes(
                        null,
                        members,
                        allocationSizes.length
                )
        );
        long nativeUpperBound = nativePeak(stagedHeapBytes, allocationSizes);
        long logicalUpperBound = YierdisDbMemoryEstimator.estimateSetWriteUpperBound(keyBytes.length, members);
        return withScopeBookkeeping(Math.max(logicalUpperBound, nativeUpperBound));
    }

    private int[] setAllocationSizes(
            boolean includeKeyAndEntry,
            byte[] keyBytes,
            int[] existingPayloadSizes,
            List<byte[]> members
    ) {
        int existingPayloadCount = existingPayloadSizes == null ? 0 : existingPayloadSizes.length;
        int newPayloadCount = nonNullValueCount(members);
        int metadataCount = includeKeyAndEntry ? 2 : 0;
        int[] sizes = new int[metadataCount + 1 + existingPayloadCount + newPayloadCount];
        int next = 0;
        if (includeKeyAndEntry) {
            sizes[next++] = Math.max(1, keyBytes.length);
            sizes[next++] = NativeStorageLayout.ENTRY_RECORD_BYTES;
        }
        sizes[next++] = NativeStorageLayout.COLLECTION_ROOT_RECORD_BYTES;
        next = appendPayloadSizes(sizes, next, existingPayloadSizes);
        next = appendValuePayloadSizes(sizes, next, members);
        if (next != sizes.length) {
            throw new IllegalStateException("set allocation size estimate count mismatch");
        }
        return sizes;
    }

    private EntryRecord liveSetRecord(ReadScope scope, byte[] keyBytes) {
        KeyHandle keyHandle = keyLifecycle.keyHandle(keyBytes);
        EntryRecord record = scope.liveEntryRecord(keyHandle);
        if (record == null) {
            return null;
        }
        requireSet(record);
        return keyLifecycle.touchRecord(keyHandle, record);
    }

    private EntryRecord setRecord(KeyHandle keyHandle, ValueHandle handle, long expireAtMillis, EntryRecord previous) {
        return keyLifecycle.newRecord(
                keyHandle,
                handle,
                ValueType.SET,
                setRoot.encoding(handle),
                expireAtMillis,
                previous
        );
    }

    private ValueHandle requireSetHandle(EntryRecord record) {
        ValueHandle handle = record.valueHandle();
        if (!setRoot.contains(handle)) {
            throw new IllegalStateException("native set value handle is not available: " + (handle == null ? "null" : handle.nativeHandle()));
        }
        return handle;
    }

    private static void requireSet(EntryRecord record) {
        if (record.type() != ValueType.SET) {
            throw new WrongTypeException();
        }
    }

    private long estimateRecordBytes(KeyHandle keyHandle, EntryRecord record) {
        return keyLifecycle.estimatedBytesForRemoval(keyHandle, record);
    }

    private static <T> PreparedChange<T> preparedNoEntry(
            MutationScope scope,
            T result,
            MutationOutcome outcome
    ) {
        return scope.unchanged(result, outcome);
    }

    private <T> PreparedChange<T> preparedDelete(
            MutationScope scope,
            CurrentEntry currentEntry,
            EntryRecord current,
            T result,
            MutationOutcome outcome,
            boolean releaseOldValue
    ) {
        long deltaBytes = -estimateRecordBytes(currentEntry.keyHandle(), current);
        return scope.delete(
                result,
                deltaBytes,
                outcome,
                currentEntry.entryHandle(),
                current,
                releaseOldValue
        );
    }

    private void abortStaged(
            StagedEntry staged,
            ValueHandle replacement,
            Throwable failure
    ) {
        if (replacement != null) {
            try {
                setRoot.release(replacement);
            } catch (RuntimeException | Error releaseFailure) {
                failure.addSuppressed(releaseFailure);
            }
        }
        keyLifecycle.abortStagedEntry(staged, failure);
    }

    private long nativePeak(long heapGrowthBytes, int... nativeAllocationSizes) {
        return memoryContext.nativeAllocationPeakAdditionalBytes(
                0L,
                Math.max(0L, heapGrowthBytes),
                nativeAllocationSizes
        );
    }

    private long withScopeBookkeeping(long upperBound) {
        return Math.max(
                Math.max(0L, upperBound),
                memoryContext.nativeAllocationScopeBookkeepingBytes(0)
        );
    }

    private static int nonNullValueCount(List<byte[]> values) {
        if (values == null || values.isEmpty()) {
            return 0;
        }
        int count = 0;
        for (byte[] value : values) {
            if (value != null) {
                count++;
            }
        }
        return count;
    }

    private static int appendPayloadSizes(int[] sizes, int offset, int[] payloadSizes) {
        if (payloadSizes == null || payloadSizes.length == 0) {
            return offset;
        }
        int next = offset;
        for (int size : payloadSizes) {
            sizes[next++] = Math.max(1, size);
        }
        return next;
    }

    private static int appendValuePayloadSizes(int[] sizes, int offset, List<byte[]> values) {
        if (values == null || values.isEmpty()) {
            return offset;
        }
        int next = offset;
        for (byte[] value : values) {
            if (value != null) {
                sizes[next++] = Math.max(1, value.length);
            }
        }
        return next;
    }

}
