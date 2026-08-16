package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.internal.ledger.PreparedDbMutation;

import yier.bubu.redis.storage.memory.YierdisDbKeyLifecycle.CurrentEntry;
import yier.bubu.redis.storage.memory.YierdisDbKeyLifecycle.StagedEntry;
import yier.bubu.redis.common.command.MutationContext;
import yier.bubu.redis.storage.api.DbMemoryConstants;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.storage.api.WriteResult;
import yier.bubu.redis.storage.api.ZSetReadOps;
import yier.bubu.redis.storage.api.ZSetWriteOps;
import yier.bubu.redis.storage.api.result.ByteSequenceSource;
import yier.bubu.redis.storage.api.result.ByteSequenceSources;
import yier.bubu.redis.storage.api.result.CollectionScanWindow;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.NativeStorageLayout;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;
import yier.bubu.redis.storage.memory.internal.entry.ZSetRoot;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;
import yier.bubu.redis.storage.memory.internal.value.SemanticResultSupport;
import yier.bubu.redis.storage.memory.internal.value.ZSetValue.ZAddResult;

import java.util.List;
import java.util.Objects;

final class YierdisZSetOps implements ZSetReadOps, ZSetWriteOps {
    private final YierdisDbKernel kernel;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final YierdisDbMemoryContext memoryContext;
    private final ZSetRoot zsetRoot;

    YierdisZSetOps(
            YierdisDbKernel kernel,
            YierdisDbKeyLifecycle keyLifecycle,
            YierdisDbMemoryContext memoryContext,
            ZSetRoot zsetRoot
    ) {
        this.kernel = Objects.requireNonNull(kernel, "kernel");
        this.keyLifecycle = Objects.requireNonNull(keyLifecycle, "keyLifecycle");
        this.memoryContext = Objects.requireNonNull(memoryContext, "memoryContext");
        this.zsetRoot = Objects.requireNonNull(zsetRoot, "zsetRoot");
    }

    @Override
    public WriteResult<Long> zadd(byte[] keyBytes, List<byte[]> scoreMemberPairs) {
        return zadd(MutationContext.none(), keyBytes, scoreMemberPairs);
    }

    WriteResult<Long> zadd(MutationContext context, byte[] keyBytes, List<byte[]> scoreMemberPairs) {
        kernel.execute(DbUse.ownerCheck());
        if (scoreMemberPairs.size() % 2 != 0) {
            throw new IllegalArgumentException("scoreMemberPairs must contain score/member pairs");
        }
        Objects.requireNonNull(keyBytes, "keyBytes");
        long now = System.currentTimeMillis();
        kernel.reclaimExpiredBeforeMutation(keyBytes, now);
        return kernel.execute(new MutationUse<WriteResult<Long>>() {
            private ZSetRoot.AddPlan cachedAddPlan;
            private boolean addPlanInitialized;

            @Override
            public MutationContext context() {
                return context;
            }

            @Override
            public long upperBoundBytes() {
                EntryRecord existing = keyLifecycle.entryRecord(keyBytes);
                if (existing == null) {
                    return newZSetUpperBound(keyBytes, addPlan(null));
                }
                KeyHandle keyHandle = keyLifecycle.keyHandle(keyBytes);
                if (keyLifecycle.isKeyExpired(keyHandle, now)) {
                    return newZSetUpperBound(keyBytes, addPlan(null));
                }
                if (existing.type() != ValueType.ZSET) {
                    return withScopeBookkeeping(0L);
                }
                ValueHandle handle = requireZSetHandle(existing);
                return existingZSetUpperBound(keyBytes, handle, scoreMemberPairs, addPlan(handle));
            }

            @Override
            public PreparedDbMutation<WriteResult<Long>> prepare(YierdisDbKernel scope) {
                CurrentEntry currentEntry = keyLifecycle.currentEntry(keyBytes);
                EntryRecord current = currentEntry.record();
                if (current != null) {
                    requireZSet(current);
                }

                StagedEntry staged = null;
                ValueHandle replacement = null;
                ZSetRoot.PreparedAddResult preparedAdd = null;
                try {
                    KeyHandle targetKey = currentEntry.keyHandle();
                    if (current == null) {
                        staged = keyLifecycle.stageEntry(keyBytes);
                        targetKey = staged.keyHandle();
                    }
                    ValueHandle sourceHandle = current == null ? null : requireZSetHandle(current);
                    preparedAdd = zsetRoot.prepareAdd(addPlan(sourceHandle));
                    ZAddResult added = preparedAdd.result();
                    MutationOutcome outcome = added.changedAny()
                            ? MutationOutcome.VALUE_CHANGED
                            : MutationOutcome.NONE;
                    if (preparedAdd.stableHandle() && !preparedAdd.changedAny()) {
                        preparedAdd.close();
                        preparedAdd = null;
                        return preparedNoEntry(scope, WriteResult.of(0L, outcome), outcome);
                    }

                    boolean stableHandle = preparedAdd.stableHandle();
                    ValueHandle nextHandle = preparedAdd.handle();
                    replacement = stableHandle ? null : nextHandle;
                    EntryRecord next = zsetRecord(
                            targetKey,
                            nextHandle,
                            current == null ? -1L : current.expireAtMillis(),
                            current,
                            preparedAdd.targetEncoding()
                    );
                    WriteResult<Long> result = WriteResult.of((long) added.added(), outcome);
                    long deltaBytes = estimateRecordBytes(targetKey, next)
                            - estimateRecordBytes(targetKey, current);
                    PreparedEntryMutation<WriteResult<Long>> prepared = scope.upsert(
                            result,
                            deltaBytes,
                            addSaturating(
                                    staged == null ? 0L : staged.stagedHeapBytes(),
                                    preparedAdd.stagedNonNativeGrowthBytes()
                            ),
                            outcome,
                            currentEntry,
                            staged,
                            next,
                            !stableHandle
                    );
                    if (stableHandle) {
                        prepared.releaseReplacedValueWith(preparedAdd::releaseSuperseded)
                                .closeOnAbort(preparedAdd)
                                .beforeEntryPublish(preparedAdd::commit);
                    }
                    staged = null;
                    replacement = null;
                    preparedAdd = null;
                    return prepared;
                } catch (RuntimeException | Error failure) {
                    if (preparedAdd != null && preparedAdd.stableHandle()) {
                        try {
                            preparedAdd.close();
                        } catch (RuntimeException | Error closeFailure) {
                            failure.addSuppressed(closeFailure);
                        }
                    }
                    abortStaged(staged, replacement, failure);
                    throw failure;
                }
            }

            private ZSetRoot.AddPlan addPlan(ValueHandle source) {
                if (!addPlanInitialized) {
                    cachedAddPlan = zsetRoot.planAdd(source, scoreMemberPairs);
                    addPlanInitialized = true;
                    return cachedAddPlan;
                }
                if (!Objects.equals(cachedAddPlan.source(), source)) {
                    throw new IllegalStateException("prepared ZADD source changed after admission");
                }
                return cachedAddPlan;
            }
        });
    }

    private static <T> PreparedDbMutation<T> preparedNoEntry(
            YierdisDbKernel scope,
            T result,
            MutationOutcome outcome
    ) {
        return scope.unchanged(result, outcome);
    }

    private void abortStaged(StagedEntry staged, ValueHandle replacement, Throwable failure) {
        if (replacement != null) {
            try {
                zsetRoot.release(replacement);
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

    private int[] zsetAllocationSizes(
            boolean includeKeyAndEntry,
            boolean includeRoot,
            byte[] keyBytes,
            int[] replacementAllocationSizes
    ) {
        int metadataCount = includeKeyAndEntry ? 2 : 0;
        int payloadCount = replacementAllocationSizes == null ? 0 : replacementAllocationSizes.length;
        int[] sizes = new int[metadataCount + (includeRoot ? 1 : 0) + payloadCount];
        int next = 0;
        if (includeKeyAndEntry) {
            sizes[next++] = Math.max(1, keyBytes.length);
            sizes[next++] = NativeStorageLayout.ENTRY_RECORD_BYTES;
        }
        if (includeRoot) {
            sizes[next++] = NativeStorageLayout.COLLECTION_ROOT_RECORD_BYTES;
        }
        if (replacementAllocationSizes != null) {
            for (int size : replacementAllocationSizes) {
                sizes[next++] = Math.max(1, size);
            }
        }
        return sizes;
    }

    private long newZSetUpperBound(byte[] keyBytes, ZSetRoot.AddPlan addPlan) {
        int[] allocationSizes = zsetAllocationSizes(
                true,
                true,
                keyBytes,
                zsetRoot.preparedAddNativeAllocationSizes(addPlan)
        );
        long stagedHeapBytes = addSaturating(
                keyLifecycle.estimatedInsertHeapGrowthBytes(),
                zsetRoot.estimatedPreparedAddHeapGrowthBytes(
                        addPlan,
                        allocationSizes.length
                )
        );
        long nativeUpperBound = nativePeak(stagedHeapBytes, allocationSizes);
        long logicalUpperBound = YierdisDbMemoryEstimator.estimateZSetWriteUpperBound(
                keyBytes.length,
                addPlan.scoreMemberPairs()
        );
        return withScopeBookkeeping(Math.max(logicalUpperBound, nativeUpperBound));
    }

    private long existingZSetUpperBound(
            byte[] keyBytes,
            ValueHandle handle,
            List<byte[]> scoreMemberPairs,
            ZSetRoot.AddPlan addPlan
    ) {
        int[] allocationSizes = zsetAllocationSizes(
                false,
                !addPlan.stableHandle(),
                keyBytes,
                zsetRoot.preparedAddNativeAllocationSizes(addPlan)
        );
        long stagedHeapBytes = zsetRoot.estimatedPreparedAddHeapGrowthBytes(
                addPlan,
                allocationSizes.length
        );
        long nativeUpperBound = nativePeak(stagedHeapBytes, allocationSizes);
        long logicalUpperBound = addSaturating(
                YierdisDbMemoryEstimator.estimateZSetWriteUpperBound(0, scoreMemberPairs),
                zsetRoot.estimatedBytes(handle)
        );
        return withScopeBookkeeping(Math.max(logicalUpperBound, nativeUpperBound));
    }

    @Override
    public ByteSequenceSource zrange(byte[] keyBytes, long start, long stop, boolean withScores) {
        return kernel.execute(DbUse.read(scope -> {
            EntryRecord record = liveZSetRecord(scope, keyBytes);
            if (record == null) {
                return ByteSequenceSources.empty();
            }
            ValueHandle handle = requireZSetHandle(record);
            return ByteSequenceSources.of(
                    zsetRoot.zrangeCount(handle, start, stop, withScores),
                    0L,
                    out -> zsetRoot.zrangeWriteTo(
                            handle, start, stop, withScores, SemanticResultSupport.lengthSink(out)
                    ),
                    out -> zsetRoot.zrangeWriteTo(handle, start, stop, withScores, out)
            );
        }));
    }

    @Override
    public ByteSequenceSource zrevrange(byte[] keyBytes, long start, long stop, boolean withScores) {
        return kernel.execute(DbUse.read(scope -> {
            EntryRecord record = liveZSetRecord(scope, keyBytes);
            if (record == null) {
                return ByteSequenceSources.empty();
            }
            ValueHandle handle = requireZSetHandle(record);
            return ByteSequenceSources.of(
                    zsetRoot.zrevrangeCount(handle, start, stop, withScores),
                    0L,
                    out -> zsetRoot.zrevrangeWriteTo(
                            handle, start, stop, withScores, SemanticResultSupport.lengthSink(out)
                    ),
                    out -> zsetRoot.zrevrangeWriteTo(handle, start, stop, withScores, out)
            );
        }));
    }

    @Override
    public ByteSequenceSource zrangeByScore(
            byte[] keyBytes,
            double min,
            boolean minExclusive,
            double max,
            boolean maxExclusive,
            boolean withScores,
            long offset,
            long count
    ) {
        return kernel.execute(DbUse.read(scope -> {
            EntryRecord record = liveZSetRecord(scope, keyBytes);
            if (record == null) {
                return ByteSequenceSources.empty();
            }
            ValueHandle handle = requireZSetHandle(record);
            return ByteSequenceSources.of(
                    zsetRoot.zrangeByScoreCount(
                            handle, min, minExclusive, max, maxExclusive, withScores, offset, count
                    ),
                    0L,
                    out -> zsetRoot.zrangeByScoreWriteTo(
                            handle,
                            min,
                            minExclusive,
                            max,
                            maxExclusive,
                            withScores,
                            offset,
                            count,
                            SemanticResultSupport.lengthSink(out)
                    ),
                    out -> zsetRoot.zrangeByScoreWriteTo(
                            handle,
                            min,
                            minExclusive,
                            max,
                            maxExclusive,
                            withScores,
                            offset,
                            count,
                            out
                    )
            );
        }));
    }

    @Override
    public ByteSequenceSource zrevrangeByScore(
            byte[] keyBytes,
            double min,
            boolean minExclusive,
            double max,
            boolean maxExclusive,
            boolean withScores,
            long offset,
            long count
    ) {
        return kernel.execute(DbUse.read(scope -> {
            EntryRecord record = liveZSetRecord(scope, keyBytes);
            if (record == null) {
                return ByteSequenceSources.empty();
            }
            ValueHandle handle = requireZSetHandle(record);
            return ByteSequenceSources.of(
                    zsetRoot.zrevrangeByScoreCount(
                            handle, min, minExclusive, max, maxExclusive, withScores, offset, count
                    ),
                    0L,
                    out -> zsetRoot.zrevrangeByScoreWriteTo(
                            handle,
                            min,
                            minExclusive,
                            max,
                            maxExclusive,
                            withScores,
                            offset,
                            count,
                            SemanticResultSupport.lengthSink(out)
                    ),
                    out -> zsetRoot.zrevrangeByScoreWriteTo(
                            handle,
                            min,
                            minExclusive,
                            max,
                            maxExclusive,
                            withScores,
                            offset,
                            count,
                            out
                    )
            );
        }));
    }

    @Override
    public CollectionScanWindow zscan(byte[] keyBytes, ScanCursorV2 cursor, byte[] globPattern, int count) {
        return kernel.execute(DbUse.read(scope -> {
            if (count <= 0) {
                throw new IllegalArgumentException("count must be > 0");
            }
            EntryRecord record = liveZSetRecord(scope, keyBytes);
            if (record == null) {
                return new MaterializedCollectionScanWindow(ScanCursorV2.start(), List.of());
            }
            return zsetRoot.zscan(
                    requireZSetHandle(record),
                    cursor == null ? ScanCursorV2.start() : cursor,
                    globPattern,
                    count
            );
        }));
    }

    @Override
    public WriteResult<Long> zrem(byte[] keyBytes, List<byte[]> members) {
        return zrem(MutationContext.none(), keyBytes, members);
    }

    WriteResult<Long> zrem(MutationContext context, byte[] keyBytes, List<byte[]> members) {
        kernel.execute(DbUse.ownerCheck());
        long now = System.currentTimeMillis();
        kernel.reclaimExpiredBeforeMutation(keyBytes, now);
        return removeInternal(context, keyBytes, new ZSetRemoval() {
            @Override
            public int count(ValueHandle handle) {
                return zsetRoot.countExistingMembers(handle, members);
            }

            @Override
            public int remove(ValueHandle handle) {
                return zsetRoot.zrem(handle, members);
            }
        });
    }

    @Override
    public WriteResult<Long> zremrangeByRank(byte[] keyBytes, long start, long stop) {
        return zremrangeByRank(MutationContext.none(), keyBytes, start, stop);
    }

    WriteResult<Long> zremrangeByRank(MutationContext context, byte[] keyBytes, long start, long stop) {
        kernel.execute(DbUse.ownerCheck());
        long now = System.currentTimeMillis();
        kernel.reclaimExpiredBeforeMutation(keyBytes, now);
        return removeInternal(context, keyBytes, new ZSetRemoval() {
            @Override
            public int count(ValueHandle handle) {
                return zsetRoot.countRemovalsByRank(handle, start, stop);
            }

            @Override
            public int remove(ValueHandle handle) {
                return zsetRoot.zremrangeByRank(handle, start, stop);
            }
        });
    }

    @Override
    public WriteResult<Long> zremrangeByScore(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive) {
        return zremrangeByScore(MutationContext.none(), keyBytes, min, minExclusive, max, maxExclusive);
    }

    WriteResult<Long> zremrangeByScore(
            MutationContext context,
            byte[] keyBytes,
            double min,
            boolean minExclusive,
            double max,
            boolean maxExclusive
    ) {
        kernel.execute(DbUse.ownerCheck());
        long now = System.currentTimeMillis();
        kernel.reclaimExpiredBeforeMutation(keyBytes, now);
        return removeInternal(context, keyBytes, new ZSetRemoval() {
            @Override
            public int count(ValueHandle handle) {
                return zsetRoot.countRemovalsByScore(handle, min, minExclusive, max, maxExclusive);
            }

            @Override
            public int remove(ValueHandle handle) {
                return zsetRoot.zremrangeByScore(handle, min, minExclusive, max, maxExclusive);
            }
        });
    }

    private WriteResult<Long> removeInternal(
            MutationContext context,
            byte[] keyBytes,
            ZSetRemoval removal
    ) {
        return kernel.execute(new MutationUse<WriteResult<Long>>() {
            @Override
            public MutationContext context() {
                return context;
            }

            @Override
            public long upperBoundBytes() {
                return 0L;
            }

            @Override
            public Admission admission() {
                return Admission.RECLAMATION;
            }

            @Override
            public PreparedDbMutation<WriteResult<Long>> prepare(YierdisDbKernel scope) {
                CurrentEntry currentEntry = keyLifecycle.currentEntry(keyBytes);
                EntryRecord current = currentEntry.record();
                if (current == null) {
                    return preparedNoEntry(scope, WriteResult.of(0L, MutationOutcome.NONE), MutationOutcome.NONE);
                }
                requireZSet(current);
                ValueHandle handle = requireZSetHandle(current);
                int removed = removal.count(handle);
                if (removed == 0) {
                    return preparedNoEntry(scope, WriteResult.of(0L, MutationOutcome.NONE), MutationOutcome.NONE);
                }

                MutationOutcome outcome = MutationOutcome.VALUE_CHANGED;
                WriteResult<Long> result = WriteResult.of((long) removed, outcome);
                if (removed >= zsetRoot.size(handle)) {
                    return preparedDelete(scope, currentEntry, current, result, outcome);
                }

                EntryRecord next = zsetRecord(
                        currentEntry.keyHandle(),
                        handle,
                        current.expireAtMillis(),
                        current
                );
                long deltaBytes = estimateRecordBytes(currentEntry.keyHandle(), next)
                        - estimateRecordBytes(currentEntry.keyHandle(), current);
                if (deltaBytes > 0L) {
                    throw new IllegalStateException("prepared zset removal must not commit positive growth");
                }
                return scope.callback(
                        result,
                        deltaBytes,
                        0L,
                        outcome,
                        () -> {
                            int actualRemoved = removal.remove(handle);
                            if (actualRemoved != removed) {
                                throw new IllegalStateException("prepared zset removal removed " + actualRemoved
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

    private long estimateZSetWriteUpperBoundForMutation(byte[] keyBytes, List<byte[]> scoreMemberPairs) {
        EntryRecord existing = keyLifecycle.liveEntryRecord(keyBytes);
        if (existing == null) {
            return YierdisDbMemoryEstimator.estimateZSetWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, scoreMemberPairs);
        }
        if (existing.type() != ValueType.ZSET) {
            return 0L;
        }
        long upperBound = YierdisDbMemoryEstimator.estimateZSetWriteUpperBound(0, scoreMemberPairs);
        ValueHandle handle = existing.valueHandle();
        if (zsetRoot.encoding(handle) == ValueEncoding.ZSET_PACKED) {
            upperBound = addSaturating(upperBound, zsetRoot.estimatedBytes(handle));
        }
        return upperBound;
    }

    private static long addSaturating(long left, long right) {
        if (left < 0 || right < 0 || Long.MAX_VALUE - left < right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    private EntryRecord liveZSetRecord(YierdisDbKernel scope, byte[] keyBytes) {
        KeyHandle keyHandle = keyLifecycle.keyHandle(keyBytes);
        EntryRecord record = scope.liveEntryRecord(keyHandle);
        if (record == null) {
            return null;
        }
        requireZSet(record);
        return keyLifecycle.touchRecord(keyHandle, record);
    }

    private <T> PreparedDbMutation<T> preparedDelete(
            YierdisDbKernel scope,
            CurrentEntry currentEntry,
            EntryRecord current,
            T result,
            MutationOutcome outcome
    ) {
        return scope.delete(
                result,
                -estimateRecordBytes(currentEntry.keyHandle(), current),
                outcome,
                currentEntry.entryHandle(),
                current,
                true
        );
    }

    private EntryRecord zsetRecord(KeyHandle keyHandle, ValueHandle handle, long expireAtMillis, EntryRecord previous) {
        return zsetRecord(
                keyHandle,
                handle,
                expireAtMillis,
                previous,
                zsetRoot.encoding(handle)
        );
    }

    private EntryRecord zsetRecord(
            KeyHandle keyHandle,
            ValueHandle handle,
            long expireAtMillis,
            EntryRecord previous,
            ValueEncoding encoding
    ) {
        return keyLifecycle.newRecord(
                keyHandle,
                handle,
                ValueType.ZSET,
                encoding,
                expireAtMillis,
                previous
        );
    }

    private ValueHandle requireZSetHandle(EntryRecord record) {
        ValueHandle handle = record.valueHandle();
        if (!zsetRoot.contains(handle)) {
            throw new IllegalStateException("native zset value handle is not available: " + (handle == null ? "null" : handle.nativeHandle()));
        }
        return handle;
    }

    private static void requireZSet(EntryRecord record) {
        if (record.type() != ValueType.ZSET) {
            throw new WrongTypeException();
        }
    }

    private long estimateRecordBytes(KeyHandle keyHandle, EntryRecord record) {
        return keyLifecycle.estimatedBytesForRemoval(keyHandle, record);
    }

    private interface ZSetRemoval {
        int count(ValueHandle handle);

        int remove(ValueHandle handle);
    }
}
