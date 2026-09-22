package yier.bubu.redis.storage.memory.internal.entry;

import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.api.ZAddOptions;
import yier.bubu.redis.storage.api.result.ByteValueSink;
import yier.bubu.redis.storage.api.result.CollectionScanWindow;
import yier.bubu.redis.storage.memory.internal.hash.HashSeed;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMaintenanceRegistry;
import yier.bubu.redis.storage.memory.internal.hash.SipHashIntIndex;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;
import yier.bubu.redis.storage.memory.internal.value.ZSetValue;
import yier.bubu.redis.storage.memory.internal.value.ZSetValue.ZAddResult;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public final class ZSetRoot implements AutoCloseable {
    private final NativeCollectionRootTable<ZSetValue> zsets;
    private final HashSeed hashSeed;
    private final HashTableMaintenanceRegistry maintenanceRegistry;
    private boolean closed;

    public ZSetRoot(
            StableMemoryBackend allocator,
            HashSeed hashSeed,
            HashTableMaintenanceRegistry maintenanceRegistry
    ) {
        this.hashSeed = Objects.requireNonNull(hashSeed, "hashSeed");
        this.maintenanceRegistry = maintenanceRegistry;
        this.zsets = new NativeCollectionRootTable<>(
                Objects.requireNonNull(allocator, "allocator"),
                NativeObjectKind.ZSET_ROOT,
                "zset"
        );
    }

    StableMemoryBackend allocator() {
        return zsets.allocator();
    }

    public synchronized ValueEncoding encoding(ValueHandle handle) {
        return requireZSet(handle).encoding();
    }

    public synchronized long heapEstimatedBytes(ValueHandle handle) {
        ensureOpen();
        return requireZSet(handle).heapEstimatedBytes();
    }

    public synchronized boolean contains(ValueHandle handle) {
        ensureOpen();
        return zsets.contains(handle);
    }

    public synchronized ValueHandle create() {
        ensureOpen();
        return zsets.create(ignored -> newZSetValue());
    }

    public synchronized PreparedAddResult prepareAdd(ValueHandle source, List<byte[]> scoreMemberPairs) {
        return prepareAdd(planAdd(source, scoreMemberPairs));
    }

    public synchronized AddPlan planAdd(ValueHandle source, List<byte[]> scoreMemberPairs) {
        return planAdd(source, scoreMemberPairs, ZAddOptions.plain());
    }

    public synchronized AddPlan planAdd(ValueHandle source, List<byte[]> scoreMemberPairs, ZAddOptions options) {
        ensureOpen();
        Objects.requireNonNull(scoreMemberPairs, "scoreMemberPairs");
        Objects.requireNonNull(options, "options");
        if (source != null) {
            ZSetValue.ZAddPlan deltaPlan = requireZSet(source).planExistingAdd(scoreMemberPairs, options);
            return new AddPlan(
                    source,
                    deltaPlan,
                    scoreMemberPairs,
                    options,
                    deltaPlan.nativeAllocationSizes()
            );
        }
        ZSetValue.PackedBuildPlan packedPlan = ZSetValue.preparedNewPackedBuildPlan(scoreMemberPairs);
        return new AddPlan(
                null,
                null,
                scoreMemberPairs,
                options,
                newValueAllocationSizes(scoreMemberPairs, packedPlan)
        );
    }

    public synchronized PreparedAddResult prepareAdd(AddPlan plan) {
        ensureOpen();
        Objects.requireNonNull(plan, "plan");
        if (plan.deltaPlan() != null) {
            ZSetValue sourceValue = requireZSet(plan.source());
            ZSetValue.PreparedExistingAdd delta = sourceValue.prepareExistingAdd(plan.deltaPlan());
            return new PreparedAddResult(
                    plan.source(),
                    delta.result(),
                    delta.stagedHeapBytes(),
                    delta.targetEncoding(),
                    delta.targetHeapEstimatedBytes(),
                    delta
            );
        }

        long heapBefore = retainedHeapBytes();
        ZSetValue.PackedBuildPlan packedPlan = ZSetValue.preparedNewPackedBuildPlan(plan.scoreMemberPairs());
        ValueHandle replacement = create();
        boolean ok = false;
        try {
            ZSetValue value = requireZSet(replacement);
            if (packedPlan == null) {
                value.prepareSkiplistForBuild();
            } else {
                value.reservePackedForBuild(packedPlan);
            }
            ZAddResult added;
            try {
                added = value.add(plan.scoreMemberPairs(), plan.options());
            } finally {
                zsets.refreshAdapter(replacement);
            }
            ok = true;
            return new PreparedAddResult(
                    replacement,
                    added,
                    positiveDelta(retainedHeapBytes(), heapBefore),
                    value.encoding(),
                    value.heapEstimatedBytes(),
                    null
            );
        } finally {
            if (!ok && replacement != null) {
                release(replacement);
            }
        }
    }

    public synchronized int[] preparedAddNativeAllocationSizes(
            ValueHandle source,
            List<byte[]> scoreMemberPairs
    ) {
        return planAdd(source, scoreMemberPairs).nativeAllocationSizes();
    }

    public synchronized int[] preparedAddNativeAllocationSizes(AddPlan plan) {
        ensureOpen();
        Objects.requireNonNull(plan, "plan");
        return plan.nativeAllocationSizes();
    }

    public synchronized long estimatedPreparedAddHeapGrowthBytes(
            ValueHandle source,
            List<byte[]> scoreMemberPairs
    ) {
        return estimatedPreparedAddHeapGrowthBytes(
                planAdd(source, scoreMemberPairs)
        );
    }

    public synchronized long estimatedPreparedAddHeapGrowthBytes(
            AddPlan plan
    ) {
        ensureOpen();
        Objects.requireNonNull(plan, "plan");
        if (plan.deltaPlan() != null) {
            return plan.deltaPlan().stagedHeapBytes();
        }
        long replacementHeapBytes = ZSetValue.preparedNewHeapUpperBound(plan.scoreMemberPairs());
        return zsets.estimatedNewAdapterHeapGrowthBytes(replacementHeapBytes);
    }

    public synchronized ZAddResult zaddResult(ValueHandle handle, List<byte[]> scoreMemberPairs) {
        ensureOpen();
        ZSetValue value = requireZSet(handle);
        try {
            return value.zaddManyResult(scoreMemberPairs);
        } finally {
            zsets.refreshAdapter(handle);
        }
    }

    public synchronized int zadd(ValueHandle handle, List<byte[]> scoreMemberPairs) {
        ensureOpen();
        ZSetValue value = requireZSet(handle);
        try {
            return value.zaddMany(scoreMemberPairs);
        } finally {
            zsets.refreshAdapter(handle);
        }
    }

    public synchronized int zrem(ValueHandle handle, List<byte[]> members) {
        ensureOpen();
        ZSetValue value = requireZSet(handle);
        try {
            return value.zrem(members);
        } finally {
            zsets.refreshAdapter(handle);
        }
    }

    public synchronized int countExistingMembers(ValueHandle handle, List<byte[]> members) {
        ensureOpen();
        return requireZSet(handle).countExistingMembers(members);
    }

    public synchronized int zremrangeByRank(ValueHandle handle, long start, long stop) {
        ensureOpen();
        ZSetValue value = requireZSet(handle);
        try {
            return value.zremrangeByRank(start, stop);
        } finally {
            zsets.refreshAdapter(handle);
        }
    }

    public synchronized int countRemovalsByRank(ValueHandle handle, long start, long stop) {
        ensureOpen();
        return requireZSet(handle).countRemovalsByRank(start, stop);
    }

    public synchronized int zremrangeByScore(
            ValueHandle handle,
            double min,
            boolean minExclusive,
            double max,
            boolean maxExclusive
    ) {
        ensureOpen();
        ZSetValue value = requireZSet(handle);
        try {
            return value.zremrangeByScore(min, minExclusive, max, maxExclusive);
        } finally {
            zsets.refreshAdapter(handle);
        }
    }

    public synchronized int countRemovalsByScore(
            ValueHandle handle,
            double min,
            boolean minExclusive,
            double max,
            boolean maxExclusive
    ) {
        ensureOpen();
        return requireZSet(handle).countRemovalsByScore(min, minExclusive, max, maxExclusive);
    }

    public synchronized void zrangeWriteTo(ValueHandle handle, long start, long stop, boolean withScores, ByteValueSink out) {
        ensureOpen();
        requireZSet(handle).zrangeWriteTo(start, stop, withScores, out);
    }

    public synchronized CollectionScanWindow zscan(
            ValueHandle handle,
            ScanCursorV2 cursor,
            byte[] globPattern,
            int count
    ) {
        ensureOpen();
        return requireZSet(handle).zscan(cursor, globPattern, count);
    }

    public synchronized void zrevrangeWriteTo(ValueHandle handle, long start, long stop, boolean withScores, ByteValueSink out) {
        ensureOpen();
        requireZSet(handle).zrevrangeWriteTo(start, stop, withScores, out);
    }

    public synchronized void zrangeByScoreWriteTo(
            ValueHandle handle,
            double min,
            boolean minExclusive,
            double max,
            boolean maxExclusive,
            boolean withScores,
            long offset,
            long count,
            ByteValueSink out
    ) {
        ensureOpen();
        requireZSet(handle).zrangeByScoreWriteTo(min, minExclusive, max, maxExclusive, withScores, offset, count, out);
    }

    public synchronized void zrevrangeByScoreWriteTo(
            ValueHandle handle,
            double min,
            boolean minExclusive,
            double max,
            boolean maxExclusive,
            boolean withScores,
            long offset,
            long count,
            ByteValueSink out
    ) {
        ensureOpen();
        requireZSet(handle).zrevrangeByScoreWriteTo(min, minExclusive, max, maxExclusive, withScores, offset, count, out);
    }

    public synchronized int size(ValueHandle handle) {
        ensureOpen();
        return requireZSet(handle).size();
    }

    public synchronized long estimatedBytes(ValueHandle handle) {
        ensureOpen();
        return requireZSet(handle).estimatedBytes();
    }

    public synchronized long nativeBytes() {
        return zsets.adapterBytes(ZSetValue::estimatedBytes);
    }

    public synchronized long heapBytes() {
        ensureOpen();
        return zsets.heapBytes();
    }

    public synchronized void forEachNativeHandle(ValueHandle handle, Consumer<NativeHandle> consumer) {
        ensureOpen();
        requireZSet(handle).forEachNativeHandle(consumer);
    }

    public synchronized void release(ValueHandle handle) {
        zsets.release(handle);
    }

    public synchronized void clear() {
        ensureOpen();
        zsets.clear();
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        zsets.close();
        closed = true;
    }

    private ZSetValue requireZSet(ValueHandle handle) {
        return zsets.require(handle);
    }

    private ZSetValue newZSetValue() {
        return new ZSetValue(zsets.allocator(), hashSeed, maintenanceRegistry);
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("zset root is closed");
        }
    }

    public record AddPlan(
            ValueHandle source,
            ZSetValue.ZAddPlan deltaPlan,
            List<byte[]> scoreMemberPairs,
            ZAddOptions options,
            int[] allocationSizes
    ) {
        public AddPlan {
            Objects.requireNonNull(scoreMemberPairs, "scoreMemberPairs");
            Objects.requireNonNull(options, "options");
            if ((source == null) != (deltaPlan == null)) {
                throw new IllegalArgumentException("ZADD plan source and delta path do not match");
            }
            allocationSizes = Objects.requireNonNull(allocationSizes, "allocationSizes").clone();
        }

        @Override
        public int[] allocationSizes() {
            return allocationSizes.clone();
        }

        public int[] nativeAllocationSizes() {
            return allocationSizes();
        }

        public boolean stableHandle() {
            return deltaPlan != null;
        }
    }

    public static final class PreparedAddResult implements AutoCloseable {
        private final ValueHandle handle;
        private final ZAddResult result;
        private final long stagedNonNativeGrowthBytes;
        private final ValueEncoding targetEncoding;
        private final long targetHeapEstimatedBytes;
        private final ZSetValue.PreparedExistingAdd delta;

        private PreparedAddResult(
                ValueHandle handle,
                ZAddResult result,
                long stagedNonNativeGrowthBytes,
                ValueEncoding targetEncoding,
                long targetHeapEstimatedBytes,
                ZSetValue.PreparedExistingAdd delta
        ) {
            this.handle = Objects.requireNonNull(handle, "handle");
            this.result = Objects.requireNonNull(result, "result");
            this.stagedNonNativeGrowthBytes = Math.max(0L, stagedNonNativeGrowthBytes);
            this.targetEncoding = Objects.requireNonNull(targetEncoding, "targetEncoding");
            if (targetHeapEstimatedBytes < 0L) {
                throw new IllegalArgumentException("targetHeapEstimatedBytes must be >= 0");
            }
            this.targetHeapEstimatedBytes = targetHeapEstimatedBytes;
            this.delta = delta;
        }

        public ValueHandle handle() {
            return handle;
        }

        public ZAddResult result() {
            return result;
        }

        public long stagedNonNativeGrowthBytes() {
            return stagedNonNativeGrowthBytes;
        }

        public ValueEncoding targetEncoding() {
            return targetEncoding;
        }

        public long targetHeapEstimatedBytes() {
            return targetHeapEstimatedBytes;
        }

        public boolean changedAny() {
            return result.changedAny();
        }

        public int added() {
            return result.added();
        }

        public boolean stableHandle() {
            return delta != null;
        }

        public void commit() {
            if (delta != null) {
                delta.commit();
            }
        }

        public void releaseSuperseded() {
            if (delta != null) {
                delta.releaseSuperseded();
            }
        }

        @Override
        public void close() {
            if (delta != null) {
                delta.close();
            }
        }
    }

    private int[] newValueAllocationSizes(
            List<byte[]> scoreMemberPairs,
            ZSetValue.PackedBuildPlan packedPlan
    ) {
        if (packedPlan != null) {
            return packedPlan.encodedBytes() == 0
                    ? new int[0]
                    : new int[]{packedPlan.encodedBytes()};
        }
        SipHashIntIndex memberIndex = new SipHashIntIndex(
                scoreMemberPairs::get,
                scoreMemberPairs.size() / 2L,
                hashSeed,
                "too many ZADD members to stage"
        );
        int uniqueCount = 0;
        for (int index = 1; index < scoreMemberPairs.size(); index += 2) {
            if (memberIndex.addIfAbsent(index)) {
                uniqueCount++;
            }
        }
        int[] sizes = new int[uniqueCount];
        int next = 0;
        memberIndex.clear();
        for (int index = 1; index < scoreMemberPairs.size(); index += 2) {
            if (memberIndex.addIfAbsent(index)) {
                sizes[next++] = Math.max(1, scoreMemberPairs.get(index).length);
            }
        }
        return sizes;
    }

    private long retainedHeapBytes() {
        long registryHeapBytes = maintenanceRegistry == null ? 0L : maintenanceRegistry.heapEstimatedBytes();
        long rootHeapBytes = zsets.heapBytes();
        return rootHeapBytes > Long.MAX_VALUE - registryHeapBytes
                ? Long.MAX_VALUE
                : rootHeapBytes + registryHeapBytes;
    }

    private static long positiveDelta(long after, long before) {
        return after > before ? after - before : 0L;
    }

}
