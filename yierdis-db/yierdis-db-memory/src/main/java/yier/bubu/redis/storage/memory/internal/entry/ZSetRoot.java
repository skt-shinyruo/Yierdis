package yier.bubu.redis.storage.memory.internal.entry;

import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisStableNativeAllocator;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.result.BulkStringSink;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;
import yier.bubu.redis.storage.memory.internal.value.ZSetValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class ZSetRoot implements TypeRoot {
    private final YierdisFfmMemoryRuntime runtime;
    private final NativeCollectionRootTable<ZSetValue> zsets;
    private boolean closed;

    public ZSetRoot(YierdisFfmMemoryRuntime runtime) {
        this(runtime, new YierdisStableNativeAllocator(Objects.requireNonNull(runtime, "runtime"), 4096), true);
    }

    public ZSetRoot(YierdisFfmMemoryRuntime runtime, NativeAllocator allocator) {
        this(runtime, allocator, false);
    }

    public ZSetRoot(NativeAllocator allocator) {
        this(null, allocator, false);
    }

    private ZSetRoot(YierdisFfmMemoryRuntime runtime, NativeAllocator allocator, boolean ownsAllocator) {
        this.runtime = runtime;
        this.zsets = new NativeCollectionRootTable<>(
                allocator,
                NativeObjectKind.ZSET_NODE,
                "zset",
                ownsAllocator
        );
    }

    NativeAllocator allocator() {
        return zsets.allocator();
    }

    @Override
    public ValueType type() {
        return ValueType.ZSET;
    }

    @Override
    public ValueEncoding encoding() {
        return ValueEncoding.ZSET_PACKED;
    }

    public synchronized ValueEncoding encoding(ValueHandle handle) {
        return requireZSet(handle).encoding();
    }

    public synchronized boolean contains(ValueHandle handle) {
        ensureOpen();
        return zsets.contains(handle);
    }

    public synchronized ValueHandle create() {
        ensureOpen();
        return zsets.create(this::newZSetValue);
    }

    public synchronized ValueHandle store(ZSetValue value) {
        ensureOpen();
        Objects.requireNonNull(value, "value");
        ValueHandle handle = create();
        boolean ok = false;
        try {
            requireZSet(handle).zaddMany(memberScorePairsToScoreMemberPairs(value.zrange(0, -1, true)));
            ok = true;
            return handle;
        } finally {
            if (!ok) {
                release(handle);
            }
        }
    }

    public synchronized int zadd(ValueHandle handle, List<byte[]> scoreMemberPairs, boolean[] changedRef) {
        ensureOpen();
        return requireZSet(handle).zaddMany(scoreMemberPairs, changedRef);
    }

    public synchronized int zadd(ValueHandle handle, List<byte[]> scoreMemberPairs) {
        ensureOpen();
        return requireZSet(handle).zaddMany(scoreMemberPairs);
    }

    public synchronized int zrem(ValueHandle handle, List<byte[]> members) {
        ensureOpen();
        return requireZSet(handle).zrem(members);
    }

    public synchronized int zremrangeByRank(ValueHandle handle, long start, long stop) {
        ensureOpen();
        return requireZSet(handle).zremrangeByRank(start, stop);
    }

    public synchronized int zremrangeByScore(
            ValueHandle handle,
            double min,
            boolean minExclusive,
            double max,
            boolean maxExclusive
    ) {
        ensureOpen();
        return requireZSet(handle).zremrangeByScore(min, minExclusive, max, maxExclusive);
    }

    public synchronized int zrangeCount(ValueHandle handle, long start, long stop, boolean withScores) {
        ensureOpen();
        return requireZSet(handle).zrangeCount(start, stop, withScores);
    }

    public synchronized void zrangeWriteTo(ValueHandle handle, long start, long stop, boolean withScores, BulkStringSink out) {
        ensureOpen();
        requireZSet(handle).zrangeWriteTo(start, stop, withScores, out);
    }

    public synchronized int zrevrangeCount(ValueHandle handle, long start, long stop, boolean withScores) {
        ensureOpen();
        return requireZSet(handle).zrevrangeCount(start, stop, withScores);
    }

    public synchronized void zrevrangeWriteTo(ValueHandle handle, long start, long stop, boolean withScores, BulkStringSink out) {
        ensureOpen();
        requireZSet(handle).zrevrangeWriteTo(start, stop, withScores, out);
    }

    public synchronized int zrangeByScoreCount(
            ValueHandle handle,
            double min,
            boolean minExclusive,
            double max,
            boolean maxExclusive,
            boolean withScores,
            long offset,
            long count
    ) {
        ensureOpen();
        return requireZSet(handle).zrangeByScoreCount(min, minExclusive, max, maxExclusive, withScores, offset, count);
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
            BulkStringSink out
    ) {
        ensureOpen();
        requireZSet(handle).zrangeByScoreWriteTo(min, minExclusive, max, maxExclusive, withScores, offset, count, out);
    }

    public synchronized int zrevrangeByScoreCount(
            ValueHandle handle,
            double min,
            boolean minExclusive,
            double max,
            boolean maxExclusive,
            boolean withScores,
            long offset,
            long count
    ) {
        ensureOpen();
        return requireZSet(handle).zrevrangeByScoreCount(min, minExclusive, max, maxExclusive, withScores, offset, count);
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
            BulkStringSink out
    ) {
        ensureOpen();
        requireZSet(handle).zrevrangeByScoreWriteTo(min, minExclusive, max, maxExclusive, withScores, offset, count, out);
    }

    public synchronized int size(ValueHandle handle) {
        ensureOpen();
        return requireZSet(handle).size();
    }

    @Override
    public synchronized long estimatedBytes(ValueHandle handle) {
        ensureOpen();
        return requireZSet(handle).estimatedBytes();
    }

    public synchronized long nativeBytes() {
        return zsets.adapterBytes(ZSetValue::estimatedBytes);
    }

    @Override
    public synchronized void release(ValueHandle handle) {
        zsets.release(handle);
    }

    @Override
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
        return runtime == null ? new ZSetValue() : new ZSetValue(runtime);
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("zset root is closed");
        }
    }

    private static List<byte[]> memberScorePairsToScoreMemberPairs(List<byte[]> memberScorePairs) {
        ArrayList<byte[]> out = new ArrayList<>(memberScorePairs.size());
        for (int i = 0; i + 1 < memberScorePairs.size(); i += 2) {
            out.add(memberScorePairs.get(i + 1));
            out.add(memberScorePairs.get(i));
        }
        return out;
    }

}
