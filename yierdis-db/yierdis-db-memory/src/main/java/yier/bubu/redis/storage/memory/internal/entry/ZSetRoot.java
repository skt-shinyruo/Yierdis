package yier.bubu.redis.storage.memory.internal.entry;

import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.result.BulkStringSink;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;
import yier.bubu.redis.storage.memory.internal.value.ZSetValue;
import yier.bubu.redis.storage.memory.internal.value.ZSetValue.ZAddResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public final class ZSetRoot implements TypeRoot {
    private final NativeCollectionRootTable<ZSetValue> zsets;
    private boolean closed;

    public ZSetRoot(NativeAllocator allocator) {
        this.zsets = new NativeCollectionRootTable<>(
                Objects.requireNonNull(allocator, "allocator"),
                NativeObjectKind.ZSET_ROOT,
                "zset",
                false
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

    public synchronized ValueHandle copy(ValueHandle source) {
        ensureOpen();
        ZSetValue current = requireZSet(source);
        ValueHandle replacement = create();
        boolean ok = false;
        try {
            requireZSet(replacement).zaddMany(memberScorePairsToScoreMemberPairs(current.zrange(0, -1, true)));
            ok = true;
            return replacement;
        } finally {
            if (!ok) {
                release(replacement);
            }
        }
    }

    public synchronized PreparedAddResult prepareAdd(ValueHandle source, List<byte[]> scoreMemberPairs) {
        ensureOpen();
        Objects.requireNonNull(scoreMemberPairs, "scoreMemberPairs");
        ValueHandle replacement = source == null ? create() : copy(source);
        boolean ok = false;
        try {
            ZAddResult added = requireZSet(replacement).prepareAdd(scoreMemberPairs);
            if (source != null && !added.changedAny()) {
                release(replacement);
                return new PreparedAddResult(null, added);
            }
            ok = true;
            return new PreparedAddResult(replacement, added);
        } finally {
            if (!ok && replacement != null) {
                release(replacement);
            }
        }
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

    public synchronized ZAddResult zaddResult(ValueHandle handle, List<byte[]> scoreMemberPairs) {
        ensureOpen();
        return requireZSet(handle).zaddManyResult(scoreMemberPairs);
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

    public synchronized List<byte[]> zrange(ValueHandle handle, long start, long stop, boolean withScores) {
        ensureOpen();
        return requireZSet(handle).zrange(start, stop, withScores);
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

    public synchronized int[] nativePayloadSizes(ValueHandle handle) {
        ensureOpen();
        return requireZSet(handle).nativePayloadSizes();
    }

    @Override
    public synchronized long estimatedBytes(ValueHandle handle) {
        ensureOpen();
        return requireZSet(handle).estimatedBytes();
    }

    public synchronized long nativeBytes() {
        return zsets.adapterBytes(ZSetValue::estimatedBytes);
    }

    public synchronized void forEachNativeHandle(ValueHandle handle, Consumer<NativeHandle> consumer) {
        ensureOpen();
        requireZSet(handle).forEachNativeHandle(consumer);
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
        return new ZSetValue(zsets.allocator());
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

    public record PreparedAddResult(ValueHandle handle, ZAddResult result) {
        public boolean changedAny() {
            return result.changedAny();
        }

        public int added() {
            return result.added();
        }
    }

}
