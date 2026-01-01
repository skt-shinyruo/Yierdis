package yier.bubu.redis.db;

import yier.bubu.redis.db.offheap.api.YierdisOffHeapAllocator;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapOutOfMemoryException;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapSlice;
import yier.bubu.redis.db.offheap.YierdisUnsafeOffHeapExpireIndex;
import yier.bubu.redis.db.offheap.YierdisUnsafeOffHeapKeyspace;
import yier.bubu.redis.db.offheap.unsafe.YierdisUnsafeOffHeapAllocator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public final class YierdisDb {
    public enum MaxmemoryPolicy {
        NOEVICTION,
        ALLKEYS_RANDOM,
        ALLKEYS_LRU
    }

    private final YierdisKeyspace<YierdisObject> store;
    private final YierdisExpireIndex expires;
    private final YierdisOffHeapAllocator offHeapAllocator;

    private static final int ENTRY_OVERHEAD_BYTES = 16;
    private static final String OOM_ERR = "OOM command not allowed when used memory > 'maxmemory'.";

    private final long maxmemoryBytes;
    private final MaxmemoryPolicy maxmemoryPolicy;
    private final int maxmemorySamples;
    private final boolean lruEnabled;

    private long usedBytes;
    private long lruClock;

    private Thread ownerThread;
    private boolean closed;

    public YierdisDb() {
        this(null, 0, "noeviction", 5);
    }

    public YierdisDb(YierdisOffHeapAllocator offHeapAllocator) {
        this(offHeapAllocator, 0, "noeviction", 5);
    }

    public YierdisDb(YierdisOffHeapAllocator offHeapAllocator, long maxmemoryBytes, String maxmemoryPolicy, int maxmemorySamples) {
        this.offHeapAllocator = offHeapAllocator;
        if (offHeapAllocator instanceof YierdisUnsafeOffHeapAllocator unsafeAllocator) {
            this.store = new YierdisUnsafeOffHeapKeyspace<>(unsafeAllocator);
            this.expires = new YierdisUnsafeOffHeapExpireIndex(unsafeAllocator);
        } else {
            this.store = new ByteArrayKeyspace<>();
            this.expires = new YierdisHeapExpireIndex();
        }
        if (maxmemoryBytes < 0) {
            throw new IllegalArgumentException("maxmemoryBytes must be >= 0");
        }
        if (maxmemorySamples <= 0) {
            throw new IllegalArgumentException("maxmemorySamples must be > 0");
        }

        this.maxmemoryBytes = maxmemoryBytes;
        this.maxmemoryPolicy = parseMaxmemoryPolicy(maxmemoryPolicy);
        this.maxmemorySamples = maxmemorySamples;
        this.lruEnabled = maxmemoryBytes > 0 && this.maxmemoryPolicy == MaxmemoryPolicy.ALLKEYS_LRU;
        // Scheduling (if any) is done by the Netty event loop in YierdisServer, not by a dedicated thread.
    }

    private static MaxmemoryPolicy parseMaxmemoryPolicy(String policy) {
        if (policy == null || policy.isBlank()) {
            return MaxmemoryPolicy.NOEVICTION;
        }
        String normalized = policy.trim().toLowerCase(java.util.Locale.ROOT).replace('_', '-');
        switch (normalized) {
            case "noeviction":
                return MaxmemoryPolicy.NOEVICTION;
            case "allkeys-random":
                return MaxmemoryPolicy.ALLKEYS_RANDOM;
            case "allkeys-lru":
                return MaxmemoryPolicy.ALLKEYS_LRU;
            default:
                throw new IllegalArgumentException("unsupported maxmemoryPolicy: " + policy);
        }
    }

    public void ensureWriteAllowed(long additionalBytes) {
        checkThread();
        if (maxmemoryBytes <= 0) {
            return;
        }
        if (maxmemoryPolicy != MaxmemoryPolicy.NOEVICTION) {
            return;
        }
        long extra = Math.max(0, additionalBytes);
        if (usedBytesForMaxmemory() + extra > maxmemoryBytes) {
            throw new YierdisCommandException(OOM_ERR);
        }
    }

    public void enforceMaxmemory() {
        checkThread();
        if (maxmemoryBytes <= 0) {
            return;
        }

        // Best-effort: try to reclaim expired keys first (Redis does this too under pressure).
        cleanupExpired();

        if (usedBytesForMaxmemory() <= maxmemoryBytes) {
            return;
        }

        if (maxmemoryPolicy == MaxmemoryPolicy.NOEVICTION) {
            throw new YierdisCommandException(OOM_ERR);
        }

        int attempts = 0;
        int maxAttempts = Math.max(64, store.size() * 2);
        long nowMillis = System.currentTimeMillis();
        while (usedBytesForMaxmemory() > maxmemoryBytes && attempts++ < maxAttempts) {
            byte[] victim = pickEvictionKey(nowMillis);
            if (victim == null) {
                break;
            }
            YierdisObject e = store.get(victim);
            if (e == null) {
                continue;
            }
            if (removeIfExpired(victim, e, nowMillis)) {
                continue;
            }
            removeExpire(victim);
            if (store.remove(victim, e)) {
                e.releasePayloadIfAny();
                usedBytes -= e.estimatedBytes;
            }
        }

        if (usedBytesForMaxmemory() > maxmemoryBytes) {
            throw new YierdisCommandException(OOM_ERR);
        }
    }

    public long memoryUsage(YierdisBytesView keyView) {
        checkThread();
        byte[] canonical = store.canonicalKey(keyView);
        if (canonical == null) {
            return -1;
        }
        YierdisObject e = store.get(canonical);
        if (e == null) {
            return -1;
        }
        long now = System.currentTimeMillis();
        if (removeIfExpired(canonical, e, now)) {
            return -1;
        }
        touch(e);
        return e.estimatedBytes;
    }

    public long memoryUsage(byte[] keyBytes) {
        checkThread();
        if (keyBytes == null) {
            return -1;
        }
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return -1;
        }
        return e.estimatedBytes;
    }

    public String objectEncoding(YierdisBytesView keyView) {
        checkThread();
        byte[] canonical = store.canonicalKey(keyView);
        if (canonical == null) {
            return null;
        }
        YierdisObject e = store.get(canonical);
        if (e == null) {
            return null;
        }
        long now = System.currentTimeMillis();
        if (removeIfExpired(canonical, e, now)) {
            return null;
        }
        touch(e);
        return encodingName(e.encoding);
    }

    public String objectEncoding(byte[] keyBytes) {
        checkThread();
        if (keyBytes == null) {
            return null;
        }
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return null;
        }
        return encodingName(e.encoding);
    }

    private byte[] pickEvictionKey(long nowMillis) {
        if (store.size() == 0) {
            return null;
        }

        if (maxmemoryPolicy == MaxmemoryPolicy.ALLKEYS_RANDOM) {
            return store.randomKey();
        }

        if (maxmemoryPolicy != MaxmemoryPolicy.ALLKEYS_LRU) {
            return null;
        }

        int total = store.size();
        byte[] bestKey = null;
        long bestLru = Long.MAX_VALUE;
        int samples = Math.max(1, maxmemorySamples);

        // If the caller asks for samples >= total keys, do a deterministic full scan.
        // This is both more effective (better victim selection) and avoids test flakiness.
        if (samples >= total) {
            final byte[][] bestKeyRef = new byte[1][];
            final long[] bestLruRef = new long[]{Long.MAX_VALUE};
            store.forEach((k, e) -> {
                if (isKeyExpired(k, nowMillis)) {
                    return;
                }
                long lru = e.lruClock;
                if (bestKeyRef[0] == null || lru < bestLruRef[0]) {
                    bestKeyRef[0] = k;
                    bestLruRef[0] = lru;
                }
            });
            return bestKeyRef[0];
        }

        for (int i = 0; i < samples; i++) {
            byte[] k = store.randomKey();
            if (k == null) {
                break;
            }
            YierdisObject e = store.get(k);
            if (e == null) {
                continue;
            }
            if (isKeyExpired(k, nowMillis)) {
                continue;
            }
            long lru = e.lruClock;
            if (bestKey == null || lru < bestLru) {
                bestKey = k;
                bestLru = lru;
            }
        }
        return bestKey;
    }

    private long usedBytesForMaxmemory() {
        return usedBytes;
    }

    private void touch(YierdisObject e) {
        if (!lruEnabled || e == null) {
            return;
        }
        e.lruClock = ++lruClock;
    }

    private static String encodingName(ValueEncoding encoding) {
        if (encoding == null) {
            return "unknown";
        }
        switch (encoding) {
            case STRING_INT:
                return "int";
            case STRING_EMBSTR:
                return "embstr";
            case STRING_RAW:
                return "raw";
            case HASH_PACKED:
            case LIST_PACKED:
            case ZSET_PACKED:
                return "listpack";
            case HASH_HT:
            case SET_HT:
                return "hashtable";
            case SET_INTSET:
                return "intset";
            case LIST_QUICKLIST:
                return "quicklist";
            case ZSET_SKIPLIST:
                return "skiplist";
            default:
                return encoding.name().toLowerCase(java.util.Locale.ROOT);
        }
    }

    public void bindToCurrentThread() {
        if (ownerThread == null) {
            ownerThread = Thread.currentThread();
        }
    }

    private void checkThread() {
        Thread owner = ownerThread;
        if (owner != null && owner != Thread.currentThread()) {
            throw new IllegalStateException("YierdisDb accessed from non-owner thread");
        }
    }

    public void shutdown() {
        if (closed) {
            return;
        }
        closed = true;
        store.forEach((k, e) -> e.releasePayloadIfAny());
        store.clear();
        expires.clear();
        usedBytes = 0;
        if (offHeapAllocator != null) {
            offHeapAllocator.close();
        }
    }

    public void flushDb() {
        checkThread();
        store.forEach((k, e) -> e.releasePayloadIfAny());
        store.clear();
        expires.clear();
        usedBytes = 0;
    }

    public int size() {
        checkThread();
        return store.size();
    }

    public long estimatedUsedBytes() {
        checkThread();
        return usedBytesForMaxmemory();
    }

    private long estimateEntryBytes(byte[] keyBytes, YierdisObject e) {
        if (keyBytes == null || e == null) {
            return 0;
        }
        int keyBytesCost = keyBytes.length;
        return ENTRY_OVERHEAD_BYTES + keyBytesCost + estimateValueBytes(e);
    }

    private long estimateValueBytes(YierdisObject e) {
        if (e == null) {
            return 0;
        }
        if (e.type == ValueType.STRING) {
            if (e.encoding == ValueEncoding.STRING_INT) {
                return Long.BYTES;
            }
            return e.rawLen;
        }

        if (e.payload instanceof HashValue hv) {
            return hv.estimatedBytes();
        }
        if (e.payload instanceof ListValue lv) {
            return lv.estimatedBytes();
        }
        if (e.payload instanceof SetValue sv) {
            return sv.estimatedBytes();
        }
        if (e.payload instanceof ZSetValue zv) {
            return zv.estimatedBytes();
        }

        return 0;
    }

    private void refreshEstimatedBytes(byte[] keyBytes, YierdisObject e) {
        if (e == null) {
            return;
        }
        e.estimatedBytes = estimateEntryBytes(keyBytes, e);
    }

    public long del(Collection<byte[]> keys) {
        checkThread();
        long now = System.currentTimeMillis();
        long removed = 0;
        for (byte[] keyBytes : keys) {
            YierdisObject e = store.get(keyBytes);
            if (e == null) {
                continue;
            }
            if (removeIfExpired(keyBytes, e, now)) {
                continue;
            }
            removeExpire(keyBytes);
            if (store.remove(keyBytes, e)) {
                e.releasePayloadIfAny();
                usedBytes -= e.estimatedBytes;
                removed++;
            }
        }
        return removed;
    }

    public boolean existsKey(YierdisBytesView keyView) {
        checkThread();
        byte[] canonical = store.canonicalKey(keyView);
        if (canonical == null) {
            return false;
        }
        return getObjectIfNotExpired(canonical) != null;
    }

    public long exists(Collection<byte[]> keys) {
        checkThread();
        long count = 0;
        for (byte[] keyBytes : keys) {
            if (getObjectIfNotExpired(keyBytes) != null) {
                count++;
            }
        }
        return count;
    }

    public ValueType typeOf(YierdisBytesView keyView) {
        checkThread();
        byte[] canonical = store.canonicalKey(keyView);
        if (canonical == null) {
            return null;
        }
        return typeOf(canonical);
    }

    public ValueType typeOf(byte[] keyBytes) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return null;
        }
        return e.type;
    }

    public boolean expire(byte[] keyBytes, long seconds) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return false;
        }
        long expireAtMillis = System.currentTimeMillis() + Math.max(0, seconds) * 1000L;
        setExpireAtMillis(keyBytes, expireAtMillis);
        return true;
    }

    public boolean expire(YierdisBytesView keyView, long seconds) {
        checkThread();
        byte[] canonical = store.canonicalKey(keyView);
        if (canonical == null) {
            return false;
        }
        return expire(canonical, seconds);
    }

    public long ttlSeconds(byte[] keyBytes) {
        checkThread();
        YierdisObject e = store.get(keyBytes);
        if (e == null) {
            return -2;
        }
        long now = System.currentTimeMillis();
        if (removeIfExpired(keyBytes, e, now)) {
            return -2;
        }
        touch(e);

        Long expireAtMillis = expires.get(keyBytes);
        if (expireAtMillis == null) {
            return -1;
        }
        long remainingMillis = expireAtMillis - now;
        return remainingMillis <= 0 ? -2 : remainingMillis / 1000L;
    }

    public long ttlSeconds(YierdisBytesView keyView) {
        checkThread();
        byte[] canonical = store.canonicalKey(keyView);
        if (canonical == null) {
            return -2;
        }
        return ttlSeconds(canonical);
    }

    public List<byte[]> keys(byte[] globPattern) {
        checkThread();
        if (globPattern == null) {
            return Collections.emptyList();
        }
        long now = System.currentTimeMillis();
        List<byte[]> out = new ArrayList<>();
        List<byte[]> expiredKeys = new ArrayList<>();
        List<YierdisObject> expiredValues = new ArrayList<>();
        store.forEach((k, e) -> {
            if (isKeyExpired(k, now)) {
                expiredKeys.add(k);
                expiredValues.add(e);
                return;
            }
            if (globMatches(globPattern, k)) {
                out.add(k);
            }
        });
        for (int i = 0; i < expiredKeys.size(); i++) {
            byte[] key = expiredKeys.get(i);
            removeExpire(key);
            if (store.remove(key, expiredValues.get(i))) {
                expiredValues.get(i).releasePayloadIfAny();
                usedBytes -= expiredValues.get(i).estimatedBytes;
            }
        }
        return out;
    }

    public boolean setString(byte[] keyBytes, byte[] value, SetMode mode, ExpireOption expireOption) {
        checkThread();
        long now = System.currentTimeMillis();
        Long expireAtMillis = expireOption == null ? null : expireOption.toExpireAtMillis(now);

        final boolean[] didSet = new boolean[]{false};
        final long[] deltaBytes = new long[]{0};
        try {
            store.compute(keyBytes, (k, old) -> {
                long oldEstimate = old == null ? 0 : old.estimatedBytes;
                if (old != null && isKeyExpired(k, now)) {
                    old.releasePayloadIfAny();
                    removeExpire(k);
                    deltaBytes[0] -= oldEstimate;
                    old = null;
                    oldEstimate = 0;
                }
                if (mode == SetMode.NX && old != null) {
                    touch(old);
                    return old;
                }
                if (mode == SetMode.XX && old == null) {
                    return null;
                }
                if (old == null) {
                    didSet[0] = true;
                    YierdisObject next = YierdisObject.newString(offHeapAllocator, value);
                    touch(next);
                    refreshEstimatedBytes(k, next);
                    deltaBytes[0] += next.estimatedBytes;
                    return next;
                }
                old.overwriteWithString(offHeapAllocator, value);
                touch(old);
                deltaBytes[0] -= oldEstimate;
                refreshEstimatedBytes(k, old);
                deltaBytes[0] += old.estimatedBytes;
                didSet[0] = true;
                return old;
            });
        } catch (YierdisOffHeapOutOfMemoryException e) {
            throw new YierdisCommandException("OOM off-heap memory limit exceeded");
        }
        usedBytes += deltaBytes[0];
        if (didSet[0]) {
            if (expireAtMillis != null) {
                setExpireAtMillis(keyBytes, expireAtMillis);
            } else {
                removeExpire(keyBytes);
            }
        }
        return didSet[0];
    }

    public byte[] getStringBytes(byte[] keyBytes) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return null;
        }
        if (e.type != ValueType.STRING) {
            throw new WrongTypeException();
        }
        return e.stringBytesView();
    }

    public void getStringForReply(YierdisBytesView keyView, YierdisBulkStringOutput out) {
        checkThread();
        if (out == null) {
            throw new IllegalArgumentException("out must not be null");
        }

        byte[] canonical = store.canonicalKey(keyView);
        if (canonical == null) {
            out.bulkStringNull();
            return;
        }
        getStringForReply(canonical, out);
    }

    public void getStringForReply(byte[] keyBytes, YierdisBulkStringOutput out) {
        checkThread();
        if (out == null) {
            throw new IllegalArgumentException("out must not be null");
        }

        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            out.bulkStringNull();
            return;
        }
        if (e.type != ValueType.STRING) {
            throw new WrongTypeException();
        }

        if (e.encoding == ValueEncoding.STRING_INT) {
            out.bulkStringLongAscii(e.intValue);
            return;
        }
        YierdisOffHeapSlice slice = e.stringOffHeapSlice();
        if (slice != null) {
            out.bulkString(slice);
            return;
        }
        byte[] buf = (byte[]) e.payload;
        out.bulkString(buf, 0, e.rawLen);
    }

    public int strlen(byte[] keyBytes) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return 0;
        }
        if (e.type != ValueType.STRING) {
            throw new WrongTypeException();
        }
        return e.stringByteLength();
    }

    public int strlen(YierdisBytesView keyView) {
        checkThread();
        byte[] canonical = store.canonicalKey(keyView);
        if (canonical == null) {
            return 0;
        }
        return strlen(canonical);
    }

    public int append(byte[] keyBytes, byte[] appendValue) {
        checkThread();
        long now = System.currentTimeMillis();
        final int[] newLen = new int[]{0};
        final long[] deltaBytes = new long[]{0};
        try {
            store.compute(keyBytes, (k, old) -> {
                long oldEstimate = old == null ? 0 : old.estimatedBytes;
                if (old != null && isKeyExpired(k, now)) {
                    old.releasePayloadIfAny();
                    removeExpire(k);
                    deltaBytes[0] -= oldEstimate;
                    old = null;
                    oldEstimate = 0;
                }
                if (old == null) {
                    YierdisObject o = YierdisObject.newString(offHeapAllocator, appendValue);
                    newLen[0] = o.stringByteLength();
                    touch(o);
                    refreshEstimatedBytes(k, o);
                    deltaBytes[0] += o.estimatedBytes;
                    return o;
                }

                if (old.type != ValueType.STRING) {
                    throw new WrongTypeException();
                }
                touch(old);
                newLen[0] = old.stringAppend(offHeapAllocator, appendValue);
                deltaBytes[0] -= oldEstimate;
                refreshEstimatedBytes(k, old);
                deltaBytes[0] += old.estimatedBytes;
                return old;
            });
        } catch (YierdisOffHeapOutOfMemoryException e) {
            throw new YierdisCommandException("OOM off-heap memory limit exceeded");
        }
        usedBytes += deltaBytes[0];
        return newLen[0];
    }

    public long incrBy(byte[] keyBytes, long delta) {
        checkThread();
        long now = System.currentTimeMillis();
        final long[] result = new long[]{0L};
        final long[] deltaBytes = new long[]{0};
        store.compute(keyBytes, (k, old) -> {
            long oldEstimate = old == null ? 0 : old.estimatedBytes;
            if (old != null && isKeyExpired(k, now)) {
                old.releasePayloadIfAny();
                removeExpire(k);
                deltaBytes[0] -= oldEstimate;
                old = null;
                oldEstimate = 0;
            }
            if (old == null) {
                long next = delta;
                result[0] = next;
                YierdisObject o = YierdisObject.newStringInt(next);
                touch(o);
                refreshEstimatedBytes(k, o);
                deltaBytes[0] += o.estimatedBytes;
                return o;
            }

            if (old.type != ValueType.STRING) {
                throw new WrongTypeException();
            }
            touch(old);
            result[0] = old.stringIncrBy(offHeapAllocator, delta);
            deltaBytes[0] -= oldEstimate;
            refreshEstimatedBytes(k, old);
            deltaBytes[0] += old.estimatedBytes;
            return old;
        });
        usedBytes += deltaBytes[0];
        return result[0];
    }

    public int lpush(byte[] keyBytes, List<byte[]> values) {
        checkThread();
        return pushInternal(keyBytes, values, true);
    }

    public int rpush(byte[] keyBytes, List<byte[]> values) {
        checkThread();
        return pushInternal(keyBytes, values, false);
    }

    private int pushInternal(byte[] keyBytes, List<byte[]> values, boolean left) {
        long now = System.currentTimeMillis();
        YierdisUnsafeOffHeapAllocator unsafeAllocator =
                offHeapAllocator instanceof YierdisUnsafeOffHeapAllocator unsafe ? unsafe : null;
        final int[] len = new int[]{0};
        final long[] deltaBytes = new long[]{0};
        store.compute(keyBytes, (k, old) -> {
            long oldEstimate = old == null ? 0 : old.estimatedBytes;
            if (old != null && isKeyExpired(k, now)) {
                old.releasePayloadIfAny();
                removeExpire(k);
                deltaBytes[0] -= oldEstimate;
                old = null;
                oldEstimate = 0;
            }
            if (old == null) {
                ListValue lv = unsafeAllocator != null ? new ListValue(unsafeAllocator) : new ListValue();
                if (left) {
                    lv.lpushAll(values);
                } else {
                    lv.rpushAll(values);
                }
                len[0] = lv.size();
                YierdisObject o = YierdisObject.newList(lv);
                touch(o);
                refreshEstimatedBytes(k, o);
                deltaBytes[0] += o.estimatedBytes;
                return o;
            }

            if (old.type != ValueType.LIST) {
                throw new WrongTypeException();
            }
            ListValue lv = (ListValue) old.payload;
            if (left) {
                lv.lpushAll(values);
            } else {
                lv.rpushAll(values);
            }
            len[0] = lv.size();
            old.refreshCompositeEncodingFromPayload();
            touch(old);
            deltaBytes[0] -= oldEstimate;
            refreshEstimatedBytes(k, old);
            deltaBytes[0] += old.estimatedBytes;
            return old;
        });
        usedBytes += deltaBytes[0];
        return len[0];
    }

    public List<byte[]> lrange(byte[] keyBytes, int start, int stop) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return new ArrayList<>();
        }
        if (e.type != ValueType.LIST) {
            throw new WrongTypeException();
        }
        return ((ListValue) e.payload).range(start, stop);
    }

    public int lrangeReplyCount(byte[] keyBytes, int start, int stop) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return 0;
        }
        if (e.type != ValueType.LIST) {
            throw new WrongTypeException();
        }
        return ((ListValue) e.payload).rangeCount(start, stop);
    }

    public void lrangeReplyInto(byte[] keyBytes, int start, int stop, YierdisBulkStringOutput out) {
        checkThread();
        if (out == null) {
            throw new IllegalArgumentException("out must not be null");
        }

        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return;
        }
        if (e.type != ValueType.LIST) {
            throw new WrongTypeException();
        }
        ((ListValue) e.payload).rangeInto(start, stop, out);
    }

    public List<byte[]> lpop(byte[] keyBytes, int count) {
        checkThread();
        return popInternal(keyBytes, count, true);
    }

    public List<byte[]> rpop(byte[] keyBytes, int count) {
        checkThread();
        return popInternal(keyBytes, count, false);
    }

    private List<byte[]> popInternal(byte[] keyBytes, int count, boolean left) {
        if (count <= 0) {
            return new ArrayList<>();
        }
        long now = System.currentTimeMillis();
        final List<byte[]>[] popped = new List[]{null};
        final long[] deltaBytes = new long[]{0};
        store.computeIfPresent(keyBytes, (k, old) -> {
            long oldEstimate = old.estimatedBytes;
            if (isKeyExpired(k, now)) {
                old.releasePayloadIfAny();
                removeExpire(k);
                popped[0] = new ArrayList<>();
                deltaBytes[0] -= oldEstimate;
                return null;
            }
            if (old.type != ValueType.LIST) {
                throw new WrongTypeException();
            }
            ListValue lv = (ListValue) old.payload;
            popped[0] = left ? lv.lpop(count) : lv.rpop(count);
            if (lv.size() == 0) {
                old.releasePayloadIfAny();
                removeExpire(k);
                deltaBytes[0] -= oldEstimate;
                return null;
            }
            old.refreshCompositeEncodingFromPayload();
            touch(old);
            refreshEstimatedBytes(k, old);
            deltaBytes[0] += old.estimatedBytes - oldEstimate;
            return old;
        });
        usedBytes += deltaBytes[0];
        return popped[0] == null ? new ArrayList<>() : popped[0];
    }

    public int hset(byte[] keyBytes, List<byte[]> fieldValuePairs) {
        checkThread();
        if (fieldValuePairs.size() % 2 != 0) {
            throw new YierdisCommandException("ERR wrong number of arguments for 'hset' command");
        }
        long now = System.currentTimeMillis();
        YierdisUnsafeOffHeapAllocator unsafeAllocator =
                offHeapAllocator instanceof YierdisUnsafeOffHeapAllocator unsafe ? unsafe : null;
        final int[] added = new int[]{0};
        final long[] deltaBytes = new long[]{0};
        store.compute(keyBytes, (k, old) -> {
            long oldEstimate = old == null ? 0 : old.estimatedBytes;
            if (old != null && isKeyExpired(k, now)) {
                old.releasePayloadIfAny();
                removeExpire(k);
                deltaBytes[0] -= oldEstimate;
                old = null;
                oldEstimate = 0;
            }
            if (old == null) {
                HashValue hv = unsafeAllocator != null ? new HashValue(unsafeAllocator) : new HashValue();
                added[0] = hv.hsetMany(fieldValuePairs);
                YierdisObject o = YierdisObject.newHash(hv);
                touch(o);
                refreshEstimatedBytes(k, o);
                deltaBytes[0] += o.estimatedBytes;
                return o;
            }
            if (old.type != ValueType.HASH) {
                throw new WrongTypeException();
            }
            added[0] = ((HashValue) old.payload).hsetMany(fieldValuePairs);
            old.refreshCompositeEncodingFromPayload();
            touch(old);
            deltaBytes[0] -= oldEstimate;
            refreshEstimatedBytes(k, old);
            deltaBytes[0] += old.estimatedBytes;
            return old;
        });
        usedBytes += deltaBytes[0];
        return added[0];
    }

    public byte[] hget(byte[] keyBytes, byte[] fieldBytes) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return null;
        }
        if (e.type != ValueType.HASH) {
            throw new WrongTypeException();
        }
        return ((HashValue) e.payload).hget(fieldBytes);
    }

    public List<byte[]> hgetall(byte[] keyBytes) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return new ArrayList<>();
        }
        if (e.type != ValueType.HASH) {
            throw new WrongTypeException();
        }
        return ((HashValue) e.payload).hgetallPairs();
    }

    public int hgetallReplyCount(byte[] keyBytes) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return 0;
        }
        if (e.type != ValueType.HASH) {
            throw new WrongTypeException();
        }
        return ((HashValue) e.payload).hgetallCount();
    }

    public void hgetallReplyInto(byte[] keyBytes, YierdisBulkStringOutput out) {
        checkThread();
        if (out == null) {
            throw new IllegalArgumentException("out must not be null");
        }

        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return;
        }
        if (e.type != ValueType.HASH) {
            throw new WrongTypeException();
        }
        ((HashValue) e.payload).hgetallPairsInto(out);
    }

    public int hlen(byte[] keyBytes) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return 0;
        }
        if (e.type != ValueType.HASH) {
            throw new WrongTypeException();
        }
        return ((HashValue) e.payload).size();
    }

    public int hdel(byte[] keyBytes, List<byte[]> fields) {
        checkThread();
        long now = System.currentTimeMillis();
        final int[] removed = new int[]{0};
        final long[] deltaBytes = new long[]{0};
        store.computeIfPresent(keyBytes, (k, old) -> {
            long oldEstimate = old.estimatedBytes;
            if (isKeyExpired(k, now)) {
                old.releasePayloadIfAny();
                removeExpire(k);
                deltaBytes[0] -= oldEstimate;
                return null;
            }
            if (old.type != ValueType.HASH) {
                throw new WrongTypeException();
            }
            HashValue hv = (HashValue) old.payload;
            removed[0] = hv.hdel(fields);
            if (hv.size() == 0) {
                old.releasePayloadIfAny();
                removeExpire(k);
                deltaBytes[0] -= oldEstimate;
                return null;
            }
            old.refreshCompositeEncodingFromPayload();
            touch(old);
            refreshEstimatedBytes(k, old);
            deltaBytes[0] += old.estimatedBytes - oldEstimate;
            return old;
        });
        usedBytes += deltaBytes[0];
        return removed[0];
    }

    public int sadd(byte[] keyBytes, List<byte[]> members) {
        checkThread();
        long now = System.currentTimeMillis();
        YierdisUnsafeOffHeapAllocator unsafeAllocator =
                offHeapAllocator instanceof YierdisUnsafeOffHeapAllocator unsafe ? unsafe : null;
        final int[] added = new int[]{0};
        final long[] deltaBytes = new long[]{0};
        store.compute(keyBytes, (k, old) -> {
            long oldEstimate = old == null ? 0 : old.estimatedBytes;
            if (old != null && isKeyExpired(k, now)) {
                old.releasePayloadIfAny();
                removeExpire(k);
                deltaBytes[0] -= oldEstimate;
                old = null;
                oldEstimate = 0;
            }
            if (old == null) {
                SetValue sv = unsafeAllocator != null ? new SetValue(unsafeAllocator) : new SetValue();
                added[0] = sv.addAll(members);
                YierdisObject o = YierdisObject.newSet(sv);
                touch(o);
                refreshEstimatedBytes(k, o);
                deltaBytes[0] += o.estimatedBytes;
                return o;
            }
            if (old.type != ValueType.SET) {
                throw new WrongTypeException();
            }
            added[0] = ((SetValue) old.payload).addAll(members);
            old.refreshCompositeEncodingFromPayload();
            touch(old);
            deltaBytes[0] -= oldEstimate;
            refreshEstimatedBytes(k, old);
            deltaBytes[0] += old.estimatedBytes;
            return old;
        });
        usedBytes += deltaBytes[0];
        return added[0];
    }

    public int srem(byte[] keyBytes, List<byte[]> members) {
        checkThread();
        long now = System.currentTimeMillis();
        final int[] removed = new int[]{0};
        final long[] deltaBytes = new long[]{0};
        store.computeIfPresent(keyBytes, (k, old) -> {
            long oldEstimate = old.estimatedBytes;
            if (isKeyExpired(k, now)) {
                old.releasePayloadIfAny();
                removeExpire(k);
                deltaBytes[0] -= oldEstimate;
                return null;
            }
            if (old.type != ValueType.SET) {
                throw new WrongTypeException();
            }
            SetValue sv = (SetValue) old.payload;
            removed[0] = sv.removeAll(members);
            if (sv.size() == 0) {
                old.releasePayloadIfAny();
                removeExpire(k);
                deltaBytes[0] -= oldEstimate;
                return null;
            }
            old.refreshCompositeEncodingFromPayload();
            touch(old);
            refreshEstimatedBytes(k, old);
            deltaBytes[0] += old.estimatedBytes - oldEstimate;
            return old;
        });
        usedBytes += deltaBytes[0];
        return removed[0];
    }

    public List<byte[]> smembers(byte[] keyBytes) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return new ArrayList<>();
        }
        if (e.type != ValueType.SET) {
            throw new WrongTypeException();
        }
        return ((SetValue) e.payload).members();
    }

    public int smembersReplyCount(byte[] keyBytes) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return 0;
        }
        if (e.type != ValueType.SET) {
            throw new WrongTypeException();
        }
        return ((SetValue) e.payload).size();
    }

    public void smembersReplyInto(byte[] keyBytes, YierdisBulkStringOutput out) {
        checkThread();
        if (out == null) {
            throw new IllegalArgumentException("out must not be null");
        }

        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return;
        }
        if (e.type != ValueType.SET) {
            throw new WrongTypeException();
        }
        ((SetValue) e.payload).membersInto(out);
    }

    public boolean sismember(byte[] keyBytes, byte[] memberBytes) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return false;
        }
        if (e.type != ValueType.SET) {
            throw new WrongTypeException();
        }
        return ((SetValue) e.payload).contains(memberBytes);
    }

    public int scard(byte[] keyBytes) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return 0;
        }
        if (e.type != ValueType.SET) {
            throw new WrongTypeException();
        }
        return ((SetValue) e.payload).size();
    }

    public int zadd(byte[] keyBytes, List<byte[]> scoreMemberPairs) {
        checkThread();
        if (scoreMemberPairs.size() % 2 != 0) {
            throw new YierdisCommandException("ERR wrong number of arguments for 'zadd' command");
        }
        long now = System.currentTimeMillis();
        YierdisUnsafeOffHeapAllocator unsafeAllocator =
                offHeapAllocator instanceof YierdisUnsafeOffHeapAllocator unsafe ? unsafe : null;
        final int[] added = new int[]{0};
        final long[] deltaBytes = new long[]{0};
        store.compute(keyBytes, (k, old) -> {
            long oldEstimate = old == null ? 0 : old.estimatedBytes;
            if (old != null && isKeyExpired(k, now)) {
                old.releasePayloadIfAny();
                removeExpire(k);
                deltaBytes[0] -= oldEstimate;
                old = null;
                oldEstimate = 0;
            }
            if (old == null) {
                ZSetValue zv = unsafeAllocator != null ? new ZSetValue(unsafeAllocator) : new ZSetValue();
                try {
                    added[0] = zv.zaddMany(scoreMemberPairs);
                } catch (RuntimeException e) {
                    zv.close();
                    throw e;
                }
                YierdisObject o = YierdisObject.newZSet(zv);
                touch(o);
                refreshEstimatedBytes(k, o);
                deltaBytes[0] += o.estimatedBytes;
                return o;
            }
            if (old.type != ValueType.ZSET) {
                throw new WrongTypeException();
            }
            added[0] = ((ZSetValue) old.payload).zaddMany(scoreMemberPairs);
            old.refreshCompositeEncodingFromPayload();
            touch(old);
            deltaBytes[0] -= oldEstimate;
            refreshEstimatedBytes(k, old);
            deltaBytes[0] += old.estimatedBytes;
            return old;
        });
        usedBytes += deltaBytes[0];
        return added[0];
    }

    public List<byte[]> zrange(byte[] keyBytes, long start, long stop, boolean withScores) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return new ArrayList<>();
        }
        if (e.type != ValueType.ZSET) {
            throw new WrongTypeException();
        }
        return ((ZSetValue) e.payload).zrange(start, stop, withScores);
    }

    public int zrangeReplyCount(byte[] keyBytes, long start, long stop, boolean withScores) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return 0;
        }
        if (e.type != ValueType.ZSET) {
            throw new WrongTypeException();
        }
        return ((ZSetValue) e.payload).zrangeReplyCount(start, stop, withScores);
    }

    public void zrangeReplyInto(byte[] keyBytes, long start, long stop, boolean withScores, YierdisBulkStringOutput out) {
        checkThread();
        if (out == null) {
            throw new IllegalArgumentException("out must not be null");
        }

        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return;
        }
        if (e.type != ValueType.ZSET) {
            throw new WrongTypeException();
        }
        ((ZSetValue) e.payload).zrangeReplyInto(start, stop, withScores, out);
    }

    public List<byte[]> zrevrange(byte[] keyBytes, long start, long stop, boolean withScores) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return new ArrayList<>();
        }
        if (e.type != ValueType.ZSET) {
            throw new WrongTypeException();
        }
        return ((ZSetValue) e.payload).zrevrange(start, stop, withScores);
    }

    public int zrevrangeReplyCount(byte[] keyBytes, long start, long stop, boolean withScores) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return 0;
        }
        if (e.type != ValueType.ZSET) {
            throw new WrongTypeException();
        }
        return ((ZSetValue) e.payload).zrevrangeReplyCount(start, stop, withScores);
    }

    public void zrevrangeReplyInto(byte[] keyBytes, long start, long stop, boolean withScores, YierdisBulkStringOutput out) {
        checkThread();
        if (out == null) {
            throw new IllegalArgumentException("out must not be null");
        }

        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return;
        }
        if (e.type != ValueType.ZSET) {
            throw new WrongTypeException();
        }
        ((ZSetValue) e.payload).zrevrangeReplyInto(start, stop, withScores, out);
    }

    public List<byte[]> zrangeByScore(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return new ArrayList<>();
        }
        if (e.type != ValueType.ZSET) {
            throw new WrongTypeException();
        }
        return ((ZSetValue) e.payload).zrangeByScore(min, minExclusive, max, maxExclusive, withScores, offset, count);
    }

    public int zrangeByScoreReplyCount(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return 0;
        }
        if (e.type != ValueType.ZSET) {
            throw new WrongTypeException();
        }
        return ((ZSetValue) e.payload).zrangeByScoreReplyCount(min, minExclusive, max, maxExclusive, withScores, offset, count);
    }

    public void zrangeByScoreReplyInto(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count, YierdisBulkStringOutput out) {
        checkThread();
        if (out == null) {
            throw new IllegalArgumentException("out must not be null");
        }

        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return;
        }
        if (e.type != ValueType.ZSET) {
            throw new WrongTypeException();
        }
        ((ZSetValue) e.payload).zrangeByScoreReplyInto(min, minExclusive, max, maxExclusive, withScores, offset, count, out);
    }

    public List<byte[]> zrevrangeByScore(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return new ArrayList<>();
        }
        if (e.type != ValueType.ZSET) {
            throw new WrongTypeException();
        }
        return ((ZSetValue) e.payload).zrevrangeByScore(min, minExclusive, max, maxExclusive, withScores, offset, count);
    }

    public int zrevrangeByScoreReplyCount(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return 0;
        }
        if (e.type != ValueType.ZSET) {
            throw new WrongTypeException();
        }
        return ((ZSetValue) e.payload).zrevrangeByScoreReplyCount(min, minExclusive, max, maxExclusive, withScores, offset, count);
    }

    public void zrevrangeByScoreReplyInto(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count, YierdisBulkStringOutput out) {
        checkThread();
        if (out == null) {
            throw new IllegalArgumentException("out must not be null");
        }

        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return;
        }
        if (e.type != ValueType.ZSET) {
            throw new WrongTypeException();
        }
        ((ZSetValue) e.payload).zrevrangeByScoreReplyInto(min, minExclusive, max, maxExclusive, withScores, offset, count, out);
    }

    public int zrem(byte[] keyBytes, List<byte[]> members) {
        checkThread();
        long now = System.currentTimeMillis();
        final int[] removed = new int[]{0};
        final long[] deltaBytes = new long[]{0};
        store.computeIfPresent(keyBytes, (k, old) -> {
            long oldEstimate = old.estimatedBytes;
            if (isKeyExpired(k, now)) {
                old.releasePayloadIfAny();
                removeExpire(k);
                deltaBytes[0] -= oldEstimate;
                return null;
            }
            if (old.type != ValueType.ZSET) {
                throw new WrongTypeException();
            }
            ZSetValue zv = (ZSetValue) old.payload;
            removed[0] = zv.zrem(members);
            if (zv.size() == 0) {
                old.releasePayloadIfAny();
                removeExpire(k);
                deltaBytes[0] -= oldEstimate;
                return null;
            }
            old.refreshCompositeEncodingFromPayload();
            touch(old);
            refreshEstimatedBytes(k, old);
            deltaBytes[0] += old.estimatedBytes - oldEstimate;
            return old;
        });
        usedBytes += deltaBytes[0];
        return removed[0];
    }

    public int zremrangeByRank(byte[] keyBytes, long start, long stop) {
        checkThread();
        long now = System.currentTimeMillis();
        final int[] removed = new int[]{0};
        final long[] deltaBytes = new long[]{0};
        store.computeIfPresent(keyBytes, (k, old) -> {
            long oldEstimate = old.estimatedBytes;
            if (isKeyExpired(k, now)) {
                old.releasePayloadIfAny();
                removeExpire(k);
                deltaBytes[0] -= oldEstimate;
                return null;
            }
            if (old.type != ValueType.ZSET) {
                throw new WrongTypeException();
            }
            ZSetValue zv = (ZSetValue) old.payload;
            removed[0] = zv.zremrangeByRank(start, stop);
            if (zv.size() == 0) {
                old.releasePayloadIfAny();
                removeExpire(k);
                deltaBytes[0] -= oldEstimate;
                return null;
            }
            old.refreshCompositeEncodingFromPayload();
            touch(old);
            refreshEstimatedBytes(k, old);
            deltaBytes[0] += old.estimatedBytes - oldEstimate;
            return old;
        });
        usedBytes += deltaBytes[0];
        return removed[0];
    }

    public int zremrangeByScore(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive) {
        checkThread();
        long now = System.currentTimeMillis();
        final int[] removed = new int[]{0};
        final long[] deltaBytes = new long[]{0};
        store.computeIfPresent(keyBytes, (k, old) -> {
            long oldEstimate = old.estimatedBytes;
            if (isKeyExpired(k, now)) {
                old.releasePayloadIfAny();
                removeExpire(k);
                deltaBytes[0] -= oldEstimate;
                return null;
            }
            if (old.type != ValueType.ZSET) {
                throw new WrongTypeException();
            }
            ZSetValue zv = (ZSetValue) old.payload;
            removed[0] = zv.zremrangeByScore(min, minExclusive, max, maxExclusive);
            if (zv.size() == 0) {
                old.releasePayloadIfAny();
                removeExpire(k);
                deltaBytes[0] -= oldEstimate;
                return null;
            }
            old.refreshCompositeEncodingFromPayload();
            touch(old);
            refreshEstimatedBytes(k, old);
            deltaBytes[0] += old.estimatedBytes - oldEstimate;
            return old;
        });
        usedBytes += deltaBytes[0];
        return removed[0];
    }

    public void cleanupExpired() {
        checkThread();
        long startNanos = System.nanoTime();
        long timeLimitNanos = startNanos + 5_000_000L; // ~5ms
        int loops = 0;

        for (; ; ) {
            int total = expires.size();
            if (total == 0) {
                return;
            }

            int samples = Math.min(20, total);
            if (samples <= 0) {
                return;
            }

            int expired = 0;
            long nowMillis = System.currentTimeMillis();

            for (int i = 0; i < samples; i++) {
                byte[] keyBytes = expires.randomKey();
                if (keyBytes == null) {
                    break;
                }

                Long expireAtMillis = expires.get(keyBytes);
                if (expireAtMillis == null) {
                    removeExpire(keyBytes);
                    continue;
                }

                YierdisObject e = store.get(keyBytes);
                if (e == null) {
                    removeExpire(keyBytes);
                    continue;
                }

                if (expireAtMillis <= nowMillis) {
                    removeExpire(keyBytes);
                    if (store.remove(keyBytes, e)) {
                        e.releasePayloadIfAny();
                        usedBytes -= e.estimatedBytes;
                    }
                    expired++;
                }
            }

            loops++;
            if (expired <= samples / 4) {
                return;
            }
            if (loops >= 16) {
                return;
            }
            if (System.nanoTime() >= timeLimitNanos) {
                return;
            }
        }
    }

    private YierdisObject getObjectIfNotExpired(byte[] keyBytes) {
        YierdisObject e = store.get(keyBytes);
        if (e == null) {
            return null;
        }
        if (removeIfExpired(keyBytes, e, System.currentTimeMillis())) {
            return null;
        }
        touch(e);
        return e;
    }

    private boolean removeIfExpired(byte[] keyBytes, YierdisObject e, long nowMillis) {
        Long expireAtMillis = expires.get(keyBytes);
        if (expireAtMillis == null || expireAtMillis > nowMillis) {
            return false;
        }
        removeExpire(keyBytes);
        if (store.remove(keyBytes, e)) {
            e.releasePayloadIfAny();
            usedBytes -= e.estimatedBytes;
            return true;
        }
        return false;
    }

    private boolean isKeyExpired(byte[] keyBytes, long nowMillis) {
        Long expireAtMillis = expires.get(keyBytes);
        return expireAtMillis != null && expireAtMillis <= nowMillis;
    }

    private void setExpireAtMillis(byte[] keyBytes, long expireAtMillis) {
        expires.setExpireAtMillis(keyBytes, expireAtMillis, store);
    }

    private void removeExpire(byte[] keyBytes) {
        expires.removeExpire(keyBytes);
    }

    private static boolean globMatches(byte[] pattern, byte[] text) {
        int p = 0;
        int t = 0;
        int star = -1;
        int match = 0;
        while (t < text.length) {
            if (p < pattern.length && (pattern[p] == '?' || pattern[p] == text[t])) {
                p++;
                t++;
                continue;
            }
            if (p < pattern.length && pattern[p] == '*') {
                star = p++;
                match = t;
                continue;
            }
            if (star != -1) {
                p = star + 1;
                t = ++match;
                continue;
            }
            return false;
        }
        while (p < pattern.length && pattern[p] == '*') {
            p++;
        }
        return p == pattern.length;
    }

    public enum SetMode {
        NORMAL,
        NX,
        XX
    }

    public static final class ExpireOption {
        final TimeUnit unit;
        final long duration;

        public ExpireOption(TimeUnit unit, long duration) {
            this.unit = unit;
            this.duration = duration;
        }

        long toExpireAtMillis(long nowMillis) {
            long ms = unit.toMillis(duration);
            if (ms <= 0) {
                return nowMillis;
            }
            return nowMillis + ms;
        }
    }

    public static final class YierdisCommandException extends RuntimeException {
        public YierdisCommandException(String message) {
            super(message);
        }
    }

    public static final class WrongTypeException extends RuntimeException {
        public WrongTypeException() {
            super("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
    }

}
