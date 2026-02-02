package yier.bubu.redis.db;

import yier.bubu.redis.db.offheap.api.YierdisOffHeapAllocator;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapAddressAllocator;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapBuf;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapOutOfMemoryException;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapSlice;
import yier.bubu.redis.db.offheap.YierdisUnsafeOffHeapExpireIndex;
import yier.bubu.redis.db.offheap.YierdisUnsafeOffHeapKeyspace;
import yier.bubu.redis.db.offheap.YierdisUnsafeOffHeapString;
import yier.bubu.redis.protocol.RespCommand;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
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
    private final boolean ownsOffHeapAllocator;
    private final boolean keysStoredOffHeap;

    private static final String OOM_ERR = "OOM command not allowed when used memory > 'maxmemory'.";

    private final long maxmemoryBytes;
    private final MaxmemoryPolicy maxmemoryPolicy;
    private final int maxmemorySamples;
    private final boolean lruEnabled;
    private final long evictionTimeLimitNanos;
    private final long expireCleanupTimeLimitNanos;

    private long usedBytes;
    private long lruClock;

    private final DbThreadGuard threadGuard = new DbThreadGuard();

    public YierdisDb() {
        this(null, false, 0, "noeviction", 5, 5, 5);
    }

    public YierdisDb(YierdisOffHeapAllocator offHeapAllocator) {
        this(offHeapAllocator, false, 0, "noeviction", 5, 5, 5);
    }

    public YierdisDb(
            YierdisOffHeapAllocator offHeapAllocator,
            long maxmemoryBytes,
            String maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis
    ) {
        this(offHeapAllocator, false, maxmemoryBytes, maxmemoryPolicy, maxmemorySamples, evictionTimeLimitMillis, expireCleanupTimeLimitMillis);
    }

    public YierdisDb(
            YierdisOffHeapAllocator offHeapAllocator,
            boolean offHeapKeysEnabled,
            long maxmemoryBytes,
            String maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis
    ) {
        this(offHeapAllocator, true, offHeapKeysEnabled, maxmemoryBytes, maxmemoryPolicy, maxmemorySamples, evictionTimeLimitMillis, expireCleanupTimeLimitMillis);
    }

    public YierdisDb(
            YierdisOffHeapAllocator offHeapAllocator,
            boolean ownsOffHeapAllocator,
            boolean offHeapKeysEnabled,
            long maxmemoryBytes,
            String maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis
    ) {
        this.offHeapAllocator = offHeapAllocator;
        this.ownsOffHeapAllocator = ownsOffHeapAllocator;
        if (offHeapKeysEnabled && !(offHeapAllocator instanceof YierdisOffHeapAddressAllocator)) {
            throw new IllegalArgumentException("offHeapKeysEnabled requires an address allocator (unsafe off-heap backend)");
        }
        if (offHeapKeysEnabled && offHeapAllocator instanceof YierdisOffHeapAddressAllocator addressAllocator) {
            this.store = new YierdisUnsafeOffHeapKeyspace<>(addressAllocator);
            this.expires = new YierdisUnsafeOffHeapExpireIndex(addressAllocator);
            this.keysStoredOffHeap = true;
        } else {
            this.store = new ByteArrayKeyspace<>();
            this.expires = new YierdisHeapExpireIndex();
            this.keysStoredOffHeap = false;
        }
        if (maxmemoryBytes < 0) {
            throw new IllegalArgumentException("maxmemoryBytes must be >= 0");
        }
        if (maxmemorySamples <= 0) {
            throw new IllegalArgumentException("maxmemorySamples must be > 0");
        }
        if (evictionTimeLimitMillis <= 0) {
            throw new IllegalArgumentException("evictionTimeLimitMillis must be > 0");
        }
        if (expireCleanupTimeLimitMillis <= 0) {
            throw new IllegalArgumentException("expireCleanupTimeLimitMillis must be > 0");
        }

        this.maxmemoryBytes = maxmemoryBytes;
        this.maxmemoryPolicy = parseMaxmemoryPolicy(maxmemoryPolicy);
        this.maxmemorySamples = maxmemorySamples;
        this.lruEnabled = maxmemoryBytes > 0 && this.maxmemoryPolicy == MaxmemoryPolicy.ALLKEYS_LRU;
        this.evictionTimeLimitNanos = TimeUnit.MILLISECONDS.toNanos(evictionTimeLimitMillis);
        this.expireCleanupTimeLimitNanos = TimeUnit.MILLISECONDS.toNanos(expireCleanupTimeLimitMillis);
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

    /**
     * Best-effort write preflight:
     * <ul>
     *   <li>Under pressure, it first reclaims expired keys.</li>
     *   <li>If maxmemory is enabled, it attempts to evict enough keys <b>before</b> the write to reserve space.</li>
     *   <li>For {@code noeviction}, it rejects the write when the estimated extra bytes would exceed the limit.</li>
     * </ul>
     * <p>
     * This method exists to reduce "write succeeded but later returned OOM" scenarios and to ensure any
     * maxmemory errors happen before writing the RESP reply (avoiding double replies / protocol corruption).
     *
     * @param estimatedExtraBytes best-effort upper bound of the additional bytes the write may consume.
     */
    public void prepareWrite(long estimatedExtraBytes) {
        checkThread();
        if (maxmemoryBytes <= 0) {
            return;
        }

        // Best-effort: try to reclaim expired keys first (Redis does this too under pressure).
        cleanupExpired();

        long extra = Math.max(0, estimatedExtraBytes);
        long limit = maxmemoryBytes - extra;
        if (limit < 0) {
            limit = 0;
        }
        if (usedBytesForMaxmemory() <= limit) {
            return;
        }

        if (maxmemoryPolicy == MaxmemoryPolicy.NOEVICTION) {
            throw new YierdisCommandException(OOM_ERR);
        }

        evictUntilUnder(limit);
        if (usedBytesForMaxmemory() > limit) {
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

        evictUntilUnder(maxmemoryBytes);
        if (usedBytesForMaxmemory() > maxmemoryBytes) {
            throw new YierdisCommandException(OOM_ERR);
        }
    }

    private void evictUntilUnder(long limitBytes) {
        if (limitBytes < 0) {
            limitBytes = 0;
        }
        if (usedBytesForMaxmemory() <= limitBytes) {
            return;
        }

        int attempts = 0;
        int maxAttempts = Math.max(64, store.size() * 2);
        long nowMillis = System.currentTimeMillis();
        long deadline = System.nanoTime() + evictionTimeLimitNanos;
        while (usedBytesForMaxmemory() > limitBytes && attempts++ < maxAttempts) {
            if (System.nanoTime() >= deadline) {
                break;
            }
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
        return e.estimatedBytes + estimateOffHeapBytesForMemoryUsage(canonical, e);
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
        return e.estimatedBytes + estimateOffHeapBytesForMemoryUsage(keyBytes, e);
    }

    private long estimateOffHeapBytesForMemoryUsage(byte[] keyBytes, YierdisObject e) {
        if (offHeapAllocator == null || e == null) {
            return 0;
        }
        long extra = 0;
        if (keysStoredOffHeap && keyBytes != null) {
            extra += keyBytes.length;
        }
        if (e.type == ValueType.STRING) {
            if (e.payload instanceof YierdisOffHeapBuf buf) {
                extra += buf.capacity();
            } else if (e.payload instanceof YierdisUnsafeOffHeapString s) {
                extra += s.capacity();
            }
        }
        return extra;
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
        // maxmemory 是 best-effort 预算：当 off-heap allocator 由当前 DB 所拥有时，将堆外 used bytes 也计入预算。
        // 对于 server 的多 DB 场景（allocator 共享且 ownsOffHeapAllocator=false），避免将同一 allocator.usedBytes() 重复计入每个 DB。
        long offHeapUsedBytes = 0;
        if (ownsOffHeapAllocator && offHeapAllocator != null) {
            try {
                offHeapUsedBytes = Math.max(0L, offHeapAllocator.usedBytes());
            } catch (Throwable ignored) {
                offHeapUsedBytes = 0;
            }
        }
        return usedBytes + offHeapUsedBytes;
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
        threadGuard.bindToCurrentThread();
    }

    private void checkThread() {
        threadGuard.checkThread();
    }

    public void shutdown() {
        threadGuard.checkThreadForShutdown();
        if (!threadGuard.tryMarkClosed()) {
            return;
        }
        store.forEach((k, e) -> e.releasePayloadIfAny());
        store.clear();
        expires.clear();
        usedBytes = 0;
        if (ownsOffHeapAllocator && offHeapAllocator != null) {
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

    public YierdisMemoryStats memoryStats() {
        checkThread();
        return DbMemoryAccounting.snapshot(
                maxmemoryBytes,
                usedBytes,
                offHeapAllocator,
                store,
                expires,
                keysStoredOffHeap,
                ownsOffHeapAllocator
        );
    }

    private long estimateEntryBytes(byte[] keyBytes, YierdisObject e) {
        if (keyBytes == null || e == null) {
            return 0;
        }
        int keyBytesCost = keysStoredOffHeap ? 0 : keyBytes.length;
        return DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE + keyBytesCost + estimateValueBytes(e);
    }

    private long estimateValueBytes(YierdisObject e) {
        if (e == null) {
            return 0;
        }
        if (e.type == ValueType.STRING) {
            if (e.encoding == ValueEncoding.STRING_INT) {
                return Long.BYTES;
            }
            // 字符串 payload 若存放在 off-heap，则其容量由 allocator.usedBytes() 统计；这里避免重复计入。
            if (offHeapAllocator != null && (e.payload instanceof YierdisOffHeapBuf || e.payload instanceof YierdisUnsafeOffHeapString)) {
                return 0;
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
        if (seconds <= 0) {
            // Redis-compatible: seconds<=0 means the key is deleted immediately (if it exists).
            removeExpire(keyBytes);
            if (store.remove(keyBytes, e)) {
                e.releasePayloadIfAny();
                usedBytes -= e.estimatedBytes;
            }
            return true;
        }

        long expireAtMillis = safeExpireAtMillis(System.currentTimeMillis(), seconds);
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

    private static long safeExpireAtMillis(long nowMillis, long seconds) {
        // Saturating math to avoid long overflow: expire time is best-effort and must never wrap negative.
        long deltaMillis;
        try {
            deltaMillis = Math.multiplyExact(seconds, 1000L);
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
        try {
            return Math.addExact(nowMillis, deltaMillis);
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    private static long safeAddMillis(long nowMillis, long deltaMillis) {
        try {
            return Math.addExact(nowMillis, deltaMillis);
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    public boolean pexpire(byte[] keyBytes, long milliseconds) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return false;
        }
        if (milliseconds <= 0) {
            // Redis-compatible: milliseconds<=0 means the key is deleted immediately (if it exists).
            removeExpire(keyBytes);
            if (store.remove(keyBytes, e)) {
                e.releasePayloadIfAny();
                usedBytes -= e.estimatedBytes;
            }
            return true;
        }

        long expireAtMillis = safeAddMillis(System.currentTimeMillis(), milliseconds);
        setExpireAtMillis(keyBytes, expireAtMillis);
        return true;
    }

    public boolean pexpire(YierdisBytesView keyView, long milliseconds) {
        checkThread();
        byte[] canonical = store.canonicalKey(keyView);
        if (canonical == null) {
            return false;
        }
        return pexpire(canonical, milliseconds);
    }

    public boolean expireAtSeconds(byte[] keyBytes, long unixSeconds) {
        checkThread();
        long expireAtMillis;
        try {
            expireAtMillis = Math.multiplyExact(unixSeconds, 1000L);
        } catch (ArithmeticException e) {
            expireAtMillis = Long.MAX_VALUE;
        }
        return expireAtMillis(keyBytes, expireAtMillis);
    }

    public boolean expireAtSeconds(YierdisBytesView keyView, long unixSeconds) {
        checkThread();
        byte[] canonical = store.canonicalKey(keyView);
        if (canonical == null) {
            return false;
        }
        return expireAtSeconds(canonical, unixSeconds);
    }

    public boolean expireAtMillis(byte[] keyBytes, long unixMillis) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return false;
        }
        long now = System.currentTimeMillis();
        if (unixMillis <= now) {
            // 过期时间在过去/现在：按 Redis 语义立刻删除 key（如果存在）。
            removeExpire(keyBytes);
            if (store.remove(keyBytes, e)) {
                e.releasePayloadIfAny();
                usedBytes -= e.estimatedBytes;
            }
            return true;
        }
        setExpireAtMillis(keyBytes, unixMillis);
        return true;
    }

    public boolean expireAtMillis(YierdisBytesView keyView, long unixMillis) {
        checkThread();
        byte[] canonical = store.canonicalKey(keyView);
        if (canonical == null) {
            return false;
        }
        return expireAtMillis(canonical, unixMillis);
    }

    public boolean persist(byte[] keyBytes) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return false;
        }
        Long expireAtMillis = expires.get(keyBytes);
        if (expireAtMillis == null) {
            return false;
        }
        removeExpire(keyBytes);
        touch(e);
        return true;
    }

    public boolean persist(YierdisBytesView keyView) {
        checkThread();
        byte[] canonical = store.canonicalKey(keyView);
        if (canonical == null) {
            return false;
        }
        return persist(canonical);
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

    public long ttlMillis(byte[] keyBytes) {
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
        return remainingMillis <= 0 ? -2 : remainingMillis;
    }

    public long ttlSeconds(YierdisBytesView keyView) {
        checkThread();
        byte[] canonical = store.canonicalKey(keyView);
        if (canonical == null) {
            return -2;
        }
        return ttlSeconds(canonical);
    }

    public long ttlMillis(YierdisBytesView keyView) {
        checkThread();
        byte[] canonical = store.canonicalKey(keyView);
        if (canonical == null) {
            return -2;
        }
        return ttlMillis(canonical);
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

    private static final class StopScan extends RuntimeException {
        static final StopScan INSTANCE = new StopScan();

        private StopScan() {
        }

        @Override
        public synchronized Throwable fillInStackTrace() {
            return this;
        }
    }

    /**
     * Redis-compatible SCAN（best-effort）。
     * <p>
     * 该实现以“遍历顺序中的偏移量”作为游标含义，支持 MATCH glob 与 COUNT hint。
     * 在数据集变化（写入/删除/过期清理）情况下不保证强一致，但尽量做到“可推进、可终止、不阻塞太久”。
     *
     * @param cursor      游标（{@code 0} 表示从头开始）
     * @param globPattern glob 过滤（可为 null 表示不过滤）
     * @param count       期望返回的最大 key 数（Redis 中 COUNT 是 hint，这里按上限处理；必须 > 0）
     * @param out         输出容器（追加写入）
     * @return 下一次扫描的游标；返回 {@code 0} 表示扫描结束
     */
    public ScanCursor scan(ScanCursor cursor, byte[] globPattern, int count, List<byte[]> out) {
        checkThread();
        Objects.requireNonNull(out, "out");
        if (count <= 0) {
            throw new IllegalArgumentException("count must be > 0");
        }

        long start = cursor == null ? 0L : cursor.value();
        if (start < 0) {
            throw new IllegalArgumentException("cursor must be >= 0");
        }
        int total = store.size();
        if (total == 0) {
            return ScanCursor.start();
        }
        if (start >= (long) total) {
            return ScanCursor.start();
        }

        long now = System.currentTimeMillis();
        List<byte[]> expiredKeys = new ArrayList<>();
        List<YierdisObject> expiredValues = new ArrayList<>();

        // COUNT 在 Redis 里是 hint：为了避免 MATCH 过滤导致“单次扫描跑完整个 keyspace”，加一个步数上限。
        int maxSteps = Math.max(64, count * 10);

        final long[] pos = new long[]{0L};
        final int[] steps = new int[]{0};
        final long[] nextCursor = new long[]{0L};

        try {
            store.forEach((k, e) -> {
                if (pos[0] < start) {
                    pos[0]++;
                    return;
                }
                if (steps[0]++ >= maxSteps) {
                    nextCursor[0] = pos[0];
                    throw StopScan.INSTANCE;
                }

                pos[0]++;

                if (isKeyExpired(k, now)) {
                    expiredKeys.add(k);
                    expiredValues.add(e);
                    return;
                }
                if (globPattern == null || globMatches(globPattern, k)) {
                    out.add(k);
                    if (out.size() >= count) {
                        nextCursor[0] = pos[0];
                        throw StopScan.INSTANCE;
                    }
                }
            });
            // 扫到结尾：按 Redis 语义返回 cursor=0 表示结束。
            nextCursor[0] = 0L;
        } catch (StopScan ignored) {
            // best-effort：如果游标已经越过当前 key 数，则直接结束。
            if (pos[0] >= (long) store.size()) {
                nextCursor[0] = 0L;
            }
        }

        // 清理本轮遍历过程中发现的过期 key（与 KEYS 类似的“顺手清理”语义）。
        for (int i = 0; i < expiredKeys.size(); i++) {
            byte[] key = expiredKeys.get(i);
            removeExpire(key);
            if (store.remove(key, expiredValues.get(i))) {
                expiredValues.get(i).releasePayloadIfAny();
                usedBytes -= expiredValues.get(i).estimatedBytes;
            }
        }

        return ScanCursor.of(nextCursor[0]);
    }

    public boolean setString(byte[] keyBytes, byte[] value, SetMode mode, ExpireOption expireOption) {
        checkThread();
        long now = System.currentTimeMillis();
        boolean keepTtl = expireOption != null && expireOption.isKeepTtl();
        Long expireAtMillis = (expireOption == null || keepTtl) ? null : expireOption.toExpireAtMillis(now);

        final boolean[] didSet = new boolean[]{false};
        final boolean[] existed = new boolean[]{false};
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
                existed[0] = old != null;
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
            if (keepTtl && existed[0]) {
                // KEEPTTL：覆盖写入但保留原有过期时间（仅当 key 原先存在时有意义）。
                return true;
            }
            if (expireAtMillis != null) {
                setExpireAtMillis(keyBytes, expireAtMillis);
                return true;
            }
            removeExpire(keyBytes);
        }
        return didSet[0];
    }

    public boolean setString(byte[] keyBytes, RespCommand cmd, int valueArgIndex, SetMode mode, ExpireOption expireOption) {
        checkThread();
        if (cmd == null) {
            throw new IllegalArgumentException("cmd must not be null");
        }
        long now = System.currentTimeMillis();
        boolean keepTtl = expireOption != null && expireOption.isKeepTtl();
        Long expireAtMillis = (expireOption == null || keepTtl) ? null : expireOption.toExpireAtMillis(now);

        final boolean[] didSet = new boolean[]{false};
        final boolean[] existed = new boolean[]{false};
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
                existed[0] = old != null;
                if (mode == SetMode.NX && old != null) {
                    touch(old);
                    return old;
                }
                if (mode == SetMode.XX && old == null) {
                    return null;
                }
                if (old == null) {
                    didSet[0] = true;
                    YierdisObject next = YierdisObject.newString(offHeapAllocator, cmd, valueArgIndex);
                    touch(next);
                    refreshEstimatedBytes(k, next);
                    deltaBytes[0] += next.estimatedBytes;
                    return next;
                }
                old.overwriteWithString(offHeapAllocator, cmd, valueArgIndex);
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
            if (keepTtl && existed[0]) {
                // KEEPTTL：覆盖写入但保留原有过期时间（仅当 key 原先存在时有意义）。
                return true;
            }
            if (expireAtMillis != null) {
                setExpireAtMillis(keyBytes, expireAtMillis);
                return true;
            }
            removeExpire(keyBytes);
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

    public int append(byte[] keyBytes, RespCommand cmd, int argIndex) {
        checkThread();
        if (cmd == null) {
            throw new IllegalArgumentException("cmd must not be null");
        }
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
                    YierdisObject o = YierdisObject.newString(offHeapAllocator, cmd, argIndex);
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
                newLen[0] = old.stringAppend(offHeapAllocator, cmd, argIndex);
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

    public int getBit(YierdisBytesView keyView, long offset) {
        checkThread();
        byte[] canonical = store.canonicalKey(keyView);
        if (canonical == null) {
            return 0;
        }
        return getBit(canonical, offset);
    }

    public int getBit(byte[] keyBytes, long offset) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return 0;
        }
        if (e.type != ValueType.STRING) {
            throw new WrongTypeException();
        }
        return e.stringGetBit(offset);
    }

    public int setBit(byte[] keyBytes, long offset, int value) {
        checkThread();
        long now = System.currentTimeMillis();
        final int[] oldBit = new int[]{0};
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
                    old = YierdisObject.newString(offHeapAllocator, (byte[]) null);
                    touch(old);
                } else {
                    if (old.type != ValueType.STRING) {
                        throw new WrongTypeException();
                    }
                    touch(old);
                }

                oldBit[0] = old.stringSetBit(offHeapAllocator, offset, value);
                deltaBytes[0] -= oldEstimate;
                refreshEstimatedBytes(k, old);
                deltaBytes[0] += old.estimatedBytes;
                return old;
            });
        } catch (YierdisOffHeapOutOfMemoryException e) {
            throw new YierdisCommandException("OOM off-heap memory limit exceeded");
        }
        usedBytes += deltaBytes[0];
        return oldBit[0];
    }

    public long bitcount(YierdisBytesView keyView) {
        checkThread();
        byte[] canonical = store.canonicalKey(keyView);
        if (canonical == null) {
            return 0L;
        }
        return bitcount(canonical);
    }

    public long bitcount(byte[] keyBytes) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return 0L;
        }
        if (e.type != ValueType.STRING) {
            throw new WrongTypeException();
        }

        int len = e.stringByteLength();
        if (len <= 0) {
            return 0L;
        }
        return bitcountRange(e, 0, len - 1);
    }

    public long bitcount(YierdisBytesView keyView, long start, long end) {
        checkThread();
        byte[] canonical = store.canonicalKey(keyView);
        if (canonical == null) {
            return 0L;
        }
        return bitcount(canonical, start, end);
    }

    public long bitcount(byte[] keyBytes, long start, long end) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return 0L;
        }
        if (e.type != ValueType.STRING) {
            throw new WrongTypeException();
        }
        int len = e.stringByteLength();
        if (len <= 0) {
            return 0L;
        }

        long s = start;
        long ed = end;
        if (s < 0) {
            s = len + s;
        }
        if (ed < 0) {
            ed = len + ed;
        }
        if (s < 0) {
            s = 0;
        }
        if (ed < 0) {
            return 0L;
        }
        if (s >= len) {
            return 0L;
        }
        if (ed >= len) {
            ed = len - 1L;
        }
        if (s > ed) {
            return 0L;
        }
        return bitcountRange(e, (int) s, (int) ed);
    }

    private static long bitcountRange(YierdisObject e, int start, int end) {
        if (start < 0 || end < start) {
            return 0L;
        }
        long count = 0L;
        if (e.encoding == ValueEncoding.STRING_INT) {
            byte[] view = e.stringBytesView();
            int to = Math.min(end, view.length - 1);
            for (int i = start; i <= to; i++) {
                count += Integer.bitCount(view[i] & 0xFF);
            }
            return count;
        }

        if (e.payload instanceof byte[] buf) {
            int to = Math.min(end, e.rawLen - 1);
            for (int i = start; i <= to; i++) {
                count += Integer.bitCount(buf[i] & 0xFF);
            }
            return count;
        }
        if (e.payload instanceof yier.bubu.redis.db.offheap.api.YierdisOffHeapBuf buf) {
            int to = Math.min(end, e.rawLen - 1);
            for (int i = start; i <= to; i++) {
                count += Integer.bitCount(buf.getByte(i) & 0xFF);
            }
            return count;
        }
        if (e.payload instanceof yier.bubu.redis.db.offheap.YierdisUnsafeOffHeapString s) {
            int to = Math.min(end, e.rawLen - 1);
            for (int i = start; i <= to; i++) {
                count += Integer.bitCount(s.getByte(i) & 0xFF);
            }
            return count;
        }
        return 0L;
    }

    public int pfadd(byte[] keyBytes, RespCommand cmd, int firstElementArgIndex) {
        checkThread();
        if (cmd == null) {
            throw new IllegalArgumentException("cmd must not be null");
        }
        long now = System.currentTimeMillis();
        final boolean[] changed = new boolean[]{false};
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
                    old = YierdisObject.newString(offHeapAllocator, YierdisHyperLogLog.newSparse());
                    touch(old);
                } else {
                    if (old.type != ValueType.STRING) {
                        throw new WrongTypeException();
                    }
                    touch(old);
                }

                changed[0] = YierdisHyperLogLog.pfAdd(old, offHeapAllocator, cmd, firstElementArgIndex);

                deltaBytes[0] -= oldEstimate;
                refreshEstimatedBytes(k, old);
                deltaBytes[0] += old.estimatedBytes;
                return old;
            });
        } catch (YierdisOffHeapOutOfMemoryException e) {
            throw new YierdisCommandException("OOM off-heap memory limit exceeded");
        }
        usedBytes += deltaBytes[0];
        return changed[0] ? 1 : 0;
    }

    public long pfcount(Collection<byte[]> keys) {
        checkThread();
        if (keys == null || keys.isEmpty()) {
            return 0L;
        }

        int[] registers = new int[YierdisHyperLogLog.REGISTERS];
        for (byte[] keyBytes : keys) {
            YierdisObject e = getObjectIfNotExpired(keyBytes);
            if (e == null) {
                continue;
            }
            if (e.type != ValueType.STRING) {
                throw new WrongTypeException();
            }
            if (!YierdisHyperLogLog.isHllString(e)) {
                throw new YierdisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
            YierdisHyperLogLog.mergeHllIntoRegisters(e.stringBytesView(), registers);
        }
        return YierdisHyperLogLog.estimateCardinality(registers);
    }

    public void pfmerge(byte[] destKeyBytes, Collection<byte[]> sourceKeys) {
        checkThread();
        if (sourceKeys == null || sourceKeys.isEmpty()) {
            throw new IllegalArgumentException("sourceKeys must not be empty");
        }

        int[] registers = new int[YierdisHyperLogLog.REGISTERS];
        for (byte[] keyBytes : sourceKeys) {
            YierdisObject e = getObjectIfNotExpired(keyBytes);
            if (e == null) {
                continue;
            }
            if (e.type != ValueType.STRING) {
                throw new WrongTypeException();
            }
            if (!YierdisHyperLogLog.isHllString(e)) {
                throw new YierdisCommandException("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
            YierdisHyperLogLog.mergeHllIntoRegisters(e.stringBytesView(), registers);
        }

        byte[] mergedDense = YierdisHyperLogLog.denseBytesFromRegisters(registers);
        long now = System.currentTimeMillis();
        final long[] deltaBytes = new long[]{0};
        try {
            store.compute(destKeyBytes, (k, old) -> {
                long oldEstimate = old == null ? 0 : old.estimatedBytes;
                if (old != null && isKeyExpired(k, now)) {
                    old.releasePayloadIfAny();
                    removeExpire(k);
                    deltaBytes[0] -= oldEstimate;
                    old = null;
                    oldEstimate = 0;
                }

                if (old == null) {
                    YierdisObject next = YierdisObject.newString(offHeapAllocator, mergedDense);
                    touch(next);
                    refreshEstimatedBytes(k, next);
                    deltaBytes[0] += next.estimatedBytes;
                    return next;
                }

                old.overwriteWithString(offHeapAllocator, mergedDense);
                touch(old);
                deltaBytes[0] -= oldEstimate;
                refreshEstimatedBytes(k, old);
                deltaBytes[0] += old.estimatedBytes;
                return old;
            });
        } catch (YierdisOffHeapOutOfMemoryException e) {
            throw new YierdisCommandException("OOM off-heap memory limit exceeded");
        }
        usedBytes += deltaBytes[0];
        // 与 SET 类似：PFMERGE 结果写入后应清除 destKey 的 TTL。
        removeExpire(destKeyBytes);
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
        YierdisOffHeapAddressAllocator addressAllocator =
                offHeapAllocator instanceof YierdisOffHeapAddressAllocator a ? a : null;
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
                ListValue lv = addressAllocator != null ? new ListValue(addressAllocator) : new ListValue();
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
        if (count == 0) {
            return Collections.emptyList();
        }
        if (count < 0) {
            throw new IllegalArgumentException("count must be >= 0");
        }
        long now = System.currentTimeMillis();
        final List<byte[]>[] popped = new List[]{null};
        final long[] deltaBytes = new long[]{0};
        store.computeIfPresent(keyBytes, (k, old) -> {
            long oldEstimate = old.estimatedBytes;
            if (isKeyExpired(k, now)) {
                old.releasePayloadIfAny();
                removeExpire(k);
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
        return popped[0];
    }

    public int hset(byte[] keyBytes, List<byte[]> fieldValuePairs) {
        checkThread();
        if (fieldValuePairs.size() % 2 != 0) {
            throw new YierdisCommandException("ERR wrong number of arguments for 'hset' command");
        }
        long now = System.currentTimeMillis();
        YierdisOffHeapAddressAllocator addressAllocator =
                offHeapAllocator instanceof YierdisOffHeapAddressAllocator a ? a : null;
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
                HashValue hv = addressAllocator != null ? new HashValue(addressAllocator) : new HashValue();
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
        YierdisOffHeapAddressAllocator addressAllocator =
                offHeapAllocator instanceof YierdisOffHeapAddressAllocator a ? a : null;
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
                SetValue sv = addressAllocator != null ? new SetValue(addressAllocator) : new SetValue();
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
        YierdisOffHeapAddressAllocator addressAllocator =
                offHeapAllocator instanceof YierdisOffHeapAddressAllocator a ? a : null;
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
                ZSetValue zv = addressAllocator != null ? new ZSetValue(addressAllocator) : new ZSetValue();
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
        long timeLimitNanos = startNanos + expireCleanupTimeLimitNanos;
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
        if (pattern == null || text == null) {
            return false;
        }

        int p = 0;
        int t = 0;
        int star = -1;
        int starText = 0;

        while (t < text.length) {
            if (p < pattern.length) {
                byte pc = pattern[p];

                if (pc == '*') {
                    star = p++;
                    starText = t;
                    continue;
                }

                if (pc == '?') {
                    p++;
                    t++;
                    continue;
                }

                if (pc == '\\') {
                    if (p + 1 < pattern.length) {
                        byte literal = pattern[p + 1];
                        if (literal == text[t]) {
                            p += 2;
                            t++;
                            continue;
                        }
                    } else {
                        // Trailing "\" is treated as a literal backslash.
                        if (text[t] == '\\') {
                            p++;
                            t++;
                            continue;
                        }
                    }
                } else if (pc == '[') {
                    int end = findGlobClassEnd(pattern, p + 1);
                    if (end >= 0) {
                        if (globClassMatches(pattern, p + 1, end, text[t])) {
                            p = end + 1;
                            t++;
                            continue;
                        }
                    } else {
                        // Unclosed "[]" is treated as a literal '['.
                        if (text[t] == '[') {
                            p++;
                            t++;
                            continue;
                        }
                    }
                } else if (pc == text[t]) {
                    p++;
                    t++;
                    continue;
                }
            }

            if (star >= 0) {
                // Backtrack: let '*' absorb one more byte.
                p = star + 1;
                t = ++starText;
                continue;
            }
            return false;
        }

        // Remaining pattern must be empty or only "*" wildcards.
        while (p < pattern.length && pattern[p] == '*') {
            p++;
        }
        return p == pattern.length;
    }

    private static int findGlobClassEnd(byte[] pattern, int start) {
        if (pattern == null) {
            return -1;
        }
        int len = pattern.length;
        if (start >= len) {
            return -1;
        }

        int i = start;
        // Optional negation marker.
        if (i < len && (pattern[i] == '^' || pattern[i] == '!')) {
            i++;
        }

        boolean first = true;
        while (i < len) {
            byte c = pattern[i];
            if (c == '\\') {
                // Escaped byte inside the class.
                i += i + 1 < len ? 2 : 1;
                first = false;
                continue;
            }
            if (c == ']' && !first) {
                return i;
            }
            i++;
            first = false;
        }
        return -1;
    }

    private static boolean globClassMatches(byte[] pattern, int start, int end, byte target) {
        if (pattern == null) {
            return false;
        }
        if (start < 0 || end < start || end >= pattern.length) {
            return false;
        }

        int i = start;
        boolean negate = false;
        if (i < end && (pattern[i] == '^' || pattern[i] == '!')) {
            negate = true;
            i++;
        }

        int tb = target & 0xff;
        boolean matched = false;

        // ']' can be included as a literal if it's the first char (after optional negation).
        if (i < end && pattern[i] == ']') {
            if (tb == (']' & 0xff)) {
                matched = true;
            }
            i++;
        }

        while (i < end) {
            int c1;
            if (pattern[i] == '\\' && i + 1 < end) {
                c1 = pattern[i + 1] & 0xff;
                i += 2;
            } else {
                c1 = pattern[i] & 0xff;
                i++;
            }

            // Range: "a-z" (only if '-' is not the last char in the class)
            if (i < end - 1 && pattern[i] == '-') {
                int j = i + 1;
                int c2;
                if (pattern[j] == '\\' && j + 1 < end) {
                    c2 = pattern[j + 1] & 0xff;
                    j += 2;
                } else {
                    c2 = pattern[j] & 0xff;
                    j++;
                }

                int lo = Math.min(c1, c2);
                int hi = Math.max(c1, c2);
                if (tb >= lo && tb <= hi) {
                    matched = true;
                }
                i = j;
                continue;
            }

            if (tb == c1) {
                matched = true;
            }
        }

        return negate ? !matched : matched;
    }

    public enum SetMode {
        NORMAL,
        NX,
        XX
    }

    public static final class ExpireOption {
        public enum Kind {
            KEEP_TTL,
            EX,
            PX,
            EXAT,
            PXAT
        }

        final Kind kind;
        final long value;

        private ExpireOption(Kind kind, long value) {
            this.kind = Objects.requireNonNull(kind, "kind");
            this.value = value;
        }

        public static ExpireOption keepTtl() {
            return new ExpireOption(Kind.KEEP_TTL, 0L);
        }

        public static ExpireOption ex(long seconds) {
            return new ExpireOption(Kind.EX, seconds);
        }

        public static ExpireOption px(long milliseconds) {
            return new ExpireOption(Kind.PX, milliseconds);
        }

        public static ExpireOption exAt(long unixSeconds) {
            return new ExpireOption(Kind.EXAT, unixSeconds);
        }

        public static ExpireOption pxAt(long unixMilliseconds) {
            return new ExpireOption(Kind.PXAT, unixMilliseconds);
        }

        boolean isKeepTtl() {
            return kind == Kind.KEEP_TTL;
        }

        long toExpireAtMillis(long nowMillis) {
            return switch (kind) {
                case KEEP_TTL -> throw new IllegalStateException("KEEP_TTL has no expireAtMillis");
                case EX -> safeExpireRelativeMillis(nowMillis, value, TimeUnit.SECONDS);
                case PX -> safeExpireRelativeMillis(nowMillis, value, TimeUnit.MILLISECONDS);
                case EXAT -> safeExpireAbsoluteMillis(value, TimeUnit.SECONDS);
                case PXAT -> safeExpireAbsoluteMillis(value, TimeUnit.MILLISECONDS);
            };
        }

        private static long safeExpireRelativeMillis(long nowMillis, long duration, TimeUnit unit) {
            if (duration <= 0) {
                return nowMillis;
            }
            long deltaMillis;
            try {
                deltaMillis = Math.multiplyExact(duration, unit == TimeUnit.SECONDS ? 1000L : 1L);
            } catch (ArithmeticException e) {
                return Long.MAX_VALUE;
            }
            try {
                return Math.addExact(nowMillis, deltaMillis);
            } catch (ArithmeticException e) {
                return Long.MAX_VALUE;
            }
        }

        private static long safeExpireAbsoluteMillis(long value, TimeUnit unit) {
            if (unit == TimeUnit.MILLISECONDS) {
                return value;
            }
            try {
                return Math.multiplyExact(value, 1000L);
            } catch (ArithmeticException e) {
                return Long.MAX_VALUE;
            }
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
