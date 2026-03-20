package yier.bubu.redis.db;

import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.offheap.api.OffHeapAddressAllocator;
import yier.bubu.redis.offheap.api.OffHeapAllocator;
import yier.bubu.redis.offheap.api.OffHeapBuf;
import yier.bubu.redis.offheap.api.OffHeapOutOfMemoryException;
import yier.bubu.redis.ops.ScanCursorV2;
import yier.bubu.redis.ops.ValueType;
import yier.bubu.redis.db.memory.offheap.YierdisUnsafeOffHeapExpireIndex;
import yier.bubu.redis.db.memory.offheap.YierdisUnsafeOffHeapKeyspace;
import yier.bubu.redis.db.memory.offheap.YierdisUnsafeOffHeapString;
import yier.bubu.redis.db.key.KeyHandle;
import yier.bubu.redis.db.memory.MemoryLedger;
import yier.bubu.redis.db.memory.MemoryLedgerOutOfMemoryException;
import yier.bubu.redis.db.memory.MemoryReservation;
import yier.bubu.redis.ops.DbLifecycleOps;
import yier.bubu.redis.ops.DbReads;
import yier.bubu.redis.ops.DbEngine;
import yier.bubu.redis.ops.DbMemoryConstants;
import yier.bubu.redis.ops.DbWrites;
import yier.bubu.redis.ops.EvictionCoordinator;
import yier.bubu.redis.ops.ExpireOption;
import yier.bubu.redis.ops.ExpirationManager;
import yier.bubu.redis.ops.KeyspaceOps;
import yier.bubu.redis.ops.MaxmemoryCandidate;
import yier.bubu.redis.ops.MaxmemoryCoordinator;
import yier.bubu.redis.ops.MaxmemoryCoordinatorAware;
import yier.bubu.redis.ops.MaxmemoryErrors;
import yier.bubu.redis.ops.MaxmemoryParticipant;
import yier.bubu.redis.ops.MemoryOps;
import yier.bubu.redis.ops.RuntimeDbEngine;
import yier.bubu.redis.ops.SetMode;
import yier.bubu.redis.ops.TtlOps;
import yier.bubu.redis.ops.ValueOps;
import yier.bubu.redis.ops.WrongTypeException;
import yier.bubu.redis.ops.YierdisMemoryStats;
import yier.bubu.redis.ops.YierdisCommandException;
import yier.bubu.redis.runtime.api.YierdisChangeTracking;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public final class YierdisDb implements YierdisSnapshot, RuntimeDbEngine, MaxmemoryCoordinatorAware, MaxmemoryParticipant {
    public enum MaxmemoryPolicy {
        NOEVICTION,
        ALLKEYS_RANDOM,
        ALLKEYS_LRU
    }

    private static final long TTL_ENTRY_BYTES_ESTIMATE = DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE;

    final YierdisKeyspace<YierdisObject> store;
    final YierdisExpireIndex expires;
    final OffHeapAllocator offHeapAllocator;
    private final boolean ownsOffHeapAllocator;
    private final boolean keysStoredOffHeap;

    /**
     * @deprecated Use {@link MaxmemoryErrors#OOM_ERR}.
     */
    @Deprecated
    static final String OOM_ERR = MaxmemoryErrors.OOM_ERR;

    private final long maxmemoryBytes;
    private final MaxmemoryPolicy maxmemoryPolicy;
    private final int maxmemorySamples;
    private final boolean lruEnabled;
    private final long evictionTimeLimitNanos;
    private final long expireCleanupTimeLimitNanos;

    long usedBytes;
    long reservedBytes;
    private long lruClock;

    private final DbThreadGuard threadGuard = new DbThreadGuard();
    private volatile MaxmemoryCoordinator maxmemoryCoordinator;
    private final MemoryLedger ledger;
    private MemoryReservation activeReservation;
    private final YierdisDbMutationExecutor mutationExecutor;

    private final DbReads reads;
    private final DbWrites writes;
    private final ValueOps values;
    private final ExpirationManager expirationManager;
    private final EvictionCoordinator evictionCoordinator;
    private final KeyspaceOps keyspaceOps;
    private final TtlOps ttlOps;
    private final MemoryOps memoryOps;
    private final DbLifecycleOps lifecycleOps;

    public YierdisDb() {
        this(null, false, 0, "noeviction", 5, 5, 5);
    }

    public YierdisDb(OffHeapAllocator offHeapAllocator) {
        this(offHeapAllocator, false, false, 0, "noeviction", 5, 5, 5);
    }

    public YierdisDb(
            OffHeapAllocator offHeapAllocator,
            long maxmemoryBytes,
            String maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis
    ) {
        this(offHeapAllocator, false, false, maxmemoryBytes, maxmemoryPolicy, maxmemorySamples, evictionTimeLimitMillis, expireCleanupTimeLimitMillis);
    }

    public YierdisDb(
            OffHeapAllocator offHeapAllocator,
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
            OffHeapAllocator offHeapAllocator,
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
        if (offHeapKeysEnabled && !(offHeapAllocator instanceof OffHeapAddressAllocator)) {
            throw new IllegalArgumentException("offHeapKeysEnabled requires an address allocator (unsafe off-heap backend)");
        }
        if (offHeapKeysEnabled && offHeapAllocator instanceof OffHeapAddressAllocator addressAllocator) {
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
        this.ledger = new DbMemoryLedger();
        this.mutationExecutor = new YierdisDbMutationExecutor(this);
        this.values = new YierdisDbValueOps(this);
        this.keyspaceOps = new YierdisDbKeyspaceOps(this);
        this.ttlOps = new YierdisDbTtlOps(this);
        this.reads = new YierdisDbReads(this, values, keyspaceOps, ttlOps);
        this.writes = new YierdisDbWrites(this, values, keyspaceOps, ttlOps);
        this.expirationManager = new YierdisDbExpirationManager(this);
        this.evictionCoordinator = new YierdisDbEvictionCoordinator(this);
        this.memoryOps = new YierdisDbMemoryOps(this);
        this.lifecycleOps = new YierdisDbLifecycleOps(this);
        // Scheduling (if any) is done by the Netty event loop in YierdisServer, not by a dedicated thread.
    }

    @Override
    public DbReads reads() {
        return reads;
    }

    @Override
    public DbWrites writes() {
        return writes;
    }

    @Override
    public ValueOps values() {
        return values;
    }

    @Override
    public ExpirationManager expiration() {
        return expirationManager;
    }

    @Override
    public EvictionCoordinator eviction() {
        return evictionCoordinator;
    }

    @Override
    public KeyspaceOps keyspace() {
        return keyspaceOps;
    }

    @Override
    public TtlOps ttl() {
        return ttlOps;
    }

    @Override
    public MemoryOps memory() {
        return memoryOps;
    }

    @Override
    public DbLifecycleOps lifecycle() {
        return lifecycleOps;
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

    @Override
    public void attachMaxmemoryCoordinator(MaxmemoryCoordinator coordinator) {
        this.maxmemoryCoordinator = coordinator;
    }

    public void ensureWriteAllowed(long additionalBytes) {
        checkThread();
        MaxmemoryCoordinator coordinator = maxmemoryCoordinator;
        if (coordinator != null) {
            if (maxmemoryPolicy == MaxmemoryPolicy.NOEVICTION) {
                coordinator.prepareWrite(Math.max(0, additionalBytes));
            }
            return;
        }
        if (maxmemoryBytes <= 0) {
            return;
        }
        if (maxmemoryPolicy != MaxmemoryPolicy.NOEVICTION) {
            return;
        }
        long extra = Math.max(0, additionalBytes);
        if (usedBytesForMaxmemory() + extra > maxmemoryBytes) {
            throw new YierdisCommandException(MaxmemoryErrors.OOM_ERR);
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
     * maxmemory errors happen before writing the reply (avoiding double replies / protocol corruption).
     *
     * @param estimatedExtraBytes best-effort upper bound of the additional bytes the write may consume.
     */
    public void prepareWrite(long estimatedExtraBytes) {
        checkThread();
        if (activeReservation != null) {
            throw new IllegalStateException("prepareWrite called with an active reservation");
        }
        try {
            activeReservation = ledger.reserve(Math.max(0L, estimatedExtraBytes));
        } catch (MemoryLedgerOutOfMemoryException e) {
            throw new YierdisCommandException(MaxmemoryErrors.OOM_ERR);
        }
    }

    public void rollbackWriteReservationIfAny() {
        checkThread();
        if (activeReservation == null) {
            return;
        }
        rollbackWrite();
    }

    void commitWrite(long actualDeltaBytes) {
        MemoryReservation reservation = activeReservation;
        activeReservation = null;
        ledger.commit(reservation, actualDeltaBytes);
    }

    void rollbackWrite() {
        MemoryReservation reservation = activeReservation;
        activeReservation = null;
        ledger.rollback(reservation);
    }

    void adjustUsedBytes(long deltaBytes) {
        ledger.commit(null, deltaBytes);
    }

    MemoryReservation reserveMutation(long estimatedUpperBoundBytes) {
        checkThread();
        return ledger.reserve(Math.max(0L, estimatedUpperBoundBytes));
    }

    void commitMutation(MemoryReservation reservation, long actualDeltaBytes) {
        ledger.commit(reservation, actualDeltaBytes);
    }

    void rollbackMutation(MemoryReservation reservation) {
        ledger.rollback(reservation);
    }

    public void enforceMaxmemory() {
        checkThread();
        // Best-effort background enforcement: rely on ledger SSOT for eviction/cleanup.
        // Note: This method intentionally does not throw on "noeviction" when already above maxmemory;
        // writes are rejected at reserve-time when they are expected to increase memory.
        try {
            ledger.reserve(0);
        } catch (MemoryLedgerOutOfMemoryException e) {
            throw new YierdisCommandException(MaxmemoryErrors.OOM_ERR);
        }
    }

    @Override
    public void enforceMaxmemoryMaintenance() {
        enforceMaxmemory();
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
                adjustUsedBytes(-e.estimatedBytes);
            }
        }
    }

    private static byte[] toByteArray(BytesView view) {
        if (view == null) {
            throw new IllegalArgumentException("view must not be null");
        }
        int len = view.len();
        if (len < 0) {
            return null;
        }
        if (len == 0) {
            return new byte[0];
        }
        byte[] out = new byte[len];
        for (int i = 0; i < len; i++) {
            out[i] = view.byteAt(i);
        }
        return out;
    }

    YierdisObject getObjectIfNotExpired(BytesView keyView) {
        KeyHandle handle = store.keyHandle(keyView);
        if (handle == null) {
            return null;
        }
        return getObjectIfNotExpired(handle);
    }

    public long memoryUsage(BytesView keyView) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyView);
        if (e == null) {
            return -1;
        }
        long keyLen = 0;
        if (keyView != null) {
            keyLen = Math.max(0L, (long) keyView.len());
        }
        return e.estimatedBytes + estimateOffHeapBytesForMemoryUsage(keyLen, e);
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
        return e.estimatedBytes + estimateOffHeapBytesForMemoryUsage(keyBytes.length, e);
    }

    private long estimateOffHeapBytesForMemoryUsage(long keyLen, YierdisObject e) {
        if (offHeapAllocator == null || e == null) {
            return 0;
        }
        long extra = 0;
        if (keysStoredOffHeap && keyLen > 0) {
            extra += keyLen;
        }
        if (e.type == ValueType.STRING) {
            if (e.payload instanceof OffHeapBuf buf) {
                extra += buf.capacity();
            } else if (e.payload instanceof YierdisUnsafeOffHeapString s) {
                extra += s.capacity();
            }
        }
        return extra;
    }

    public String objectEncoding(BytesView keyView) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyView);
        if (e == null) {
            return null;
        }
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

    @Override
    public long usedBytesForMaxmemory() {
        checkThread();
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
        long ttlBytes = estimateTtlBytesForMaxmemory();
        long total = usedBytes + offHeapUsedBytes;
        if (ttlBytes <= 0) {
            return total;
        }
        if (Long.MAX_VALUE - total < ttlBytes) {
            return Long.MAX_VALUE;
        }
        return total + ttlBytes;
    }

    private long estimateTtlBytesForMaxmemory() {
        if (TTL_ENTRY_BYTES_ESTIMATE <= 0) {
            return 0;
        }
        int ttlCount;
        try {
            ttlCount = expires.size();
        } catch (Throwable ignored) {
            ttlCount = 0;
        }
        if (ttlCount <= 0) {
            return 0;
        }
        try {
            return Math.multiplyExact((long) ttlCount, TTL_ENTRY_BYTES_ESTIMATE);
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    @Override
    public int keyCountEstimate() {
        checkThread();
        int size;
        try {
            size = store.size();
        } catch (Throwable ignored) {
            size = 0;
        }
        return Math.max(0, size);
    }

    @Override
    public void cleanupExpired(long nowMillis) {
        checkThread();
        cleanupExpiredAtMillis(nowMillis);
    }

    private void cleanupExpiredAtMillis(long nowMillis) {
        long startNanos = System.nanoTime();
        long timeLimitNanos = startNanos + expireCleanupTimeLimitNanos;
        int loops = 0;

        long nowFixed = nowMillis <= 0 ? System.currentTimeMillis() : nowMillis;

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

            for (int i = 0; i < samples; i++) {
                if (keysStoredOffHeap) {
                    KeyHandle keyHandle = expires.randomKeyHandle();
                    if (keyHandle == null) {
                        break;
                    }

                    Long expireAtMillis = expires.get(keyHandle);
                    if (expireAtMillis == null) {
                        removeExpire(keyHandle);
                        continue;
                    }

                    YierdisObject e = store.get(keyHandle);
                    if (e == null) {
                        removeExpire(keyHandle);
                        continue;
                    }

                    if (expireAtMillis <= nowFixed) {
                        removeExpire(keyHandle);
                        if (store.remove(keyHandle, e)) {
                            e.releasePayloadIfAny();
                            adjustUsedBytes(-e.estimatedBytes);
                        }
                        expired++;
                    }
                    continue;
                }

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

                if (expireAtMillis <= nowFixed) {
                    removeExpire(keyBytes);
                    if (store.remove(keyBytes, e)) {
                        e.releasePayloadIfAny();
                        adjustUsedBytes(-e.estimatedBytes);
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

    @Override
    public MaxmemoryCandidate sampleCandidate(yier.bubu.redis.ops.MaxmemoryPolicy policy, long nowMillis) {
        checkThread();
        if (policy == null) {
            return null;
        }
        if (policy == yier.bubu.redis.ops.MaxmemoryPolicy.NOEVICTION) {
            return null;
        }
        if (store.size() == 0) {
            return null;
        }

        byte[] key = store.randomKey();
        if (key == null) {
            return null;
        }
        YierdisObject e = store.get(key);
        if (e == null) {
            return null;
        }
        if (isKeyExpired(key, nowMillis)) {
            return null;
        }

        long lruClock = policy == yier.bubu.redis.ops.MaxmemoryPolicy.ALLKEYS_LRU ? e.lruClock : 0L;
        return new MaxmemoryCandidate(this, key, lruClock);
    }

    @Override
    public MaxmemoryCandidate scanBestCandidate(yier.bubu.redis.ops.MaxmemoryPolicy policy, long nowMillis) {
        checkThread();
        if (policy != yier.bubu.redis.ops.MaxmemoryPolicy.ALLKEYS_LRU) {
            return null;
        }
        if (store.size() == 0) {
            return null;
        }

        final KeyHandle[] bestKeyHandleRef = new KeyHandle[1];
        final long[] bestLruRef = new long[]{Long.MAX_VALUE};
        store.forEachKeyHandle((k, e) -> {
            if (k == null || e == null) {
                return;
            }
            if (isKeyExpired(k, nowMillis)) {
                return;
            }
            long lru = e.lruClock;
            if (bestKeyHandleRef[0] == null || lru < bestLruRef[0]) {
                bestKeyHandleRef[0] = k;
                bestLruRef[0] = lru;
            }
        });

        KeyHandle bestKeyHandle = bestKeyHandleRef[0];
        if (bestKeyHandle == null) {
            return null;
        }
        byte[] keyBytes = toByteArray(bestKeyHandle);
        if (keyBytes == null) {
            return null;
        }
        return new MaxmemoryCandidate(this, keyBytes, bestLruRef[0]);
    }

    @Override
    public boolean evict(MaxmemoryCandidate candidate, long nowMillis) {
        checkThread();
        if (candidate == null) {
            return false;
        }
        if (candidate.owner() != this) {
            return false;
        }

        byte[] key = candidate.key();
        YierdisObject e = store.get(key);
        if (e == null) {
            return false;
        }
        if (removeIfExpired(key, e, nowMillis)) {
            return true;
        }
        removeExpire(key);
        if (store.remove(key, e)) {
            e.releasePayloadIfAny();
            adjustUsedBytes(-e.estimatedBytes);
            return true;
        }
        return false;
    }

    void touch(YierdisObject e) {
        if (!lruEnabled || e == null) {
            return;
        }
        MaxmemoryCoordinator coordinator = maxmemoryCoordinator;
        if (coordinator != null) {
            e.lruClock = coordinator.nextLruClock();
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

    void checkThread() {
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
        reservedBytes = 0;
        activeReservation = null;
        if (ownsOffHeapAllocator && offHeapAllocator != null) {
            offHeapAllocator.close();
        }
    }

    public void flushDb() {
        checkThread();
        boolean hadKeys = store.size() != 0;
        boolean hadTtl = expires.size() != 0;
        store.forEach((k, e) -> e.releasePayloadIfAny());
        store.clear();
        expires.clear();
        usedBytes = 0;
        reservedBytes = 0;
        activeReservation = null;
        if (hadKeys) {
            YierdisChangeTracking.markValueChanged();
        }
        if (hadTtl) {
            YierdisChangeTracking.markTtlChanged();
        }
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
                reservedBytes,
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

    private long estimateEntryBytes(KeyHandle keyHandle, YierdisObject e) {
        if (keyHandle == null || e == null) {
            return 0;
        }
        int keyLen = Math.max(0, keyHandle.len());
        int keyBytesCost = keysStoredOffHeap ? 0 : keyLen;
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
            if (offHeapAllocator != null && (e.payload instanceof OffHeapBuf || e.payload instanceof YierdisUnsafeOffHeapString)) {
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

    void refreshEstimatedBytes(byte[] keyBytes, YierdisObject e) {
        if (e == null) {
            return;
        }
        e.estimatedBytes = estimateEntryBytes(keyBytes, e);
    }

    void refreshEstimatedBytes(KeyHandle keyHandle, YierdisObject e) {
        if (e == null) {
            return;
        }
        e.estimatedBytes = estimateEntryBytes(keyHandle, e);
    }

    public long del(Collection<byte[]> keys) {
        checkThread();
        return mutationExecutor.execute(new YierdisDbMutationExecutor.MutationPlan<Long>() {
            @Override
            public long upperBoundBytes() {
                return 0;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<Long> apply() {
                long now = System.currentTimeMillis();
                long removed = 0;
                long deltaBytes = 0;
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
                        deltaBytes -= e.estimatedBytes;
                        removed++;
                    }
                }
                if (removed > 0) {
                    YierdisChangeTracking.markValueChanged();
                }
                return YierdisDbMutationExecutor.MutationResult.of(removed, deltaBytes);
            }
        });
    }

    public boolean existsKey(BytesView keyView) {
        checkThread();
        return getObjectIfNotExpired(keyView) != null;
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

    public ValueType typeOf(BytesView keyView) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyView);
        if (e == null) {
            return null;
        }
        return e.type;
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
        KeyHandle handle = store.keyHandle(keyBytes);
        if (handle == null) {
            return false;
        }
        YierdisObject e = store.get(handle);
        if (e == null) {
            return false;
        }
        long nowMillis = System.currentTimeMillis();
        if (removeIfExpired(handle, e, nowMillis)) {
            return false;
        }
        if (seconds <= 0) {
            // Redis-compatible: seconds<=0 means the key is deleted immediately (if it exists).
            return mutationExecutor.execute(new YierdisDbMutationExecutor.MutationPlan<Boolean>() {
                @Override
                public long upperBoundBytes() {
                    return 0;
                }

                @Override
                public YierdisDbMutationExecutor.MutationResult<Boolean> apply() {
                    long deltaBytes = 0;
                    removeExpire(handle);
                    if (store.remove(handle, e)) {
                        e.releasePayloadIfAny();
                        deltaBytes -= e.estimatedBytes;
                    }
                    YierdisChangeTracking.markValueChanged();
                    return YierdisDbMutationExecutor.MutationResult.of(true, deltaBytes);
                }
            });
        }

        long expireAtMillis = safeExpireAtMillis(nowMillis, seconds);
        long upperBound = expires.get(handle) == null ? TTL_ENTRY_BYTES_ESTIMATE : 0;
        return mutationExecutor.execute(new YierdisDbMutationExecutor.MutationPlan<Boolean>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<Boolean> apply() {
                setExpireAtMillis(handle, expireAtMillis);
                touch(e);
                YierdisChangeTracking.markTtlChanged();
                return YierdisDbMutationExecutor.MutationResult.of(true, 0);
            }
        });
    }

    public boolean expire(BytesView keyView, long seconds) {
        checkThread();
        KeyHandle handle = store.keyHandle(keyView);
        if (handle == null) {
            return false;
        }
        YierdisObject e = store.get(handle);
        if (e == null) {
            return false;
        }
        long nowMillis = System.currentTimeMillis();
        if (removeIfExpired(handle, e, nowMillis)) {
            return false;
        }
        if (seconds <= 0) {
            // Redis-compatible: seconds<=0 means the key is deleted immediately (if it exists).
            return mutationExecutor.execute(new YierdisDbMutationExecutor.MutationPlan<Boolean>() {
                @Override
                public long upperBoundBytes() {
                    return 0;
                }

                @Override
                public YierdisDbMutationExecutor.MutationResult<Boolean> apply() {
                    long deltaBytes = 0;
                    removeExpire(handle);
                    if (store.remove(handle, e)) {
                        e.releasePayloadIfAny();
                        deltaBytes -= e.estimatedBytes;
                    }
                    YierdisChangeTracking.markValueChanged();
                    return YierdisDbMutationExecutor.MutationResult.of(true, deltaBytes);
                }
            });
        }

        long expireAtMillis = safeExpireAtMillis(nowMillis, seconds);
        long upperBound = expires.get(handle) == null ? TTL_ENTRY_BYTES_ESTIMATE : 0;
        return mutationExecutor.execute(new YierdisDbMutationExecutor.MutationPlan<Boolean>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<Boolean> apply() {
                setExpireAtMillis(handle, expireAtMillis);
                touch(e);
                YierdisChangeTracking.markTtlChanged();
                return YierdisDbMutationExecutor.MutationResult.of(true, 0);
            }
        });
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

    private static long estimateStringWriteUpperBound(int keyLength, int valueLength) {
        return (long) Math.max(0, keyLength) + Math.max(0, valueLength) + DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE;
    }

    public boolean pexpire(byte[] keyBytes, long milliseconds) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return false;
        }
        if (milliseconds <= 0) {
            // Redis-compatible: milliseconds<=0 means the key is deleted immediately (if it exists).
            return mutationExecutor.execute(new YierdisDbMutationExecutor.MutationPlan<Boolean>() {
                @Override
                public long upperBoundBytes() {
                    return 0;
                }

                @Override
                public YierdisDbMutationExecutor.MutationResult<Boolean> apply() {
                    long deltaBytes = 0;
                    removeExpire(keyBytes);
                    if (store.remove(keyBytes, e)) {
                        e.releasePayloadIfAny();
                        deltaBytes -= e.estimatedBytes;
                    }
                    YierdisChangeTracking.markValueChanged();
                    return YierdisDbMutationExecutor.MutationResult.of(true, deltaBytes);
                }
            });
        }

        KeyHandle handle = store.keyHandle(keyBytes);
        long expireAtMillis = safeAddMillis(System.currentTimeMillis(), milliseconds);
        long upperBound = handle != null && expires.get(handle) == null ? TTL_ENTRY_BYTES_ESTIMATE : 0;
        return mutationExecutor.execute(new YierdisDbMutationExecutor.MutationPlan<Boolean>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<Boolean> apply() {
                if (handle != null) {
                    setExpireAtMillis(handle, expireAtMillis);
                } else {
                    setExpireAtMillis(keyBytes, expireAtMillis);
                }
                YierdisChangeTracking.markTtlChanged();
                return YierdisDbMutationExecutor.MutationResult.of(true, 0);
            }
        });
    }

    public boolean pexpire(BytesView keyView, long milliseconds) {
        checkThread();
        KeyHandle handle = store.keyHandle(keyView);
        if (handle == null) {
            return false;
        }
        YierdisObject e = store.get(handle);
        if (e == null) {
            return false;
        }
        long nowMillis = System.currentTimeMillis();
        if (removeIfExpired(handle, e, nowMillis)) {
            return false;
        }
        touch(e);

        if (milliseconds <= 0) {
            // Redis-compatible: milliseconds<=0 means the key is deleted immediately (if it exists).
            return mutationExecutor.execute(new YierdisDbMutationExecutor.MutationPlan<Boolean>() {
                @Override
                public long upperBoundBytes() {
                    return 0;
                }

                @Override
                public YierdisDbMutationExecutor.MutationResult<Boolean> apply() {
                    long deltaBytes = 0;
                    removeExpire(handle);
                    if (store.remove(handle, e)) {
                        e.releasePayloadIfAny();
                        deltaBytes -= e.estimatedBytes;
                    }
                    YierdisChangeTracking.markValueChanged();
                    return YierdisDbMutationExecutor.MutationResult.of(true, deltaBytes);
                }
            });
        }

        long expireAtMillis = safeAddMillis(nowMillis, milliseconds);
        long upperBound = expires.get(handle) == null ? TTL_ENTRY_BYTES_ESTIMATE : 0;
        return mutationExecutor.execute(new YierdisDbMutationExecutor.MutationPlan<Boolean>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<Boolean> apply() {
                setExpireAtMillis(handle, expireAtMillis);
                YierdisChangeTracking.markTtlChanged();
                return YierdisDbMutationExecutor.MutationResult.of(true, 0);
            }
        });
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

    public boolean expireAtSeconds(BytesView keyView, long unixSeconds) {
        checkThread();
        long expireAtMillis;
        try {
            expireAtMillis = Math.multiplyExact(unixSeconds, 1000L);
        } catch (ArithmeticException e) {
            expireAtMillis = Long.MAX_VALUE;
        }
        return expireAtMillis(keyView, expireAtMillis);
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
            return mutationExecutor.execute(new YierdisDbMutationExecutor.MutationPlan<Boolean>() {
                @Override
                public long upperBoundBytes() {
                    return 0;
                }

                @Override
                public YierdisDbMutationExecutor.MutationResult<Boolean> apply() {
                    long deltaBytes = 0;
                    removeExpire(keyBytes);
                    if (store.remove(keyBytes, e)) {
                        e.releasePayloadIfAny();
                        deltaBytes -= e.estimatedBytes;
                    }
                    YierdisChangeTracking.markValueChanged();
                    return YierdisDbMutationExecutor.MutationResult.of(true, deltaBytes);
                }
            });
        }
        KeyHandle handle = store.keyHandle(keyBytes);
        long upperBound = handle != null && expires.get(handle) == null ? TTL_ENTRY_BYTES_ESTIMATE : 0;
        return mutationExecutor.execute(new YierdisDbMutationExecutor.MutationPlan<Boolean>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<Boolean> apply() {
                if (handle != null) {
                    setExpireAtMillis(handle, unixMillis);
                } else {
                    setExpireAtMillis(keyBytes, unixMillis);
                }
                YierdisChangeTracking.markTtlChanged();
                return YierdisDbMutationExecutor.MutationResult.of(true, 0);
            }
        });
    }

    public boolean expireAtMillis(BytesView keyView, long unixMillis) {
        checkThread();
        KeyHandle handle = store.keyHandle(keyView);
        if (handle == null) {
            return false;
        }
        YierdisObject e = store.get(handle);
        if (e == null) {
            return false;
        }
        long now = System.currentTimeMillis();
        if (removeIfExpired(handle, e, now)) {
            return false;
        }
        touch(e);

        if (unixMillis <= now) {
            // 过期时间在过去/现在：按 Redis 语义立刻删除 key（如果存在）。
            return mutationExecutor.execute(new YierdisDbMutationExecutor.MutationPlan<Boolean>() {
                @Override
                public long upperBoundBytes() {
                    return 0;
                }

                @Override
                public YierdisDbMutationExecutor.MutationResult<Boolean> apply() {
                    long deltaBytes = 0;
                    removeExpire(handle);
                    if (store.remove(handle, e)) {
                        e.releasePayloadIfAny();
                        deltaBytes -= e.estimatedBytes;
                    }
                    YierdisChangeTracking.markValueChanged();
                    return YierdisDbMutationExecutor.MutationResult.of(true, deltaBytes);
                }
            });
        }

        long upperBound = expires.get(handle) == null ? TTL_ENTRY_BYTES_ESTIMATE : 0;
        return mutationExecutor.execute(new YierdisDbMutationExecutor.MutationPlan<Boolean>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<Boolean> apply() {
                setExpireAtMillis(handle, unixMillis);
                YierdisChangeTracking.markTtlChanged();
                return YierdisDbMutationExecutor.MutationResult.of(true, 0);
            }
        });
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
        return mutationExecutor.execute(new YierdisDbMutationExecutor.MutationPlan<Boolean>() {
            @Override
            public long upperBoundBytes() {
                return 0;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<Boolean> apply() {
                removeExpire(keyBytes);
                touch(e);
                YierdisChangeTracking.markTtlChanged();
                return YierdisDbMutationExecutor.MutationResult.of(true, 0);
            }
        });
    }

    public boolean persist(BytesView keyView) {
        checkThread();
        KeyHandle handle = store.keyHandle(keyView);
        if (handle == null) {
            return false;
        }
        YierdisObject e = store.get(handle);
        if (e == null) {
            return false;
        }
        long nowMillis = System.currentTimeMillis();
        if (removeIfExpired(handle, e, nowMillis)) {
            return false;
        }

        touch(e);
        Long expireAtMillis = expires.get(handle);
        if (expireAtMillis == null) {
            return false;
        }
        return mutationExecutor.execute(new YierdisDbMutationExecutor.MutationPlan<Boolean>() {
            @Override
            public long upperBoundBytes() {
                return 0;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<Boolean> apply() {
                removeExpire(handle);
                YierdisChangeTracking.markTtlChanged();
                return YierdisDbMutationExecutor.MutationResult.of(true, 0);
            }
        });
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

    public long ttlSeconds(BytesView keyView) {
        checkThread();
        KeyHandle handle = store.keyHandle(keyView);
        if (handle == null) {
            return -2;
        }
        YierdisObject e = store.get(handle);
        if (e == null) {
            return -2;
        }

        long now = System.currentTimeMillis();
        if (removeIfExpired(handle, e, now)) {
            return -2;
        }
        touch(e);

        Long expireAtMillis = expires.get(handle);
        if (expireAtMillis == null) {
            return -1;
        }
        long remainingMillis = expireAtMillis - now;
        return remainingMillis <= 0 ? -2 : remainingMillis / 1000L;
    }

    public long ttlMillis(BytesView keyView) {
        checkThread();
        KeyHandle handle = store.keyHandle(keyView);
        if (handle == null) {
            return -2;
        }
        YierdisObject e = store.get(handle);
        if (e == null) {
            return -2;
        }

        long now = System.currentTimeMillis();
        if (removeIfExpired(handle, e, now)) {
            return -2;
        }
        touch(e);

        Long expireAtMillis = expires.get(handle);
        if (expireAtMillis == null) {
            return -1;
        }
        long remainingMillis = expireAtMillis - now;
        return remainingMillis <= 0 ? -2 : remainingMillis;
    }

    public List<byte[]> keys(byte[] globPattern) {
        return keys(globPattern, Integer.MAX_VALUE, 0L);
    }

    public List<byte[]> keys(byte[] globPattern, int maxMatches, long timeBudgetNanos) {
        checkThread();
        if (globPattern == null) {
            return Collections.emptyList();
        }
        int limit = maxMatches <= 0 ? 0 : maxMatches;
        if (limit == 0) {
            return Collections.emptyList();
        }

        long deadlineNanos = Long.MAX_VALUE;
        if (timeBudgetNanos > 0) {
            long nowNanos = System.nanoTime();
            try {
                deadlineNanos = Math.addExact(nowNanos, timeBudgetNanos);
            } catch (ArithmeticException e) {
                deadlineNanos = Long.MAX_VALUE;
            }
        }
        final long deadline = deadlineNanos;

        long nowMillis = System.currentTimeMillis();
        List<byte[]> out = new ArrayList<>();
        List<KeyHandle> expiredKeys = new ArrayList<>();
        List<YierdisObject> expiredValues = new ArrayList<>();
        final boolean[] timedOut = new boolean[]{false};

        ScanCursorV2 cursor = ScanCursorV2.start();
        int guard = 0;
        while (true) {
            if (System.nanoTime() >= deadline) {
                timedOut[0] = true;
                break;
            }
            ScanCursorV2 next = store.scan(cursor, 1024, (k, e) -> {
                if (k == null || e == null) {
                    return true;
                }
                if (isKeyExpired(k, nowMillis)) {
                    expiredKeys.add(k);
                    expiredValues.add(e);
                    return true;
                }
                if (globMatches(globPattern, k)) {
                    out.add(toByteArray(k));
                    if (out.size() >= limit) {
                        return false;
                    }
                }
                if (System.nanoTime() >= deadline) {
                    timedOut[0] = true;
                    return false;
                }
                return true;
            });
            cursor = next;
            if (cursor.value() == 0) {
                break;
            }
            if (out.size() >= limit || timedOut[0]) {
                break;
            }
            // 防御：避免意外 bug 导致死循环（例如 cursor 不前进）。
            if (++guard > 1_000_000) {
                throw new IllegalStateException("KEYS scan did not make progress");
            }
        }

        // KEYS 的历史行为：顺手清理扫描过程中发现的过期 key（best-effort）。
        for (int i = 0; i < expiredKeys.size(); i++) {
            KeyHandle key = expiredKeys.get(i);
            removeExpire(key);
            if (store.remove(key, expiredValues.get(i))) {
                expiredValues.get(i).releasePayloadIfAny();
                adjustUsedBytes(-expiredValues.get(i).estimatedBytes);
            }
        }

        // Redis 生态兼容：当时间预算耗尽或结果达到上限时，返回已收集到的部分结果（可能被截断），不再 fail-fast 抛错。
        // 若调用方需要可证明的完整遍历，请使用 SCAN。
        return out;
    }

    /**
     * Redis-compatible SCAN（best-effort）。
     * <p>
     * v2 游标通过 keyspace 层 iterator 实现 rehash-aware；仍保持 bulk string 数字兼容（{@code 0} 表示结束）。
     * 在数据集变化（写入/删除/过期清理）情况下不保证强一致，但尽量做到“可推进、可终止、不阻塞太久”。
     *
     * @param cursor      游标（{@code 0} 表示从头开始）
     * @param globPattern glob 过滤（可为 null 表示不过滤）
     * @param count       期望返回的最大 key 数（Redis 中 COUNT 是 hint，这里按上限处理；必须 > 0）
     * @param out         输出容器（追加写入）
     * @return 下一次扫描的游标；返回 {@code 0} 表示扫描结束
     */
    public ScanCursorV2 scan(ScanCursorV2 cursor, byte[] globPattern, int count, List<byte[]> out) {
        checkThread();
        Objects.requireNonNull(out, "out");
        if (count <= 0) {
            throw new IllegalArgumentException("count must be > 0");
        }

        long now = System.currentTimeMillis();
        List<KeyHandle> expiredKeys = new ArrayList<>();
        List<YierdisObject> expiredValues = new ArrayList<>();

        // COUNT 在 Redis 里是 hint：为了避免 MATCH 过滤导致“单次扫描跑完整个 keyspace”，加一个步数上限。
        int maxSteps = Math.max(64, count * 10);
        final int[] remaining = new int[]{count};

        ScanCursorV2 next = store.scan(cursor == null ? ScanCursorV2.start() : cursor, maxSteps, (k, e) -> {
            if (k == null || e == null) {
                return true;
            }
            if (isKeyExpired(k, now)) {
                expiredKeys.add(k);
                expiredValues.add(e);
                return true;
            }
            if (globPattern == null || globMatches(globPattern, k)) {
                out.add(toByteArray(k));
                remaining[0]--;
                if (remaining[0] <= 0) {
                    return false;
                }
            }
            return true;
        });

        // 清理本轮遍历过程中发现的过期 key（与 KEYS 类似的“顺手清理”语义）。
        for (int i = 0; i < expiredKeys.size(); i++) {
            KeyHandle key = expiredKeys.get(i);
            removeExpire(key);
            if (store.remove(key, expiredValues.get(i))) {
                expiredValues.get(i).releasePayloadIfAny();
                adjustUsedBytes(-expiredValues.get(i).estimatedBytes);
            }
        }
        return next;
    }

    @Override
    public ScanCursorV2 snapshot(ScanCursorV2 cursor, int count, List<YierdisSnapshotEntry> out) {
        checkThread();
        Objects.requireNonNull(out, "out");
        if (count <= 0) {
            throw new IllegalArgumentException("count must be > 0");
        }

        long now = System.currentTimeMillis();
        int maxSteps = Math.max(64, count * 10);
        final int[] remaining = new int[]{count};

        // 约束：快照读取不应产生副作用；过期 key 仅跳过，不在此处执行删除（删除由读/写/维护路径推进）。
        return store.scan(cursor == null ? ScanCursorV2.start() : cursor, maxSteps, (k, e) -> {
            if (k == null || e == null) {
                return true;
            }
            if (isKeyExpired(k, now)) {
                return true;
            }

            byte[] keyBytes = toByteArray(k);
            ValueType type = e.type;
            byte[] stringValue = null;
            if (type == ValueType.STRING) {
                byte[] view = e.stringBytesView();
                stringValue = view == null ? null : java.util.Arrays.copyOf(view, view.length);
            }
            Long expireAtMillis = expires.get(k);
            out.add(new YierdisSnapshotEntry(keyBytes, type, stringValue, expireAtMillis));

            remaining[0]--;
            return remaining[0] > 0;
        });
    }

    public boolean setString(byte[] keyBytes, byte[] value, SetMode mode, ExpireOption expireOption) {
        checkThread();
        long now = System.currentTimeMillis();
        boolean keepTtl = expireOption != null && expireOption.isKeepTtl();
        Long expireAtMillis = (expireOption == null || keepTtl) ? null : expireOption.toExpireAtMillis(now);
        long upperBound = estimateStringWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, value == null ? 0 : value.length);
        if (expireAtMillis != null) {
            upperBound += TTL_ENTRY_BYTES_ESTIMATE;
        }
        final long finalUpperBound = upperBound;

        return mutationExecutor.execute(new YierdisDbMutationExecutor.MutationPlan<Boolean>() {
            @Override
            public long upperBoundBytes() {
                return finalUpperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<Boolean> apply() {
                final boolean[] didSet = new boolean[]{false};
                final boolean[] existed = new boolean[]{false};
                final KeyHandle[] handleRef = new KeyHandle[]{null};
                final long[] deltaBytes = new long[]{0};

                store.computeWithHandle(keyBytes, (k, old) -> {
                    handleRef[0] = k;
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

                if (didSet[0]) {
                    YierdisChangeTracking.markValueChanged();
                    if (keepTtl && existed[0]) {
                        return YierdisDbMutationExecutor.MutationResult.of(true, deltaBytes[0]);
                    }
                    if (expireAtMillis != null) {
                        setExpireAtMillis(handleRef[0], expireAtMillis);
                        YierdisChangeTracking.markTtlChanged();
                        return YierdisDbMutationExecutor.MutationResult.of(true, deltaBytes[0]);
                    }
                    Long beforeTtl = expires.get(handleRef[0]);
                    removeExpire(handleRef[0]);
                    if (beforeTtl != null) {
                        YierdisChangeTracking.markTtlChanged();
                    }
                }
                return YierdisDbMutationExecutor.MutationResult.of(didSet[0], deltaBytes[0]);
            }
        });
    }

    public boolean setString(byte[] keyBytes, BytesSlice value, SetMode mode, ExpireOption expireOption) {
        checkThread();
        long now = System.currentTimeMillis();
        boolean keepTtl = expireOption != null && expireOption.isKeepTtl();
        Long expireAtMillis = (expireOption == null || keepTtl) ? null : expireOption.toExpireAtMillis(now);
        long upperBound = estimateStringWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, value == null ? 0 : value.len());
        if (expireAtMillis != null) {
            upperBound += TTL_ENTRY_BYTES_ESTIMATE;
        }
        final long finalUpperBound = upperBound;

        return mutationExecutor.execute(new YierdisDbMutationExecutor.MutationPlan<Boolean>() {
            @Override
            public long upperBoundBytes() {
                return finalUpperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<Boolean> apply() {
                final boolean[] didSet = new boolean[]{false};
                final boolean[] existed = new boolean[]{false};
                final KeyHandle[] handleRef = new KeyHandle[]{null};
                final long[] deltaBytes = new long[]{0};

                store.computeWithHandle(keyBytes, (k, old) -> {
                    handleRef[0] = k;
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

                if (didSet[0]) {
                    YierdisChangeTracking.markValueChanged();
                    if (keepTtl && existed[0]) {
                        return YierdisDbMutationExecutor.MutationResult.of(true, deltaBytes[0]);
                    }
                    if (expireAtMillis != null) {
                        setExpireAtMillis(handleRef[0], expireAtMillis);
                        YierdisChangeTracking.markTtlChanged();
                        return YierdisDbMutationExecutor.MutationResult.of(true, deltaBytes[0]);
                    }
                    Long beforeTtl = expires.get(handleRef[0]);
                    removeExpire(handleRef[0]);
                    if (beforeTtl != null) {
                        YierdisChangeTracking.markTtlChanged();
                    }
                }
                return YierdisDbMutationExecutor.MutationResult.of(didSet[0], deltaBytes[0]);
            }
        });
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

    public int strlen(BytesView keyView) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyView);
        if (e == null) {
            return 0;
        }
        if (e.type != ValueType.STRING) {
            throw new WrongTypeException();
        }
        return e.stringByteLength();
    }

    public int append(byte[] keyBytes, byte[] appendValue) {
        checkThread();
        return append(keyBytes, new BytesSlice() {
            @Override
            public void writeTo(yier.bubu.redis.bytes.BytesSink out) {
                out.writeBytes(appendValue, 0, appendValue.length);
            }

            @Override
            public int length() {
                return appendValue.length;
            }

            @Override
            public byte getByte(int index) {
                return appendValue[index];
            }
        });
    }

    public int append(byte[] keyBytes, BytesSlice appendValue) {
        checkThread();
        long now = System.currentTimeMillis();
        long upperBound = estimateStringWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, appendValue == null ? 0 : appendValue.len());
        return mutationExecutor.execute(new YierdisDbMutationExecutor.MutationPlan<Integer>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<Integer> apply() {
                final int[] newLen = new int[]{0};
                final long[] deltaBytes = new long[]{0};
                store.computeWithHandle(keyBytes, (k, old) -> {
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
                YierdisChangeTracking.markValueChanged();
                return YierdisDbMutationExecutor.MutationResult.of(newLen[0], deltaBytes[0]);
            }
        });
    }

    public int getBit(BytesView keyView, long offset) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyView);
        if (e == null) {
            return 0;
        }
        if (e.type != ValueType.STRING) {
            throw new WrongTypeException();
        }
        return e.stringGetBit(offset);
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
        long currentLen = strlen(keyBytes);
        long requiredBytes = (offset >>> 3) + 1;
        long growth = Math.max(0L, requiredBytes - currentLen);
        long upperBound = estimateStringWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, (int) growth);
        return mutationExecutor.execute(new YierdisDbMutationExecutor.MutationPlan<Integer>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<Integer> apply() {
                final int[] oldBit = new int[]{0};
                final long[] deltaBytes = new long[]{0};
                store.computeWithHandle(keyBytes, (k, old) -> {
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
                YierdisChangeTracking.markValueChanged();
                return YierdisDbMutationExecutor.MutationResult.of(oldBit[0], deltaBytes[0]);
            }
        });
    }

    public long bitcount(BytesView keyView) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyView);
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

    public long bitcount(BytesView keyView, long start, long end) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyView);
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
        if (e.payload instanceof OffHeapBuf buf) {
            int to = Math.min(end, e.rawLen - 1);
            for (int i = start; i <= to; i++) {
                count += Integer.bitCount(buf.getByte(i) & 0xFF);
            }
            return count;
        }
        if (e.payload instanceof yier.bubu.redis.db.memory.offheap.YierdisUnsafeOffHeapString s) {
            int to = Math.min(end, e.rawLen - 1);
            for (int i = start; i <= to; i++) {
                count += Integer.bitCount(s.getByte(i) & 0xFF);
            }
            return count;
        }
        return 0L;
    }

    public long incrBy(byte[] keyBytes, long delta) {
        checkThread();
        long now = System.currentTimeMillis();
        long upperBound = estimateStringWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, 0);
        return mutationExecutor.execute(new YierdisDbMutationExecutor.MutationPlan<Long>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<Long> apply() {
                final long[] result = new long[]{0L};
                final long[] deltaBytes = new long[]{0};
                store.computeWithHandle(keyBytes, (k, old) -> {
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
                if (delta != 0) {
                    YierdisChangeTracking.markValueChanged();
                }
                return YierdisDbMutationExecutor.MutationResult.of(result[0], deltaBytes[0]);
            }
        });
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
        OffHeapAddressAllocator addressAllocator =
                offHeapAllocator instanceof OffHeapAddressAllocator a ? a : null;
        final int[] len = new int[]{0};
        final long[] deltaBytes = new long[]{0};
        store.computeWithHandle(keyBytes, (k, old) -> {
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
        commitWrite(deltaBytes[0]);
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
        store.computeIfPresentWithHandle(keyBytes, (k, old) -> {
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
        commitWrite(deltaBytes[0]);
        return popped[0];
    }

    public int hset(byte[] keyBytes, List<byte[]> fieldValuePairs) {
        checkThread();
        if (fieldValuePairs.size() % 2 != 0) {
            throw new YierdisCommandException("ERR wrong number of arguments for 'hset' command");
        }
        long now = System.currentTimeMillis();
        OffHeapAddressAllocator addressAllocator =
                offHeapAllocator instanceof OffHeapAddressAllocator a ? a : null;
        final int[] added = new int[]{0};
        final long[] deltaBytes = new long[]{0};
        store.computeWithHandle(keyBytes, (k, old) -> {
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
        commitWrite(deltaBytes[0]);
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
        store.computeIfPresentWithHandle(keyBytes, (k, old) -> {
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
        commitWrite(deltaBytes[0]);
        return removed[0];
    }

    public int sadd(byte[] keyBytes, List<byte[]> members) {
        checkThread();
        long now = System.currentTimeMillis();
        OffHeapAddressAllocator addressAllocator =
                offHeapAllocator instanceof OffHeapAddressAllocator a ? a : null;
        final int[] added = new int[]{0};
        final long[] deltaBytes = new long[]{0};
        store.computeWithHandle(keyBytes, (k, old) -> {
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
        commitWrite(deltaBytes[0]);
        return added[0];
    }

    public int srem(byte[] keyBytes, List<byte[]> members) {
        checkThread();
        long now = System.currentTimeMillis();
        final int[] removed = new int[]{0};
        final long[] deltaBytes = new long[]{0};
        store.computeIfPresentWithHandle(keyBytes, (k, old) -> {
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
        commitWrite(deltaBytes[0]);
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
        OffHeapAddressAllocator addressAllocator =
                offHeapAllocator instanceof OffHeapAddressAllocator a ? a : null;
        final int[] added = new int[]{0};
        final long[] deltaBytes = new long[]{0};
        store.computeWithHandle(keyBytes, (k, old) -> {
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
        commitWrite(deltaBytes[0]);
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

    public int zrem(byte[] keyBytes, List<byte[]> members) {
        checkThread();
        long now = System.currentTimeMillis();
        final int[] removed = new int[]{0};
        final long[] deltaBytes = new long[]{0};
        store.computeIfPresentWithHandle(keyBytes, (k, old) -> {
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
        commitWrite(deltaBytes[0]);
        return removed[0];
    }

    public int zremrangeByRank(byte[] keyBytes, long start, long stop) {
        checkThread();
        long now = System.currentTimeMillis();
        final int[] removed = new int[]{0};
        final long[] deltaBytes = new long[]{0};
        store.computeIfPresentWithHandle(keyBytes, (k, old) -> {
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
        commitWrite(deltaBytes[0]);
        return removed[0];
    }

    public int zremrangeByScore(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive) {
        checkThread();
        long now = System.currentTimeMillis();
        final int[] removed = new int[]{0};
        final long[] deltaBytes = new long[]{0};
        store.computeIfPresentWithHandle(keyBytes, (k, old) -> {
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
        commitWrite(deltaBytes[0]);
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
                if (keysStoredOffHeap) {
                    KeyHandle keyHandle = expires.randomKeyHandle();
                    if (keyHandle == null) {
                        break;
                    }

                    Long expireAtMillis = expires.get(keyHandle);
                    if (expireAtMillis == null) {
                        removeExpire(keyHandle);
                        continue;
                    }

                    YierdisObject e = store.get(keyHandle);
                    if (e == null) {
                        removeExpire(keyHandle);
                        continue;
                    }

                    if (expireAtMillis <= nowMillis) {
                        removeExpire(keyHandle);
                        if (store.remove(keyHandle, e)) {
                            e.releasePayloadIfAny();
                            adjustUsedBytes(-e.estimatedBytes);
                        }
                        expired++;
                    }
                    continue;
                }

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
                        adjustUsedBytes(-e.estimatedBytes);
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

    YierdisObject getObjectIfNotExpired(byte[] keyBytes) {
        KeyHandle handle = store.keyHandle(keyBytes);
        if (handle == null) {
            return null;
        }
        return getObjectIfNotExpired(handle);
    }

    boolean removeIfExpired(byte[] keyBytes, YierdisObject e, long nowMillis) {
        KeyHandle handle = store.keyHandle(keyBytes);
        if (handle == null) {
            return false;
        }
        return removeIfExpired(handle, e, nowMillis);
    }

    YierdisObject getObjectIfNotExpired(KeyHandle keyHandle) {
        YierdisObject e = store.get(keyHandle);
        if (e == null) {
            return null;
        }
        if (removeIfExpired(keyHandle, e, System.currentTimeMillis())) {
            return null;
        }
        touch(e);
        return e;
    }

    boolean removeIfExpired(KeyHandle keyHandle, YierdisObject e, long nowMillis) {
        Long expireAtMillis = expires.get(keyHandle);
        if (expireAtMillis == null || expireAtMillis > nowMillis) {
            return false;
        }
        removeExpire(keyHandle);
        if (store.remove(keyHandle, e)) {
            e.releasePayloadIfAny();
            adjustUsedBytes(-e.estimatedBytes);
            return true;
        }
        return false;
    }

    boolean isKeyExpired(byte[] keyBytes, long nowMillis) {
        KeyHandle handle = store.keyHandle(keyBytes);
        if (handle == null) {
            return false;
        }
        return isKeyExpired(handle, nowMillis);
    }

    boolean isKeyExpired(KeyHandle keyHandle, long nowMillis) {
        Long expireAtMillis = expires.get(keyHandle);
        return expireAtMillis != null && expireAtMillis <= nowMillis;
    }

    void setExpireAtMillis(byte[] keyBytes, long expireAtMillis) {
        KeyHandle handle = store.keyHandle(keyBytes);
        if (handle != null) {
            expires.setExpireAtMillis(handle, expireAtMillis);
            return;
        }
        expires.setExpireAtMillis(keyBytes, expireAtMillis, store);
    }

    void setExpireAtMillis(KeyHandle keyHandle, long expireAtMillis) {
        expires.setExpireAtMillis(keyHandle, expireAtMillis);
    }

    void removeExpire(byte[] keyBytes) {
        KeyHandle handle = store.keyHandle(keyBytes);
        if (handle != null) {
            removeExpire(handle);
            return;
        }
        if (!keysStoredOffHeap) {
            expires.removeExpire(keyBytes);
        }
    }

    void removeExpire(KeyHandle keyHandle) {
        expires.removeExpire(keyHandle);
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

    private static boolean globMatches(byte[] pattern, BytesView text) {
        if (pattern == null || text == null) {
            return false;
        }
        int textLen = text.len();
        if (textLen < 0) {
            return false;
        }

        int p = 0;
        int t = 0;
        int star = -1;
        int starText = 0;

        while (t < textLen) {
            byte tb = text.byteAt(t);
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
                        if (literal == tb) {
                            p += 2;
                            t++;
                            continue;
                        }
                    } else {
                        // Trailing "\" is treated as a literal backslash.
                        if (tb == '\\') {
                            p++;
                            t++;
                            continue;
                        }
                    }
                } else if (pc == '[') {
                    int end = findGlobClassEnd(pattern, p + 1);
                    if (end >= 0) {
                        if (globClassMatches(pattern, p + 1, end, tb)) {
                            p = end + 1;
                            t++;
                            continue;
                        }
                    } else {
                        // Unclosed "[]" is treated as a literal '['.
                        if (tb == '[') {
                            p++;
                            t++;
                            continue;
                        }
                    }
                } else if (pc == tb) {
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

    private final class DbMemoryLedger implements MemoryLedger {
        @Override
        public long limitBytes() {
            return Math.max(0L, maxmemoryBytes);
        }

        @Override
        public long usedBytes() {
            return usedBytes;
        }

        @Override
        public long reservedBytes() {
            return reservedBytes;
        }

        @Override
        public MemoryReservation reserve(long estimatedExtraBytes) {
            if (estimatedExtraBytes < 0) {
                throw new IllegalArgumentException("estimatedExtraBytes must be >= 0");
            }

            MaxmemoryCoordinator coordinator = maxmemoryCoordinator;
            if (coordinator != null) {
                try {
                    coordinator.prepareWrite(estimatedExtraBytes);
                } catch (YierdisCommandException e) {
                    throw new MemoryLedgerOutOfMemoryException();
                }
                if (estimatedExtraBytes == 0) {
                    return NoopReservation.INSTANCE;
                }
                reservedBytes += estimatedExtraBytes;
                return new ReservationToken(this, estimatedExtraBytes);
            }

            if (maxmemoryBytes > 0) {
                // Best-effort: reclaim expired keys first (align with Redis behavior under pressure).
                cleanupExpired();

                if (estimatedExtraBytes > 0 && estimatedExtraBytes > maxmemoryBytes) {
                    // The write cannot fit even if we evict everything; fail-fast for stable OOM semantics.
                    throw new MemoryLedgerOutOfMemoryException();
                }

                long limit = maxmemoryBytes - estimatedExtraBytes;
                if (limit < 0) {
                    limit = 0;
                }
                if (usedBytesForMaxmemory() > limit) {
                    if (maxmemoryPolicy == MaxmemoryPolicy.NOEVICTION) {
                        if (estimatedExtraBytes > 0) {
                            throw new MemoryLedgerOutOfMemoryException();
                        }
                        // estimatedExtraBytes == 0: allow "no growth" operations even when already above maxmemory.
                        // (Redis-style behavior: reject only when the write is expected to increase memory.)
                        return NoopReservation.INSTANCE;
                    }
                    evictUntilUnder(limit);
                    if (usedBytesForMaxmemory() > limit) {
                        if (estimatedExtraBytes > 0) {
                            throw new MemoryLedgerOutOfMemoryException();
                        }
                        // Best-effort enforcement: if we cannot evict enough, still allow "no growth" reservations.
                        return NoopReservation.INSTANCE;
                    }
                }
            }

            if (estimatedExtraBytes == 0) {
                return NoopReservation.INSTANCE;
            }
            reservedBytes += estimatedExtraBytes;
            return new ReservationToken(this, estimatedExtraBytes);
        }

        @Override
        public void commit(MemoryReservation reservation, long actualDeltaBytes) {
            ReservationToken token = ReservationToken.validate(reservation, this);
            if (token != null) {
                token.finish();
                reservedBytes -= token.reservedBytes;
                if (reservedBytes < 0) {
                    throw new IllegalStateException("reservedBytes underflow");
                }
            }

            if (actualDeltaBytes == 0) {
                return;
            }
            usedBytes += actualDeltaBytes;
            if (usedBytes < 0) {
                throw new IllegalStateException("usedBytes underflow");
            }
        }

        @Override
        public void rollback(MemoryReservation reservation) {
            ReservationToken token = ReservationToken.validate(reservation, this);
            if (token == null) {
                return;
            }
            token.finish();
            reservedBytes -= token.reservedBytes;
            if (reservedBytes < 0) {
                throw new IllegalStateException("reservedBytes underflow");
            }
        }

        private enum NoopReservation implements MemoryReservation {
            INSTANCE;

            @Override
            public long reservedBytes() {
                return 0;
            }
        }

        private static final class ReservationToken implements MemoryReservation {
            private final DbMemoryLedger owner;
            private final long reservedBytes;
            private boolean finished;

            private ReservationToken(DbMemoryLedger owner, long reservedBytes) {
                this.owner = Objects.requireNonNull(owner, "owner");
                this.reservedBytes = reservedBytes;
            }

            @Override
            public long reservedBytes() {
                return reservedBytes;
            }

            private void finish() {
                if (finished) {
                    throw new IllegalStateException("reservation already finished");
                }
                finished = true;
            }

            private static ReservationToken validate(MemoryReservation reservation, DbMemoryLedger expectedOwner) {
                if (reservation == null) {
                    return null;
                }
                if (reservation instanceof NoopReservation) {
                    return null;
                }
                if (!(reservation instanceof ReservationToken token)) {
                    throw new IllegalArgumentException("unknown reservation type: " + reservation.getClass().getName());
                }
                if (token.owner != expectedOwner) {
                    throw new IllegalArgumentException("reservation does not belong to this ledger");
                }
                return token;
            }
        }
    }

}
