package yier.bubu.redis.db;

import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.db.memory.ffm.YierdisFfmBlobStore;
import yier.bubu.redis.db.memory.ffm.YierdisFfmExpireIndex;
import yier.bubu.redis.db.memory.ffm.YierdisFfmKeyspace;
import yier.bubu.redis.db.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.db.memory.foreign.YierdisForeignOffHeapAllocator;
import yier.bubu.redis.offheap.api.OffHeapAllocator;
import yier.bubu.redis.offheap.api.OffHeapBuf;
import yier.bubu.redis.offheap.api.OffHeapOutOfMemoryException;
import yier.bubu.redis.offheap.api.OffHeapSlice;
import yier.bubu.redis.ops.ScanCursorV2;
import yier.bubu.redis.ops.ValueType;
import yier.bubu.redis.db.key.KeyHandle;
import yier.bubu.redis.db.memory.MemoryLedger;
import yier.bubu.redis.db.memory.MemoryLedgerOutOfMemoryException;
import yier.bubu.redis.db.memory.MemoryReservation;
import yier.bubu.redis.ops.DbLifecycleOps;
import yier.bubu.redis.ops.DbReads;
import yier.bubu.redis.ops.DbEngine;
import yier.bubu.redis.ops.DbMemoryConstants;
import yier.bubu.redis.ops.DbWrites;
import yier.bubu.redis.ops.ExpireOption;
import yier.bubu.redis.ops.ExpirationManager;
import yier.bubu.redis.ops.MaxmemoryCandidate;
import yier.bubu.redis.ops.MaxmemoryCoordinator;
import yier.bubu.redis.ops.MaxmemoryCoordinatorAware;
import yier.bubu.redis.ops.MaxmemoryErrors;
import yier.bubu.redis.ops.MaxmemoryParticipant;
import yier.bubu.redis.ops.MemoryOps;
import yier.bubu.redis.ops.RuntimeDbEngine;
import yier.bubu.redis.ops.SetMode;
import yier.bubu.redis.ops.StringWriteOps;
import yier.bubu.redis.ops.WrongTypeException;
import yier.bubu.redis.ops.YierdisMemoryStats;
import yier.bubu.redis.ops.YierdisCommandException;
import yier.bubu.redis.ops.result.BulkStringSink;
import yier.bubu.redis.ops.result.BulkStringValue;
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
    private static final long LIST_ELEMENT_OVERHEAD_BYTES_ESTIMATE = 32L;
    private static final long HASH_PAIR_OVERHEAD_BYTES_ESTIMATE = 64L;
    private static final long SET_MEMBER_OVERHEAD_BYTES_ESTIMATE = 32L;
    private static final long ZSET_MEMBER_OVERHEAD_BYTES_ESTIMATE = 96L;

    final YierdisKeyspace<YierdisObject> store;
    final YierdisExpireIndex expires;
    private final YierdisFfmMemoryRuntime memoryRuntime;
    final OffHeapAllocator offHeapAllocator;
    private final boolean ownsOffHeapAllocator;
    private final boolean ownsMemoryRuntime;
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
    private final YierdisDbMutationExecutor mutationExecutor;
    private final YierdisDbExpirationSupport expirationSupport;
    private final YierdisDbMaxmemorySupport maxmemorySupport;
    private final YierdisDbInternals internals;
    private final YierdisStringOps stringOps;
    private final YierdisHashOps hashOps;
    private final YierdisListOps listOps;
    private final YierdisSetOps setOps;
    private final YierdisZSetOps zsetOps;
    private final YierdisHllOps hllOps;
    private final YierdisTtlOps ttlOps;
    private final YierdisKeyspaceOps keyspaceOps;

    private final DbReads reads;
    private final DbWrites writes;
    private final ExpirationManager expirationManager;
    private final MemoryOps memoryOps;
    private final DbLifecycleOps lifecycleOps;

    public YierdisDb() {
        this(new YierdisFfmMemoryRuntime("db"), true, 0, "noeviction", 5, 5, 5);
    }

    public YierdisDb(OffHeapAllocator offHeapAllocator) {
        this(offHeapAllocator, 0, "noeviction", 5, 5, 5);
    }

    private YierdisDb(
            YierdisFfmMemoryRuntime memoryRuntime,
            long maxmemoryBytes,
            String maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis
    ) {
        this(memoryRuntime, false, maxmemoryBytes, maxmemoryPolicy, maxmemorySamples, evictionTimeLimitMillis, expireCleanupTimeLimitMillis);
    }

    public static YierdisDb createWithSharedFfmRuntime(
            YierdisFfmMemoryRuntime memoryRuntime,
            long maxmemoryBytes,
            String maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis
    ) {
        return new YierdisDb(memoryRuntime, false, maxmemoryBytes, maxmemoryPolicy, maxmemorySamples, evictionTimeLimitMillis, expireCleanupTimeLimitMillis);
    }

    public YierdisDb(
            OffHeapAllocator offHeapAllocator,
            long maxmemoryBytes,
            String maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis
    ) {
        this(null, offHeapAllocator, false, false, maxmemoryBytes, maxmemoryPolicy, maxmemorySamples, evictionTimeLimitMillis, expireCleanupTimeLimitMillis);
    }

    private YierdisDb(
            YierdisFfmMemoryRuntime memoryRuntime,
            OffHeapAllocator offHeapAllocator,
            boolean ownsOffHeapAllocator,
            boolean ownsMemoryRuntime,
            long maxmemoryBytes,
            String maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis
    ) {
        YierdisFfmMemoryRuntime resolvedRuntime = memoryRuntime;
        OffHeapAllocator resolvedAllocator = offHeapAllocator;
        boolean resolvedOwnsAllocator = ownsOffHeapAllocator;
        boolean resolvedOwnsRuntime = ownsMemoryRuntime;

        if (resolvedRuntime == null && resolvedAllocator == null) {
            resolvedRuntime = new YierdisFfmMemoryRuntime("db");
            resolvedAllocator = new YierdisForeignOffHeapAllocator(resolvedRuntime, 0);
            resolvedOwnsAllocator = true;
            resolvedOwnsRuntime = true;
        } else if (resolvedRuntime == null) {
            if (!(resolvedAllocator instanceof YierdisForeignOffHeapAllocator foreignAllocator)) {
                throw new IllegalArgumentException("Only the foreign off-heap allocator is supported");
            }
            resolvedRuntime = foreignAllocator.memoryRuntime();
        } else if (resolvedAllocator == null) {
            resolvedAllocator = new YierdisForeignOffHeapAllocator(resolvedRuntime, 0);
            resolvedOwnsAllocator = true;
        } else if (!(resolvedAllocator instanceof YierdisForeignOffHeapAllocator)) {
            throw new IllegalArgumentException("Only the foreign off-heap allocator is supported");
        }

        this.memoryRuntime = resolvedRuntime;
        this.offHeapAllocator = resolvedAllocator;
        this.ownsOffHeapAllocator = resolvedOwnsAllocator;
        this.ownsMemoryRuntime = resolvedOwnsRuntime;
        YierdisFfmBlobStore blobStore = new YierdisFfmBlobStore(this.memoryRuntime, "ffm-key");
        this.store = new YierdisFfmKeyspace<>(blobStore);
        this.expires = new YierdisFfmExpireIndex(blobStore);
        this.keysStoredOffHeap = true;
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
        this.expirationSupport = new YierdisDbExpirationSupport(this, this.keysStoredOffHeap, this.expireCleanupTimeLimitNanos);
        this.maxmemorySupport = new YierdisDbMaxmemorySupport(this, this.maxmemoryPolicy, this.maxmemorySamples, this.evictionTimeLimitNanos);
        this.internals = new DbInternals();
        this.stringOps = new YierdisStringOps(internals);
        this.hashOps = new YierdisHashOps(internals);
        this.listOps = new YierdisListOps(internals);
        this.setOps = new YierdisSetOps(internals);
        this.zsetOps = new YierdisZSetOps(internals);
        this.hllOps = new YierdisHllOps(internals);
        this.ttlOps = new YierdisTtlOps(internals);
        this.keyspaceOps = new YierdisKeyspaceOps(internals);
        this.reads = new YierdisDbReads(stringOps, hashOps, listOps, setOps, zsetOps, hllOps, keyspaceOps, ttlOps);
        this.writes = new YierdisDbWrites(stringOps, hashOps, listOps, setOps, zsetOps, hllOps, keyspaceOps, ttlOps);
        this.expirationManager = new YierdisDbExpirationManager(expirationSupport);
        this.memoryOps = new YierdisDbMemoryOps(this);
        this.lifecycleOps = new YierdisDbLifecycleOps(this);
        // Scheduling (if any) is done by the Netty event loop in YierdisServer, not by a dedicated thread.
    }

    private YierdisDb(
            YierdisFfmMemoryRuntime memoryRuntime,
            boolean ownsMemoryRuntime,
            long maxmemoryBytes,
            String maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis
    ) {
        this(
                memoryRuntime,
                null,
                false,
                ownsMemoryRuntime,
                maxmemoryBytes,
                maxmemoryPolicy,
                maxmemorySamples,
                evictionTimeLimitMillis,
                expireCleanupTimeLimitMillis
        );
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
    public ExpirationManager expiration() {
        return expirationManager;
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
        maxmemorySupport.evictUntilUnder(limitBytes);
    }

    static byte[] toByteArray(BytesView view) {
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

    @Override
    public long usedBytesForMaxmemory() {
        checkThread();
        long nativeBytes = maxmemoryCoordinator == null ? runtimeUsedBytes() : 0L;
        long ttlBytes = estimateTtlBytesForMaxmemory();
        long total = usedBytes + nativeBytes;
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
        expirationSupport.cleanupExpired(nowMillis);
    }

    @Override
    public MaxmemoryCandidate sampleCandidate(yier.bubu.redis.ops.MaxmemoryPolicy policy, long nowMillis) {
        checkThread();
        return maxmemorySupport.sampleCandidate(policy, nowMillis);
    }

    @Override
    public MaxmemoryCandidate scanBestCandidate(yier.bubu.redis.ops.MaxmemoryPolicy policy, long nowMillis) {
        checkThread();
        return maxmemorySupport.scanBestCandidate(policy, nowMillis);
    }

    @Override
    public boolean evict(MaxmemoryCandidate candidate, long nowMillis) {
        checkThread();
        return maxmemorySupport.evict(candidate, nowMillis);
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
        if (ownsOffHeapAllocator && offHeapAllocator != null) {
            offHeapAllocator.close();
        }
        if (ownsMemoryRuntime && memoryRuntime != null) {
            memoryRuntime.close();
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
        long runtimeUsedBytes = runtimeUsedBytes();
        return DbMemoryAccounting.snapshot(
                maxmemoryBytes,
                usedBytes,
                reservedBytes,
                null,
                runtimeUsedBytes,
                store,
                expires,
                keysStoredOffHeap,
                maxmemoryCoordinator == null
        );
    }

    private long runtimeUsedBytes() {
        if (memoryRuntime == null) {
            return 0L;
        }
        try {
            return Math.max(0L, memoryRuntime.usedBytes());
        } catch (Throwable ignored) {
            return 0L;
        }
    }

    private long directRuntimeUsedBytes() {
        long bytes = 0L;
        if (store instanceof YierdisFfmKeyspace<?> ffmStore) {
            bytes += Math.max(0L, ffmStore.nativeBytes());
        }
        if (expires instanceof YierdisFfmExpireIndex ffmExpires) {
            bytes += Math.max(0L, ffmExpires.nativeBytes());
        }
        return bytes;
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
            if (offHeapAllocator != null && e.payload instanceof OffHeapBuf) {
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

    static long estimateStringWriteUpperBound(int keyLength, int valueLength) {
        return (long) Math.max(0, keyLength) + Math.max(0, valueLength) + DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE;
    }

    static long sumByteLengths(List<byte[]> values) {
        if (values == null || values.isEmpty()) {
            return 0L;
        }
        long total = 0L;
        for (byte[] value : values) {
            if (value != null) {
                total += value.length;
            }
        }
        return total;
    }

    private static long estimateCollectionWriteUpperBound(int keyLength, long payloadBytes, long structuralBytes) {
        return estimateStringWriteUpperBound(keyLength, 0) + Math.max(0L, payloadBytes) + Math.max(0L, structuralBytes);
    }

    static long estimateListWriteUpperBound(int keyLength, List<byte[]> values) {
        int itemCount = values == null ? 0 : values.size();
        return estimateCollectionWriteUpperBound(
                keyLength,
                sumByteLengths(values),
                Math.multiplyExact((long) itemCount, LIST_ELEMENT_OVERHEAD_BYTES_ESTIMATE)
        );
    }

    static long estimateHashWriteUpperBound(int keyLength, List<byte[]> fieldValuePairs) {
        int pairCount = fieldValuePairs == null ? 0 : fieldValuePairs.size() / 2;
        return estimateCollectionWriteUpperBound(
                keyLength,
                sumByteLengths(fieldValuePairs),
                Math.multiplyExact((long) pairCount, HASH_PAIR_OVERHEAD_BYTES_ESTIMATE)
        );
    }

    static long estimateSetWriteUpperBound(int keyLength, List<byte[]> members) {
        int memberCount = members == null ? 0 : members.size();
        return estimateCollectionWriteUpperBound(
                keyLength,
                sumByteLengths(members),
                Math.multiplyExact((long) memberCount, SET_MEMBER_OVERHEAD_BYTES_ESTIMATE)
        );
    }

    static long estimateZSetWriteUpperBound(int keyLength, List<byte[]> scoreMemberPairs) {
        int memberCount = scoreMemberPairs == null ? 0 : scoreMemberPairs.size() / 2;
        long memberBytes = 0L;
        if (scoreMemberPairs != null) {
            for (int i = 1; i < scoreMemberPairs.size(); i += 2) {
                byte[] member = scoreMemberPairs.get(i);
                if (member != null) {
                    memberBytes += member.length;
                }
            }
        }
        return estimateCollectionWriteUpperBound(
                keyLength,
                memberBytes,
                Math.multiplyExact((long) memberCount, ZSET_MEMBER_OVERHEAD_BYTES_ESTIMATE)
        );
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

    public void cleanupExpired() {
        checkThread();
        expirationSupport.cleanupExpired();
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
        expires.removeExpire(keyBytes);
    }

    void removeExpire(KeyHandle keyHandle) {
        expires.removeExpire(keyHandle);
    }

    static boolean globMatches(byte[] pattern, byte[] text) {
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

    static boolean globMatches(byte[] pattern, BytesView text) {
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

    private final class DbInternals implements YierdisDbInternals {
        @Override
        public YierdisKeyspace<YierdisObject> store() {
            return store;
        }

        @Override
        public YierdisExpireIndex expires() {
            return expires;
        }

        @Override
        public OffHeapAllocator offHeapAllocator() {
            return offHeapAllocator;
        }

        @Override
        public YierdisFfmMemoryRuntime memoryRuntime() {
            return memoryRuntime;
        }

        @Override
        public <T> T executeMutation(YierdisDbMutationExecutor.MutationPlan<T> plan) {
            return mutationExecutor.execute(plan);
        }

        @Override
        public void checkThread() {
            YierdisDb.this.checkThread();
        }

        @Override
        public void touch(YierdisObject object) {
            YierdisDb.this.touch(object);
        }

        @Override
        public void refreshEstimatedBytes(KeyHandle keyHandle, YierdisObject object) {
            YierdisDb.this.refreshEstimatedBytes(keyHandle, object);
        }

        @Override
        public boolean removeIfExpired(byte[] keyBytes, YierdisObject object, long nowMillis) {
            return YierdisDb.this.removeIfExpired(keyBytes, object, nowMillis);
        }

        @Override
        public boolean removeIfExpired(KeyHandle keyHandle, YierdisObject object, long nowMillis) {
            return YierdisDb.this.removeIfExpired(keyHandle, object, nowMillis);
        }

        @Override
        public boolean isKeyExpired(KeyHandle keyHandle, long nowMillis) {
            return YierdisDb.this.isKeyExpired(keyHandle, nowMillis);
        }

        @Override
        public void setExpireAtMillis(byte[] keyBytes, long expireAtMillis) {
            YierdisDb.this.setExpireAtMillis(keyBytes, expireAtMillis);
        }

        @Override
        public void setExpireAtMillis(KeyHandle keyHandle, long expireAtMillis) {
            YierdisDb.this.setExpireAtMillis(keyHandle, expireAtMillis);
        }

        @Override
        public void removeExpire(byte[] keyBytes) {
            YierdisDb.this.removeExpire(keyBytes);
        }

        @Override
        public void removeExpire(KeyHandle keyHandle) {
            YierdisDb.this.removeExpire(keyHandle);
        }

        @Override
        public void adjustUsedBytes(long deltaBytes) {
            YierdisDb.this.adjustUsedBytes(deltaBytes);
        }

        @Override
        public YierdisObject getObjectIfNotExpired(byte[] keyBytes) {
            return YierdisDb.this.getObjectIfNotExpired(keyBytes);
        }

        @Override
        public YierdisObject getObjectIfNotExpired(BytesView keyView) {
            return YierdisDb.this.getObjectIfNotExpired(keyView);
        }

        @Override
        public YierdisObject getObjectIfNotExpired(KeyHandle keyHandle) {
            return YierdisDb.this.getObjectIfNotExpired(keyHandle);
        }
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
