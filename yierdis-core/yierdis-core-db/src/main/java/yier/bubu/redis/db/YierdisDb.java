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
import yier.bubu.redis.ops.ValueType;
import yier.bubu.redis.db.key.KeyHandle;
import yier.bubu.redis.db.memory.MemoryLedger;
import yier.bubu.redis.db.memory.MemoryLedgerOutOfMemoryException;
import yier.bubu.redis.ops.DbLifecycleOps;
import yier.bubu.redis.ops.DbReads;
import yier.bubu.redis.ops.DbEngine;
import yier.bubu.redis.ops.DbMemoryConstants;
import yier.bubu.redis.ops.DbWrites;
import yier.bubu.redis.ops.ExpireOption;
import yier.bubu.redis.ops.ExpirationManager;
import yier.bubu.redis.ops.MaxmemoryCandidate;
import yier.bubu.redis.ops.MaxmemoryCoordinator;
import yier.bubu.redis.ops.MaxmemoryErrors;
import yier.bubu.redis.ops.MemoryOps;
import yier.bubu.redis.ops.RuntimeDbEngine;
import yier.bubu.redis.ops.SetMode;
import yier.bubu.redis.ops.StringWriteOps;
import yier.bubu.redis.ops.WrongTypeException;
import yier.bubu.redis.ops.YierdisCommandException;
import yier.bubu.redis.ops.result.BulkStringSink;
import yier.bubu.redis.ops.result.BulkStringValue;
import yier.bubu.redis.runtime.api.YierdisChangeTracking;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public final class YierdisDb implements RuntimeDbEngine {
    public enum MaxmemoryPolicy {
        NOEVICTION,
        ALLKEYS_RANDOM,
        ALLKEYS_LRU
    }

    private static final long TTL_ENTRY_BYTES_ESTIMATE = DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE;
    private static final long SET_MEMBER_OVERHEAD_BYTES_ESTIMATE = 32L;
    private static final long ZSET_MEMBER_OVERHEAD_BYTES_ESTIMATE = 96L;

    final YierdisKeyspace<YierdisObject> store;
    final YierdisExpireIndex expires;
    private final YierdisFfmMemoryRuntime memoryRuntime;
    final OffHeapAllocator offHeapAllocator;
    private final YierdisDbOwnedResources resources;
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

    private long lruClock;

    private final DbThreadGuard threadGuard = new DbThreadGuard();
    private volatile MaxmemoryCoordinator maxmemoryCoordinator;
    private final YierdisDbMemoryLedger ledger;
    private final YierdisDbMutationExecutor mutationExecutor;
    private final YierdisDbExpirationSupport expirationSupport;
    private final YierdisDbMaxmemorySupport maxmemorySupport;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final YierdisDbInternals internals;
    private final YierdisStringOps stringOps;
    private final YierdisHashOps hashOps;
    private final YierdisListOps listOps;
    private final YierdisSetOps setOps;
    private final YierdisZSetOps zsetOps;
    private final YierdisHllOps hllOps;
    private final YierdisTtlOps ttlOps;
    private final YierdisKeyspaceOps keyspaceOps;
    private final YierdisDbMemoryReporter memoryReporter;
    private final YierdisDbIntrospection introspection;

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

    public static YierdisDb createWithOwnedFfmRuntime(
            long maxmemoryBytes,
            String maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis
    ) {
        return new YierdisDb(new YierdisFfmMemoryRuntime("db"), true,
                maxmemoryBytes, maxmemoryPolicy, maxmemorySamples, evictionTimeLimitMillis, expireCleanupTimeLimitMillis);
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
        this.resources = new YierdisDbOwnedResources(
                this.memoryRuntime,
                this.offHeapAllocator,
                resolvedOwnsRuntime,
                resolvedOwnsAllocator
        );
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
        this.ledger = new YierdisDbMemoryLedger(
                this.maxmemoryBytes,
                this.maxmemoryPolicy,
                this::cleanupExpired,
                this::evictUntilUnder,
                this::usedBytesForMaxmemory,
                () -> maxmemoryCoordinator
        );
        this.mutationExecutor = new YierdisDbMutationExecutor(this::checkThread, this.ledger);
        this.expirationSupport = new YierdisDbExpirationSupport(this, this.keysStoredOffHeap, this.expireCleanupTimeLimitNanos);
        this.maxmemorySupport = new YierdisDbMaxmemorySupport(this, this.maxmemoryPolicy, this.maxmemorySamples, this.evictionTimeLimitNanos);
        this.keyLifecycle = new YierdisDbKeyLifecycle(
                this.store,
                this.expires,
                this.offHeapAllocator,
                this.memoryRuntime,
                this::touch,
                this::adjustUsedBytes
        );
        this.internals = new DbInternals();
        this.stringOps = new YierdisStringOps(internals, this::estimateEntryBytes);
        this.hashOps = new YierdisHashOps(internals, this::estimateEntryBytes);
        this.listOps = new YierdisListOps(internals, this::estimateEntryBytes);
        this.setOps = new YierdisSetOps(internals, this::estimateEntryBytes);
        this.zsetOps = new YierdisZSetOps(internals, this::estimateEntryBytes);
        this.hllOps = new YierdisHllOps(internals, this::estimateEntryBytes);
        this.ttlOps = new YierdisTtlOps(internals);
        this.keyspaceOps = new YierdisKeyspaceOps(internals);
        this.memoryReporter = new YierdisDbMemoryReporter(
                this::checkThread,
                this.keyLifecycle,
                this.store,
                this.expires,
                this.maxmemoryBytes,
                this.keysStoredOffHeap,
                this.ledger,
                () -> maxmemoryCoordinator == null
        );
        this.introspection = new YierdisDbIntrospection(this::checkThread, this.keyLifecycle);
        this.reads = new YierdisDbReads(stringOps, hashOps, listOps, setOps, zsetOps, hllOps, keyspaceOps, ttlOps);
        this.writes = new YierdisDbWrites(stringOps, hashOps, listOps, setOps, zsetOps, hllOps, keyspaceOps, ttlOps);
        this.expirationManager = new YierdisDbExpirationManager(expirationSupport);
        this.memoryOps = new YierdisDbMemoryOps(memoryReporter, introspection);
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
        return keyLifecycle.getLiveObject(keyView);
    }

    @Override
    public long usedBytesForMaxmemory() {
        return memoryReporter.usedBytesForMaxmemory();
    }

    @Override
    public int keyCountEstimate() {
        return memoryReporter.keyCountEstimate();
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

    public void bindToCurrentThread() {
        threadGuard.bindToCurrentThread();
    }

    void checkThread() {
        threadGuard.checkThread();
    }

    YierdisDbMemoryLedger memoryLedger() {
        return ledger;
    }

    public void shutdown() {
        threadGuard.checkThreadForShutdown();
        if (!threadGuard.tryMarkClosed()) {
            return;
        }
        ledger.resetUsage();
        resources.releaseAll(store, expires);
    }

    public void flushDb() {
        checkThread();
        boolean hadKeys = store.size() != 0;
        boolean hadTtl = expires.size() != 0;
        resources.clearData(store, expires);
        ledger.resetUsage();
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
        return memoryReporter.estimatedUsedBytes();
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

    YierdisDbIntrospection introspection() {
        return introspection;
    }

    public void cleanupExpired() {
        checkThread();
        expirationSupport.cleanupExpired();
    }

    YierdisObject getObjectIfNotExpired(byte[] keyBytes) {
        return keyLifecycle.getLiveObject(keyBytes);
    }

    boolean removeIfExpired(byte[] keyBytes, YierdisObject e, long nowMillis) {
        return keyLifecycle.removeIfExpired(keyBytes, e, nowMillis);
    }

    YierdisObject getObjectIfNotExpired(KeyHandle keyHandle) {
        return keyLifecycle.getLiveObject(keyHandle);
    }

    boolean removeIfExpired(KeyHandle keyHandle, YierdisObject e, long nowMillis) {
        return keyLifecycle.removeIfExpired(keyHandle, e, nowMillis);
    }

    boolean isKeyExpired(byte[] keyBytes, long nowMillis) {
        KeyHandle handle = keyLifecycle.keyHandle(keyBytes);
        if (handle == null) {
            return false;
        }
        return isKeyExpired(handle, nowMillis);
    }

    boolean isKeyExpired(KeyHandle keyHandle, long nowMillis) {
        return keyLifecycle.isKeyExpired(keyHandle, nowMillis);
    }

    void setExpireAtMillis(byte[] keyBytes, long expireAtMillis) {
        keyLifecycle.setExpireAtMillis(keyBytes, expireAtMillis);
    }

    void setExpireAtMillis(KeyHandle keyHandle, long expireAtMillis) {
        keyLifecycle.setExpireAtMillis(keyHandle, expireAtMillis);
    }

    void removeExpire(byte[] keyBytes) {
        keyLifecycle.removeExpire(keyBytes);
    }

    void removeExpire(KeyHandle keyHandle) {
        keyLifecycle.removeExpire(keyHandle);
    }

    private final class DbInternals implements YierdisDbInternals {
        @Override
        public <T> T executeMutation(YierdisDbMutationExecutor.MutationPlan<T> plan) {
            return mutationExecutor.execute(plan);
        }

        @Override
        public YierdisDbKeyLifecycle keyLifecycle() {
            return keyLifecycle;
        }

        @Override
        public MemoryLedger ledger() {
            return ledger;
        }

        @Override
        public void checkThread() {
            YierdisDb.this.checkThread();
        }
    }

}
