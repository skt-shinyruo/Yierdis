package yier.bubu.redis.db;

import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.db.key.KeyHandle;
import yier.bubu.redis.db.memory.MemoryLedgerOutOfMemoryException;
import yier.bubu.redis.db.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.offheap.api.OffHeapAllocator;
import yier.bubu.redis.offheap.api.OffHeapOutOfMemoryException;
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
import yier.bubu.redis.ops.MaxmemoryPolicy;
import yier.bubu.redis.ops.MemoryOps;
import yier.bubu.redis.ops.RuntimeDbEngine;
import yier.bubu.redis.ops.SetMode;
import yier.bubu.redis.ops.StringWriteOps;
import yier.bubu.redis.ops.WrongTypeException;
import yier.bubu.redis.ops.YierdisCommandException;
import yier.bubu.redis.ops.result.BulkStringSink;
import yier.bubu.redis.ops.result.BulkStringValue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

public final class YierdisDb implements RuntimeDbEngine {
    private static final long TTL_ENTRY_BYTES_ESTIMATE = DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE;

    private final YierdisKeyspace<YierdisObject> store;
    private final YierdisExpireIndex expires;
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
        this(null, null, false, false, 0, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
    }

    public YierdisDb(OffHeapAllocator offHeapAllocator) {
        this(offHeapAllocator, 0, MaxmemoryPolicy.NOEVICTION, 5, 5, 5);
    }

    private static MaxmemoryPolicy compatibilityMaxmemoryPolicy(String policy) {
        if (policy == null || policy.isBlank()) {
            return MaxmemoryPolicy.NOEVICTION;
        }
        return MaxmemoryPolicy.parse(policy);
    }

    private YierdisDb(
            YierdisFfmMemoryRuntime memoryRuntime,
            long maxmemoryBytes,
            MaxmemoryPolicy maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis
    ) {
        this(memoryRuntime, false, maxmemoryBytes, maxmemoryPolicy, maxmemorySamples, evictionTimeLimitMillis, expireCleanupTimeLimitMillis);
    }

    public static YierdisDb createWithSharedFfmRuntime(
            YierdisFfmMemoryRuntime memoryRuntime,
            long maxmemoryBytes,
            MaxmemoryPolicy maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis
    ) {
        return new YierdisDb(memoryRuntime, false, maxmemoryBytes, maxmemoryPolicy, maxmemorySamples, evictionTimeLimitMillis, expireCleanupTimeLimitMillis);
    }

    @Deprecated
    public static YierdisDb createWithSharedFfmRuntime(
            YierdisFfmMemoryRuntime memoryRuntime,
            long maxmemoryBytes,
            String maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis
    ) {
        return createWithSharedFfmRuntime(
                memoryRuntime,
                maxmemoryBytes,
                compatibilityMaxmemoryPolicy(maxmemoryPolicy),
                maxmemorySamples,
                evictionTimeLimitMillis,
                expireCleanupTimeLimitMillis
        );
    }

    public static YierdisDb createWithOwnedFfmRuntime(
            long maxmemoryBytes,
            MaxmemoryPolicy maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis
    ) {
        return new YierdisDb(null, null, false, false,
                maxmemoryBytes, maxmemoryPolicy, maxmemorySamples, evictionTimeLimitMillis, expireCleanupTimeLimitMillis);
    }

    @Deprecated
    public static YierdisDb createWithOwnedFfmRuntime(
            long maxmemoryBytes,
            String maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis
    ) {
        return createWithOwnedFfmRuntime(
                maxmemoryBytes,
                compatibilityMaxmemoryPolicy(maxmemoryPolicy),
                maxmemorySamples,
                evictionTimeLimitMillis,
                expireCleanupTimeLimitMillis
        );
    }

    public YierdisDb(
            OffHeapAllocator offHeapAllocator,
            long maxmemoryBytes,
            MaxmemoryPolicy maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis
    ) {
        this(null, offHeapAllocator, false, false, maxmemoryBytes, maxmemoryPolicy, maxmemorySamples, evictionTimeLimitMillis, expireCleanupTimeLimitMillis);
    }

    @Deprecated
    public YierdisDb(
            OffHeapAllocator offHeapAllocator,
            long maxmemoryBytes,
            String maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis
    ) {
        this(
                offHeapAllocator,
                maxmemoryBytes,
                compatibilityMaxmemoryPolicy(maxmemoryPolicy),
                maxmemorySamples,
                evictionTimeLimitMillis,
                expireCleanupTimeLimitMillis
        );
    }

    private YierdisDb(
            YierdisFfmMemoryRuntime memoryRuntime,
            OffHeapAllocator offHeapAllocator,
            boolean ownsOffHeapAllocator,
            boolean ownsMemoryRuntime,
            long maxmemoryBytes,
            MaxmemoryPolicy maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis
    ) {
        YierdisDbComponents components = YierdisDbComponentFactory.create(
                new YierdisDbComponentFactory.OwnerCallbacks() {
                    @Override
                    public YierdisDb db() {
                        return YierdisDb.this;
                    }

                    @Override
                    public void checkThread() {
                        YierdisDb.this.checkThread();
                    }

                    @Override
                    public void cleanupExpired() {
                        YierdisDb.this.cleanupExpired();
                    }

                    @Override
                    public void evictUntilUnder(long limitBytes) {
                        YierdisDb.this.evictUntilUnder(limitBytes);
                    }

                    @Override
                    public long usedBytesForMaxmemory() {
                        return YierdisDb.this.usedBytesForMaxmemory();
                    }

                    @Override
                    public MaxmemoryCoordinator maxmemoryCoordinator() {
                        return YierdisDb.this.maxmemoryCoordinator;
                    }

                    @Override
                    public void touch(YierdisObject object) {
                        YierdisDb.this.touch(object);
                    }

                    @Override
                    public void adjustUsedBytes(long deltaBytes) {
                        YierdisDb.this.adjustUsedBytes(deltaBytes);
                    }
                },
                memoryRuntime,
                offHeapAllocator,
                ownsOffHeapAllocator,
                ownsMemoryRuntime,
                maxmemoryBytes,
                maxmemoryPolicy,
                maxmemorySamples,
                evictionTimeLimitMillis,
                expireCleanupTimeLimitMillis
        );

        this.memoryRuntime = components.storage.memoryRuntime;
        this.offHeapAllocator = components.storage.offHeapAllocator;
        this.resources = components.storage.resources;
        this.store = components.storage.store;
        this.expires = components.storage.expires;
        this.keysStoredOffHeap = components.storage.keysStoredOffHeap;
        this.maxmemoryBytes = components.config.maxmemoryBytes;
        this.maxmemoryPolicy = components.config.maxmemoryPolicy;
        this.maxmemorySamples = components.config.maxmemorySamples;
        this.lruEnabled = components.config.lruEnabled;
        this.evictionTimeLimitNanos = components.config.evictionTimeLimitNanos;
        this.expireCleanupTimeLimitNanos = components.config.expireCleanupTimeLimitNanos;
        this.ledger = components.ledger;
        this.mutationExecutor = components.mutationExecutor;
        this.expirationSupport = components.expirationSupport;
        this.maxmemorySupport = components.maxmemorySupport;
        this.keyLifecycle = components.keyLifecycle;
        this.internals = components.internals;
        this.stringOps = components.stringOps;
        this.hashOps = components.hashOps;
        this.listOps = components.listOps;
        this.setOps = components.setOps;
        this.zsetOps = components.zsetOps;
        this.hllOps = components.hllOps;
        this.ttlOps = components.ttlOps;
        this.keyspaceOps = components.keyspaceOps;
        this.memoryReporter = components.memoryReporter;
        this.introspection = components.introspection;
        this.reads = components.reads;
        this.writes = components.writes;
        this.expirationManager = components.expirationManager;
        this.memoryOps = components.memoryOps;
        this.lifecycleOps = components.lifecycleOps;
        // Scheduling (if any) is done by the Netty event loop in YierdisServer, not by a dedicated thread.
    }

    private YierdisDb(
            YierdisFfmMemoryRuntime memoryRuntime,
            boolean ownsMemoryRuntime,
            long maxmemoryBytes,
            MaxmemoryPolicy maxmemoryPolicy,
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
    public MaxmemoryCandidate sampleCandidate(MaxmemoryPolicy policy, long nowMillis) {
        checkThread();
        return maxmemorySupport.sampleCandidate(policy, nowMillis);
    }

    @Override
    public MaxmemoryCandidate scanBestCandidate(MaxmemoryPolicy policy, long nowMillis) {
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

    public yier.bubu.redis.ops.MutationOutcome flushDb() {
        checkThread();
        boolean hadKeys = store.size() != 0;
        boolean hadTtl = expires.size() != 0;
        resources.clearData(store, expires);
        ledger.resetUsage();
        return yier.bubu.redis.ops.MutationOutcome.of(hadKeys, hadTtl);
    }

    public int size() {
        checkThread();
        return store.size();
    }

    public long estimatedUsedBytes() {
        return memoryReporter.estimatedUsedBytes();
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

    YierdisDbKeyLifecycle keyLifecycle() {
        return keyLifecycle;
    }

}
