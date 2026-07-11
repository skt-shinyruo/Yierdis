package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.entry.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.memory.api.NativeDefragOptions;
import yier.bubu.redis.memory.api.NativeDefragReport;
import yier.bubu.redis.memory.api.NativeAllocator;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.storage.api.DbLifecycleOps;
import yier.bubu.redis.storage.api.DbReads;
import yier.bubu.redis.storage.api.DbWrites;
import yier.bubu.redis.storage.api.ExpirationManager;
import yier.bubu.redis.storage.api.MaxmemoryCandidate;
import yier.bubu.redis.storage.api.MaxmemoryCoordinator;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.MemoryOps;
import yier.bubu.redis.storage.api.RuntimeDbEngine;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.StringWriteOps;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.storage.api.result.BulkStringSink;
import yier.bubu.redis.storage.api.result.BulkStringValue;

public final class YierdisDb implements RuntimeDbEngine {
    /**
     * @deprecated Use {@link MaxmemoryErrors#OOM_ERR}.
     */
    @Deprecated
    static final String OOM_ERR = yier.bubu.redis.storage.api.MaxmemoryErrors.OOM_ERR;

    private final YierdisDbRuntimeState runtimeState;
    private final YierdisDbMemoryLedger ledger;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final YierdisDbIntrospection introspection;
    private final YierdisDbDataMaintenance maintenance;

    private final DbReads reads;
    private final DbWrites writes;
    private final ExpirationManager expirationManager;
    private final MemoryOps memoryOps;
    private final DbLifecycleOps lifecycleOps;

    public YierdisDb() {
        this(null, false, 0, 0, MaxmemoryPolicy.NOEVICTION, 5, 5, 5, null);
    }

    public static YierdisDb createWithSharedFfmRuntime(
            YierdisFfmMemoryRuntime memoryRuntime,
            long maxmemoryBytes,
            MaxmemoryPolicy maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis
    ) {
        return new YierdisDb(memoryRuntime, false, 0, maxmemoryBytes, maxmemoryPolicy, maxmemorySamples, evictionTimeLimitMillis, expireCleanupTimeLimitMillis, null);
    }

    public static YierdisDb createWithSharedFfmRuntime(
            YierdisFfmMemoryRuntime memoryRuntime,
            long maxmemoryBytes,
            MaxmemoryPolicy maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis,
            NativeDefragOptions nativeDefragOptions
    ) {
        return createWithSharedFfmRuntimeAndNativeSlotCapacity(
                memoryRuntime,
                maxmemoryBytes,
                maxmemoryPolicy,
                maxmemorySamples,
                evictionTimeLimitMillis,
                expireCleanupTimeLimitMillis,
                nativeDefragOptions,
                0
        );
    }

    public static YierdisDb createWithSharedFfmRuntimeAndNativeSlotCapacity(
            YierdisFfmMemoryRuntime memoryRuntime,
            long maxmemoryBytes,
            MaxmemoryPolicy maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis,
            NativeDefragOptions nativeDefragOptions,
            int nativeSlotCapacity
    ) {
        return new YierdisDb(
                memoryRuntime,
                false,
                nativeSlotCapacity,
                maxmemoryBytes,
                maxmemoryPolicy,
                maxmemorySamples,
                evictionTimeLimitMillis,
                expireCleanupTimeLimitMillis,
                nativeDefragOptions
        );
    }

    static YierdisDb createWithSharedFfmRuntime(
            YierdisFfmMemoryRuntime memoryRuntime,
            long maxmemoryBytes,
            MaxmemoryPolicy maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis,
            NativeDefragOptions nativeDefragOptions,
            int dbIndex
    ) {
        return createWithSharedFfmRuntime(
                memoryRuntime,
                maxmemoryBytes,
                maxmemoryPolicy,
                maxmemorySamples,
                evictionTimeLimitMillis,
                expireCleanupTimeLimitMillis,
                nativeDefragOptions,
                0,
                dbIndex
        );
    }

    static YierdisDb createWithSharedFfmRuntime(
            YierdisFfmMemoryRuntime memoryRuntime,
            long maxmemoryBytes,
            MaxmemoryPolicy maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis,
            NativeDefragOptions nativeDefragOptions,
            int nativeSlotCapacity,
            int dbIndex
    ) {
        return new YierdisDb(
                memoryRuntime,
                false,
                nativeSlotCapacity,
                maxmemoryBytes,
                maxmemoryPolicy,
                maxmemorySamples,
                evictionTimeLimitMillis,
                expireCleanupTimeLimitMillis,
                nativeDefragOptions,
                dbIndex
        );
    }

    public static YierdisDb createWithOwnedFfmRuntime(
            long maxmemoryBytes,
            MaxmemoryPolicy maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis
    ) {
        return new YierdisDb(
                null,
                true,
                0,
                maxmemoryBytes,
                maxmemoryPolicy,
                maxmemorySamples,
                evictionTimeLimitMillis,
                expireCleanupTimeLimitMillis,
                null
        );
    }

    public static YierdisDb createWithOwnedFfmRuntime(
            long maxmemoryBytes,
            MaxmemoryPolicy maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis,
            NativeDefragOptions nativeDefragOptions
    ) {
        return createWithOwnedFfmRuntimeAndNativeSlotCapacity(
                maxmemoryBytes,
                maxmemoryPolicy,
                maxmemorySamples,
                evictionTimeLimitMillis,
                expireCleanupTimeLimitMillis,
                nativeDefragOptions,
                0
        );
    }

    public static YierdisDb createWithOwnedFfmRuntimeAndNativeSlotCapacity(
            long maxmemoryBytes,
            MaxmemoryPolicy maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis,
            NativeDefragOptions nativeDefragOptions,
            int nativeSlotCapacity
    ) {
        return new YierdisDb(
                null,
                true,
                nativeSlotCapacity,
                maxmemoryBytes,
                maxmemoryPolicy,
                maxmemorySamples,
                evictionTimeLimitMillis,
                expireCleanupTimeLimitMillis,
                nativeDefragOptions
        );
    }

    static YierdisDb createWithOwnedFfmRuntime(
            long maxmemoryBytes,
            MaxmemoryPolicy maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis,
            NativeDefragOptions nativeDefragOptions,
            int dbIndex
    ) {
        return createWithOwnedFfmRuntime(
                maxmemoryBytes,
                maxmemoryPolicy,
                maxmemorySamples,
                evictionTimeLimitMillis,
                expireCleanupTimeLimitMillis,
                nativeDefragOptions,
                0,
                dbIndex
        );
    }

    static YierdisDb createWithOwnedFfmRuntime(
            long maxmemoryBytes,
            MaxmemoryPolicy maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis,
            NativeDefragOptions nativeDefragOptions,
            int nativeSlotCapacity,
            int dbIndex
    ) {
        return new YierdisDb(
                null,
                true,
                nativeSlotCapacity,
                maxmemoryBytes,
                maxmemoryPolicy,
                maxmemorySamples,
                evictionTimeLimitMillis,
                expireCleanupTimeLimitMillis,
                nativeDefragOptions,
                dbIndex
        );
    }

    private YierdisDb(
            YierdisFfmMemoryRuntime memoryRuntime,
            boolean ownsMemoryRuntime,
            int nativeSlotCapacity,
            long maxmemoryBytes,
            MaxmemoryPolicy maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis,
            NativeDefragOptions nativeDefragOptions
    ) {
        this(
                memoryRuntime,
                ownsMemoryRuntime,
                nativeSlotCapacity,
                maxmemoryBytes,
                maxmemoryPolicy,
                maxmemorySamples,
                evictionTimeLimitMillis,
                expireCleanupTimeLimitMillis,
                nativeDefragOptions,
                0
        );
    }

    private YierdisDb(
            YierdisFfmMemoryRuntime memoryRuntime,
            boolean ownsMemoryRuntime,
            int nativeSlotCapacity,
            long maxmemoryBytes,
            MaxmemoryPolicy maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis,
            NativeDefragOptions nativeDefragOptions,
            int dbIndex
    ) {
        YierdisDbRuntimeState runtimeState = new YierdisDbRuntimeState(dbIndex);
        YierdisDbComponents components = YierdisDbComponentFactory.create(
                new YierdisDbComponentFactory.OwnerCallbacks() {
                    @Override
                    public int dbIndex() {
                        return runtimeState.dbIndex();
                    }

                    @Override
                    public void checkThread() {
                        runtimeState.checkThread();
                    }
                },
                runtimeState,
                memoryRuntime,
                ownsMemoryRuntime,
                nativeSlotCapacity,
                maxmemoryBytes,
                maxmemoryPolicy,
                maxmemorySamples,
                evictionTimeLimitMillis,
                expireCleanupTimeLimitMillis,
                nativeDefragOptions
        );

        this.runtimeState = components.runtimeState;
        this.ledger = components.ledger;
        this.keyLifecycle = components.keyLifecycle;
        this.introspection = components.introspection;
        this.maintenance = components.maintenance;
        this.reads = components.reads;
        this.writes = components.writes;
        this.expirationManager = components.expirationManager;
        this.memoryOps = components.memoryOps;
        this.lifecycleOps = components.lifecycleOps;
        // 这里不启动独立调度线程；过期清理、maxmemory 淘汰和 defrag 只会在上层 event loop/调用线程里触发。
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
        runtimeState.attachMaxmemoryCoordinator(coordinator);
    }

    @Override
    public Object globalSharedOffHeapUsageIdentity() {
        return runtimeState.hasNoMaxmemoryCoordinator() ? null : keyLifecycle.memoryRuntime();
    }

    @Override
    public long globalSharedOffHeapUsedBytes() {
        if (runtimeState.hasNoMaxmemoryCoordinator()) {
            return 0L;
        }
        YierdisFfmMemoryRuntime runtime = keyLifecycle.memoryRuntime();
        return runtime == null ? 0L : Math.max(0L, runtime.usedBytes());
    }

    public void adjustUsedBytes(long deltaBytes) {
        runtimeState.adjustUsedBytes(deltaBytes);
    }

    public void enforceMaxmemory() {
        maintenance.enforceMaxmemory();
    }

    @Override
    public void enforceMaxmemoryMaintenance() {
        enforceMaxmemory();
    }

    @Override
    public void defragMaintenance() {
        maintenance.defragMaintenance();
    }

    NativeDefragReport lastNativeDefragReport() {
        return runtimeState.lastNativeDefragReport();
    }

    static byte[] toByteArray(BytesView view) {
        if (view == null) {
            throw new IllegalArgumentException("view must not be null");
        }
        int len = view.length();
        if (len < 0) {
            return null;
        }
        if (len == 0) {
            return new byte[0];
        }
        byte[] out = new byte[len];
        for (int i = 0; i < len; i++) {
            out[i] = view.getByte(i);
        }
        return out;
    }

    @Override
    public long usedBytesForMaxmemory() {
        return maintenance.usedBytesForMaxmemory();
    }

    @Override
    public int keyCountEstimate() {
        return maintenance.keyCountEstimate();
    }

    @Override
    public void cleanupExpired(long nowMillis) {
        maintenance.cleanupExpired(nowMillis);
    }

    @Override
    public MaxmemoryCandidate sampleCandidate(MaxmemoryPolicy policy, long nowMillis) {
        return maintenance.sampleCandidate(this, policy, nowMillis);
    }

    @Override
    public MaxmemoryCandidate scanBestCandidate(MaxmemoryPolicy policy, long nowMillis) {
        return maintenance.scanBestCandidate(this, policy, nowMillis);
    }

    @Override
    public boolean evict(MaxmemoryCandidate candidate, long nowMillis) {
        return maintenance.evict(this, candidate, nowMillis);
    }

    long nextLruClock() {
        return runtimeState.nextLruClock();
    }

    public void bindToCurrentThread() {
        runtimeState.bindToCurrentThread();
    }

    public void checkThread() {
        runtimeState.checkThread();
    }

    public YierdisDbMemoryLedger memoryLedger() {
        return ledger;
    }

    public NativeAllocator nativeAllocator() {
        return keyLifecycle.nativeAllocator();
    }

    public void shutdown() {
        maintenance.shutdown();
    }

    public yier.bubu.redis.storage.api.MutationOutcome flushDb() {
        return maintenance.flushDb();
    }

    public int size() {
        return maintenance.size();
    }

    public long estimatedUsedBytes() {
        return maintenance.estimatedUsedBytes();
    }

    YierdisDbIntrospection introspection() {
        return introspection;
    }

    public void cleanupExpired() {
        maintenance.cleanupExpired();
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

    public void removeExpire(KeyHandle keyHandle) {
        keyLifecycle.removeExpire(keyHandle);
    }

    public YierdisDbKeyLifecycle keyLifecycle() {
        return keyLifecycle;
    }

}
