package yier.bubu.redis.storage.memory;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.common.memory.MemoryReclaimResult;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.memory.api.NativeDefragOptions;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.storage.api.DbDefragConfig;
import yier.bubu.redis.storage.api.DbEngineConfig;
import yier.bubu.redis.storage.api.DbHealthSnapshot;
import yier.bubu.redis.storage.api.HashOps;
import yier.bubu.redis.storage.api.HllOps;
import yier.bubu.redis.storage.api.KeyspaceOps;
import yier.bubu.redis.storage.api.ListOps;
import yier.bubu.redis.storage.api.MaxmemoryCandidate;
import yier.bubu.redis.storage.api.MaxmemoryCoordinator;
import yier.bubu.redis.storage.api.MaxmemoryParticipant;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.MutationOutcome;
import yier.bubu.redis.storage.api.RuntimeDbEngine;
import yier.bubu.redis.storage.api.SetOps;
import yier.bubu.redis.storage.api.StringOps;
import yier.bubu.redis.storage.api.TtlOps;
import yier.bubu.redis.storage.api.YierdisMemoryStats;
import yier.bubu.redis.storage.api.ZSetOps;
import yier.bubu.redis.storage.memory.internal.hash.HashSeed;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMaintenanceResult;
import yier.bubu.redis.storage.memory.internal.hash.HashTableWorkBudget;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.key.AllocatorKeyHandle;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMemoryLedger;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMutationExecutor;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

/**
 * 基于稳定内存后端的单 owner DB；只能由 {@link YierdisDbEngineFactory} 组合。
 */
public final class YierdisDb
        implements RuntimeDbEngine, MaxmemoryParticipant, AutoCloseable {
    private final YierdisDbRuntimeState runtimeState;
    private final YierdisDbStorage storage;
    private final YierdisDbKernel kernel;
    private final YierdisDbHealth health;
    private final YierdisDbMemoryLedger ledger;
    private final YierdisDbMemoryReporter memoryReporter;
    private final YierdisDbDataMaintenance maintenance;
    private final StringOps strings;
    private final HashOps hashes;
    private final ListOps lists;
    private final SetOps sets;
    private final ZSetOps zsets;
    private final HllOps hll;
    private final KeyspaceOps keyspace;
    private final TtlOps ttl;

    static YierdisDb create(
            DbEngineConfig config,
            StableMemoryBackend stableMemoryBackend,
            DbThreadGuard threadGuard,
            HashSeed hashSeed
    ) {
        return new YierdisDb(config, stableMemoryBackend, threadGuard, hashSeed);
    }

    private YierdisDb(
            DbEngineConfig config,
            StableMemoryBackend stableMemoryBackend,
            DbThreadGuard threadGuard,
            HashSeed hashSeed
    ) {
        StableMemoryBackend backend = Objects.requireNonNull(stableMemoryBackend, "stableMemoryBackend");
        YierdisDbStorage storage = null;
        boolean backendOwnershipTransferred = false;
        try {
            DbEngineConfig checkedConfig = Objects.requireNonNull(config, "config");
            long maxmemoryBytes = checkedConfig.maxmemoryBytes();
            MaxmemoryPolicy maxmemoryPolicy = checkedConfig.maxmemoryPolicy();
            YierdisDbRuntimeState runtimeState = new YierdisDbRuntimeState(
                    checkedConfig.dbIndex(),
                    Objects.requireNonNull(threadGuard, "threadGuard"),
                    maxmemoryBytes > 0L && maxmemoryPolicy == MaxmemoryPolicy.ALLKEYS_LRU,
                    nativeDefragOptions(checkedConfig.defrag())
            );
            HashSeed checkedHashSeed = Objects.requireNonNull(hashSeed, "hashSeed");
            backendOwnershipTransferred = true;
            storage = YierdisDbStorage.create(
                    backend,
                    checkedHashSeed,
                    runtimeState::nextLruClock
            );
            Runnable threadChecker = runtimeState::checkThread;
            long evictionTimeLimitNanos = TimeUnit.MILLISECONDS.toNanos(
                    checkedConfig.evictionTimeLimitMillis()
            );
            long expireCleanupTimeLimitNanos = TimeUnit.MILLISECONDS.toNanos(
                    checkedConfig.expireCleanupTimeLimitMillis()
            );
            YierdisDbHealth health = new YierdisDbHealth(threadChecker);
            // ledger 的 admission 回调依赖稍后创建的 expiry/maxmemory 模块；该占位对象只负责收敛这条构造环。
            YierdisDbMemoryBudgetCallbacks memoryBudgetCallbacks = new YierdisDbMemoryBudgetCallbacks();
            YierdisDbMemoryLedger ledger = new YierdisDbMemoryLedger(
                    maxmemoryBytes,
                    maxmemoryPolicy,
                    memoryBudgetCallbacks::cleanupExpired,
                    memoryBudgetCallbacks::evictUntilUnder,
                    memoryBudgetCallbacks::usedBytesForMaxmemory,
                    runtimeState::maxmemoryCoordinator,
                    runtimeState::maxmemoryParticipant
            );
            YierdisDbMutationExecutor mutationExecutor = new YierdisDbMutationExecutor(
                    threadChecker,
                    ledger,
                    backend,
                    health
            );
            YierdisDbKeyLifecycle keyLifecycle = storage.keyLifecycle();
            YierdisDbMemoryContext memoryContext = new YierdisDbMemoryContext(ledger, backend);
            YierdisDbKernel kernel = new YierdisDbKernel(
                    threadChecker,
                    mutationExecutor,
                    keyLifecycle
            );
            YierdisDbExpirationSupport expirationSupport = new YierdisDbExpirationSupport(
                    kernel,
                    keyLifecycle,
                    expireCleanupTimeLimitNanos
            );
            YierdisStringOps stringOps = keyLifecycle.createStringOps(kernel, memoryContext);
            YierdisHashOps hashOps = keyLifecycle.createHashOps(kernel, memoryContext);
            YierdisListOps listOps = keyLifecycle.createListOps(kernel, memoryContext);
            YierdisSetOps setOps = keyLifecycle.createSetOps(kernel, memoryContext);
            YierdisZSetOps zsetOps = keyLifecycle.createZSetOps(kernel, memoryContext);
            YierdisHllOps hllOps = keyLifecycle.createHllOps(kernel, memoryContext);
            YierdisTtlOps ttlOps = new YierdisTtlOps(kernel, keyLifecycle, memoryContext);
            YierdisKeyspaceOps keyspaceOps = new YierdisKeyspaceOps(kernel, keyLifecycle, memoryContext);
            YierdisDbMemoryReporter memoryReporter = new YierdisDbMemoryReporter(
                    kernel,
                    memoryContext,
                    keyLifecycle,
                    storage.hashTableMaintenanceRegistry(),
                    maxmemoryBytes,
                    ledger,
                    runtimeState::lastNativeDefragReport
            );
            YierdisDbMaxmemorySupport maxmemorySupport = new YierdisDbMaxmemorySupport(
                    kernel,
                    memoryContext,
                    keyLifecycle,
                    memoryReporter::usedBytesForMaxmemory,
                    maxmemoryPolicy,
                    checkedConfig.maxmemorySamples(),
                    evictionTimeLimitNanos
            );
            memoryBudgetCallbacks.bind(
                    () -> expirationSupport.cleanupExpired(0L),
                    maxmemorySupport::evictUntilUnder,
                    memoryReporter::usedBytesForMaxmemory
            );
            YierdisDbDataMaintenance maintenance = new YierdisDbDataMaintenance(
                    runtimeState,
                    storage,
                    kernel,
                    memoryContext,
                    ledger,
                    health,
                    storage.hashTableMaintenanceRegistry(),
                    expirationSupport,
                    maxmemorySupport,
                    memoryReporter,
                    expireCleanupTimeLimitNanos
            );
            this.runtimeState = runtimeState;
            this.storage = storage;
            this.kernel = kernel;
            this.health = health;
            this.ledger = ledger;
            this.memoryReporter = memoryReporter;
            this.maintenance = maintenance;
            this.strings = stringOps;
            this.hashes = hashOps;
            this.lists = listOps;
            this.sets = setOps;
            this.zsets = zsetOps;
            this.hll = hllOps;
            this.keyspace = keyspaceOps;
            this.ttl = ttlOps;
            runtimeState.bindMaxmemoryParticipant(this);
        } catch (Throwable failure) {
            try {
                if (!backendOwnershipTransferred) {
                    backend.close();
                } else if (storage != null) {
                    storage.close();
                }
            } catch (Throwable closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    private static NativeDefragOptions nativeDefragOptions(DbDefragConfig defrag) {
        DbDefragConfig checkedDefrag = Objects.requireNonNull(defrag, "defrag");
        if (!checkedDefrag.enabled()) {
            return null;
        }
        return new NativeDefragOptions(
                checkedDefrag.maxMoveBytes(),
                checkedDefrag.maxObjects(),
                TimeUnit.MILLISECONDS.toNanos(checkedDefrag.timeLimitMillis())
        );
    }

    @Override
    public StringOps strings() {
        return strings;
    }

    @Override
    public HashOps hashes() {
        return hashes;
    }

    @Override
    public ListOps lists() {
        return lists;
    }

    @Override
    public SetOps sets() {
        return sets;
    }

    @Override
    public ZSetOps zsets() {
        return zsets;
    }

    @Override
    public HllOps hll() {
        return hll;
    }

    @Override
    public KeyspaceOps keyspace() {
        return keyspace;
    }

    @Override
    public TtlOps ttl() {
        return ttl;
    }

    @Override
    public long memoryUsage(BytesView keyView) {
        return memoryReporter.memoryUsage(keyView);
    }

    @Override
    public YierdisMemoryStats memoryStats() {
        return memoryReporter.memoryStats();
    }

    @Override
    public String objectEncoding(BytesView keyView) {
        kernel.checkOwner();
        AllocatorKeyHandle keyHandle = storage.keyLifecycle().keyHandle(keyView);
        EntryRecord record = kernel.liveEntryRecord(keyHandle);
        return record == null ? null : encodingName(record.encoding());
    }

    @Override
    public DbHealthSnapshot health() {
        return health.snapshot();
    }

    @Override
    public void bindToCurrentThread() {
        kernel.bindToCurrentThread();
    }

    @Override
    public void runMaintenance() {
        maintenance.runMaintenance();
    }

    @Override
    public void runDeferredReclamation() {
        maintenance.runDeferredReclamation();
    }

    @Override
    public void shutdown() {
        maintenance.shutdown();
    }

    @Override
    public void close() {
        shutdown();
    }

    public void attachMaxmemoryCoordinator(MaxmemoryCoordinator coordinator) {
        runtimeState.attachMaxmemoryCoordinator(coordinator);
    }

    @Override
    public void defragMaintenance() {
        maintenance.defragMaintenance();
    }

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

    @Override
    public MemoryUsageSnapshot memoryUsage() {
        return memoryReporter.memoryUsage();
    }

    @Override
    public MemoryReclaimResult trimMemory(MemoryPressureBudget budget) {
        runtimeState.checkThread();
        return storage.keyLifecycle().trimEmptyNativePages(budget);
    }

    void enforceMaxmemory() {
        maintenance.enforceMaxmemory();
    }

    HashTableMaintenanceResult rehashMaintenance(HashTableWorkBudget budget) {
        return maintenance.rehashMaintenance(budget);
    }

    static byte[] toByteArray(BytesView view) {
        if (view == null) {
            throw new IllegalArgumentException("view must not be null");
        }
        int length = view.length();
        if (length < 0) {
            return null;
        }
        // ponytail: BytesView lookup 每次分配一个 heap byte[], allocation profile 证明该 copy 是热点时，再为 NativeKeyDirectory 增加 direct lookup。
        byte[] bytes = new byte[length];
        view.getBytes(0, bytes, 0, length);
        return bytes;
    }

    void checkThread() {
        runtimeState.checkThread();
    }

    YierdisDbMemoryLedger memoryLedger() {
        return ledger;
    }

    YierdisDbHealth healthMonitor() {
        return health;
    }

    @Override
    public MutationOutcome flushDb() {
        return maintenance.flushDb();
    }

    @Override
    public MutationOutcome flushDbAsync() {
        return maintenance.flushDbAsync();
    }

    int size() {
        return maintenance.size();
    }

    long detachedEntryCount() {
        return maintenance.detachedEntryCount();
    }

    void cleanupExpired() {
        maintenance.cleanupExpired();
    }

    boolean isKeyExpired(byte[] keyBytes, long nowMillis) {
        AllocatorKeyHandle handle = storage.keyLifecycle().keyHandle(keyBytes);
        return handle != null && isKeyExpired(handle, nowMillis);
    }

    boolean isKeyExpired(AllocatorKeyHandle keyHandle, long nowMillis) {
        return storage.keyLifecycle().isKeyExpired(keyHandle, nowMillis);
    }

    YierdisDbKeyLifecycle keyLifecycle() {
        return storage.keyLifecycle();
    }

    private static String encodingName(ValueEncoding encoding) {
        if (encoding == null) {
            return "unknown";
        }
        return switch (encoding) {
            case STRING_INT -> "int";
            case STRING_EMBSTR -> "embstr";
            case STRING_RAW -> "raw";
            case HASH_PACKED, LIST_PACKED, ZSET_PACKED -> "listpack";
            case HASH_HT, SET_HT -> "hashtable";
            case SET_INTSET -> "intset";
            case LIST_QUICKLIST -> "quicklist";
            case ZSET_SKIPLIST -> "skiplist";
        };
    }
}
