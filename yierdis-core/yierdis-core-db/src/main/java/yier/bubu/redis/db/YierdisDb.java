package yier.bubu.redis.db;

import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.offheap.api.OffHeapAddressAllocator;
import yier.bubu.redis.offheap.api.OffHeapAllocator;
import yier.bubu.redis.offheap.api.OffHeapBuf;
import yier.bubu.redis.offheap.api.OffHeapOutOfMemoryException;
import yier.bubu.redis.offheap.api.OffHeapSlice;
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
    private final YierdisDbMutationExecutor mutationExecutor;
    private final YierdisDbExpirationSupport expirationSupport;
    private final YierdisDbMaxmemorySupport maxmemorySupport;
    private final YierdisDbInternals internals;
    private final YierdisStringOps stringOps;
    private final YierdisTtlOps ttlOps;
    private final YierdisKeyspaceOps keyspaceOps;

    private final DbReads reads;
    private final DbWrites writes;
    private final ExpirationManager expirationManager;
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
        this.expirationSupport = new YierdisDbExpirationSupport(this, this.keysStoredOffHeap, this.expireCleanupTimeLimitNanos);
        this.maxmemorySupport = new YierdisDbMaxmemorySupport(this, this.maxmemoryPolicy, this.maxmemorySamples, this.evictionTimeLimitNanos);
        this.internals = new DbInternals();
        this.stringOps = new YierdisStringOps(internals);
        this.ttlOps = new YierdisTtlOps(internals);
        this.keyspaceOps = new YierdisKeyspaceOps(internals);
        this.reads = new YierdisDbReads(this, stringOps, keyspaceOps, ttlOps);
        this.writes = new YierdisDbWrites(this, stringOps, keyspaceOps, ttlOps);
        this.expirationManager = new YierdisDbExpirationManager(expirationSupport);
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

    private static long estimateStringWriteUpperBound(int keyLength, int valueLength) {
        return (long) Math.max(0, keyLength) + Math.max(0, valueLength) + DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE;
    }

    private static long sumByteLengths(List<byte[]> values) {
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

    private static long estimateListWriteUpperBound(int keyLength, List<byte[]> values) {
        int itemCount = values == null ? 0 : values.size();
        return estimateCollectionWriteUpperBound(
                keyLength,
                sumByteLengths(values),
                Math.multiplyExact((long) itemCount, LIST_ELEMENT_OVERHEAD_BYTES_ESTIMATE)
        );
    }

    private static long estimateHashWriteUpperBound(int keyLength, List<byte[]> fieldValuePairs) {
        int pairCount = fieldValuePairs == null ? 0 : fieldValuePairs.size() / 2;
        return estimateCollectionWriteUpperBound(
                keyLength,
                sumByteLengths(fieldValuePairs),
                Math.multiplyExact((long) pairCount, HASH_PAIR_OVERHEAD_BYTES_ESTIMATE)
        );
    }

    private static long estimateSetWriteUpperBound(int keyLength, List<byte[]> members) {
        int memberCount = members == null ? 0 : members.size();
        return estimateCollectionWriteUpperBound(
                keyLength,
                sumByteLengths(members),
                Math.multiplyExact((long) memberCount, SET_MEMBER_OVERHEAD_BYTES_ESTIMATE)
        );
    }

    private static long estimateZSetWriteUpperBound(int keyLength, List<byte[]> scoreMemberPairs) {
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

    public int lpush(byte[] keyBytes, List<byte[]> values) {
        checkThread();
        long upperBound = estimateListWriteUpperBoundForMutation(keyBytes, values);
        return pushInternal(keyBytes, values, true, upperBound);
    }

    public int rpush(byte[] keyBytes, List<byte[]> values) {
        checkThread();
        long upperBound = estimateListWriteUpperBoundForMutation(keyBytes, values);
        return pushInternal(keyBytes, values, false, upperBound);
    }

    private int pushInternal(byte[] keyBytes, List<byte[]> values, boolean left, long upperBound) {
        long now = System.currentTimeMillis();
        return mutationExecutor.execute(new YierdisDbMutationExecutor.MutationPlan<Integer>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<Integer> apply() {
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
                YierdisChangeTracking.markValueChanged();
                return YierdisDbMutationExecutor.MutationResult.of(len[0], deltaBytes[0]);
            }
        });
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

    public int lrangeCount(byte[] keyBytes, int start, int stop) {
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

    public void lrangeWriteTo(byte[] keyBytes, int start, int stop, BulkStringSink out) {
        checkThread();
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
        return popInternal(keyBytes, count, true, 0L);
    }

    public List<byte[]> rpop(byte[] keyBytes, int count) {
        checkThread();
        return popInternal(keyBytes, count, false, 0L);
    }

    private List<byte[]> popInternal(byte[] keyBytes, int count, boolean left, long upperBound) {
        if (count == 0) {
            return Collections.emptyList();
        }
        if (count < 0) {
            throw new IllegalArgumentException("count must be >= 0");
        }
        long now = System.currentTimeMillis();
        return mutationExecutor.execute(new YierdisDbMutationExecutor.MutationPlan<List<byte[]>>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<List<byte[]>> apply() {
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
                if (popped[0] != null && !popped[0].isEmpty()) {
                    YierdisChangeTracking.markValueChanged();
                }
                return YierdisDbMutationExecutor.MutationResult.of(popped[0], deltaBytes[0]);
            }
        });
    }

    public int hset(byte[] keyBytes, List<byte[]> fieldValuePairs) {
        checkThread();
        if (fieldValuePairs.size() % 2 != 0) {
            throw new YierdisCommandException("ERR wrong number of arguments for 'hset' command");
        }
        long now = System.currentTimeMillis();
        long upperBound = estimateHashWriteUpperBoundForMutation(keyBytes, fieldValuePairs);
        return mutationExecutor.execute(new YierdisDbMutationExecutor.MutationPlan<Integer>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<Integer> apply() {
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
                YierdisChangeTracking.markValueChanged();
                return YierdisDbMutationExecutor.MutationResult.of(added[0], deltaBytes[0]);
            }
        });
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

    public int hgetallCount(byte[] keyBytes) {
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

    public void hgetallWriteTo(byte[] keyBytes, BulkStringSink out) {
        checkThread();
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
        return mutationExecutor.execute(new YierdisDbMutationExecutor.MutationPlan<Integer>() {
            @Override
            public long upperBoundBytes() {
                return 0;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<Integer> apply() {
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
                if (removed[0] > 0) {
                    YierdisChangeTracking.markValueChanged();
                }
                return YierdisDbMutationExecutor.MutationResult.of(removed[0], deltaBytes[0]);
            }
        });
    }

    public int sadd(byte[] keyBytes, List<byte[]> members) {
        checkThread();
        long now = System.currentTimeMillis();
        long upperBound = estimateSetWriteUpperBoundForMutation(keyBytes, members);
        return mutationExecutor.execute(new YierdisDbMutationExecutor.MutationPlan<Integer>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<Integer> apply() {
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
                if (added[0] > 0) {
                    YierdisChangeTracking.markValueChanged();
                }
                return YierdisDbMutationExecutor.MutationResult.of(added[0], deltaBytes[0]);
            }
        });
    }

    public int srem(byte[] keyBytes, List<byte[]> members) {
        checkThread();
        long now = System.currentTimeMillis();
        return mutationExecutor.execute(new YierdisDbMutationExecutor.MutationPlan<Integer>() {
            @Override
            public long upperBoundBytes() {
                return 0;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<Integer> apply() {
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
                if (removed[0] > 0) {
                    YierdisChangeTracking.markValueChanged();
                }
                return YierdisDbMutationExecutor.MutationResult.of(removed[0], deltaBytes[0]);
            }
        });
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

    public int smembersCount(byte[] keyBytes) {
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

    public void smembersWriteTo(byte[] keyBytes, BulkStringSink out) {
        checkThread();
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
        long upperBound = estimateZSetWriteUpperBoundForMutation(keyBytes, scoreMemberPairs);
        return mutationExecutor.execute(new YierdisDbMutationExecutor.MutationPlan<Integer>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<Integer> apply() {
                OffHeapAddressAllocator addressAllocator =
                        offHeapAllocator instanceof OffHeapAddressAllocator a ? a : null;
                final int[] added = new int[]{0};
                final boolean[] changedAny = new boolean[]{false};
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
                            added[0] = zv.zaddMany(scoreMemberPairs, changedAny);
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
                    added[0] = ((ZSetValue) old.payload).zaddMany(scoreMemberPairs, changedAny);
                    old.refreshCompositeEncodingFromPayload();
                    touch(old);
                    deltaBytes[0] -= oldEstimate;
                    refreshEstimatedBytes(k, old);
                    deltaBytes[0] += old.estimatedBytes;
                    return old;
                });
                if (changedAny[0]) {
                    YierdisChangeTracking.markValueChanged();
                }
                return YierdisDbMutationExecutor.MutationResult.of(added[0], deltaBytes[0]);
            }
        });
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

    public int zrangeCount(byte[] keyBytes, long start, long stop, boolean withScores) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return 0;
        }
        if (e.type != ValueType.ZSET) {
            throw new WrongTypeException();
        }
        return ((ZSetValue) e.payload).zrangeCount(start, stop, withScores);
    }

    public void zrangeWriteTo(byte[] keyBytes, long start, long stop, boolean withScores, BulkStringSink out) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return;
        }
        if (e.type != ValueType.ZSET) {
            throw new WrongTypeException();
        }
        ((ZSetValue) e.payload).zrangeWriteTo(start, stop, withScores, out);
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

    public int zrevrangeCount(byte[] keyBytes, long start, long stop, boolean withScores) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return 0;
        }
        if (e.type != ValueType.ZSET) {
            throw new WrongTypeException();
        }
        return ((ZSetValue) e.payload).zrevrangeCount(start, stop, withScores);
    }

    public void zrevrangeWriteTo(byte[] keyBytes, long start, long stop, boolean withScores, BulkStringSink out) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return;
        }
        if (e.type != ValueType.ZSET) {
            throw new WrongTypeException();
        }
        ((ZSetValue) e.payload).zrevrangeWriteTo(start, stop, withScores, out);
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

    public int zrangeByScoreCount(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return 0;
        }
        if (e.type != ValueType.ZSET) {
            throw new WrongTypeException();
        }
        return ((ZSetValue) e.payload).zrangeByScoreCount(min, minExclusive, max, maxExclusive, withScores, offset, count);
    }

    public void zrangeByScoreWriteTo(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count, BulkStringSink out) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return;
        }
        if (e.type != ValueType.ZSET) {
            throw new WrongTypeException();
        }
        ((ZSetValue) e.payload).zrangeByScoreWriteTo(min, minExclusive, max, maxExclusive, withScores, offset, count, out);
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

    public int zrevrangeByScoreCount(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return 0;
        }
        if (e.type != ValueType.ZSET) {
            throw new WrongTypeException();
        }
        return ((ZSetValue) e.payload).zrevrangeByScoreCount(min, minExclusive, max, maxExclusive, withScores, offset, count);
    }

    public void zrevrangeByScoreWriteTo(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count, BulkStringSink out) {
        checkThread();
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return;
        }
        if (e.type != ValueType.ZSET) {
            throw new WrongTypeException();
        }
        ((ZSetValue) e.payload).zrevrangeByScoreWriteTo(min, minExclusive, max, maxExclusive, withScores, offset, count, out);
    }

    public int zrem(byte[] keyBytes, List<byte[]> members) {
        checkThread();
        long now = System.currentTimeMillis();
        return mutationExecutor.execute(new YierdisDbMutationExecutor.MutationPlan<Integer>() {
            @Override
            public long upperBoundBytes() {
                return 0;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<Integer> apply() {
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
                if (removed[0] > 0) {
                    YierdisChangeTracking.markValueChanged();
                }
                return YierdisDbMutationExecutor.MutationResult.of(removed[0], deltaBytes[0]);
            }
        });
    }

    public int zremrangeByRank(byte[] keyBytes, long start, long stop) {
        checkThread();
        long now = System.currentTimeMillis();
        return mutationExecutor.execute(new YierdisDbMutationExecutor.MutationPlan<Integer>() {
            @Override
            public long upperBoundBytes() {
                return 0;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<Integer> apply() {
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
                if (removed[0] > 0) {
                    YierdisChangeTracking.markValueChanged();
                }
                return YierdisDbMutationExecutor.MutationResult.of(removed[0], deltaBytes[0]);
            }
        });
    }

    public int zremrangeByScore(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive) {
        checkThread();
        long now = System.currentTimeMillis();
        return mutationExecutor.execute(new YierdisDbMutationExecutor.MutationPlan<Integer>() {
            @Override
            public long upperBoundBytes() {
                return 0;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<Integer> apply() {
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
                if (removed[0] > 0) {
                    YierdisChangeTracking.markValueChanged();
                }
                return YierdisDbMutationExecutor.MutationResult.of(removed[0], deltaBytes[0]);
            }
        });
    }

    public int pfadd(byte[] keyBytes, List<byte[]> elements) {
        checkThread();
        long now = System.currentTimeMillis();
        long upperBound = estimatePfaddUpperBound(keyBytes, elements);
        return mutationExecutor.execute(new YierdisDbMutationExecutor.MutationPlan<Integer>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<Integer> apply() {
                final boolean[] changed = new boolean[]{false};
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
                        old = YierdisObject.newString(offHeapAllocator, YierdisHyperLogLog.newSparse());
                        touch(old);
                    } else {
                        if (old.type != ValueType.STRING) {
                            throw new WrongTypeException();
                        }
                        touch(old);
                    }

                    changed[0] = YierdisHyperLogLog.pfAdd(old, offHeapAllocator, elements);

                    deltaBytes[0] -= oldEstimate;
                    refreshEstimatedBytes(k, old);
                    deltaBytes[0] += old.estimatedBytes;
                    return old;
                });
                if (changed[0]) {
                    YierdisChangeTracking.markValueChanged();
                }
                return YierdisDbMutationExecutor.MutationResult.of(changed[0] ? 1 : 0, deltaBytes[0]);
            }
        });
    }

    public long pfcount(List<byte[]> keys) {
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
                throw new WrongTypeException();
            }
            YierdisHyperLogLog.mergeHllIntoRegisters(e.stringBytesView(), registers);
        }
        return YierdisHyperLogLog.estimateCardinality(registers);
    }

    public void pfmerge(byte[] destKeyBytes, List<byte[]> sourceKeys) {
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
                throw new WrongTypeException();
            }
            YierdisHyperLogLog.mergeHllIntoRegisters(e.stringBytesView(), registers);
        }

        byte[] mergedDense = YierdisHyperLogLog.denseBytesFromRegisters(registers);
        long now = System.currentTimeMillis();
        long upperBound = estimatePfmergeUpperBound(destKeyBytes, mergedDense.length);
        mutationExecutor.execute(new YierdisDbMutationExecutor.MutationPlan<Boolean>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<Boolean> apply() {
                final long[] deltaBytes = new long[]{0};
                store.computeWithHandle(destKeyBytes, (k, old) -> {
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
                removeExpire(destKeyBytes);
                YierdisChangeTracking.markValueChanged();
                return YierdisDbMutationExecutor.MutationResult.of(true, deltaBytes[0]);
            }
        });
    }

    private long estimatePfaddUpperBound(byte[] keyBytes, List<byte[]> elements) {
        YierdisObject existing = getObjectIfNotExpired(keyBytes);
        if (existing == null) {
            int upperValueLength = YierdisHyperLogLog.sparseLengthUpperBoundForElements(elements);
            return estimateStringWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, upperValueLength);
        }
        if (existing.type != ValueType.STRING || !YierdisHyperLogLog.isHllString(existing)) {
            return 0L;
        }
        if (YierdisHyperLogLog.isDense(existing)) {
            return 0L;
        }
        int sparseUpperBound = YierdisHyperLogLog.sparseLengthUpperBoundForElements(elements);
        int targetLength = Math.min(YierdisHyperLogLog.denseLength(), Math.max(existing.rawLen, existing.rawLen + sparseUpperBound - YierdisHyperLogLog.HEADER_BYTES));
        return Math.max(0L, (long) targetLength - existing.rawLen);
    }

    private long estimatePfmergeUpperBound(byte[] keyBytes, int mergedDenseLength) {
        YierdisObject existing = getObjectIfNotExpired(keyBytes);
        if (existing == null) {
            return estimateStringWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, mergedDenseLength);
        }
        if (existing.type != ValueType.STRING || !YierdisHyperLogLog.isHllString(existing)) {
            return 0L;
        }
        return Math.max(0L, (long) mergedDenseLength - existing.rawLen);
    }

    private long estimateListWriteUpperBoundForMutation(byte[] keyBytes, List<byte[]> values) {
        YierdisObject existing = getObjectIfNotExpired(keyBytes);
        if (existing == null) {
            return estimateListWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, values);
        }
        if (existing.type != ValueType.LIST) {
            return 0L;
        }
        return sumByteLengths(values);
    }

    private long estimateHashWriteUpperBoundForMutation(byte[] keyBytes, List<byte[]> fieldValuePairs) {
        YierdisObject existing = getObjectIfNotExpired(keyBytes);
        if (existing == null) {
            return estimateHashWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, fieldValuePairs);
        }
        if (existing.type != ValueType.HASH) {
            return 0L;
        }
        return sumByteLengths(fieldValuePairs);
    }

    private long estimateSetWriteUpperBoundForMutation(byte[] keyBytes, List<byte[]> members) {
        YierdisObject existing = getObjectIfNotExpired(keyBytes);
        if (existing == null) {
            return estimateSetWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, members);
        }
        if (existing.type != ValueType.SET) {
            return 0L;
        }
        return sumByteLengths(members);
    }

    private long estimateZSetWriteUpperBoundForMutation(byte[] keyBytes, List<byte[]> scoreMemberPairs) {
        YierdisObject existing = getObjectIfNotExpired(keyBytes);
        if (existing == null) {
            return estimateZSetWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, scoreMemberPairs);
        }
        if (existing.type != ValueType.ZSET) {
            return 0L;
        }
        long memberBytes = 0L;
        if (scoreMemberPairs != null) {
            for (int i = 1; i < scoreMemberPairs.size(); i += 2) {
                byte[] member = scoreMemberPairs.get(i);
                if (member != null) {
                    memberBytes += member.length;
                }
            }
        }
        return memberBytes;
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
        if (!keysStoredOffHeap) {
            expires.removeExpire(keyBytes);
        }
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
