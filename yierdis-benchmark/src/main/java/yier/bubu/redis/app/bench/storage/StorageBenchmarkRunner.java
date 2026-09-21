package yier.bubu.redis.app.bench.storage;

import yier.bubu.redis.app.bench.LatencyRecorder;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.memory.api.StableMemoryBackendFactory;
import yier.bubu.redis.memory.foreign.YierdisFfmStableMemoryBackend;
import yier.bubu.redis.storage.api.DbDefragConfig;
import yier.bubu.redis.storage.api.DbEngineConfig;
import yier.bubu.redis.storage.api.MaxmemoryParticipant;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.RuntimeDbEngine;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.StringOps;
import yier.bubu.redis.storage.api.WriteResult;
import yier.bubu.redis.storage.memory.YierdisDbEngineFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public final class StorageBenchmarkRunner {
    private static final int MAX_HASH_TABLE_STABILIZATION_TICKS = 100_000;

    private final Supplier<RuntimeDbEngine> engineFactory;
    private final LongSupplier nanoClock;
    private final Supplier<OptionalLong> rssReader;

    public StorageBenchmarkRunner() {
        this(defaultEngineFactory(), System::nanoTime, ProcessRssReader::currentBytes);
    }

    StorageBenchmarkRunner(
            Supplier<RuntimeDbEngine> engineFactory,
            LongSupplier nanoClock,
            Supplier<OptionalLong> rssReader
    ) {
        this.engineFactory = Objects.requireNonNull(engineFactory, "engineFactory");
        this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
        this.rssReader = Objects.requireNonNull(rssReader, "rssReader");
    }

    public StorageBenchmarkResult run(StorageBenchmarkConfig config) {
        StorageBenchmarkConfig requiredConfig = Objects.requireNonNull(config, "config");
        warmUp(requiredConfig);

        try (EngineLease lease = openEngine()) {
            RuntimeDbEngine engine = lease.engine();
            byte[] key = keyBuffer(requiredConfig.keySizeBytes());
            byte[] value = value(requiredConfig.valueSizeBytes());
            StringOps strings = engine.strings();
            StorageMemorySnapshot baseline = snapshot(engine);

            StorageBenchmarkResult.Phase set = measurePhase(requiredConfig, key, (index, k) -> {
                WriteResult<Boolean> write = strings.setString(k, value, SetMode.NORMAL, null);
                if (!Boolean.TRUE.equals(write.value()) || !write.mutationOutcome().changedAny()) {
                    throw new IllegalStateException("SET did not store key index " + index);
                }
            });
            stabilizeHashTables(engine);
            StorageMemorySnapshot loaded = snapshot(engine);
            if (loaded.keyCount() != requiredConfig.keys()) {
                throw new IllegalStateException(
                        "expected " + requiredConfig.keys() + " stored keys but observed " + loaded.keyCount()
                );
            }
            if (loaded.pendingHashTableCount() != 0) {
                throw new IllegalStateException(
                        "storage footprint snapshot still has "
                                + loaded.pendingHashTableCount()
                                + " pending hash tables"
                );
            }
            StorageBenchmarkResult.Phase ttlChurn = measureTtlChurn(requiredConfig, engine, strings, value);
            StorageBenchmarkResult.Phase deletion = measureDeletion(requiredConfig, engine, key);
            if (snapshot(engine).keyCount() != 0L) {
                throw new IllegalStateException("delete phase did not drain the keyspace");
            }
            return StorageBenchmarkResult.from(
                    requiredConfig.keys(),
                    set.elapsedNanos(),
                    set.latency(),
                    ttlChurn,
                    deletion,
                    baseline,
                    loaded
            );
        }
    }

    /**
     * 在已加载 keyspace 上持续 SET 后立即 PEXPIRE(0)（TTL<=0 立即删除路径）。
     * 每次操作带一次删除，per-op 成本不应随 background key 数增长。
     */
    private StorageBenchmarkResult.Phase measureTtlChurn(
            StorageBenchmarkConfig config,
            RuntimeDbEngine engine,
            StringOps strings,
            byte[] value
    ) {
        byte[] churnKey = keyBuffer(config.keySizeBytes());
        // churn key 换前缀，避免覆盖已加载的 'k' keyspace：SET 必须是插入，后续 DEL phase 才能删到全部已加载 key。
        churnKey[0] = 'c';
        return measurePhase(config, churnKey, (index, k) -> {
            WriteResult<Boolean> write = strings.setString(k, value, SetMode.NORMAL, null);
            if (!Boolean.TRUE.equals(write.value()) || !write.mutationOutcome().changedAny()) {
                throw new IllegalStateException("churn SET did not store key index " + index);
            }
            WriteResult<Boolean> expired = engine.ttl().pexpire(view(k), 0L);
            if (!Boolean.TRUE.equals(expired.value()) || !expired.mutationOutcome().changedAny()) {
                throw new IllegalStateException("churn PEXPIRE did not delete key index " + index);
            }
        });
    }

    private StorageBenchmarkResult.Phase measureDeletion(
            StorageBenchmarkConfig config,
            RuntimeDbEngine engine,
            byte[] key
    ) {
        return measurePhase(config, key, (index, k) -> {
            WriteResult<Long> removed = engine.keyspace().del(List.of(k));
            if (removed.value() == null || removed.value() != 1L) {
                throw new IllegalStateException("DEL did not remove key index " + index);
            }
        });
    }

    private StorageBenchmarkResult.Phase measurePhase(
            StorageBenchmarkConfig config,
            byte[] key,
            TimedOperation operation
    ) {
        LatencyRecorder latency = LatencyRecorder.nanos(config.precision());
        long measuredStart = nanoClock.getAsLong();
        for (int index = 0; index < config.keys(); index++) {
            encodeKeyIndex(key, index);
            long operationStart = nanoClock.getAsLong();
            operation.run(index, key);
            long operationStop = nanoClock.getAsLong();
            latency.record(Math.max(0L, operationStop - operationStart));
        }
        long measuredStop = nanoClock.getAsLong();
        return new StorageBenchmarkResult.Phase(
                Math.max(0L, measuredStop - measuredStart),
                latency.summary()
        );
    }

    @FunctionalInterface
    private interface TimedOperation {
        void run(int index, byte[] key);
    }

    private static BytesView view(byte[] key) {
        return new BytesView() {
            @Override
            public int length() {
                return key.length;
            }

            @Override
            public byte getByte(int index) {
                return key[index];
            }
        };
    }

    private void warmUp(StorageBenchmarkConfig config) {
        if (config.warmupOperations() == 0) {
            return;
        }
        try (EngineLease lease = openEngine()) {
            byte[] key = keyBuffer(config.keySizeBytes());
            byte[] value = value(config.valueSizeBytes());
            StringOps strings = lease.engine().strings();
            for (int operation = 0; operation < config.warmupOperations(); operation++) {
                encodeKeyIndex(key, operation % config.keys());
                WriteResult<Boolean> write = strings.setString(key, value, SetMode.NORMAL, null);
                if (!Boolean.TRUE.equals(write.value())) {
                    throw new IllegalStateException("warmup SET failed at operation " + operation);
                }
            }
        }
    }

    private EngineLease openEngine() {
        RuntimeDbEngine engine = Objects.requireNonNull(engineFactory.get(), "engineFactory result");
        boolean bound = false;
        try {
            engine.bindToCurrentThread();
            bound = true;
            return new EngineLease(engine);
        } finally {
            if (!bound) {
                engine.shutdown();
            }
        }
    }

    private StorageMemorySnapshot snapshot(RuntimeDbEngine engine) {
        MaxmemoryParticipant participant = requirePhysicalMemoryCapability(engine);
        OptionalLong rss;
        try {
            rss = Objects.requireNonNull(rssReader.get(), "rssReader result");
        } catch (RuntimeException ignored) {
            rss = OptionalLong.empty();
        }
        return StorageMemorySnapshot.from(
                participant.memoryUsage(),
                engine.memoryStats(),
                rss
        );
    }

    private static MaxmemoryParticipant requirePhysicalMemoryCapability(RuntimeDbEngine engine) {
        Objects.requireNonNull(engine, "engine");
        if (engine instanceof MaxmemoryParticipant participant) {
            return participant;
        }
        throw new IllegalStateException("storage benchmark requires MaxmemoryParticipant");
    }

    private static void stabilizeHashTables(RuntimeDbEngine engine) {
        int pendingTableCount = engine.memoryStats().pendingHashTableCount();
        for (int tick = 0;
                pendingTableCount != 0 && tick < MAX_HASH_TABLE_STABILIZATION_TICKS;
                tick++) {
            engine.runMaintenance();
            pendingTableCount = engine.memoryStats().pendingHashTableCount();
        }
        if (pendingTableCount != 0) {
            throw new IllegalStateException(
                    "hash-table stabilization stopped with "
                            + pendingTableCount
                            + " pending tables"
            );
        }
    }

    static byte[] keyBuffer(int size) {
        byte[] key = new byte[size];
        Arrays.fill(key, (byte) '0');
        key[0] = 'k';
        return key;
    }

    static void encodeKeyIndex(byte[] key, int index) {
        Objects.requireNonNull(key, "key");
        if (index < 0) {
            throw new IllegalArgumentException("index must be >= 0");
        }
        int cursor = key.length - 1;
        int remaining = index;
        do {
            if (cursor == 0) {
                throw new IllegalArgumentException("key buffer is too small for index " + index);
            }
            key[cursor--] = (byte) ('0' + remaining % 10);
            remaining /= 10;
        } while (remaining != 0);
        while (cursor > 0) {
            key[cursor--] = '0';
        }
    }

    private static byte[] value(int size) {
        byte[] value = new byte[size];
        for (int index = 0; index < size; index++) {
            value[index] = (byte) ('a' + index % 26);
        }
        return value;
    }

    private static Supplier<RuntimeDbEngine> defaultEngineFactory() {
        StableMemoryBackendFactory backendFactory = YierdisFfmStableMemoryBackend::new;
        YierdisDbEngineFactory factory = new YierdisDbEngineFactory(
                backendFactory,
                0
        );
        DbEngineConfig engineConfig = new DbEngineConfig(
                0,
                0L,
                MaxmemoryPolicy.NOEVICTION,
                5,
                5L,
                5L,
                new DbDefragConfig(false, 0L, 0L, 0L)
        );
        return () -> factory.create(engineConfig);
    }

    private record EngineLease(RuntimeDbEngine engine) implements AutoCloseable {
        private EngineLease {
            Objects.requireNonNull(engine, "engine");
        }

        @Override
        public void close() {
            engine.shutdown();
        }
    }
}
