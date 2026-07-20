package yier.bubu.redis.app.bench.storage;

import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.RuntimeDbEngine;
import yier.bubu.redis.storage.api.SetMode;
import yier.bubu.redis.storage.api.StringWriteOps;
import yier.bubu.redis.storage.api.WriteResult;
import yier.bubu.redis.storage.memory.YierdisDb;
import yier.bubu.redis.storage.memory.YierdisDbEngineFactory;
import yier.bubu.redis.storage.memory.internal.hash.HashTableMaintenanceResult;
import yier.bubu.redis.storage.memory.internal.hash.HashTableWorkBudget;

import java.util.Arrays;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public final class StorageBenchmarkRunner {
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
            StorageLatencyRecorder latency = new StorageLatencyRecorder(requiredConfig.precision());
            byte[] key = keyBuffer(requiredConfig.keySizeBytes());
            byte[] value = value(requiredConfig.valueSizeBytes());
            StringWriteOps strings = engine.writes().strings();
            StorageMemorySnapshot baseline = snapshot(engine);

            long measuredStart = nanoClock.getAsLong();
            for (int index = 0; index < requiredConfig.keys(); index++) {
                encodeKeyIndex(key, index);
                long operationStart = nanoClock.getAsLong();
                WriteResult<Boolean> write = strings.setString(key, value, SetMode.NORMAL, null);
                long operationStop = nanoClock.getAsLong();
                if (!Boolean.TRUE.equals(write.value()) || !write.changedAny()) {
                    throw new IllegalStateException("SET did not store key index " + index);
                }
                latency.recordNanos(Math.max(0L, operationStop - operationStart));
            }
            long measuredStop = nanoClock.getAsLong();
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
            return StorageBenchmarkResult.from(
                    requiredConfig.keys(),
                    Math.max(0L, measuredStop - measuredStart),
                    latency.summary(),
                    baseline,
                    loaded
            );
        }
    }

    private void warmUp(StorageBenchmarkConfig config) {
        if (config.warmupOperations() == 0) {
            return;
        }
        try (EngineLease lease = openEngine()) {
            byte[] key = keyBuffer(config.keySizeBytes());
            byte[] value = value(config.valueSizeBytes());
            StringWriteOps strings = lease.engine().writes().strings();
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
        OptionalLong rss;
        try {
            rss = Objects.requireNonNull(rssReader.get(), "rssReader result");
        } catch (RuntimeException ignored) {
            rss = OptionalLong.empty();
        }
        return StorageMemorySnapshot.from(
                engine.memoryUsage(),
                engine.memory().memoryStats(),
                rss
        );
    }

    private static void stabilizeHashTables(RuntimeDbEngine engine) {
        if (!(engine instanceof YierdisDb db)) {
            return;
        }
        HashTableMaintenanceResult result = db.rehashMaintenance(
                HashTableWorkBudget.of(Long.MAX_VALUE, Long.MAX_VALUE)
        );
        if (result.pendingTableCount() != 0
                || result.stopReason() != HashTableMaintenanceResult.StopReason.COMPLETE) {
            throw new IllegalStateException(
                    "hash-table stabilization stopped with "
                            + result.pendingTableCount()
                            + " pending tables: "
                            + result.stopReason()
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
        YierdisDbEngineFactory factory = new YierdisDbEngineFactory();
        return () -> factory.create(0, 0L, MaxmemoryPolicy.NOEVICTION, 5, 5L, 5L);
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
