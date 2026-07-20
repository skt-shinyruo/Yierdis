package yier.bubu.redis.app.bench.storage;

import java.util.Locale;
import java.util.Objects;
import java.util.OptionalLong;

public final class StorageBenchmarkRenderer {
    private static final String CSV_HEADER = "\"keys\",\"key_size_bytes\",\"value_size_bytes\","
            + "\"warmup_operations\",\"elapsed_seconds\",\"ops_per_second\","
            + "\"p50_latency_ns\",\"p99_latency_ns\",\"heap_estimated_bytes\","
            + "\"native_metadata_committed_bytes\",\"native_data_committed_bytes\","
            + "\"native_data_live_bytes\",\"native_reclaimable_bytes\",\"accounted_bytes\","
            + "\"baseline_accounted_bytes\",\"accounted_delta_bytes\","
            + "\"accounted_delta_bytes_per_key\",\"live_object_count\","
            + "\"pending_hash_table_count\",\"rss_bytes\",\"rss_delta_bytes\"";

    public String render(StorageBenchmarkConfig config, StorageBenchmarkResult result) {
        StorageBenchmarkConfig requiredConfig = Objects.requireNonNull(config, "config");
        StorageBenchmarkResult requiredResult = Objects.requireNonNull(result, "result");
        return switch (requiredConfig.format()) {
            case HUMAN -> renderHuman(requiredConfig, requiredResult);
            case QUIET -> renderQuiet(requiredResult);
            case CSV -> renderCsv(requiredConfig, requiredResult);
        };
    }

    private static String renderHuman(StorageBenchmarkConfig config, StorageBenchmarkResult result) {
        StorageMemorySnapshot loaded = result.loaded();
        StringBuilder out = new StringBuilder("====== Yierdis storage SET ======\n");
        append(out, "  keys: %d\n", result.completedOperations());
        append(out, "  key size: %d bytes\n", config.keySizeBytes());
        append(out, "  value size: %d bytes\n", config.valueSizeBytes());
        append(out, "  warmup operations: %d\n", config.warmupOperations());
        append(out, "  elapsed: %.6f seconds\n", result.elapsedNanos() / 1_000_000_000.0);
        append(out, "  throughput: %.2f ops/s\n", result.operationsPerSecond());
        append(out, "  latency p50: %d ns\n", result.latency().p50Nanos());
        append(out, "  latency p99: %d ns\n", result.latency().p99Nanos());
        append(out, "  heap estimated: %d bytes\n", loaded.heapEstimatedBytes());
        append(out, "  native metadata committed: %d bytes\n", loaded.nativeMetadataCommittedBytes());
        append(out, "  native data committed: %d bytes\n", loaded.nativeDataCommittedBytes());
        append(out, "  native data live: %d bytes\n", loaded.nativeDataLiveBytes());
        append(out, "  native reclaimable: %d bytes\n", loaded.nativeReclaimableBytes());
        append(out, "  accounted footprint: %d bytes\n", loaded.accountedBytes());
        append(out, "  empty baseline: %d bytes\n", result.baseline().accountedBytes());
        append(out, "  accounted footprint delta: %d bytes\n", result.accountedDeltaBytes());
        append(out, "  accounted delta per key: %.3f bytes/key\n", result.accountedDeltaBytesPerKey());
        append(out, "  live native objects: %d\n", loaded.liveObjectCount());
        append(out, "  pending hash tables: %d\n", loaded.pendingHashTableCount());
        appendOptional(out, "  process RSS: %s bytes\n", loaded.rssBytes());
        appendOptional(out, "  process RSS delta: %s bytes\n", result.rssDeltaBytes());
        return out.toString();
    }

    private static String renderQuiet(StorageBenchmarkResult result) {
        return rootFormat(
                "storage-set: %.2f ops/s, p50=%d ns, p99=%d ns, %.3f bytes/key, "
                        + "live_objects=%d, pending_tables=%d, rss=%s\n",
                result.operationsPerSecond(),
                result.latency().p50Nanos(),
                result.latency().p99Nanos(),
                result.accountedDeltaBytesPerKey(),
                result.loaded().liveObjectCount(),
                result.loaded().pendingHashTableCount(),
                optional(result.loaded().rssBytes())
        );
    }

    private static String renderCsv(StorageBenchmarkConfig config, StorageBenchmarkResult result) {
        StorageMemorySnapshot loaded = result.loaded();
        return CSV_HEADER + '\n' + rootFormat(
                "%d,%d,%d,%d,%.6f,%.2f,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%.3f,%d,%d,%s,%s\n",
                result.completedOperations(),
                config.keySizeBytes(),
                config.valueSizeBytes(),
                config.warmupOperations(),
                result.elapsedNanos() / 1_000_000_000.0,
                result.operationsPerSecond(),
                result.latency().p50Nanos(),
                result.latency().p99Nanos(),
                loaded.heapEstimatedBytes(),
                loaded.nativeMetadataCommittedBytes(),
                loaded.nativeDataCommittedBytes(),
                loaded.nativeDataLiveBytes(),
                loaded.nativeReclaimableBytes(),
                loaded.accountedBytes(),
                result.baseline().accountedBytes(),
                result.accountedDeltaBytes(),
                result.accountedDeltaBytesPerKey(),
                loaded.liveObjectCount(),
                loaded.pendingHashTableCount(),
                optionalCsv(loaded.rssBytes()),
                optionalCsv(result.rssDeltaBytes())
        );
    }

    private static void append(StringBuilder out, String format, Object... arguments) {
        out.append(rootFormat(format, arguments));
    }

    private static void appendOptional(StringBuilder out, String format, OptionalLong value) {
        append(out, format, optional(value));
    }

    private static String optional(OptionalLong value) {
        return value.isPresent() ? Long.toString(value.getAsLong()) : "unavailable";
    }

    private static String optionalCsv(OptionalLong value) {
        return value.isPresent() ? Long.toString(value.getAsLong()) : "";
    }

    private static String rootFormat(String format, Object... arguments) {
        return String.format(Locale.ROOT, format, arguments);
    }
}
