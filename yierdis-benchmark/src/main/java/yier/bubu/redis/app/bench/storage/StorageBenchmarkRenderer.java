package yier.bubu.redis.app.bench.storage;

import java.util.Objects;
import java.util.OptionalLong;

import static yier.bubu.redis.app.bench.BenchOutput.append;
import static yier.bubu.redis.app.bench.BenchOutput.format;

public final class StorageBenchmarkRenderer {
    private static final String CSV_HEADER = "\"keys\",\"key_size_bytes\",\"value_size_bytes\","
            + "\"warmup_operations\",\"elapsed_seconds\",\"ops_per_second\","
            + "\"p50_latency_ns\",\"p99_latency_ns\",\"heap_estimated_bytes\","
            + "\"native_metadata_committed_bytes\",\"native_data_committed_bytes\","
            + "\"native_data_live_bytes\",\"native_reclaimable_bytes\",\"accounted_bytes\","
            + "\"baseline_accounted_bytes\",\"accounted_delta_bytes\","
            + "\"accounted_delta_bytes_per_key\",\"live_object_count\","
            + "\"pending_hash_table_count\",\"rss_bytes\",\"rss_delta_bytes\","
            + "\"ttl_churn_elapsed_seconds\",\"ttl_churn_ops_per_second\","
            + "\"ttl_churn_p50_latency_ns\",\"ttl_churn_p99_latency_ns\","
            + "\"del_elapsed_seconds\",\"del_ops_per_second\","
            + "\"del_p50_latency_ns\",\"del_p99_latency_ns\"";

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
        append(out, "  latency p50: %d ns\n", result.latency().p50());
        append(out, "  latency p99: %d ns\n", result.latency().p99());
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
        appendPhase(out, "TTL churn (SET + PEXPIRE 0)", result.ttlChurn());
        appendPhase(out, "DEL", result.deletion());
        return out.toString();
    }

    private static void appendPhase(StringBuilder out, String name, StorageBenchmarkResult.Phase phase) {
        append(out, "====== %s ======\n", name);
        append(out, "  operations: %d\n", phase.latency().count());
        append(out, "  elapsed: %.6f seconds\n", phase.elapsedNanos() / 1_000_000_000.0);
        append(out, "  throughput: %.2f ops/s\n", phase.operationsPerSecond());
        append(out, "  latency p50: %d ns\n", phase.latency().p50());
        append(out, "  latency p99: %d ns\n", phase.latency().p99());
    }

    private static String renderQuiet(StorageBenchmarkResult result) {
        return format(
                "storage-set: %.2f ops/s, p50=%d ns, p99=%d ns, %.3f bytes/key, "
                        + "live_objects=%d, pending_tables=%d, rss=%s, "
                        + "ttl_churn_p50=%d ns, ttl_churn_p99=%d ns, "
                        + "del_p50=%d ns, del_p99=%d ns\n",
                result.operationsPerSecond(),
                result.latency().p50(),
                result.latency().p99(),
                result.accountedDeltaBytesPerKey(),
                result.loaded().liveObjectCount(),
                result.loaded().pendingHashTableCount(),
                optional(result.loaded().rssBytes()),
                result.ttlChurn().latency().p50(),
                result.ttlChurn().latency().p99(),
                result.deletion().latency().p50(),
                result.deletion().latency().p99()
        );
    }

    private static String renderCsv(StorageBenchmarkConfig config, StorageBenchmarkResult result) {
        StorageMemorySnapshot loaded = result.loaded();
        return CSV_HEADER + '\n' + format(
                "%d,%d,%d,%d,%.6f,%.2f,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%.3f,%d,%d,%s,%s,"
                        + "%.6f,%.2f,%d,%d,%.6f,%.2f,%d,%d\n",
                result.completedOperations(),
                config.keySizeBytes(),
                config.valueSizeBytes(),
                config.warmupOperations(),
                result.elapsedNanos() / 1_000_000_000.0,
                result.operationsPerSecond(),
                result.latency().p50(),
                result.latency().p99(),
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
                optionalCsv(result.rssDeltaBytes()),
                result.ttlChurn().elapsedNanos() / 1_000_000_000.0,
                result.ttlChurn().operationsPerSecond(),
                result.ttlChurn().latency().p50(),
                result.ttlChurn().latency().p99(),
                result.deletion().elapsedNanos() / 1_000_000_000.0,
                result.deletion().operationsPerSecond(),
                result.deletion().latency().p50(),
                result.deletion().latency().p99()
        );
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
}
