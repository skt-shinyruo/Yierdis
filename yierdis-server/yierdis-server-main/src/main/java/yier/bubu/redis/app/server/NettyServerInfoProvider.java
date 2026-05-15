package yier.bubu.redis.app.server;

// INFO/STATS 提供器：基于 transport-neutral executor 统计与连接态输出可观测性摘要，避免在热路径做额外分配。

import yier.bubu.redis.command.api.ServerInfoProvider;
import yier.bubu.redis.storage.api.YierdisMemoryStats;
import yier.bubu.redis.execution.api.CommandContext;
import yier.bubu.redis.execution.api.ConnectionStatsView;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.ReplyWriter;
import yier.bubu.redis.app.server.args.YierdisServerRuntimeConfig;
import yier.bubu.redis.execution.executor.CommandExecutor;
import yier.bubu.redis.runtime.embedded.YierdisInstanceObservability;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Objects;

/**
 * Server-side INFO/STATS provider backed by {@link CommandExecutor}.
 * <p>
 * This object is intentionally lightweight: it is called only when clients execute INFO/STATS, and uses
 * pre-aggregated counters updated on the hot path.
 */
final class NettyServerInfoProvider implements ServerInfoProvider {
    private static final byte[] KEY_SERVER = ascii("server");
    private static final byte[] VALUE_SERVER = ascii("yierdis");
    private static final byte[] KEY_VERSION = ascii("version");
    private static final byte[] VALUE_VERSION = YierdisBuildInfo.versionAsciiBytes();
    private static final byte[] KEY_PORT = ascii("port");
    private static final byte[] KEY_IO_THREADS = ascii("io_threads");
    private static final byte[] KEY_EXECUTOR_POLICY = ascii("executor_policy");
    private static final byte[] KEY_EXECUTOR_QUEUE_CAPACITY = ascii("executor_queue_capacity");
    private static final byte[] KEY_EXECUTOR_QUEUE_MAX_BYTES = ascii("executor_queue_max_bytes");
    private static final byte[] KEY_BACKPRESSURE_HIGH = ascii("backpressure_high");
    private static final byte[] KEY_BACKPRESSURE_LOW = ascii("backpressure_low");
    private static final byte[] KEY_BACKPRESSURE_BYTES_HIGH = ascii("backpressure_bytes_high");
    private static final byte[] KEY_BACKPRESSURE_BYTES_LOW = ascii("backpressure_bytes_low");
    private static final byte[] KEY_EXECUTOR_MAX_DRAIN = ascii("executor_max_drain");
    private static final byte[] KEY_EXECUTOR_DRAIN_MILLIS = ascii("executor_drain_millis");
    private static final byte[] KEY_STARTED_MILLIS = ascii("started_millis");
    private static final byte[] KEY_UPTIME_MILLIS = ascii("uptime_millis");

    private static final byte[] KEY_QUEUED_TASKS = ascii("queued_tasks");
    private static final byte[] KEY_QUEUED_BYTES = ascii("queued_bytes");
    private static final byte[] KEY_CHANNELS_AUTOREAD_DISABLED = ascii("channels_autoread_disabled");
    private static final byte[] KEY_SUBMIT_ACCEPTED_TOTAL = ascii("submit_accepted_total");
    private static final byte[] KEY_SUBMIT_REJECTED_NOT_RUNNING_TOTAL = ascii("submit_rejected_not_running_total");
    private static final byte[] KEY_SUBMIT_REJECTED_QUEUE_FULL_TOTAL = ascii("submit_rejected_queue_full_total");
    private static final byte[] KEY_SUBMIT_REJECTED_BYTES_BUDGET_TOTAL = ascii("submit_rejected_bytes_budget_total");
    private static final byte[] KEY_SUBMIT_REJECTED_OFFER_FAILED_TOTAL = ascii("submit_rejected_offer_failed_total");
    private static final byte[] KEY_COMMANDS_EXECUTED_TOTAL = ascii("commands_executed_total");
    private static final byte[] KEY_COMMANDS_SKIPPED_CLOSING_TOTAL = ascii("commands_skipped_closing_total");
    private static final byte[] KEY_CLOSE_AFTER_REPLY_TOTAL = ascii("close_after_reply_total");
    private static final byte[] KEY_BACKPRESSURE_ENTER_TOTAL = ascii("backpressure_enter_total");
    private static final byte[] KEY_BACKPRESSURE_EXIT_TOTAL = ascii("backpressure_exit_total");
    private static final byte[] KEY_DRAIN_LIMITED_MAX_COMMANDS_TOTAL = ascii("drain_limited_max_commands_total");
    private static final byte[] KEY_DRAIN_LIMITED_TIME_BUDGET_TOTAL = ascii("drain_limited_time_budget_total");

    private static final byte[] KEY_CONN_PENDING = ascii("conn_pending");
    private static final byte[] KEY_CONN_PENDING_BYTES = ascii("conn_pending_bytes");
    private static final byte[] KEY_CONN_AUTOREAD_DISABLED = ascii("conn_autoread_disabled_by_executor");
    private static final byte[] KEY_CONN_CLOSING = ascii("conn_closing");
    private static final byte[] KEY_CONN_COMMANDS_ENQUEUED = ascii("conn_commands_enqueued");
    private static final byte[] KEY_CONN_COMMANDS_EXECUTED = ascii("conn_commands_executed");
    private static final byte[] KEY_CONN_COMMANDS_REJECTED = ascii("conn_commands_rejected");
    private static final byte[] KEY_CONN_COMMANDS_SKIPPED_CLOSING = ascii("conn_commands_skipped_closing");
    private static final byte[] KEY_CONN_CLOSE_AFTER_REPLY = ascii("conn_close_after_reply");
    private static final byte[] KEY_CONN_BACKPRESSURE_ENTER = ascii("conn_backpressure_enter");
    private static final byte[] KEY_CONN_BACKPRESSURE_EXIT = ascii("conn_backpressure_exit");

    private final YierdisServerRuntimeConfig config;
    private final long startedMillis;
    private volatile CommandExecutor<NettyExecutionConnection> executor;
    private volatile YierdisInstanceObservability observability;

    NettyServerInfoProvider(YierdisServerRuntimeConfig config) {
        this.config = Objects.requireNonNull(config, "config");
        this.startedMillis = System.currentTimeMillis();
    }

    void bindExecutor(CommandExecutor<NettyExecutionConnection> executor) {
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    CommandExecutor<NettyExecutionConnection> boundExecutorForTests() {
        return executor;
    }

    void bindObservability(YierdisInstanceObservability observability) {
        this.observability = Objects.requireNonNull(observability, "observability");
    }

    @Override
    public void info(ExecutionRequest request, CommandContext ctx) {
        Objects.requireNonNull(ctx, "ctx");
        ReplyWriter out = Objects.requireNonNull(ctx.out(), "out");
        CommandExecutor<NettyExecutionConnection> ex = executor;
        if (ex == null) {
            out.error("ERR INFO not ready");
            return;
        }

        String section = request != null && request.argc() == 2 ? asciiLower(request, 1) : null;
        if ("yierdis".equals(section)) {
            writeYierdisStructuredInfo(out, ex);
            return;
        }

        out.bulkString(buildRedisInfo(section, ex).getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void stats(ExecutionRequest request, CommandContext ctx) {
        Objects.requireNonNull(ctx, "ctx");
        ReplyWriter out = Objects.requireNonNull(ctx.out(), "out");
        CommandExecutor<NettyExecutionConnection> ex = executor;
        if (ex == null) {
            out.error("ERR STATS not ready");
            return;
        }

        CommandExecutor.StatsSnapshot s = ex.statsSnapshot();
        ConnectionStatsView stats = connectionStats(ctx);

        int pairs = 15 + (stats == null ? 0 : 11);
        writeHeader(out, pairs);

        writePair(out, KEY_QUEUED_TASKS, s.queuedTasks());
        writePair(out, KEY_QUEUED_BYTES, s.queuedBytes());
        writePair(out, KEY_CHANNELS_AUTOREAD_DISABLED, s.channelsAutoReadDisabled());
        writePair(out, KEY_SUBMIT_ACCEPTED_TOTAL, s.submitAccepted());
        writePair(out, KEY_SUBMIT_REJECTED_NOT_RUNNING_TOTAL, s.submitRejectedNotRunning());
        writePair(out, KEY_SUBMIT_REJECTED_QUEUE_FULL_TOTAL, s.submitRejectedQueueFull());
        writePair(out, KEY_SUBMIT_REJECTED_BYTES_BUDGET_TOTAL, s.submitRejectedBytesBudget());
        writePair(out, KEY_SUBMIT_REJECTED_OFFER_FAILED_TOTAL, s.submitRejectedOfferFailed());
        writePair(out, KEY_COMMANDS_EXECUTED_TOTAL, s.commandsExecuted());
        writePair(out, KEY_COMMANDS_SKIPPED_CLOSING_TOTAL, s.commandsSkippedClosing());
        writePair(out, KEY_CLOSE_AFTER_REPLY_TOTAL, s.closeAfterReply());
        writePair(out, KEY_BACKPRESSURE_ENTER_TOTAL, s.backpressureEnter());
        writePair(out, KEY_BACKPRESSURE_EXIT_TOTAL, s.backpressureExit());
        writePair(out, KEY_DRAIN_LIMITED_MAX_COMMANDS_TOTAL, s.drainLimitedByMaxCommands());
        writePair(out, KEY_DRAIN_LIMITED_TIME_BUDGET_TOTAL, s.drainLimitedByTimeBudget());

        if (stats == null) {
            return;
        }

        writePair(out, KEY_CONN_PENDING, stats.pending());
        writePair(out, KEY_CONN_PENDING_BYTES, stats.pendingBytes());
        writePair(out, KEY_CONN_AUTOREAD_DISABLED, stats.inputDisabledByExecutor() ? 1 : 0);
        writePair(out, KEY_CONN_CLOSING, stats.closing() ? 1 : 0);
        writePair(out, KEY_CONN_COMMANDS_ENQUEUED, stats.commandsEnqueued());
        writePair(out, KEY_CONN_COMMANDS_EXECUTED, stats.commandsExecuted());
        writePair(out, KEY_CONN_COMMANDS_REJECTED, stats.commandsRejected());
        writePair(out, KEY_CONN_COMMANDS_SKIPPED_CLOSING, stats.commandsSkippedClosing());
        writePair(out, KEY_CONN_CLOSE_AFTER_REPLY, stats.closeAfterReply());
        writePair(out, KEY_CONN_BACKPRESSURE_ENTER, stats.backpressureEnter());
        writePair(out, KEY_CONN_BACKPRESSURE_EXIT, stats.backpressureExit());
    }

    @Override
    public YierdisMemoryStats memoryStats(CommandContext ctx) {
        if (config.maxmemoryScope() != YierdisServerRuntimeConfig.MaxmemoryScope.GLOBAL) {
            return null;
        }
        return aggregatedMemoryStats();
    }

    private void writeYierdisStructuredInfo(ReplyWriter out, CommandExecutor<NettyExecutionConnection> ex) {
        CommandExecutor.StatsSnapshot s = ex.statsSnapshot();
        long nowMillis = System.currentTimeMillis();
        long uptimeMillis = Math.max(0, nowMillis - startedMillis);

        int pairs = 15;
        writeHeader(out, pairs);

        writePair(out, KEY_SERVER, VALUE_SERVER);
        writePair(out, KEY_VERSION, VALUE_VERSION);
        writePair(out, KEY_PORT, config.port());
        writePair(out, KEY_IO_THREADS, config.ioThreads());
        writePair(out, KEY_EXECUTOR_POLICY, ascii(String.valueOf(s.schedulingPolicy())));
        writePair(out, KEY_EXECUTOR_QUEUE_CAPACITY, config.executorQueueCapacity());
        writePair(out, KEY_EXECUTOR_QUEUE_MAX_BYTES, config.executorQueueMaxBytes());
        writePair(out, KEY_BACKPRESSURE_HIGH, config.backpressureHighWatermark());
        writePair(out, KEY_BACKPRESSURE_LOW, config.backpressureLowWatermark());
        writePair(out, KEY_BACKPRESSURE_BYTES_HIGH, config.backpressureBytesHighWatermark());
        writePair(out, KEY_BACKPRESSURE_BYTES_LOW, config.backpressureBytesLowWatermark());
        writePair(out, KEY_EXECUTOR_MAX_DRAIN, config.executorMaxDrainCommands());
        writePair(out, KEY_EXECUTOR_DRAIN_MILLIS, config.executorDrainTimeLimitMillis());
        writePair(out, KEY_STARTED_MILLIS, startedMillis);
        writePair(out, KEY_UPTIME_MILLIS, uptimeMillis);
    }

    private String buildRedisInfo(String section, CommandExecutor<NettyExecutionConnection> ex) {
        CommandExecutor.StatsSnapshot s = ex.statsSnapshot();
        long nowMillis = System.currentTimeMillis();
        long uptimeMillis = Math.max(0, nowMillis - startedMillis);
        long uptimeSeconds = Math.max(0, uptimeMillis / 1000L);

        boolean all = section == null || section.isBlank() || "default".equals(section) || "all".equals(section);
        boolean server = all || "server".equals(section);
        boolean clients = all || "clients".equals(section);
        boolean memory = all || "memory".equals(section);
        boolean stats = all || "stats".equals(section);
        boolean keyspace = all || "keyspace".equals(section);

        StringBuilder sb = new StringBuilder(512);

        if (server) {
            sb.append("# Server\r\n");
            sb.append("redis_version:").append(YierdisBuildInfo.version()).append("\r\n");
            sb.append("tcp_port:").append(config.port()).append("\r\n");
            sb.append("uptime_in_seconds:").append(uptimeSeconds).append("\r\n");
            sb.append("uptime_in_milliseconds:").append(uptimeMillis).append("\r\n");
            sb.append("\r\n");
        }

        if (clients) {
            sb.append("# Clients\r\n");
            sb.append("connected_clients:0\r\n");
            sb.append("blocked_clients:0\r\n");
            sb.append("\r\n");
        }

        if (memory) {
            YierdisMemoryStats memStats = aggregatedMemoryStats();
            long usedMemoryBytes = memStats.heapDataBytesEstimate() + memStats.offHeapUsedBytes();
            long overheadBytesEstimate = memStats.keyspaceTableOverheadBytesEstimate()
                    + memStats.expireTableOverheadBytesEstimate()
                    + memStats.expireValueObjectsBytesEstimate();
            sb.append("# Memory\r\n");
            sb.append("used_memory:").append(usedMemoryBytes).append("\r\n");
            sb.append("used_memory_dataset:").append(memStats.heapDataBytesEstimate()).append("\r\n");
            sb.append("used_memory_overhead:").append(overheadBytesEstimate).append("\r\n");
            sb.append("maxmemory:").append(config.maxmemoryBytes()).append("\r\n");
            sb.append("maxmemory_policy:").append(config.maxmemoryPolicy().redisName()).append("\r\n");
            sb.append("yierdis_maxmemory_scope:")
                    .append(config.maxmemoryScope().argvValue())
                    .append("\r\n");
            if (config.maxmemoryScope() == YierdisServerRuntimeConfig.MaxmemoryScope.PER_DB && config.maxmemoryBytes() > 0) {
                long perDb = config.maxmemoryBytes() / Math.max(1L, (long) config.databases());
                sb.append("yierdis_maxmemory_per_db_bytes:").append(perDb).append("\r\n");
            }
            sb.append("yierdis_ledger_used_bytes:").append(memStats.heapDataBytesEstimate()).append("\r\n");
            sb.append("yierdis_ledger_reserved_bytes:").append(memStats.reservedBytes()).append("\r\n");
            sb.append("yierdis_ledger_effective_used_bytes:").append(memStats.heapDataBytesEstimate() + memStats.reservedBytes()).append("\r\n");
            sb.append("yierdis_maxmemory_used_bytes:").append(memStats.usedBytesForMaxmemory()).append("\r\n");
            sb.append("yierdis_maxmemory_effective_used_bytes:").append(memStats.effectiveUsedBytesForMaxmemory()).append("\r\n");
            sb.append("yierdis_offheap_included_in_maxmemory:").append(memStats.offHeapIncludedInMaxmemory() ? 1 : 0).append("\r\n");
            sb.append("yierdis_offheap_used_bytes:").append(memStats.offHeapUsedBytes()).append("\r\n");
            sb.append("yierdis_offheap_max_bytes:0\r\n");
            sb.append("yierdis_native_defrag_last_scanned_objects:").append(memStats.nativeDefragLastScannedObjects()).append("\r\n");
            sb.append("yierdis_native_defrag_last_moved_objects:").append(memStats.nativeDefragLastMovedObjects()).append("\r\n");
            sb.append("yierdis_native_defrag_last_moved_bytes:").append(memStats.nativeDefragLastMovedBytes()).append("\r\n");
            sb.append("yierdis_native_defrag_last_skipped_pinned_objects:").append(memStats.nativeDefragLastSkippedPinnedObjects()).append("\r\n");
            sb.append("yierdis_native_defrag_last_skipped_budget_objects:").append(memStats.nativeDefragLastSkippedBudgetObjects()).append("\r\n");
            sb.append("yierdis_native_defrag_last_failed_moves:").append(memStats.nativeDefragLastFailedMoves()).append("\r\n");
            sb.append("yierdis_native_defrag_moved_bytes:").append(memStats.nativeDefragMovedBytes()).append("\r\n");
            sb.append("yierdis_native_defrag_skipped_pinned_objects:").append(memStats.nativeDefragSkippedPinnedObjects()).append("\r\n");
            sb.append("yierdis_native_defrag_quarantined_objects:").append(memStats.nativeDefragQuarantinedObjects()).append("\r\n");
            sb.append("yierdis_native_defrag_quarantine_bytes:").append(memStats.nativeDefragQuarantineBytes()).append("\r\n");
            sb.append("yierdis_native_stale_handle_detections:").append(memStats.nativeStaleHandleDetections()).append("\r\n");
            sb.append("yierdis_native_defrag_reclaimed_pages:").append(memStats.nativeDefragReclaimedPages()).append("\r\n");
            sb.append("\r\n");
        }

        if (stats) {
            sb.append("# Stats\r\n");
            sb.append("total_commands_processed:").append(s.commandsExecuted()).append("\r\n");
            sb.append("rejected_connections:0\r\n");
            sb.append("total_connections_received:0\r\n");
            sb.append("instantaneous_ops_per_sec:0\r\n");
            sb.append("yierdis_queued_tasks:").append(s.queuedTasks()).append("\r\n");
            sb.append("yierdis_queued_bytes:").append(s.queuedBytes()).append("\r\n");
            sb.append("\r\n");
        }

        if (keyspace) {
            sb.append("# Keyspace\r\n");
            appendRuntimeKeyspaceSummary(sb);
            sb.append("\r\n");
        }

        return sb.toString();
    }

    private void appendRuntimeKeyspaceSummary(StringBuilder sb) {
        YierdisInstanceObservability runtimeObservability = observability;
        if (runtimeObservability == null) {
            return;
        }
        for (YierdisInstanceObservability.YierdisDbSummary summary : runtimeObservability.dbSummaries()) {
            if (summary.keyCount() <= 0 && summary.expireCount() <= 0) {
                continue;
            }
            sb.append("db").append(summary.dbIndex())
                    .append(":keys=").append(summary.keyCount())
                    .append(",expires=").append(summary.expireCount())
                    .append("\r\n");
        }
    }

    private YierdisMemoryStats aggregatedMemoryStats() {
        YierdisInstanceObservability runtimeObservability = observability;
        if (runtimeObservability != null) {
            return runtimeObservability.memoryStats();
        }
        return new YierdisMemoryStats(
                config.maxmemoryBytes(),
                0,
                0,
                0,
                0,
                0,
                config.maxmemoryScope() == YierdisServerRuntimeConfig.MaxmemoryScope.GLOBAL,
                false,
                0,
                0,
                false,
                0,
                0,
                0,
                false,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0
        );
    }

    private static ConnectionStatsView connectionStats(CommandContext ctx) {
        if (ctx == null) {
            return null;
        }
        return ctx.session().connectionStats();
    }

    private static void writeHeader(ReplyWriter out, int pairs) {
        try {
            out.mapHeader(pairs);
        } catch (RuntimeException e) {
            // Best-effort compatibility: some reply writers may not support maps.
            out.arrayHeader(pairs * 2);
        }
    }

    private static void writePair(ReplyWriter out, byte[] key, byte[] value) {
        out.bulkString(key);
        out.bulkString(value);
    }

    private static void writePair(ReplyWriter out, byte[] key, long value) {
        out.bulkString(key);
        out.integer(value);
    }

    private static String asciiLower(ExecutionRequest request, int argIndex) {
        if (request == null || argIndex < 0 || argIndex >= request.argc() || request.isNull(argIndex) || request.len(argIndex) <= 0) {
            return null;
        }
        byte[] raw = request.toByteArray(argIndex);
        if (raw == null || raw.length == 0) {
            return null;
        }
        return new String(raw, StandardCharsets.US_ASCII).trim().toLowerCase(Locale.ROOT);
    }

    private static byte[] ascii(String s) {
        if (s == null || s.isEmpty()) {
            return new byte[0];
        }
        return s.getBytes(StandardCharsets.US_ASCII);
    }
}
