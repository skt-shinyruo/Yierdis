package yier.bubu.redis;

// INFO/STATS 提供器：基于执行器统计与连接态（ServerSessionState + ServerRuntimeState）输出可观测性摘要，避免在热路径做额外分配。

import yier.bubu.redis.command.ServerInfoProvider;
import yier.bubu.redis.ops.YierdisMemoryStats;
import yier.bubu.redis.ops.DbEngine;
import yier.bubu.redis.protocol.Command;
import yier.bubu.redis.protocol.CommandContext;
import yier.bubu.redis.protocol.ReplyWriter;
import yier.bubu.redis.protocol.Session;
import yier.bubu.redis.protocol.YierdisBuildInfo;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Server-side INFO/STATS provider backed by {@link NettyCommandExecutor}.
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

    private final ServerConfig config;
    private final long startedMillis;
    private volatile NettyCommandExecutor executor;
    private volatile DbEngine[] engines;

    NettyServerInfoProvider(ServerConfig config) {
        this.config = Objects.requireNonNull(config, "config");
        this.startedMillis = System.currentTimeMillis();
    }

    void bindExecutor(NettyCommandExecutor executor) {
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    void bindEngines(DbEngine[] engines) {
        this.engines = engines;
    }

    @Override
    public void info(Command cmd, CommandContext ctx) {
        Objects.requireNonNull(ctx, "ctx");
        ReplyWriter out = Objects.requireNonNull(ctx.out(), "out");
        NettyCommandExecutor ex = executor;
        if (ex == null) {
            out.error("ERR INFO not ready");
            return;
        }

        String section = cmd != null && cmd.argc() == 2 ? asciiLower(cmd, 1) : null;
        if ("yierdis".equals(section)) {
            writeYierdisStructuredInfo(out, ex);
            return;
        }

        out.bulkString(buildRedisInfo(section, ex).getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void stats(Command cmd, CommandContext ctx) {
        Objects.requireNonNull(ctx, "ctx");
        ReplyWriter out = Objects.requireNonNull(ctx.out(), "out");
        NettyCommandExecutor ex = executor;
        if (ex == null) {
            out.error("ERR STATS not ready");
            return;
        }

        NettyCommandExecutor.StatsSnapshot s = ex.statsSnapshot();
        ServerRuntimeState rt = runtimeState(ctx);

        int pairs = 15 + (rt == null ? 0 : 11);
        writeHeader(out, pairs);

        writePair(out, KEY_QUEUED_TASKS, s.queuedTasks);
        writePair(out, KEY_QUEUED_BYTES, s.queuedBytes);
        writePair(out, KEY_CHANNELS_AUTOREAD_DISABLED, s.channelsAutoReadDisabled);
        writePair(out, KEY_SUBMIT_ACCEPTED_TOTAL, s.submitAccepted);
        writePair(out, KEY_SUBMIT_REJECTED_NOT_RUNNING_TOTAL, s.submitRejectedNotRunning);
        writePair(out, KEY_SUBMIT_REJECTED_QUEUE_FULL_TOTAL, s.submitRejectedQueueFull);
        writePair(out, KEY_SUBMIT_REJECTED_BYTES_BUDGET_TOTAL, s.submitRejectedBytesBudget);
        writePair(out, KEY_SUBMIT_REJECTED_OFFER_FAILED_TOTAL, s.submitRejectedOfferFailed);
        writePair(out, KEY_COMMANDS_EXECUTED_TOTAL, s.commandsExecuted);
        writePair(out, KEY_COMMANDS_SKIPPED_CLOSING_TOTAL, s.commandsSkippedClosing);
        writePair(out, KEY_CLOSE_AFTER_REPLY_TOTAL, s.closeAfterReply);
        writePair(out, KEY_BACKPRESSURE_ENTER_TOTAL, s.backpressureEnter);
        writePair(out, KEY_BACKPRESSURE_EXIT_TOTAL, s.backpressureExit);
        writePair(out, KEY_DRAIN_LIMITED_MAX_COMMANDS_TOTAL, s.drainLimitedByMaxCommands);
        writePair(out, KEY_DRAIN_LIMITED_TIME_BUDGET_TOTAL, s.drainLimitedByTimeBudget);

        if (rt == null) {
            return;
        }

        writePair(out, KEY_CONN_PENDING, rt.pendingCounter().get());
        writePair(out, KEY_CONN_PENDING_BYTES, rt.pendingBytesCounter().get());
        writePair(out, KEY_CONN_AUTOREAD_DISABLED, rt.autoReadDisabledByExecutor() ? 1 : 0);
        writePair(out, KEY_CONN_CLOSING, rt.isClosing() ? 1 : 0);
        writePair(out, KEY_CONN_COMMANDS_ENQUEUED, rt.commandsEnqueuedCounter().get());
        writePair(out, KEY_CONN_COMMANDS_EXECUTED, rt.commandsExecutedCounter().get());
        writePair(out, KEY_CONN_COMMANDS_REJECTED, rt.commandsRejectedCounter().get());
        writePair(out, KEY_CONN_COMMANDS_SKIPPED_CLOSING, rt.commandsSkippedClosingCounter().get());
        writePair(out, KEY_CONN_CLOSE_AFTER_REPLY, rt.closeAfterReplyCounter().get());
        writePair(out, KEY_CONN_BACKPRESSURE_ENTER, rt.backpressureEnterCounter().get());
        writePair(out, KEY_CONN_BACKPRESSURE_EXIT, rt.backpressureExitCounter().get());
    }

    @Override
    public YierdisMemoryStats memoryStats(CommandContext ctx) {
        if (config.maxmemoryScope != ServerConfig.MaxmemoryScope.GLOBAL) {
            return null;
        }

        DbEngine[] local = engines;
        if (local == null || local.length == 0) {
            return new YierdisMemoryStats(
                    config.maxmemoryBytes,
                    0,
                    0,
                    0,
                    0,
                    0,
                    true,
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
                    0
            );
        }

        long heap = 0;
        long keyspaceOverhead = 0;
        long expireOverhead = 0;
        long expireValueObjects = 0;
        long offHeap = 0;
        long reserved = 0;
        int keyCount = 0;
        int expireCount = 0;
        boolean keysStoredOffHeap = false;
        boolean keyspaceRehashing = false;
        boolean expireRehashing = false;
        int keyspaceCap0 = 0;
        int keyspaceCap1 = 0;
        int expireCap0 = 0;
        int expireCap1 = 0;

        for (int i = 0; i < local.length; i++) {
            DbEngine db = local[i];
            if (db == null) {
                continue;
            }
            YierdisMemoryStats s = db.memory().memoryStats();
            heap += s.heapDataBytesEstimate();
            keyspaceOverhead += s.keyspaceTableOverheadBytesEstimate();
            expireOverhead += s.expireTableOverheadBytesEstimate();
            expireValueObjects += s.expireValueObjectsBytesEstimate();
            keyCount += s.keyCount();
            expireCount += s.expireCount();
            reserved += s.reservedBytes();
            keysStoredOffHeap |= s.keysStoredOffHeap();
            keyspaceRehashing |= s.keyspaceRehashing();
            expireRehashing |= s.expireRehashing();
            keyspaceCap0 += s.keyspaceTable0Capacity();
            keyspaceCap1 += s.keyspaceTable1Capacity();
            expireCap0 += s.expireTable0Capacity();
            expireCap1 += s.expireTable1Capacity();
            offHeap = Math.max(offHeap, s.offHeapUsedBytes());
        }

        long usedBytesForMaxmemory = heap + offHeap;
        long effectiveUsedBytesForMaxmemory = usedBytesForMaxmemory + Math.max(0L, reserved);
        long totalEstimatedBytes = heap + offHeap + keyspaceOverhead + expireOverhead + expireValueObjects;

        return new YierdisMemoryStats(
                config.maxmemoryBytes,
                usedBytesForMaxmemory,
                heap,
                offHeap,
                reserved,
                effectiveUsedBytesForMaxmemory,
                true,
                keysStoredOffHeap,
                keyCount,
                expireCount,
                keyspaceRehashing,
                keyspaceCap0,
                keyspaceCap1,
                keyspaceOverhead,
                expireRehashing,
                expireCap0,
                expireCap1,
                expireOverhead,
                expireValueObjects,
                totalEstimatedBytes
        );
    }

    private void writeYierdisStructuredInfo(ReplyWriter out, NettyCommandExecutor ex) {
        NettyCommandExecutor.StatsSnapshot s = ex.statsSnapshot();
        long nowMillis = System.currentTimeMillis();
        long uptimeMillis = Math.max(0, nowMillis - startedMillis);
        long drainMillis = TimeUnit.NANOSECONDS.toMillis(s.drainTimeLimitNanos);

        int pairs = 15;
        writeHeader(out, pairs);

        writePair(out, KEY_SERVER, VALUE_SERVER);
        writePair(out, KEY_VERSION, VALUE_VERSION);
        writePair(out, KEY_PORT, config.port);
        writePair(out, KEY_IO_THREADS, config.ioThreads);
        writePair(out, KEY_EXECUTOR_POLICY, ascii(String.valueOf(s.schedulingPolicy)));
        writePair(out, KEY_EXECUTOR_QUEUE_CAPACITY, s.queueCapacity);
        writePair(out, KEY_EXECUTOR_QUEUE_MAX_BYTES, s.queueMaxBytes);
        writePair(out, KEY_BACKPRESSURE_HIGH, s.backpressureHighWatermark);
        writePair(out, KEY_BACKPRESSURE_LOW, s.backpressureLowWatermark);
        writePair(out, KEY_BACKPRESSURE_BYTES_HIGH, s.backpressureBytesHighWatermark);
        writePair(out, KEY_BACKPRESSURE_BYTES_LOW, s.backpressureBytesLowWatermark);
        writePair(out, KEY_EXECUTOR_MAX_DRAIN, s.maxDrainCommands);
        writePair(out, KEY_EXECUTOR_DRAIN_MILLIS, drainMillis);
        writePair(out, KEY_STARTED_MILLIS, startedMillis);
        writePair(out, KEY_UPTIME_MILLIS, uptimeMillis);
    }

    private String buildRedisInfo(String section, NettyCommandExecutor ex) {
        NettyCommandExecutor.StatsSnapshot s = ex.statsSnapshot();
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
            sb.append("tcp_port:").append(config.port).append("\r\n");
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
            MemorySummary m = memorySummary();
            long ledgerReservedBytes = 0;
            long maxmemoryUsedBytes = 0;
            long maxmemoryEffectiveUsedBytes = 0;
            boolean offHeapIncludedInMaxmemory = config.maxmemoryScope == ServerConfig.MaxmemoryScope.GLOBAL;
            if (config.maxmemoryScope == ServerConfig.MaxmemoryScope.GLOBAL) {
                YierdisMemoryStats memStats = memoryStats(null);
                if (memStats != null) {
                    ledgerReservedBytes = memStats.reservedBytes();
                    maxmemoryUsedBytes = memStats.usedBytesForMaxmemory();
                    maxmemoryEffectiveUsedBytes = memStats.effectiveUsedBytesForMaxmemory();
                    offHeapIncludedInMaxmemory = memStats.offHeapIncludedInMaxmemory();
                }
            } else {
                DbEngine[] local = engines;
                if (local != null) {
                    for (int i = 0; i < local.length; i++) {
                        DbEngine db = local[i];
                        if (db == null) {
                            continue;
                        }
                        YierdisMemoryStats dbStats = db.memory().memoryStats();
                        ledgerReservedBytes += dbStats.reservedBytes();
                        maxmemoryUsedBytes += dbStats.usedBytesForMaxmemory();
                        maxmemoryEffectiveUsedBytes += dbStats.effectiveUsedBytesForMaxmemory();
                    }
                }
            }
            sb.append("# Memory\r\n");
            sb.append("used_memory:").append(m.usedMemoryBytes).append("\r\n");
            sb.append("used_memory_dataset:").append(m.heapDataBytesEstimate).append("\r\n");
            sb.append("used_memory_overhead:").append(m.overheadBytesEstimate).append("\r\n");
            sb.append("maxmemory:").append(config.maxmemoryBytes).append("\r\n");
            sb.append("maxmemory_policy:").append(config.maxmemoryPolicy).append("\r\n");
            sb.append("yierdis_maxmemory_scope:")
                    .append(config.maxmemoryScope == ServerConfig.MaxmemoryScope.PER_DB ? "per-db" : "global")
                    .append("\r\n");
            if (config.maxmemoryScope == ServerConfig.MaxmemoryScope.PER_DB && config.maxmemoryBytes > 0) {
                long perDb = config.maxmemoryBytes / Math.max(1L, (long) config.databases);
                sb.append("yierdis_maxmemory_per_db_bytes:").append(perDb).append("\r\n");
            }
            sb.append("yierdis_ledger_used_bytes:").append(m.heapDataBytesEstimate).append("\r\n");
            sb.append("yierdis_ledger_reserved_bytes:").append(ledgerReservedBytes).append("\r\n");
            sb.append("yierdis_ledger_effective_used_bytes:").append(m.heapDataBytesEstimate + ledgerReservedBytes).append("\r\n");
            sb.append("yierdis_maxmemory_used_bytes:").append(maxmemoryUsedBytes).append("\r\n");
            sb.append("yierdis_maxmemory_effective_used_bytes:").append(maxmemoryEffectiveUsedBytes).append("\r\n");
            sb.append("yierdis_offheap_included_in_maxmemory:").append(offHeapIncludedInMaxmemory ? 1 : 0).append("\r\n");
            sb.append("yierdis_offheap_used_bytes:").append(m.offHeapUsedBytes).append("\r\n");
            sb.append("yierdis_offheap_max_bytes:").append(config.offheapMaxBytes).append("\r\n");
            sb.append("\r\n");
        }

        if (stats) {
            sb.append("# Stats\r\n");
            sb.append("total_commands_processed:").append(s.commandsExecuted).append("\r\n");
            sb.append("rejected_connections:0\r\n");
            sb.append("total_connections_received:0\r\n");
            sb.append("instantaneous_ops_per_sec:0\r\n");
            sb.append("yierdis_queued_tasks:").append(s.queuedTasks).append("\r\n");
            sb.append("yierdis_queued_bytes:").append(s.queuedBytes).append("\r\n");
            sb.append("\r\n");
        }

        if (keyspace) {
            sb.append("# Keyspace\r\n");
            appendKeyspace(sb);
            sb.append("\r\n");
        }

        return sb.toString();
    }

    private void appendKeyspace(StringBuilder sb) {
        DbEngine[] local = engines;
        if (local == null || local.length == 0) {
            return;
        }
        for (int i = 0; i < local.length; i++) {
            DbEngine db = local[i];
            if (db == null) {
                continue;
            }
            YierdisMemoryStats s = db.memory().memoryStats();
            int keys = s.keyCount();
            int expires = s.expireCount();
            if (keys <= 0 && expires <= 0) {
                continue;
            }
            sb.append("db").append(i)
                    .append(":keys=").append(keys)
                    .append(",expires=").append(expires)
                    .append("\r\n");
        }
    }

    private MemorySummary memorySummary() {
        DbEngine[] local = engines;
        if (local == null || local.length == 0) {
            return new MemorySummary(0, 0, 0, 0);
        }
        long heap = 0;
        long keyspaceOverhead = 0;
        long expireOverhead = 0;
        long expireValueObjects = 0;
        long offHeap = 0;

        for (int i = 0; i < local.length; i++) {
            DbEngine db = local[i];
            if (db == null) {
                continue;
            }
            YierdisMemoryStats s = db.memory().memoryStats();
            heap += s.heapDataBytesEstimate();
            keyspaceOverhead += s.keyspaceTableOverheadBytesEstimate();
            expireOverhead += s.expireTableOverheadBytesEstimate();
            expireValueObjects += s.expireValueObjectsBytesEstimate();
            if (i == 0) {
                offHeap = s.offHeapUsedBytes();
            }
        }

        long overhead = keyspaceOverhead + expireOverhead + expireValueObjects;
        long used = heap + offHeap;
        return new MemorySummary(used, heap, offHeap, overhead);
    }

    private static final class MemorySummary {
        final long usedMemoryBytes;
        final long heapDataBytesEstimate;
        final long offHeapUsedBytes;
        final long overheadBytesEstimate;

        private MemorySummary(long usedMemoryBytes, long heapDataBytesEstimate, long offHeapUsedBytes, long overheadBytesEstimate) {
            this.usedMemoryBytes = usedMemoryBytes;
            this.heapDataBytesEstimate = heapDataBytesEstimate;
            this.offHeapUsedBytes = offHeapUsedBytes;
            this.overheadBytesEstimate = overheadBytesEstimate;
        }
    }

    private static ServerRuntimeState runtimeState(CommandContext ctx) {
        if (ctx == null) {
            return null;
        }
        Session session = ctx.session();
        if (session instanceof ServerSessionState s) {
            return s.runtime();
        }
        return null;
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

    private static String asciiLower(Command cmd, int argIndex) {
        if (cmd == null || argIndex < 0 || argIndex >= cmd.argc() || cmd.isNull(argIndex) || cmd.len(argIndex) <= 0) {
            return null;
        }
        byte[] raw = cmd.toByteArray(argIndex);
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
