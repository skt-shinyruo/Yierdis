package yier.bubu.redis.app.server;

// INFO/STATS 提供器：基于 transport-neutral executor 统计与连接态输出可观测性摘要，避免在热路径做额外分配。
// 所有 INFO/STATS/HEALTH 字段在 fields() 中只声明一次（名称 + 取值 + 分组 + 生效渲染面），
// 由 mapReply / appendText 两个渲染器过滤输出；map 输出顺序 = 声明顺序，文本输出顺序由组序列给出。

import yier.bubu.redis.command.api.ServerInfoProvider;
import yier.bubu.redis.storage.api.YierdisMemoryStats;
import yier.bubu.redis.command.api.CommandArgs;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.ConnectionStatsView;
import yier.bubu.redis.execution.api.RedisReplies;
import yier.bubu.redis.execution.api.RedisReply;
import yier.bubu.redis.app.server.args.YierdisServerRuntimeConfig;
import yier.bubu.redis.execution.executor.CommandExecutor;
import yier.bubu.redis.protocol.resp.netty.InboundMemoryBudget;
import yier.bubu.redis.protocol.resp.netty.InboundMemoryBudgetStats;
import yier.bubu.redis.runtime.embedded.YierdisInstanceObservability;
import yier.bubu.redis.runtime.api.YierdisInstanceConfig;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Server-side INFO/STATS provider backed by {@link CommandExecutor}.
 * <p>
 * This object is intentionally lightweight: it is called only when clients execute INFO/STATS, and uses
 * pre-aggregated counters updated on the hot path.
 */
final class NettyServerInfoProvider implements ServerInfoProvider {

    /** 渲染面：字段清单的一个消费输出。 */
    private enum Surface {STATS_MAP, STRUCTURED_MAP, HEALTH_MAP, STATS_TEXT, HEALTH_TEXT, CLIENTS_TEXT}

    /** 渲染分组：map 渲染面按声明顺序输出，文本渲染面按各自组序列输出。 */
    private enum Group {
        SERVER, QUEUED, EXECUTOR, DEFERRED, INBOUND, REPLY_CAPACITY, OUTBOUND, EGRESS, LIVE_CHANNELS,
        HEALTH, DEGRADED, DATABASES, FAILURE, CONNECTIONS_TOTAL, CONNECTIONS_REJECTED, MAX_CLIENTS,
        CLIENTS, CONNECTION
    }

    private static final EnumSet<Surface> STRUCTURED_ONLY = EnumSet.of(Surface.STRUCTURED_MAP);
    private static final EnumSet<Surface> STATS_ONLY = EnumSet.of(Surface.STATS_MAP);
    private static final EnumSet<Surface> MAPS_ONLY = EnumSet.of(Surface.STATS_MAP, Surface.STRUCTURED_MAP);
    private static final EnumSet<Surface> STATS_MAP_AND_TEXT = EnumSet.of(Surface.STATS_MAP, Surface.STATS_TEXT);
    private static final EnumSet<Surface> MAPS_AND_TEXT = EnumSet.of(
            Surface.STATS_MAP, Surface.STRUCTURED_MAP, Surface.STATS_TEXT);
    private static final EnumSet<Surface> MAPS_AND_HEALTH_TEXT = EnumSet.of(
            Surface.STATS_MAP, Surface.STRUCTURED_MAP, Surface.HEALTH_MAP, Surface.HEALTH_TEXT);
    private static final EnumSet<Surface> MAPS_HEALTH_AND_STATS_TEXT = EnumSet.of(
            Surface.STATS_MAP, Surface.STRUCTURED_MAP, Surface.HEALTH_MAP, Surface.HEALTH_TEXT, Surface.STATS_TEXT);

    /**
     * 一个可观测字段：名称 + 取值（Long 或 String）+ 分组 + 渲染面集合。
     * <p>
     * textHidden 仅作用于文本渲染面，用于 first_failure_* 这类无故障时省略的行；sanitize 让文本值过滤 CR/LF。
     */
    private record Field(
            Group group,
            String name,
            Object value,
            EnumSet<Surface> surfaces,
            boolean sanitize,
            boolean textHidden
    ) {
        /** first_failure_*：仅在存在首个故障时出现在文本输出中。 */
        static Field failure(String name, String value, boolean hasFailure) {
            return new Field(Group.FAILURE, name, value == null ? "" : value,
                    MAPS_AND_HEALTH_TEXT, true, !hasFailure);
        }
    }

    /** INFO 文本 # Health 段组顺序（与 map 不同：databases/degraded 互换、connected_clients 插入、first_failure 置尾）。 */
    private static final Group[] HEALTH_TEXT_ORDER = {
            Group.HEALTH, Group.DATABASES, Group.DEGRADED, Group.CLIENTS,
            Group.CONNECTIONS_TOTAL, Group.CONNECTIONS_REJECTED, Group.MAX_CLIENTS, Group.FAILURE};

    /** INFO 文本 # Stats 段 yierdis_ 前缀字段的组顺序。 */
    private static final Group[] STATS_TEXT_ORDER = {
            Group.QUEUED, Group.INBOUND, Group.REPLY_CAPACITY,
            Group.OUTBOUND, Group.EGRESS, Group.LIVE_CHANNELS, Group.DEFERRED};

    private final YierdisServerRuntimeConfig config;
    private final long startedMillis;
    private volatile CommandExecutor executor;
    private volatile YierdisInstanceObservability observability;
    private volatile InboundMemoryBudget inboundMemoryBudget;
    private volatile OutboundMemoryBudget outboundMemoryBudget;
    private volatile ChildChannelRegistry childChannelRegistry;
    private volatile ReplyEgressStats replyEgressStats = new ReplyEgressStats();
    private volatile Supplier<String> lifecycleState = () -> "STARTING";

    NettyServerInfoProvider(YierdisServerRuntimeConfig config) {
        this.config = Objects.requireNonNull(config, "config");
        this.startedMillis = System.currentTimeMillis();
    }

    void bindExecutor(CommandExecutor executor) {
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    CommandExecutor boundExecutorForTests() {
        return executor;
    }

    void bindObservability(YierdisInstanceObservability observability) {
        this.observability = Objects.requireNonNull(observability, "observability");
    }

    void bindInboundMemoryBudget(InboundMemoryBudget inboundMemoryBudget) {
        this.inboundMemoryBudget = Objects.requireNonNull(inboundMemoryBudget, "inboundMemoryBudget");
    }

    void bindOutboundMemoryBudget(OutboundMemoryBudget outboundMemoryBudget) {
        this.outboundMemoryBudget = Objects.requireNonNull(outboundMemoryBudget, "outboundMemoryBudget");
    }

    void bindChildChannelRegistry(ChildChannelRegistry childChannelRegistry) {
        this.childChannelRegistry = Objects.requireNonNull(childChannelRegistry, "childChannelRegistry");
    }

    void bindReplyEgressStats(ReplyEgressStats replyEgressStats) {
        this.replyEgressStats = Objects.requireNonNull(replyEgressStats, "replyEgressStats");
    }

    void bindLifecycleState(Supplier<String> lifecycleState) {
        this.lifecycleState = Objects.requireNonNull(lifecycleState, "lifecycleState");
    }

    @Override
    public RedisReply info(CommandArgs args, CommandSession session) {
        Objects.requireNonNull(session, "session");
        CommandExecutor ex = executor;
        if (ex == null) {
            return RedisReplies.error("ERR INFO not ready");
        }

        ServerStatsSnapshot snapshot = serverStatsSnapshot(ex);
        String section = args != null && args.argc() == 2 ? asciiLower(args, 1) : null;
        List<Field> fields = fields(snapshot, null);
        if ("health".equals(section)) {
            return mapReply(fields, Surface.HEALTH_MAP);
        }
        if ("yierdis".equals(section)) {
            return mapReply(fields, Surface.STRUCTURED_MAP);
        }

        byte[] response = buildRedisInfo(section, snapshot, fields).getBytes(StandardCharsets.UTF_8);
        return RedisReplies.bulkString(response);
    }

    @Override
    public RedisReply stats(CommandSession session) {
        Objects.requireNonNull(session, "session");
        CommandExecutor ex = executor;
        if (ex == null) {
            return RedisReplies.error("ERR STATS not ready");
        }

        return mapReply(fields(serverStatsSnapshot(ex), connectionStats(session)), Surface.STATS_MAP);
    }

    @Override
    public YierdisMemoryStats memoryStats(CommandSession session) {
        if (config.maxmemoryScope() != YierdisInstanceConfig.MaxmemoryScope.GLOBAL) {
            return null;
        }
        return aggregatedMemoryStats();
    }

    /**
     * 全量字段清单：每个字段只声明一次；声明顺序即 map 渲染面（STATS / INFO yierdis / INFO health）的输出顺序。
     */
    private List<Field> fields(ServerStatsSnapshot snapshot, ConnectionStatsView connectionStats) {
        CommandExecutor.StatsSnapshot stats = snapshot.executor();
        InboundMemoryBudgetStats inbound = snapshot.inbound();
        OutboundMemoryBudgetStats outbound = snapshot.outbound();
        ReplyEgressStats.Snapshot egress = snapshot.egress();
        ChildChannelRegistry.StatsSnapshot children = snapshot.children();
        HealthView health = snapshot.health();
        boolean hasFailure = health.firstFailureType() != null && !health.firstFailureType().isBlank();

        List<Field> fields = new ArrayList<>(96);
        add(fields, Group.SERVER, STRUCTURED_ONLY,
                "server", "yierdis", "version", YierdisBuildInfo.version());
        add(fields, Group.SERVER, STRUCTURED_ONLY,
                "port", config.port(), "io_threads", config.ioThreads());
        add(fields, Group.SERVER, STRUCTURED_ONLY,
                "executor_policy", String.valueOf(stats.schedulingPolicy()),
                "executor_queue_capacity", config.executorQueueCapacity(),
                "executor_queue_max_bytes", config.executorQueueMaxBytes(),
                "backpressure_high", config.backpressureHighWatermark(),
                "backpressure_low", config.backpressureLowWatermark(),
                "backpressure_bytes_high", config.backpressureBytesHighWatermark(),
                "backpressure_bytes_low", config.backpressureBytesLowWatermark(),
                "executor_max_drain", config.executorMaxDrainCommands(),
                "executor_drain_millis", config.executorDrainTimeLimitMillis(),
                "started_millis", startedMillis,
                "uptime_millis", snapshot.uptimeMillis());

        add(fields, Group.QUEUED, STATS_MAP_AND_TEXT,
                "queued_tasks", stats.queuedTasks(), "queued_bytes", stats.queuedBytes());
        add(fields, Group.EXECUTOR, STATS_ONLY,
                "channels_autoread_disabled", stats.channelsAutoReadDisabled(),
                "submit_accepted_total", stats.submitAccepted(),
                "submit_rejected_not_running_total", stats.submitRejectedNotRunning(),
                "submit_rejected_closing_total", stats.submitRejectedClosing(),
                "submit_rejected_queue_full_total", stats.submitRejectedQueueFull(),
                "submit_rejected_bytes_budget_total", stats.submitRejectedBytesBudget(),
                "submit_rejected_offer_failed_total", stats.submitRejectedOfferFailed(),
                "commands_executed_total", stats.commandsExecuted(),
                "commands_skipped_closing_total", stats.commandsSkippedClosing(),
                "close_after_reply_total", stats.closeAfterReply(),
                "backpressure_enter_total", stats.backpressureEnter(),
                "backpressure_exit_total", stats.backpressureExit(),
                "drain_limited_max_commands_total", stats.drainLimitedByMaxCommands(),
                "drain_limited_time_budget_total", stats.drainLimitedByTimeBudget());
        add(fields, Group.DEFERRED, MAPS_AND_TEXT,
                "deferred_fair_reply_heads", stats.deferredFairReplyHeads(),
                "deferred_global_reply_heads", stats.deferredGlobalReplyHeads());

        add(fields, Group.INBOUND, MAPS_AND_TEXT,
                "inbound_capacity_bytes", inbound.capacityBytes(),
                "inbound_reserved_bytes", inbound.reservedBytes(),
                "inbound_peak_reserved_bytes", inbound.peakReservedBytes(),
                "inbound_waiting_connections", inbound.waitingConnections(),
                "inbound_backpressured", inbound.backpressured() ? 1L : 0L,
                "inbound_rejected_connections", inbound.rejectedConnections(),
                "inbound_closed", inbound.closed() ? 1L : 0L);

        add(fields, Group.REPLY_CAPACITY, MAPS_AND_TEXT,
                "reply_global_capacity_bytes", config.replyGlobalCapacityBytes(),
                "reply_per_connection_capacity_bytes", config.replyPerConnectionCapacityBytes(),
                "reply_max_total_bytes", config.replyMaxTotalBytes(),
                "reply_chunk_payload_bytes", config.replyChunkPayloadBytes(),
                "reply_control_reservation_bytes", config.replyControlReservationBytes(),
                "reply_drain_timeout_millis", config.replyDrainTimeoutMillis());

        add(fields, Group.OUTBOUND, MAPS_AND_TEXT,
                "outbound_reserved_bytes", outbound.reservedBytes(),
                "outbound_allocated_bytes", outbound.allocatedBytes(),
                "outbound_peak_reserved_bytes", outbound.peakReservedBytes(),
                "outbound_peak_allocated_bytes", outbound.peakAllocatedBytes(),
                "outbound_capacity_rejects", outbound.capacityRejectedReservations(),
                "outbound_waiting_connections", outbound.waitingConnections(),
                "outbound_active_connections", outbound.activeConnections(),
                "outbound_active_slots", outbound.activeSlots());
        add(fields, Group.OUTBOUND, MAPS_ONLY,
                "outbound_closed", outbound.closed() ? 1L : 0L);
        add(fields, Group.EGRESS, MAPS_AND_TEXT,
                "outbound_active_chunks", egress.activeChunks(),
                "outbound_active_sources", egress.activeSources(),
                "outbound_oversized_replies", egress.oversizedReplies(),
                "outbound_cancelled_slots", egress.cancelledSlots(),
                "outbound_failed_slots", egress.failedSlots(),
                "outbound_write_failures", egress.writeFailures(),
                "result_unknown_closes", egress.resultUnknownCloses(),
                "reply_shutdown_timeouts", egress.shutdownTimeouts());
        add(fields, Group.LIVE_CHANNELS, MAPS_AND_TEXT, "live_child_channels", snapshot.liveChildChannels());

        add(fields, Group.HEALTH, MAPS_AND_HEALTH_TEXT,
                "lifecycle_state", health.lifecycleState(),
                "ready", health.ready() ? 1L : 0L,
                "writable", health.writable() ? 1L : 0L);
        add(fields, Group.DEGRADED, MAPS_AND_HEALTH_TEXT, "degraded_databases", health.degradedDatabases());
        add(fields, Group.DATABASES, MAPS_AND_HEALTH_TEXT, "databases", health.databases());
        fields.add(Field.failure("first_failure_type", health.firstFailureType(), hasFailure));
        fields.add(Field.failure("first_failure_message", health.firstFailureMessage(), hasFailure));
        add(fields, Group.CONNECTIONS_TOTAL, MAPS_HEALTH_AND_STATS_TEXT,
                "total_connections_received", children.acceptedConnections());
        add(fields, Group.CONNECTIONS_REJECTED, MAPS_HEALTH_AND_STATS_TEXT,
                "rejected_connections", children.rejectedConnections());
        add(fields, Group.MAX_CLIENTS, MAPS_AND_HEALTH_TEXT, "max_clients", config.maxClients());
        add(fields, Group.CLIENTS, EnumSet.of(Surface.HEALTH_TEXT, Surface.CLIENTS_TEXT),
                "connected_clients", children.activeConnections());

        if (connectionStats != null) {
            add(fields, Group.CONNECTION, STATS_ONLY,
                    "conn_pending", connectionStats.pending(),
                    "conn_pending_bytes", connectionStats.pendingBytes(),
                    "conn_autoread_disabled_by_executor", connectionStats.inputDisabledByExecutor() ? 1L : 0L,
                    "conn_closing", connectionStats.closing() ? 1L : 0L,
                    "conn_commands_enqueued", connectionStats.commandsEnqueued(),
                    "conn_commands_executed", connectionStats.commandsExecuted(),
                    "conn_commands_rejected", connectionStats.commandsRejected(),
                    "conn_commands_skipped_closing", connectionStats.commandsSkippedClosing(),
                    "conn_close_after_reply", connectionStats.closeAfterReply(),
                    "conn_backpressure_enter", connectionStats.backpressureEnter(),
                    "conn_backpressure_exit", connectionStats.backpressureExit());
        }
        return fields;
    }

    private static void add(List<Field> fields, Group group, EnumSet<Surface> surfaces, Object... nameValues) {
        if (nameValues.length % 2 != 0) {
            throw new IllegalArgumentException(group + ": name/value list must contain whole pairs");
        }
        for (int i = 0; i < nameValues.length; i += 2) {
            Object name = nameValues[i];
            Object value = nameValues[i + 1];
            if (!(name instanceof String) || !(value instanceof Number || value instanceof String)) {
                throw new IllegalArgumentException(group + ": expected (String name, Number|String value) pairs");
            }
            fields.add(new Field(group, (String) name,
                    value instanceof Number number ? number.longValue() : value, surfaces, false, false));
        }
    }

    /** map 渲染器：按声明顺序输出某渲染面的字段（key 为 bulkString，数值为 integer，字符串为 bulkString）。 */
    private static RedisReply mapReply(List<Field> fields, Surface surface) {
        List<RedisReply> reply = new ArrayList<>(fields.size() * 2);
        for (Field field : fields) {
            if (!field.surfaces().contains(surface)) {
                continue;
            }
            reply.add(RedisReplies.bulkString(ascii(field.name())));
            reply.add(field.value() instanceof String text
                    ? RedisReplies.bulkString(ascii(text))
                    : RedisReplies.integer((Long) field.value()));
        }
        return RedisReplies.map(reply);
    }

    /** INFO 文本渲染器：按组序列输出某文本渲染面的字段，格式为 "prefix+name:value\r\n"。 */
    private static void appendText(
            StringBuilder sb,
            List<Field> fields,
            String prefix,
            Surface surface,
            Group... groups
    ) {
        for (Group group : groups) {
            for (Field field : fields) {
                if (field.group() != group || !field.surfaces().contains(surface) || field.textHidden()) {
                    continue;
                }
                sb.append(prefix).append(field.name()).append(':');
                if (field.value() instanceof String text) {
                    sb.append(field.sanitize() ? sanitizeInfoValue(text) : text);
                } else {
                    sb.append((Long) field.value());
                }
                sb.append("\r\n");
            }
        }
    }

    private String buildRedisInfo(String section, ServerStatsSnapshot snapshot, List<Field> fields) {
        CommandExecutor.StatsSnapshot statsSnapshot = snapshot.executor();
        long uptimeMillis = snapshot.uptimeMillis();
        long uptimeSeconds = Math.max(0, uptimeMillis / 1000L);

        boolean all = section == null || section.isBlank() || "default".equals(section) || "all".equals(section);
        boolean server = all || "server".equals(section);
        boolean clients = all || "clients".equals(section);
        boolean health = all || "health".equals(section);
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

        if (health) {
            sb.append("# Health\r\n");
            appendText(sb, fields, "", Surface.HEALTH_TEXT, HEALTH_TEXT_ORDER);
            sb.append("\r\n");
        }

        if (clients) {
            sb.append("# Clients\r\n");
            appendText(sb, fields, "", Surface.CLIENTS_TEXT, Group.CLIENTS);
            sb.append("blocked_clients:0\r\n");
            sb.append("\r\n");
        }

        if (memory) {
            YierdisMemoryStats memStats = aggregatedMemoryStats();
            long usedMemoryBytes = memStats.heapDataBytesEstimate() + memStats.offHeapUsedBytes();
            sb.append("# Memory\r\n");
            sb.append("used_memory:").append(usedMemoryBytes).append("\r\n");
            sb.append("used_memory_dataset:").append(memStats.heapDataBytesEstimate()).append("\r\n");
            sb.append("used_memory_overhead:0\r\n");
            sb.append("maxmemory:").append(config.maxmemoryBytes()).append("\r\n");
            sb.append("maxmemory_policy:").append(config.maxmemoryPolicy().redisName()).append("\r\n");
            sb.append("yierdis_maxmemory_scope:")
                    .append(config.maxmemoryScope() == YierdisInstanceConfig.MaxmemoryScope.PER_DB
                            ? "per-db"
                            : "global")
                    .append("\r\n");
            if (config.maxmemoryScope() == YierdisInstanceConfig.MaxmemoryScope.PER_DB && config.maxmemoryBytes() > 0) {
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
            sb.append("yierdis_native_metadata_committed_bytes:")
                    .append(memStats.nativeMetadataCommittedBytes()).append("\r\n");
            sb.append("yierdis_native_data_committed_bytes:")
                    .append(memStats.nativeDataCommittedBytes()).append("\r\n");
            sb.append("yierdis_native_data_live_bytes:").append(memStats.nativeDataLiveBytes()).append("\r\n");
            sb.append("yierdis_native_reclaimable_bytes:").append(memStats.nativeReclaimableBytes()).append("\r\n");
            sb.append("yierdis_native_live_objects:").append(memStats.nativeLiveObjects()).append("\r\n");
            sb.append("yierdis_native_live_regions:").append(memStats.nativeLiveRegions()).append("\r\n");
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
            sb.append("total_commands_processed:").append(statsSnapshot.commandsExecuted()).append("\r\n");
            appendText(sb, fields, "", Surface.STATS_TEXT, Group.CONNECTIONS_REJECTED);
            appendText(sb, fields, "", Surface.STATS_TEXT, Group.CONNECTIONS_TOTAL);
            sb.append("instantaneous_ops_per_sec:")
                    .append(instantaneousOpsPerSecond(statsSnapshot, uptimeSeconds)).append("\r\n");
            appendText(sb, fields, "yierdis_", Surface.STATS_TEXT, STATS_TEXT_ORDER);
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
        return YierdisMemoryStats.empty(
                config.maxmemoryBytes(),
                config.maxmemoryScope() == YierdisInstanceConfig.MaxmemoryScope.GLOBAL
        );
    }

    private static ConnectionStatsView connectionStats(CommandSession session) {
        return session == null ? null : session.connectionStats();
    }

    private InboundMemoryBudgetStats inboundStats() {
        InboundMemoryBudget budget = inboundMemoryBudget;
        if (budget != null) {
            return budget.stats();
        }
        return new InboundMemoryBudgetStats(
                config.protocolGlobalInFlightBytes(),
                0L,
                0,
                false,
                0L,
                0L,
                0L,
                0L,
                0L,
                false
        );
    }

    private OutboundMemoryBudgetStats outboundStats() {
        OutboundMemoryBudget budget = outboundMemoryBudget;
        if (budget != null) {
            return budget.stats();
        }
        return new OutboundMemoryBudgetStats(
                config.replyGlobalCapacityBytes(),
                0L,
                0L,
                0L,
                0L,
                0L,
                0,
                0,
                0L,
                false
        );
    }

    private ReplyEgressStats.Snapshot replyEgressStats() {
        return replyEgressStats.snapshot();
    }

    private ChildChannelRegistry.StatsSnapshot childStats() {
        ChildChannelRegistry registry = childChannelRegistry;
        return registry == null
                ? new ChildChannelRegistry.StatsSnapshot(0, 0L, 0L, 0L)
                : registry.statsSnapshot();
    }

    private ServerStatsSnapshot serverStatsSnapshot(CommandExecutor executor) {
        InboundMemoryBudgetStats inbound = inboundStats();
        OutboundMemoryBudgetStats outbound = outboundStats();
        YierdisInstanceObservability runtimeObservability = observability;
        YierdisInstanceObservability.RuntimeHealthSnapshot databaseHealth = runtimeObservability == null
                ? new YierdisInstanceObservability.RuntimeHealthSnapshot(0, 0, null, null, 0L)
                : runtimeObservability.healthSnapshot();
        ChildChannelRegistry.StatsSnapshot children = childStats();
        String state;
        try {
            state = lifecycleState.get();
        } catch (Throwable ignored) {
            state = "FAILED";
        }
        if (state == null || state.isBlank()) {
            state = "UNKNOWN";
        }
        HealthView health = healthView(state, databaseHealth, inbound, outbound);
        return new ServerStatsSnapshot(
                executor.statsSnapshot(),
                Math.max(0L, System.currentTimeMillis() - startedMillis),
                inbound,
                outbound,
                replyEgressStats(),
                children,
                health
        );
    }

    private static HealthView healthView(
            String lifecycleState,
            YierdisInstanceObservability.RuntimeHealthSnapshot databaseHealth,
            InboundMemoryBudgetStats inbound,
            OutboundMemoryBudgetStats outbound
    ) {
        boolean infrastructureHealthy = "RUNNING".equals(lifecycleState) && !inbound.closed() && !outbound.closed();
        boolean ready = infrastructureHealthy && databaseHealth.healthy();
        return new HealthView(
                lifecycleState,
                ready,
                ready,
                databaseHealth.databaseCount(),
                databaseHealth.degradedDatabaseCount(),
                databaseHealth.firstFailureType(),
                databaseHealth.firstFailureMessage()
        );
    }

    private static long instantaneousOpsPerSecond(CommandExecutor.StatsSnapshot stats, long uptimeSeconds) {
        long elapsed = Math.max(1L, uptimeSeconds);
        long commands = Math.max(0L, stats.commandsExecuted());
        return commands / elapsed;
    }

    private static String sanitizeInfoValue(String value) {
        if (value == null) {
            return "";
        }
        return value.replace('\r', ' ').replace('\n', ' ');
    }

    private record HealthView(
            String lifecycleState,
            boolean ready,
            boolean writable,
            int databases,
            int degradedDatabases,
            String firstFailureType,
            String firstFailureMessage
    ) {
    }

    private record ServerStatsSnapshot(
            CommandExecutor.StatsSnapshot executor,
            long uptimeMillis,
            InboundMemoryBudgetStats inbound,
            OutboundMemoryBudgetStats outbound,
            ReplyEgressStats.Snapshot egress,
            ChildChannelRegistry.StatsSnapshot children,
            HealthView health
    ) {
        private int liveChildChannels() {
            return children.activeConnections();
        }
    }

    private static String asciiLower(CommandArgs args, int argIndex) {
        if (args == null || argIndex < 0 || argIndex >= args.argc() || args.isNull(argIndex) || args.length(argIndex) <= 0) {
            return null;
        }
        byte[] raw = args.bytes(argIndex);
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
