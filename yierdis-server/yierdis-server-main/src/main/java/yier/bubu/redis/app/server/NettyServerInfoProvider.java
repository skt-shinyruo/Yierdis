package yier.bubu.redis.app.server;

// INFO/STATS 提供器：基于 transport-neutral executor 统计与连接态输出可观测性摘要，避免在热路径做额外分配。

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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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
    private static final byte[] KEY_INBOUND_CAPACITY_BYTES = ascii("inbound_capacity_bytes");
    private static final byte[] KEY_INBOUND_RESERVED_BYTES = ascii("inbound_reserved_bytes");
    private static final byte[] KEY_INBOUND_PEAK_RESERVED_BYTES = ascii("inbound_peak_reserved_bytes");
    private static final byte[] KEY_INBOUND_WAITING_CONNECTIONS = ascii("inbound_waiting_connections");
    private static final byte[] KEY_INBOUND_BACKPRESSURED = ascii("inbound_backpressured");
    private static final byte[] KEY_INBOUND_REJECTED_CONNECTIONS = ascii("inbound_rejected_connections");
    private static final byte[] KEY_INBOUND_CLOSED = ascii("inbound_closed");
    private static final byte[] KEY_REPLY_GLOBAL_CAPACITY_BYTES = ascii("reply_global_capacity_bytes");
    private static final byte[] KEY_REPLY_PER_CONNECTION_CAPACITY_BYTES = ascii("reply_per_connection_capacity_bytes");
    private static final byte[] KEY_REPLY_MAX_TOTAL_BYTES = ascii("reply_max_total_bytes");
    private static final byte[] KEY_REPLY_CHUNK_PAYLOAD_BYTES = ascii("reply_chunk_payload_bytes");
    private static final byte[] KEY_REPLY_CONTROL_RESERVATION_BYTES = ascii("reply_control_reservation_bytes");
    private static final byte[] KEY_REPLY_DRAIN_TIMEOUT_MILLIS = ascii("reply_drain_timeout_millis");
    private static final byte[] KEY_OUTBOUND_RESERVED_BYTES = ascii("outbound_reserved_bytes");
    private static final byte[] KEY_OUTBOUND_ALLOCATED_BYTES = ascii("outbound_allocated_bytes");
    private static final byte[] KEY_OUTBOUND_PEAK_RESERVED_BYTES = ascii("outbound_peak_reserved_bytes");
    private static final byte[] KEY_OUTBOUND_PEAK_ALLOCATED_BYTES = ascii("outbound_peak_allocated_bytes");
    private static final byte[] KEY_OUTBOUND_CAPACITY_REJECTS = ascii("outbound_capacity_rejects");
    private static final byte[] KEY_OUTBOUND_WAITING_CONNECTIONS = ascii("outbound_waiting_connections");
    private static final byte[] KEY_OUTBOUND_ACTIVE_CONNECTIONS = ascii("outbound_active_connections");
    private static final byte[] KEY_OUTBOUND_ACTIVE_SLOTS = ascii("outbound_active_slots");
    private static final byte[] KEY_OUTBOUND_CLOSED = ascii("outbound_closed");
    private static final byte[] KEY_OUTBOUND_ACTIVE_CHUNKS = ascii("outbound_active_chunks");
    private static final byte[] KEY_OUTBOUND_ACTIVE_SOURCES = ascii("outbound_active_sources");
    private static final byte[] KEY_OUTBOUND_OVERSIZED_REPLIES = ascii("outbound_oversized_replies");
    private static final byte[] KEY_OUTBOUND_CANCELLED_SLOTS = ascii("outbound_cancelled_slots");
    private static final byte[] KEY_OUTBOUND_FAILED_SLOTS = ascii("outbound_failed_slots");
    private static final byte[] KEY_OUTBOUND_WRITE_FAILURES = ascii("outbound_write_failures");
    private static final byte[] KEY_RESULT_UNKNOWN_CLOSES = ascii("result_unknown_closes");
    private static final byte[] KEY_REPLY_SHUTDOWN_TIMEOUTS = ascii("reply_shutdown_timeouts");
    private static final byte[] KEY_LIVE_CHILD_CHANNELS = ascii("live_child_channels");
    private static final byte[] KEY_LIFECYCLE_STATE = ascii("lifecycle_state");
    private static final byte[] KEY_READY = ascii("ready");
    private static final byte[] KEY_WRITABLE = ascii("writable");
    private static final byte[] KEY_DEGRADED_DATABASES = ascii("degraded_databases");
    private static final byte[] KEY_DATABASES = ascii("databases");
    private static final byte[] KEY_TOTAL_CONNECTIONS_RECEIVED = ascii("total_connections_received");
    private static final byte[] KEY_REJECTED_CONNECTIONS = ascii("rejected_connections");
    private static final byte[] KEY_MAX_CLIENTS = ascii("max_clients");
    private static final byte[] KEY_FIRST_FAILURE_TYPE = ascii("first_failure_type");
    private static final byte[] KEY_FIRST_FAILURE_MESSAGE = ascii("first_failure_message");

    private static final byte[] KEY_QUEUED_TASKS = ascii("queued_tasks");
    private static final byte[] KEY_QUEUED_BYTES = ascii("queued_bytes");
    private static final byte[] KEY_CHANNELS_AUTOREAD_DISABLED = ascii("channels_autoread_disabled");
    private static final byte[] KEY_SUBMIT_ACCEPTED_TOTAL = ascii("submit_accepted_total");
    private static final byte[] KEY_SUBMIT_REJECTED_NOT_RUNNING_TOTAL = ascii("submit_rejected_not_running_total");
    private static final byte[] KEY_SUBMIT_REJECTED_CLOSING_TOTAL = ascii("submit_rejected_closing_total");
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
    private static final byte[] KEY_DEFERRED_FAIR_REPLY_HEADS = ascii("deferred_fair_reply_heads");
    private static final byte[] KEY_DEFERRED_GLOBAL_REPLY_HEADS = ascii("deferred_global_reply_heads");

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
    private volatile InboundMemoryBudget inboundMemoryBudget;
    private volatile OutboundMemoryBudget outboundMemoryBudget;
    private volatile ChildChannelRegistry childChannelRegistry;
    private volatile ReplyEgressStats replyEgressStats;
    private volatile Supplier<String> lifecycleState = () -> "STARTING";

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
        CommandExecutor<NettyExecutionConnection> ex = executor;
        if (ex == null) {
            return RedisReplies.error("ERR INFO not ready");
        }

        ServerStatsSnapshot snapshot = serverStatsSnapshot(ex);
        String section = args != null && args.argc() == 2 ? asciiLower(args, 1) : null;
        if ("health".equals(section)) {
            return healthReply(snapshot);
        }
        if ("yierdis".equals(section)) {
            return yierdisStructuredInfo(snapshot);
        }

        byte[] response = buildRedisInfo(section, snapshot).getBytes(StandardCharsets.UTF_8);
        return RedisReplies.bulkString(response);
    }

    @Override
    public RedisReply stats(CommandSession session) {
        Objects.requireNonNull(session, "session");
        CommandExecutor<NettyExecutionConnection> ex = executor;
        if (ex == null) {
            return RedisReplies.error("ERR STATS not ready");
        }

        return statsReply(serverStatsSnapshot(ex), connectionStats(session));
    }

    @Override
    public YierdisMemoryStats memoryStats(CommandSession session) {
        if (config.maxmemoryScope() != YierdisServerRuntimeConfig.MaxmemoryScope.GLOBAL) {
            return null;
        }
        return aggregatedMemoryStats();
    }

    private RedisReply statsReply(
            ServerStatsSnapshot snapshot,
            ConnectionStatsView connectionStats
    ) {
        CommandExecutor.StatsSnapshot stats = snapshot.executor();
        List<RedisReply> fields = new ArrayList<>(160);

        addPair(fields, KEY_QUEUED_TASKS, stats.queuedTasks());
        addPair(fields, KEY_QUEUED_BYTES, stats.queuedBytes());
        addPair(fields, KEY_CHANNELS_AUTOREAD_DISABLED, stats.channelsAutoReadDisabled());
        addPair(fields, KEY_SUBMIT_ACCEPTED_TOTAL, stats.submitAccepted());
        addPair(fields, KEY_SUBMIT_REJECTED_NOT_RUNNING_TOTAL, stats.submitRejectedNotRunning());
        addPair(fields, KEY_SUBMIT_REJECTED_CLOSING_TOTAL, stats.submitRejectedClosing());
        addPair(fields, KEY_SUBMIT_REJECTED_QUEUE_FULL_TOTAL, stats.submitRejectedQueueFull());
        addPair(fields, KEY_SUBMIT_REJECTED_BYTES_BUDGET_TOTAL, stats.submitRejectedBytesBudget());
        addPair(fields, KEY_SUBMIT_REJECTED_OFFER_FAILED_TOTAL, stats.submitRejectedOfferFailed());
        addPair(fields, KEY_COMMANDS_EXECUTED_TOTAL, stats.commandsExecuted());
        addPair(fields, KEY_COMMANDS_SKIPPED_CLOSING_TOTAL, stats.commandsSkippedClosing());
        addPair(fields, KEY_CLOSE_AFTER_REPLY_TOTAL, stats.closeAfterReply());
        addPair(fields, KEY_BACKPRESSURE_ENTER_TOTAL, stats.backpressureEnter());
        addPair(fields, KEY_BACKPRESSURE_EXIT_TOTAL, stats.backpressureExit());
        addPair(fields, KEY_DRAIN_LIMITED_MAX_COMMANDS_TOTAL, stats.drainLimitedByMaxCommands());
        addPair(fields, KEY_DRAIN_LIMITED_TIME_BUDGET_TOTAL, stats.drainLimitedByTimeBudget());
        addPair(fields, KEY_DEFERRED_FAIR_REPLY_HEADS, stats.deferredFairReplyHeads());
        addPair(fields, KEY_DEFERRED_GLOBAL_REPLY_HEADS, stats.deferredGlobalReplyHeads());
        addInboundStats(fields, snapshot.inbound());
        addOutboundStats(fields, snapshot.outbound(), snapshot.egress(), snapshot.liveChildChannels());
        addHealthPairs(fields, snapshot.health(), snapshot.children(), config.maxClients());

        if (connectionStats == null) {
            return mapReply(fields);
        }

        addPair(fields, KEY_CONN_PENDING, connectionStats.pending());
        addPair(fields, KEY_CONN_PENDING_BYTES, connectionStats.pendingBytes());
        addPair(fields, KEY_CONN_AUTOREAD_DISABLED, connectionStats.inputDisabledByExecutor() ? 1 : 0);
        addPair(fields, KEY_CONN_CLOSING, connectionStats.closing() ? 1 : 0);
        addPair(fields, KEY_CONN_COMMANDS_ENQUEUED, connectionStats.commandsEnqueued());
        addPair(fields, KEY_CONN_COMMANDS_EXECUTED, connectionStats.commandsExecuted());
        addPair(fields, KEY_CONN_COMMANDS_REJECTED, connectionStats.commandsRejected());
        addPair(fields, KEY_CONN_COMMANDS_SKIPPED_CLOSING, connectionStats.commandsSkippedClosing());
        addPair(fields, KEY_CONN_CLOSE_AFTER_REPLY, connectionStats.closeAfterReply());
        addPair(fields, KEY_CONN_BACKPRESSURE_ENTER, connectionStats.backpressureEnter());
        addPair(fields, KEY_CONN_BACKPRESSURE_EXIT, connectionStats.backpressureExit());
        return mapReply(fields);
    }

    private RedisReply yierdisStructuredInfo(ServerStatsSnapshot snapshot) {
        CommandExecutor.StatsSnapshot stats = snapshot.executor();
        List<RedisReply> fields = new ArrayList<>(136);

        addPair(fields, KEY_SERVER, VALUE_SERVER);
        addPair(fields, KEY_VERSION, VALUE_VERSION);
        addPair(fields, KEY_PORT, config.port());
        addPair(fields, KEY_IO_THREADS, config.ioThreads());
        addPair(fields, KEY_EXECUTOR_POLICY, ascii(String.valueOf(stats.schedulingPolicy())));
        addPair(fields, KEY_EXECUTOR_QUEUE_CAPACITY, config.executorQueueCapacity());
        addPair(fields, KEY_EXECUTOR_QUEUE_MAX_BYTES, config.executorQueueMaxBytes());
        addPair(fields, KEY_BACKPRESSURE_HIGH, config.backpressureHighWatermark());
        addPair(fields, KEY_BACKPRESSURE_LOW, config.backpressureLowWatermark());
        addPair(fields, KEY_BACKPRESSURE_BYTES_HIGH, config.backpressureBytesHighWatermark());
        addPair(fields, KEY_BACKPRESSURE_BYTES_LOW, config.backpressureBytesLowWatermark());
        addPair(fields, KEY_EXECUTOR_MAX_DRAIN, config.executorMaxDrainCommands());
        addPair(fields, KEY_EXECUTOR_DRAIN_MILLIS, config.executorDrainTimeLimitMillis());
        addPair(fields, KEY_STARTED_MILLIS, startedMillis);
        addPair(fields, KEY_UPTIME_MILLIS, snapshot.uptimeMillis());
        addPair(fields, KEY_DEFERRED_FAIR_REPLY_HEADS, stats.deferredFairReplyHeads());
        addPair(fields, KEY_DEFERRED_GLOBAL_REPLY_HEADS, stats.deferredGlobalReplyHeads());
        addInboundStats(fields, snapshot.inbound());
        addOutboundStats(fields, snapshot.outbound(), snapshot.egress(), snapshot.liveChildChannels());
        addHealthPairs(fields, snapshot.health(), snapshot.children(), config.maxClients());
        return mapReply(fields);
    }

    private String buildRedisInfo(String section, ServerStatsSnapshot snapshot) {
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
        ChildChannelRegistry.StatsSnapshot childStats = snapshot.children();
        HealthView healthView = snapshot.health();

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
            appendHealthText(sb, healthView, childStats, config.maxClients());
            sb.append("\r\n");
        }

        if (clients) {
            sb.append("# Clients\r\n");
            sb.append("connected_clients:").append(childStats.activeConnections()).append("\r\n");
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
            InboundMemoryBudgetStats inboundStats = snapshot.inbound();
            OutboundMemoryBudgetStats outboundStats = snapshot.outbound();
            ReplyEgressStats.Snapshot egressStats = snapshot.egress();
            sb.append("# Stats\r\n");
            sb.append("total_commands_processed:").append(statsSnapshot.commandsExecuted()).append("\r\n");
            sb.append("rejected_connections:").append(childStats.rejectedConnections()).append("\r\n");
            sb.append("total_connections_received:").append(childStats.acceptedConnections()).append("\r\n");
            sb.append("instantaneous_ops_per_sec:")
                    .append(instantaneousOpsPerSecond(statsSnapshot, uptimeSeconds)).append("\r\n");
            sb.append("yierdis_queued_tasks:").append(statsSnapshot.queuedTasks()).append("\r\n");
            sb.append("yierdis_queued_bytes:").append(statsSnapshot.queuedBytes()).append("\r\n");
            sb.append("yierdis_inbound_capacity_bytes:").append(inboundStats.capacityBytes()).append("\r\n");
            sb.append("yierdis_inbound_reserved_bytes:").append(inboundStats.reservedBytes()).append("\r\n");
            sb.append("yierdis_inbound_peak_reserved_bytes:").append(inboundStats.peakReservedBytes()).append("\r\n");
            sb.append("yierdis_inbound_waiting_connections:").append(inboundStats.waitingConnections()).append("\r\n");
            sb.append("yierdis_inbound_backpressured:").append(inboundStats.backpressured() ? 1 : 0).append("\r\n");
            sb.append("yierdis_inbound_rejected_connections:").append(inboundStats.rejectedConnections()).append("\r\n");
            sb.append("yierdis_inbound_closed:").append(inboundStats.closed() ? 1 : 0).append("\r\n");
            sb.append("yierdis_reply_global_capacity_bytes:").append(config.replyGlobalCapacityBytes()).append("\r\n");
            sb.append("yierdis_reply_per_connection_capacity_bytes:")
                    .append(config.replyPerConnectionCapacityBytes()).append("\r\n");
            sb.append("yierdis_reply_max_total_bytes:").append(config.replyMaxTotalBytes()).append("\r\n");
            sb.append("yierdis_reply_chunk_payload_bytes:").append(config.replyChunkPayloadBytes()).append("\r\n");
            sb.append("yierdis_reply_control_reservation_bytes:")
                    .append(config.replyControlReservationBytes()).append("\r\n");
            sb.append("yierdis_reply_drain_timeout_millis:").append(config.replyDrainTimeoutMillis()).append("\r\n");
            sb.append("yierdis_outbound_reserved_bytes:").append(outboundStats.reservedBytes()).append("\r\n");
            sb.append("yierdis_outbound_allocated_bytes:").append(outboundStats.allocatedBytes()).append("\r\n");
            sb.append("yierdis_outbound_peak_reserved_bytes:").append(outboundStats.peakReservedBytes()).append("\r\n");
            sb.append("yierdis_outbound_peak_allocated_bytes:").append(outboundStats.peakAllocatedBytes()).append("\r\n");
            sb.append("yierdis_outbound_capacity_rejects:").append(outboundStats.capacityRejects()).append("\r\n");
            sb.append("yierdis_outbound_waiting_connections:").append(outboundStats.waitingConnections()).append("\r\n");
            sb.append("yierdis_outbound_active_connections:").append(outboundStats.activeConnections()).append("\r\n");
            sb.append("yierdis_outbound_active_slots:").append(outboundStats.activeSlots()).append("\r\n");
            sb.append("yierdis_outbound_active_chunks:").append(egressStats.activeChunks()).append("\r\n");
            sb.append("yierdis_outbound_active_sources:").append(egressStats.activeSources()).append("\r\n");
            sb.append("yierdis_outbound_oversized_replies:").append(egressStats.oversizedReplies()).append("\r\n");
            sb.append("yierdis_outbound_cancelled_slots:").append(egressStats.cancelledSlots()).append("\r\n");
            sb.append("yierdis_outbound_failed_slots:").append(egressStats.failedSlots()).append("\r\n");
            sb.append("yierdis_outbound_write_failures:").append(egressStats.writeFailures()).append("\r\n");
            sb.append("yierdis_result_unknown_closes:").append(egressStats.resultUnknownCloses()).append("\r\n");
            sb.append("yierdis_reply_shutdown_timeouts:").append(egressStats.shutdownTimeouts()).append("\r\n");
            sb.append("yierdis_live_child_channels:").append(snapshot.liveChildChannels()).append("\r\n");
            sb.append("yierdis_deferred_fair_reply_heads:")
                    .append(statsSnapshot.deferredFairReplyHeads()).append("\r\n");
            sb.append("yierdis_deferred_global_reply_heads:")
                    .append(statsSnapshot.deferredGlobalReplyHeads()).append("\r\n");
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
                config.maxmemoryScope() == YierdisServerRuntimeConfig.MaxmemoryScope.GLOBAL
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
        ReplyEgressStats stats = replyEgressStats;
        return stats == null ? ReplyEgressStats.noop().snapshot() : stats.snapshot();
    }

    private ChildChannelRegistry.StatsSnapshot childStats() {
        ChildChannelRegistry registry = childChannelRegistry;
        return registry == null
                ? new ChildChannelRegistry.StatsSnapshot(0, 0L, 0L, 0L)
                : registry.statsSnapshot();
    }

    private ServerStatsSnapshot serverStatsSnapshot(CommandExecutor<NettyExecutionConnection> executor) {
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

    private RedisReply healthReply(ServerStatsSnapshot snapshot) {
        List<RedisReply> fields = new ArrayList<>(22);
        addHealthPairs(fields, snapshot.health(), snapshot.children(), config.maxClients());
        return mapReply(fields);
    }

    private static void addHealthPairs(
            List<RedisReply> fields,
            HealthView health,
            ChildChannelRegistry.StatsSnapshot child,
            int maxClients
    ) {
        addPair(fields, KEY_LIFECYCLE_STATE, ascii(health.lifecycleState()));
        addPair(fields, KEY_READY, health.ready ? 1L : 0L);
        addPair(fields, KEY_WRITABLE, health.writable ? 1L : 0L);
        addPair(fields, KEY_DEGRADED_DATABASES, health.degradedDatabases);
        addPair(fields, KEY_DATABASES, health.databases);
        addPair(fields, KEY_FIRST_FAILURE_TYPE, ascii(health.firstFailureType));
        addPair(fields, KEY_FIRST_FAILURE_MESSAGE, ascii(health.firstFailureMessage));
        addPair(fields, KEY_TOTAL_CONNECTIONS_RECEIVED, child.acceptedConnections());
        addPair(fields, KEY_REJECTED_CONNECTIONS, child.rejectedConnections());
        addPair(fields, KEY_MAX_CLIENTS, maxClients);
    }

    private static void appendHealthText(
            StringBuilder sb,
            HealthView health,
            ChildChannelRegistry.StatsSnapshot child,
            int maxClients
    ) {
        sb.append("lifecycle_state:").append(health.lifecycleState).append("\r\n");
        sb.append("ready:").append(health.ready ? 1 : 0).append("\r\n");
        sb.append("writable:").append(health.writable ? 1 : 0).append("\r\n");
        sb.append("databases:").append(health.databases).append("\r\n");
        sb.append("degraded_databases:").append(health.degradedDatabases).append("\r\n");
        sb.append("connected_clients:").append(child.activeConnections()).append("\r\n");
        sb.append("total_connections_received:").append(child.acceptedConnections()).append("\r\n");
        sb.append("rejected_connections:").append(child.rejectedConnections()).append("\r\n");
        sb.append("max_clients:").append(maxClients).append("\r\n");
        if (health.firstFailureType != null && !health.firstFailureType.isBlank()) {
            sb.append("first_failure_type:").append(sanitizeInfoValue(health.firstFailureType)).append("\r\n");
            sb.append("first_failure_message:").append(sanitizeInfoValue(health.firstFailureMessage)).append("\r\n");
        }
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

    private void addOutboundStats(
            List<RedisReply> fields,
            OutboundMemoryBudgetStats outboundStats,
            ReplyEgressStats.Snapshot egressStats,
            int liveChildChannels
    ) {
        addPair(fields, KEY_REPLY_GLOBAL_CAPACITY_BYTES, config.replyGlobalCapacityBytes());
        addPair(fields, KEY_REPLY_PER_CONNECTION_CAPACITY_BYTES, config.replyPerConnectionCapacityBytes());
        addPair(fields, KEY_REPLY_MAX_TOTAL_BYTES, config.replyMaxTotalBytes());
        addPair(fields, KEY_REPLY_CHUNK_PAYLOAD_BYTES, config.replyChunkPayloadBytes());
        addPair(fields, KEY_REPLY_CONTROL_RESERVATION_BYTES, config.replyControlReservationBytes());
        addPair(fields, KEY_REPLY_DRAIN_TIMEOUT_MILLIS, config.replyDrainTimeoutMillis());
        addPair(fields, KEY_OUTBOUND_RESERVED_BYTES, outboundStats.reservedBytes());
        addPair(fields, KEY_OUTBOUND_ALLOCATED_BYTES, outboundStats.allocatedBytes());
        addPair(fields, KEY_OUTBOUND_PEAK_RESERVED_BYTES, outboundStats.peakReservedBytes());
        addPair(fields, KEY_OUTBOUND_PEAK_ALLOCATED_BYTES, outboundStats.peakAllocatedBytes());
        addPair(fields, KEY_OUTBOUND_CAPACITY_REJECTS, outboundStats.capacityRejects());
        addPair(fields, KEY_OUTBOUND_WAITING_CONNECTIONS, outboundStats.waitingConnections());
        addPair(fields, KEY_OUTBOUND_ACTIVE_CONNECTIONS, outboundStats.activeConnections());
        addPair(fields, KEY_OUTBOUND_ACTIVE_SLOTS, outboundStats.activeSlots());
        addPair(fields, KEY_OUTBOUND_CLOSED, outboundStats.closed() ? 1L : 0L);
        addPair(fields, KEY_OUTBOUND_ACTIVE_CHUNKS, egressStats.activeChunks());
        addPair(fields, KEY_OUTBOUND_ACTIVE_SOURCES, egressStats.activeSources());
        addPair(fields, KEY_OUTBOUND_OVERSIZED_REPLIES, egressStats.oversizedReplies());
        addPair(fields, KEY_OUTBOUND_CANCELLED_SLOTS, egressStats.cancelledSlots());
        addPair(fields, KEY_OUTBOUND_FAILED_SLOTS, egressStats.failedSlots());
        addPair(fields, KEY_OUTBOUND_WRITE_FAILURES, egressStats.writeFailures());
        addPair(fields, KEY_RESULT_UNKNOWN_CLOSES, egressStats.resultUnknownCloses());
        addPair(fields, KEY_REPLY_SHUTDOWN_TIMEOUTS, egressStats.shutdownTimeouts());
        addPair(fields, KEY_LIVE_CHILD_CHANNELS, liveChildChannels);
    }

    private static void addInboundStats(List<RedisReply> fields, InboundMemoryBudgetStats stats) {
        addPair(fields, KEY_INBOUND_CAPACITY_BYTES, stats.capacityBytes());
        addPair(fields, KEY_INBOUND_RESERVED_BYTES, stats.reservedBytes());
        addPair(fields, KEY_INBOUND_PEAK_RESERVED_BYTES, stats.peakReservedBytes());
        addPair(fields, KEY_INBOUND_WAITING_CONNECTIONS, stats.waitingConnections());
        addPair(fields, KEY_INBOUND_BACKPRESSURED, stats.backpressured() ? 1L : 0L);
        addPair(fields, KEY_INBOUND_REJECTED_CONNECTIONS, stats.rejectedConnections());
        addPair(fields, KEY_INBOUND_CLOSED, stats.closed() ? 1L : 0L);
    }

    private static void addPair(List<RedisReply> fields, byte[] key, byte[] value) {
        fields.add(RedisReplies.bulkString(key));
        fields.add(RedisReplies.bulkString(value));
    }

    private static void addPair(List<RedisReply> fields, byte[] key, long value) {
        fields.add(RedisReplies.bulkString(key));
        fields.add(RedisReplies.integer(value));
    }

    private static RedisReply mapReply(List<RedisReply> fields) {
        return RedisReplies.map(fields);
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
