package yier.bubu.redis.app.server;

// INFO/STATS 提供器：基于 transport-neutral executor 统计与连接态输出可观测性摘要，避免在热路径做额外分配。

import yier.bubu.redis.command.api.ServerInfoProvider;
import yier.bubu.redis.storage.api.YierdisMemoryStats;
import yier.bubu.redis.execution.api.CommandExecutionContext;
import yier.bubu.redis.execution.api.CommandPreparationContext;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.ConnectionStatsView;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.execution.api.ReplyShape;
import yier.bubu.redis.execution.api.ReplyShapes;
import yier.bubu.redis.execution.api.ValidationResult;
import yier.bubu.redis.app.server.args.YierdisServerRuntimeConfig;
import yier.bubu.redis.execution.executor.CommandExecutor;
import yier.bubu.redis.protocol.resp.netty.InboundMemoryBudget;
import yier.bubu.redis.protocol.resp.netty.InboundMemoryBudgetStats;
import yier.bubu.redis.runtime.embedded.CommitStreamStats;
import yier.bubu.redis.runtime.embedded.YierdisInstanceObservability;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Consumer;
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
    private static final byte[] KEY_COMMIT_STREAM_STATE = ascii("commit_stream_state");
    private static final byte[] KEY_COMMIT_STREAM_RESERVED_EVENTS = ascii("commit_stream_reserved_events");
    private static final byte[] KEY_COMMIT_STREAM_RESERVED_BYTES = ascii("commit_stream_reserved_bytes");
    private static final byte[] KEY_COMMIT_STREAM_REJECTED_WRITES = ascii("commit_stream_rejected_writes");
    private static final byte[] KEY_COMMIT_STREAM_LAST_ASSIGNED_SEQUENCE = ascii("commit_stream_last_assigned_sequence");
    private static final byte[] KEY_COMMIT_STREAM_LAST_ACKNOWLEDGED_SEQUENCE = ascii("commit_stream_last_acknowledged_sequence");
    private static final byte[] KEY_COMMIT_STREAM_CALLBACK_ACTIVE = ascii("commit_stream_callback_active");
    private static final byte[] KEY_COMMIT_STREAM_SHUTDOWN_TIMED_OUT = ascii("commit_stream_shutdown_timed_out");
    private static final byte[] KEY_COMMIT_STREAM_FIRST_FAILURE_TYPE = ascii("commit_stream_first_failure_type");
    private static final byte[] KEY_COMMIT_STREAM_FIRST_FAILURE_MESSAGE = ascii("commit_stream_first_failure_message");
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
    public PreparedCommand prepareInfo(ExecutionRequest request, CommandPreparationContext context) {
        Objects.requireNonNull(context, "context");
        return maximumReply(execution -> writeInfo(request, execution.session(), execution.reply()));
    }

    private void writeInfo(ExecutionRequest request, CommandSession session, RedisReplyWriter out) {
        Objects.requireNonNull(session, "session");
        Objects.requireNonNull(out, "out");
        CommandExecutor<NettyExecutionConnection> ex = executor;
        if (ex == null) {
            out.error("ERR INFO not ready");
            return;
        }

        ServerStatsSnapshot snapshot = serverStatsSnapshot(ex);
        String section = request != null && request.argc() == 2 ? asciiLower(request, 1) : null;
        if ("health".equals(section)) {
            writeHealth(out, snapshot);
            return;
        }
        if ("yierdis".equals(section)) {
            writeYierdisStructuredInfo(out, snapshot);
            return;
        }

        byte[] response = buildRedisInfo(section, snapshot).getBytes(StandardCharsets.UTF_8);
        out.bulkString(response);
    }

    @Override
    public PreparedCommand prepareStats(ExecutionRequest request, CommandPreparationContext context) {
        Objects.requireNonNull(context, "context");
        return maximumReply(execution -> writeStats(request, execution.session(), execution.reply()));
    }

    private void writeStats(ExecutionRequest request, CommandSession session, RedisReplyWriter out) {
        Objects.requireNonNull(session, "session");
        Objects.requireNonNull(out, "out");
        CommandExecutor<NettyExecutionConnection> ex = executor;
        if (ex == null) {
            out.error("ERR STATS not ready");
            return;
        }

        writeStats(out, serverStatsSnapshot(ex), connectionStats(session));
    }

    @Override
    public YierdisMemoryStats memoryStats(CommandPreparationContext context) {
        if (config.maxmemoryScope() != YierdisServerRuntimeConfig.MaxmemoryScope.GLOBAL) {
            return null;
        }
        return aggregatedMemoryStats();
    }

    private static PreparedCommand maximumReply(Consumer<CommandExecutionContext> execution) {
        Objects.requireNonNull(execution, "execution");
        return new PreparedCommand() {
            @Override
            public ReplyShape replyShape() {
                return ReplyShapes.maximum();
            }

            @Override
            public ValidationResult validateBeforeExecute() {
                return ValidationResult.VALID;
            }

            @Override
            public void execute(CommandExecutionContext context) {
                execution.accept(context);
            }

            @Override
            public void close() {
            }
        };
    }

    private void writeStats(
            RedisReplyWriter out,
            ServerStatsSnapshot snapshot,
            ConnectionStatsView connectionStats
    ) {
        CommandExecutor.StatsSnapshot stats = snapshot.executor();
        int pairs = 69 + (connectionStats == null ? 0 : 11);
        writeHeader(out, pairs);

        writePair(out, KEY_QUEUED_TASKS, stats.queuedTasks());
        writePair(out, KEY_QUEUED_BYTES, stats.queuedBytes());
        writePair(out, KEY_CHANNELS_AUTOREAD_DISABLED, stats.channelsAutoReadDisabled());
        writePair(out, KEY_SUBMIT_ACCEPTED_TOTAL, stats.submitAccepted());
        writePair(out, KEY_SUBMIT_REJECTED_NOT_RUNNING_TOTAL, stats.submitRejectedNotRunning());
        writePair(out, KEY_SUBMIT_REJECTED_CLOSING_TOTAL, stats.submitRejectedClosing());
        writePair(out, KEY_SUBMIT_REJECTED_QUEUE_FULL_TOTAL, stats.submitRejectedQueueFull());
        writePair(out, KEY_SUBMIT_REJECTED_BYTES_BUDGET_TOTAL, stats.submitRejectedBytesBudget());
        writePair(out, KEY_SUBMIT_REJECTED_OFFER_FAILED_TOTAL, stats.submitRejectedOfferFailed());
        writePair(out, KEY_COMMANDS_EXECUTED_TOTAL, stats.commandsExecuted());
        writePair(out, KEY_COMMANDS_SKIPPED_CLOSING_TOTAL, stats.commandsSkippedClosing());
        writePair(out, KEY_CLOSE_AFTER_REPLY_TOTAL, stats.closeAfterReply());
        writePair(out, KEY_BACKPRESSURE_ENTER_TOTAL, stats.backpressureEnter());
        writePair(out, KEY_BACKPRESSURE_EXIT_TOTAL, stats.backpressureExit());
        writePair(out, KEY_DRAIN_LIMITED_MAX_COMMANDS_TOTAL, stats.drainLimitedByMaxCommands());
        writePair(out, KEY_DRAIN_LIMITED_TIME_BUDGET_TOTAL, stats.drainLimitedByTimeBudget());
        writePair(out, KEY_DEFERRED_FAIR_REPLY_HEADS, stats.deferredFairReplyHeads());
        writePair(out, KEY_DEFERRED_GLOBAL_REPLY_HEADS, stats.deferredGlobalReplyHeads());
        writeInboundStats(out, snapshot.inbound());
        writeCommitStreamStats(out, snapshot.commitStream());
        writeOutboundStats(out, snapshot.outbound(), snapshot.egress(), snapshot.liveChildChannels());
        writeHealthPairs(out, snapshot.health(), snapshot.children(), config.maxClients(), false);

        if (connectionStats == null) {
            return;
        }

        writePair(out, KEY_CONN_PENDING, connectionStats.pending());
        writePair(out, KEY_CONN_PENDING_BYTES, connectionStats.pendingBytes());
        writePair(out, KEY_CONN_AUTOREAD_DISABLED, connectionStats.inputDisabledByExecutor() ? 1 : 0);
        writePair(out, KEY_CONN_CLOSING, connectionStats.closing() ? 1 : 0);
        writePair(out, KEY_CONN_COMMANDS_ENQUEUED, connectionStats.commandsEnqueued());
        writePair(out, KEY_CONN_COMMANDS_EXECUTED, connectionStats.commandsExecuted());
        writePair(out, KEY_CONN_COMMANDS_REJECTED, connectionStats.commandsRejected());
        writePair(out, KEY_CONN_COMMANDS_SKIPPED_CLOSING, connectionStats.commandsSkippedClosing());
        writePair(out, KEY_CONN_CLOSE_AFTER_REPLY, connectionStats.closeAfterReply());
        writePair(out, KEY_CONN_BACKPRESSURE_ENTER, connectionStats.backpressureEnter());
        writePair(out, KEY_CONN_BACKPRESSURE_EXIT, connectionStats.backpressureExit());
    }

    private void writeYierdisStructuredInfo(
            RedisReplyWriter out,
            ServerStatsSnapshot snapshot
    ) {
        CommandExecutor.StatsSnapshot stats = snapshot.executor();
        int pairs = 68;
        writeHeader(out, pairs);

        writePair(out, KEY_SERVER, VALUE_SERVER);
        writePair(out, KEY_VERSION, VALUE_VERSION);
        writePair(out, KEY_PORT, config.port());
        writePair(out, KEY_IO_THREADS, config.ioThreads());
        writePair(out, KEY_EXECUTOR_POLICY, ascii(String.valueOf(stats.schedulingPolicy())));
        writePair(out, KEY_EXECUTOR_QUEUE_CAPACITY, config.executorQueueCapacity());
        writePair(out, KEY_EXECUTOR_QUEUE_MAX_BYTES, config.executorQueueMaxBytes());
        writePair(out, KEY_BACKPRESSURE_HIGH, config.backpressureHighWatermark());
        writePair(out, KEY_BACKPRESSURE_LOW, config.backpressureLowWatermark());
        writePair(out, KEY_BACKPRESSURE_BYTES_HIGH, config.backpressureBytesHighWatermark());
        writePair(out, KEY_BACKPRESSURE_BYTES_LOW, config.backpressureBytesLowWatermark());
        writePair(out, KEY_EXECUTOR_MAX_DRAIN, config.executorMaxDrainCommands());
        writePair(out, KEY_EXECUTOR_DRAIN_MILLIS, config.executorDrainTimeLimitMillis());
        writePair(out, KEY_STARTED_MILLIS, startedMillis);
        writePair(out, KEY_UPTIME_MILLIS, snapshot.uptimeMillis());
        writePair(out, KEY_DEFERRED_FAIR_REPLY_HEADS, stats.deferredFairReplyHeads());
        writePair(out, KEY_DEFERRED_GLOBAL_REPLY_HEADS, stats.deferredGlobalReplyHeads());
        writeInboundStats(out, snapshot.inbound());
        writeCommitStreamStats(out, snapshot.commitStream());
        writeOutboundStats(out, snapshot.outbound(), snapshot.egress(), snapshot.liveChildChannels());
        writeHealthPairs(out, snapshot.health(), snapshot.children(), config.maxClients(), false);
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
            sb.append("yierdis_expired_entries_awaiting_physical_deletion:")
                    .append(memStats.expiredEntriesAwaitingPhysicalDeletion()).append("\r\n");
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
            CommitStreamStats streamStats = snapshot.commitStream();
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
            sb.append("yierdis_commit_stream_state:").append(streamStats.state()).append("\r\n");
            sb.append("yierdis_commit_stream_reserved_events:").append(streamStats.reservedEvents()).append("\r\n");
            sb.append("yierdis_commit_stream_reserved_bytes:").append(streamStats.reservedBytes()).append("\r\n");
            sb.append("yierdis_commit_stream_rejected_writes:").append(streamStats.rejectedWrites()).append("\r\n");
            sb.append("yierdis_commit_stream_last_assigned_sequence:").append(streamStats.lastAssignedSequence()).append("\r\n");
            sb.append("yierdis_commit_stream_last_acknowledged_sequence:")
                    .append(streamStats.lastAcknowledgedSequence()).append("\r\n");
            sb.append("yierdis_commit_stream_callback_active:").append(streamStats.callbackActive() ? 1 : 0).append("\r\n");
            sb.append("yierdis_commit_stream_shutdown_timed_out:")
                    .append(streamStats.shutdownTimedOut() ? 1 : 0).append("\r\n");
            sb.append("yierdis_commit_stream_first_failure_type:")
                    .append(streamStats.firstFailureType() == null ? "" : streamStats.firstFailureType()).append("\r\n");
            sb.append("yierdis_commit_stream_first_failure_message:")
                    .append(streamStats.firstFailureMessage() == null ? "" : streamStats.firstFailureMessage()).append("\r\n");
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
        CommitStreamStats commitStream = runtimeObservability == null
                ? CommitStreamStats.disabled()
                : runtimeObservability.commitStreamStats();
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
        HealthView health = healthView(state, databaseHealth, commitStream, inbound, outbound);
        return new ServerStatsSnapshot(
                executor.statsSnapshot(),
                Math.max(0L, System.currentTimeMillis() - startedMillis),
                inbound,
                commitStream,
                outbound,
                replyEgressStats(),
                children,
                health
        );
    }

    private static HealthView healthView(
            String lifecycleState,
            YierdisInstanceObservability.RuntimeHealthSnapshot databaseHealth,
            CommitStreamStats commitStream,
            InboundMemoryBudgetStats inbound,
            OutboundMemoryBudgetStats outbound
    ) {
        boolean commitHealthy = commitStream.state() == yier.bubu.redis.runtime.embedded.CommitStreamState.DISABLED
                || commitStream.state() == yier.bubu.redis.runtime.embedded.CommitStreamState.RUNNING;
        boolean infrastructureHealthy = "RUNNING".equals(lifecycleState) && !inbound.closed() && !outbound.closed();
        boolean ready = infrastructureHealthy && databaseHealth.healthy() && commitHealthy;
        return new HealthView(
                lifecycleState,
                ready,
                ready,
                databaseHealth.databaseCount(),
                databaseHealth.degradedDatabaseCount(),
                commitStream.state().name(),
                databaseHealth.firstFailureType(),
                databaseHealth.firstFailureMessage()
        );
    }

    private void writeHealth(RedisReplyWriter out, ServerStatsSnapshot snapshot) {
        writeHeader(out, 11);
        writeHealthPairs(out, snapshot.health(), snapshot.children(), config.maxClients(), true);
    }

    private static void writeHealthPairs(
            RedisReplyWriter out,
            HealthView health,
            ChildChannelRegistry.StatsSnapshot child,
            int maxClients,
            boolean includeCommitStreamState
    ) {
        writePair(out, KEY_LIFECYCLE_STATE, ascii(health.lifecycleState()));
        writePair(out, KEY_READY, health.ready ? 1L : 0L);
        writePair(out, KEY_WRITABLE, health.writable ? 1L : 0L);
        writePair(out, KEY_DEGRADED_DATABASES, health.degradedDatabases);
        writePair(out, KEY_DATABASES, health.databases);
        if (includeCommitStreamState) {
            writePair(out, KEY_COMMIT_STREAM_STATE, ascii(health.commitStreamState));
        }
        writePair(out, KEY_FIRST_FAILURE_TYPE, ascii(health.firstFailureType));
        writePair(out, KEY_FIRST_FAILURE_MESSAGE, ascii(health.firstFailureMessage));
        writePair(out, KEY_TOTAL_CONNECTIONS_RECEIVED, child.acceptedConnections());
        writePair(out, KEY_REJECTED_CONNECTIONS, child.rejectedConnections());
        writePair(out, KEY_MAX_CLIENTS, maxClients);
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
        sb.append("commit_stream_state:").append(health.commitStreamState).append("\r\n");
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
            String commitStreamState,
            String firstFailureType,
            String firstFailureMessage
    ) {
    }

    private record ServerStatsSnapshot(
            CommandExecutor.StatsSnapshot executor,
            long uptimeMillis,
            InboundMemoryBudgetStats inbound,
            CommitStreamStats commitStream,
            OutboundMemoryBudgetStats outbound,
            ReplyEgressStats.Snapshot egress,
            ChildChannelRegistry.StatsSnapshot children,
            HealthView health
    ) {
        private int liveChildChannels() {
            return children.activeConnections();
        }
    }

    private void writeOutboundStats(
            RedisReplyWriter out,
            OutboundMemoryBudgetStats outboundStats,
            ReplyEgressStats.Snapshot egressStats,
            int liveChildChannels
    ) {
        writePair(out, KEY_REPLY_GLOBAL_CAPACITY_BYTES, config.replyGlobalCapacityBytes());
        writePair(out, KEY_REPLY_PER_CONNECTION_CAPACITY_BYTES, config.replyPerConnectionCapacityBytes());
        writePair(out, KEY_REPLY_MAX_TOTAL_BYTES, config.replyMaxTotalBytes());
        writePair(out, KEY_REPLY_CHUNK_PAYLOAD_BYTES, config.replyChunkPayloadBytes());
        writePair(out, KEY_REPLY_CONTROL_RESERVATION_BYTES, config.replyControlReservationBytes());
        writePair(out, KEY_REPLY_DRAIN_TIMEOUT_MILLIS, config.replyDrainTimeoutMillis());
        writePair(out, KEY_OUTBOUND_RESERVED_BYTES, outboundStats.reservedBytes());
        writePair(out, KEY_OUTBOUND_ALLOCATED_BYTES, outboundStats.allocatedBytes());
        writePair(out, KEY_OUTBOUND_PEAK_RESERVED_BYTES, outboundStats.peakReservedBytes());
        writePair(out, KEY_OUTBOUND_PEAK_ALLOCATED_BYTES, outboundStats.peakAllocatedBytes());
        writePair(out, KEY_OUTBOUND_CAPACITY_REJECTS, outboundStats.capacityRejects());
        writePair(out, KEY_OUTBOUND_WAITING_CONNECTIONS, outboundStats.waitingConnections());
        writePair(out, KEY_OUTBOUND_ACTIVE_CONNECTIONS, outboundStats.activeConnections());
        writePair(out, KEY_OUTBOUND_ACTIVE_SLOTS, outboundStats.activeSlots());
        writePair(out, KEY_OUTBOUND_CLOSED, outboundStats.closed() ? 1L : 0L);
        writePair(out, KEY_OUTBOUND_ACTIVE_CHUNKS, egressStats.activeChunks());
        writePair(out, KEY_OUTBOUND_ACTIVE_SOURCES, egressStats.activeSources());
        writePair(out, KEY_OUTBOUND_OVERSIZED_REPLIES, egressStats.oversizedReplies());
        writePair(out, KEY_OUTBOUND_CANCELLED_SLOTS, egressStats.cancelledSlots());
        writePair(out, KEY_OUTBOUND_FAILED_SLOTS, egressStats.failedSlots());
        writePair(out, KEY_OUTBOUND_WRITE_FAILURES, egressStats.writeFailures());
        writePair(out, KEY_RESULT_UNKNOWN_CLOSES, egressStats.resultUnknownCloses());
        writePair(out, KEY_REPLY_SHUTDOWN_TIMEOUTS, egressStats.shutdownTimeouts());
        writePair(out, KEY_LIVE_CHILD_CHANNELS, liveChildChannels);
    }

    private static void writeInboundStats(RedisReplyWriter out, InboundMemoryBudgetStats stats) {
        writePair(out, KEY_INBOUND_CAPACITY_BYTES, stats.capacityBytes());
        writePair(out, KEY_INBOUND_RESERVED_BYTES, stats.reservedBytes());
        writePair(out, KEY_INBOUND_PEAK_RESERVED_BYTES, stats.peakReservedBytes());
        writePair(out, KEY_INBOUND_WAITING_CONNECTIONS, stats.waitingConnections());
        writePair(out, KEY_INBOUND_BACKPRESSURED, stats.backpressured() ? 1L : 0L);
        writePair(out, KEY_INBOUND_REJECTED_CONNECTIONS, stats.rejectedConnections());
        writePair(out, KEY_INBOUND_CLOSED, stats.closed() ? 1L : 0L);
    }

    private static void writeCommitStreamStats(RedisReplyWriter out, CommitStreamStats stats) {
        writePair(out, KEY_COMMIT_STREAM_STATE, ascii(stats.state().name()));
        writePair(out, KEY_COMMIT_STREAM_RESERVED_EVENTS, stats.reservedEvents());
        writePair(out, KEY_COMMIT_STREAM_RESERVED_BYTES, stats.reservedBytes());
        writePair(out, KEY_COMMIT_STREAM_REJECTED_WRITES, stats.rejectedWrites());
        writePair(out, KEY_COMMIT_STREAM_LAST_ASSIGNED_SEQUENCE, stats.lastAssignedSequence());
        writePair(out, KEY_COMMIT_STREAM_LAST_ACKNOWLEDGED_SEQUENCE, stats.lastAcknowledgedSequence());
        writePair(out, KEY_COMMIT_STREAM_CALLBACK_ACTIVE, stats.callbackActive() ? 1L : 0L);
        writePair(out, KEY_COMMIT_STREAM_SHUTDOWN_TIMED_OUT, stats.shutdownTimedOut() ? 1L : 0L);
        writePair(out, KEY_COMMIT_STREAM_FIRST_FAILURE_TYPE, ascii(stats.firstFailureType()));
        writePair(out, KEY_COMMIT_STREAM_FIRST_FAILURE_MESSAGE, ascii(stats.firstFailureMessage()));
    }

    private static void writeHeader(RedisReplyWriter out, int pairs) {
        try {
            out.mapHeader(pairs);
        } catch (RuntimeException e) {
            // Best-effort compatibility: some reply writers may not support maps.
            out.arrayHeader(pairs * 2);
        }
    }

    private static void writePair(RedisReplyWriter out, byte[] key, byte[] value) {
        out.bulkString(key);
        out.bulkString(value);
    }

    private static void writePair(RedisReplyWriter out, byte[] key, long value) {
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
