package yier.bubu.redis;

import yier.bubu.redis.command.ServerInfoProvider;
import yier.bubu.redis.protocol.RespCommand;
import yier.bubu.redis.protocol.RespProtocol;
import yier.bubu.redis.protocol.RespSession;
import yier.bubu.redis.protocol.RespWriter;
import yier.bubu.redis.protocol.netty.ConnectionContext;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Properties;
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
    private static final byte[] VALUE_VERSION = loadVersionBytes();
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

    NettyServerInfoProvider(ServerConfig config) {
        this.config = Objects.requireNonNull(config, "config");
        this.startedMillis = System.currentTimeMillis();
    }

    void bindExecutor(NettyCommandExecutor executor) {
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    @Override
    public void info(RespCommand cmd, RespWriter out) {
        Objects.requireNonNull(out, "out");
        NettyCommandExecutor ex = executor;
        if (ex == null) {
            out.error("ERR INFO not ready");
            return;
        }

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

    @Override
    public void stats(RespCommand cmd, RespWriter out) {
        Objects.requireNonNull(out, "out");
        NettyCommandExecutor ex = executor;
        if (ex == null) {
            out.error("ERR STATS not ready");
            return;
        }

        NettyCommandExecutor.StatsSnapshot s = ex.statsSnapshot();
        ConnectionContext conn = connectionContext(out);

        int pairs = 15 + (conn == null ? 0 : 11);
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

        if (conn == null) {
            return;
        }

        writePair(out, KEY_CONN_PENDING, conn.pendingCounter().get());
        writePair(out, KEY_CONN_PENDING_BYTES, conn.pendingBytesCounter().get());
        writePair(out, KEY_CONN_AUTOREAD_DISABLED, conn.autoReadDisabledByExecutor() ? 1 : 0);
        writePair(out, KEY_CONN_CLOSING, conn.isClosing() ? 1 : 0);
        writePair(out, KEY_CONN_COMMANDS_ENQUEUED, conn.commandsEnqueuedCounter().get());
        writePair(out, KEY_CONN_COMMANDS_EXECUTED, conn.commandsExecutedCounter().get());
        writePair(out, KEY_CONN_COMMANDS_REJECTED, conn.commandsRejectedCounter().get());
        writePair(out, KEY_CONN_COMMANDS_SKIPPED_CLOSING, conn.commandsSkippedClosingCounter().get());
        writePair(out, KEY_CONN_CLOSE_AFTER_REPLY, conn.closeAfterReplyCounter().get());
        writePair(out, KEY_CONN_BACKPRESSURE_ENTER, conn.backpressureEnterCounter().get());
        writePair(out, KEY_CONN_BACKPRESSURE_EXIT, conn.backpressureExitCounter().get());
    }

    private static ConnectionContext connectionContext(RespWriter out) {
        RespSession session = out.session();
        if (session instanceof ConnectionContext ctx) {
            return ctx;
        }
        return null;
    }

    private static void writeHeader(RespWriter out, int pairs) {
        RespProtocol protocol = out.protocol();
        if (protocol == RespProtocol.RESP3) {
            out.mapHeader(pairs);
            return;
        }
        out.arrayHeader(pairs * 2);
    }

    private static void writePair(RespWriter out, byte[] key, byte[] value) {
        out.bulkString(key);
        out.bulkString(value);
    }

    private static void writePair(RespWriter out, byte[] key, long value) {
        out.bulkString(key);
        out.integer(value);
    }

    private static byte[] loadVersionBytes() {
        String version = "unknown";
        try (InputStream in = NettyServerInfoProvider.class.getResourceAsStream("/yierdis-version.properties")) {
            if (in != null) {
                Properties props = new Properties();
                props.load(in);
                String v = props.getProperty("version");
                if (v != null && !v.isBlank()) {
                    version = v.trim();
                }
            }
        } catch (IOException ignored) {
            // ignore
        }
        return version.getBytes(StandardCharsets.US_ASCII);
    }

    private static byte[] ascii(String s) {
        if (s == null || s.isEmpty()) {
            return new byte[0];
        }
        return s.getBytes(StandardCharsets.US_ASCII);
    }
}
