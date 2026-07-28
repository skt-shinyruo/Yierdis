package yier.bubu.redis.app.server.args;

import yier.bubu.redis.execution.executor.CommandExecutorConfig;
import yier.bubu.redis.execution.executor.SchedulingPolicy;
import yier.bubu.redis.protocol.resp.RespProtocolLimits;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;

import java.util.Locale;
import java.util.Objects;

public record YierdisServerRuntimeConfig(
        String bind,
        int port,
        int maxClients,
        int databases,
        long cleanupIntervalMillis,
        int ioThreads,
        int executorQueueCapacity,
        long executorQueueMaxBytes,
        ExecutorSchedulingPolicy executorSchedulingPolicy,
        int backpressureHighWatermark,
        int backpressureLowWatermark,
        long backpressureBytesHighWatermark,
        long backpressureBytesLowWatermark,
        int executorMaxDrainCommands,
        long executorDrainTimeLimitMillis,
        int transactionQueueMaxCommands,
        long transactionQueueMaxBytes,
        int protocolMaxBulkBytes,
        int protocolMaxArgs,
        int protocolMaxLineBytes,
        int protocolMaxCommandBytes,
        long clientIdleTimeoutMillis,
        long clientOutputBufferLimitBytes,
        long clientOutputBufferOverLimitMillis,
        long replyGlobalCapacityBytes,
        long replyPerConnectionCapacityBytes,
        long replyMaxTotalBytes,
        int replyChunkPayloadBytes,
        long replyControlReservationBytes,
        long replyDrainTimeoutMillis,
        long maxmemoryBytes,
        MaxmemoryScope maxmemoryScope,
        MaxmemoryPolicy maxmemoryPolicy,
        int maxmemorySamples,
        long evictionTimeLimitMillis,
        long expireCleanupTimeLimitMillis,
        boolean nativeDefragEnabled,
        long nativeDefragMaxMoveBytes,
        long nativeDefragMaxObjects,
        long nativeDefragTimeLimitMillis,
        int nativeSlotCapacity,
        long keysTimeBudgetMillis,
        int keysMaxResults,
        long protocolGlobalInFlightBytes
) {
    public static final int REPLY_FIXED_OVERHEAD_BYTES = 1_024;
    public static final int REPLY_MAX_CONTROL_ERROR_FRAME_BYTES = 515;
    public static final long MIN_REPLY_CONTROL_RESERVATION_BYTES =
            (long) REPLY_FIXED_OVERHEAD_BYTES + REPLY_MAX_CONTROL_ERROR_FRAME_BYTES;

    public YierdisServerRuntimeConfig {
        Objects.requireNonNull(executorSchedulingPolicy, "executorSchedulingPolicy");
        Objects.requireNonNull(maxmemoryScope, "maxmemoryScope");
        Objects.requireNonNull(maxmemoryPolicy, "maxmemoryPolicy");
        if (bind == null || bind.isBlank()) {
            throw new IllegalArgumentException("bind must not be blank");
        }
        requireRange(port, 0, 65_535, "port");
        if (maxClients <= 0) {
            throw new IllegalArgumentException("maxClients must be > 0");
        }
        requireRange(databases, 1, 1_024, "databases");
        requireNonNegative(cleanupIntervalMillis, "cleanupIntervalMillis");
        requirePositive(ioThreads, "ioThreads");
        requireNonNegative(transactionQueueMaxCommands, "transactionQueueMaxCommands");
        requireNonNegative(transactionQueueMaxBytes, "transactionQueueMaxBytes");
        requireRange(protocolMaxBulkBytes, 1, RespProtocolLimits.MAX_BULK_BYTES, "protocolMaxBulkBytes");
        requireRange(protocolMaxArgs, 1, RespProtocolLimits.MAX_ARGS, "protocolMaxArgs");
        requirePositive(protocolMaxLineBytes, "protocolMaxLineBytes");
        requireRange(
                protocolMaxCommandBytes,
                1,
                RespProtocolLimits.MAX_COMMAND_BYTES,
                "protocolMaxCommandBytes"
        );
        requireNonNegative(clientIdleTimeoutMillis, "clientIdleTimeoutMillis");
        requireNonNegative(clientOutputBufferLimitBytes, "clientOutputBufferLimitBytes");
        requireNonNegative(clientOutputBufferOverLimitMillis, "clientOutputBufferOverLimitMillis");
        if (clientOutputBufferLimitBytes > 0L && clientOutputBufferOverLimitMillis <= 0L) {
            throw new IllegalArgumentException(
                    "clientOutputBufferOverLimitMillis must be > 0 when clientOutputBufferLimitBytes is enabled"
            );
        }
        requireNonNegative(maxmemoryBytes, "maxmemoryBytes");
        requirePositive(maxmemorySamples, "maxmemorySamples");
        requirePositive(evictionTimeLimitMillis, "evictionTimeLimitMillis");
        requirePositive(expireCleanupTimeLimitMillis, "expireCleanupTimeLimitMillis");
        if (nativeDefragMaxMoveBytes < 0) {
            throw new IllegalArgumentException("nativeDefragMaxMoveBytes must be >= 0");
        }
        if (nativeDefragMaxObjects < 0) {
            throw new IllegalArgumentException("nativeDefragMaxObjects must be >= 0");
        }
        if (nativeDefragTimeLimitMillis < 0) {
            throw new IllegalArgumentException("nativeDefragTimeLimitMillis must be >= 0");
        }
        if (nativeSlotCapacity < 0) {
            throw new IllegalArgumentException("nativeSlotCapacity must be >= 0");
        }
        if (protocolGlobalInFlightBytes < 0) {
            throw new IllegalArgumentException("protocolGlobalInFlightBytes must be >= 0");
        }
        requireNonNegative(keysTimeBudgetMillis, "keysTimeBudgetMillis");
        requireNonNegative(keysMaxResults, "keysMaxResults");
        if (replyGlobalCapacityBytes <= 0L) {
            throw new IllegalArgumentException("replyGlobalCapacityBytes must be > 0");
        }
        if (replyPerConnectionCapacityBytes <= 0L) {
            throw new IllegalArgumentException("replyPerConnectionCapacityBytes must be > 0");
        }
        if (replyMaxTotalBytes <= 0L) {
            throw new IllegalArgumentException("replyMaxTotalBytes must be > 0");
        }
        if (replyChunkPayloadBytes <= 0) {
            throw new IllegalArgumentException("replyChunkPayloadBytes must be > 0");
        }
        if (replyControlReservationBytes <= 0L) {
            throw new IllegalArgumentException("replyControlReservationBytes must be > 0");
        }
        if (replyControlReservationBytes < MIN_REPLY_CONTROL_RESERVATION_BYTES) {
            throw new IllegalArgumentException(
                    "replyControlReservationBytes must fit reply fixed overhead and the largest scalar error frame"
            );
        }
        if (replyDrainTimeoutMillis <= 0L) {
            throw new IllegalArgumentException("replyDrainTimeoutMillis must be > 0");
        }
        if (replyControlReservationBytes > replyMaxTotalBytes) {
            throw new IllegalArgumentException("replyControlReservationBytes must be <= replyMaxTotalBytes");
        }
        if (replyMaxTotalBytes > replyPerConnectionCapacityBytes) {
            throw new IllegalArgumentException("replyMaxTotalBytes must be <= replyPerConnectionCapacityBytes");
        }
        if (replyPerConnectionCapacityBytes > replyGlobalCapacityBytes) {
            throw new IllegalArgumentException("replyPerConnectionCapacityBytes must be <= replyGlobalCapacityBytes");
        }
        long minimumReplyCharge = saturatedAdd(
                saturatedAdd(replyControlReservationBytes, replyChunkPayloadBytes),
                REPLY_FIXED_OVERHEAD_BYTES
        );
        if (minimumReplyCharge > replyMaxTotalBytes) {
            throw new IllegalArgumentException("reply chunk, control, and fixed overhead must fit replyMaxTotalBytes");
        }
    }

    public CommandExecutorConfig executorConfig() {
        return new CommandExecutorConfig(
                executorQueueCapacity,
                executorQueueMaxBytes,
                backpressureHighWatermark,
                backpressureLowWatermark,
                backpressureBytesHighWatermark,
                backpressureBytesLowWatermark,
                executorMaxDrainCommands,
                executorDrainTimeLimitMillis,
                switch (executorSchedulingPolicy) {
                    case GLOBAL -> SchedulingPolicy.GLOBAL;
                    case FAIR -> SchedulingPolicy.FAIR;
                }
        );
    }

    private static void requirePositive(long value, String name) {
        if (value <= 0L) {
            throw new IllegalArgumentException(name + " must be > 0");
        }
    }

    private static void requireNonNegative(long value, String name) {
        if (value < 0L) {
            throw new IllegalArgumentException(name + " must be >= 0");
        }
    }

    private static void requireRange(long value, long minimum, long maximum, String name) {
        if (value < minimum || value > maximum) {
            throw new IllegalArgumentException(name + " must be in range " + minimum + ".." + maximum);
        }
    }

    private static long saturatedAdd(long left, long right) {
        if (left < 0L || right < 0L || left > Long.MAX_VALUE - right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    public enum ExecutorSchedulingPolicy {
        GLOBAL("global"),
        FAIR("fair");

        private final String argvValue;

        ExecutorSchedulingPolicy(String argvValue) {
            this.argvValue = argvValue;
        }

        public String argvValue() {
            return argvValue;
        }

        public static ExecutorSchedulingPolicy parseCliValue(String rawValue) {
            if (rawValue == null || rawValue.isBlank()) {
                throw new IllegalArgumentException("executorSchedulingPolicy must not be blank");
            }
            String normalized = rawValue.trim().toLowerCase(Locale.ROOT);
            return switch (normalized) {
                case "global" -> GLOBAL;
                case "fair" -> FAIR;
                default -> throw new IllegalArgumentException("unsupported executorSchedulingPolicy: " + rawValue);
            };
        }

        public static ExecutorSchedulingPolicy fromArgvValue(String argvValue) {
            return switch (argvValue) {
                case "global" -> GLOBAL;
                case "fair" -> FAIR;
                default -> throw new IllegalStateException("executorSchedulingPolicy is not normalized: " + argvValue);
            };
        }
    }

    public enum MaxmemoryScope {
        GLOBAL("global"),
        PER_DB("per-db");

        private final String argvValue;

        MaxmemoryScope(String argvValue) {
            this.argvValue = argvValue;
        }

        public String argvValue() {
            return argvValue;
        }

        public static MaxmemoryScope parseCliValue(String rawValue) {
            if (rawValue == null || rawValue.isBlank()) {
                throw new IllegalArgumentException("maxmemoryScope must not be blank");
            }
            String normalized = rawValue.trim().toLowerCase(Locale.ROOT).replace('_', '-');
            if ("perdb".equals(normalized)) {
                normalized = "per-db";
            }
            return switch (normalized) {
                case "global" -> GLOBAL;
                case "per-db" -> PER_DB;
                default -> throw new IllegalArgumentException("unsupported maxmemoryScope: " + rawValue);
            };
        }

        public static MaxmemoryScope fromArgvValue(String argvValue) {
            return switch (argvValue) {
                case "global" -> GLOBAL;
                case "per-db" -> PER_DB;
                default -> throw new IllegalStateException("maxmemoryScope is not normalized: " + argvValue);
            };
        }
    }

}
