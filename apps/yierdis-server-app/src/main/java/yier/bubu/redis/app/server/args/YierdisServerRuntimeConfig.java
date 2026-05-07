package yier.bubu.redis.app.server.args;

import yier.bubu.redis.storage.api.MaxmemoryPolicy;

import java.util.Locale;
import java.util.Objects;

public record YierdisServerRuntimeConfig(
        int port,
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
        long maxmemoryBytes,
        MaxmemoryScope maxmemoryScope,
        MaxmemoryPolicy maxmemoryPolicy,
        int maxmemorySamples,
        long evictionTimeLimitMillis,
        long expireCleanupTimeLimitMillis,
        long keysTimeBudgetMillis,
        int keysMaxResults
) {
    public YierdisServerRuntimeConfig {
        Objects.requireNonNull(executorSchedulingPolicy, "executorSchedulingPolicy");
        Objects.requireNonNull(maxmemoryScope, "maxmemoryScope");
        Objects.requireNonNull(maxmemoryPolicy, "maxmemoryPolicy");
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
