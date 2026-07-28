package yier.bubu.redis.runtime.api;

import yier.bubu.redis.storage.api.DbEngineFactory;
import yier.bubu.redis.storage.api.DbDefragConfig;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;

import java.util.Objects;

public final class YierdisInstanceConfig {
    public enum MaxmemoryScope {
        PER_DB,
        GLOBAL
    }

    public record EngineFactoryBinding(DbEngineFactory engineFactory, AutoCloseable ownedResource) {
        public EngineFactoryBinding {
            Objects.requireNonNull(engineFactory, "engineFactory");
        }

        public EngineFactoryBinding(DbEngineFactory engineFactory) {
            this(engineFactory, null);
        }
    }

    private final int databases;
    private final EngineFactoryBinding engineFactoryBinding;
    private final YierdisChangeSink changeSink;

    private final long maxmemoryBytes;
    private final MaxmemoryScope maxmemoryScope;
    private final MaxmemoryPolicy maxmemoryPolicy;
    private final int maxmemorySamples;
    private final long evictionTimeLimitMillis;
    private final long expireCleanupTimeLimitMillis;
    private final DbDefragConfig defrag;
    private final int commitStreamMaxEvents;
    private final long commitStreamMaxRetainedBytes;
    private final long commitStreamShutdownTimeoutMillis;

    private YierdisInstanceConfig(
            Builder b,
            int databases,
            MaxmemoryScope maxmemoryScope,
            MaxmemoryPolicy maxmemoryPolicy
    ) {
        this.databases = databases;
        this.engineFactoryBinding = b.engineFactoryBinding;
        this.changeSink = b.changeSink == null ? YierdisChangeSink.NOOP : b.changeSink;
        this.maxmemoryBytes = b.maxmemoryBytes;
        this.maxmemoryScope = maxmemoryScope;
        this.maxmemoryPolicy = maxmemoryPolicy;
        this.maxmemorySamples = b.maxmemorySamples;
        this.evictionTimeLimitMillis = b.evictionTimeLimitMillis;
        this.expireCleanupTimeLimitMillis = b.expireCleanupTimeLimitMillis;
        this.defrag = b.defrag;
        this.commitStreamMaxEvents = b.commitStreamMaxEvents;
        this.commitStreamMaxRetainedBytes = b.commitStreamMaxRetainedBytes;
        this.commitStreamShutdownTimeoutMillis = b.commitStreamShutdownTimeoutMillis;
    }

    public static Builder builder() {
        return new Builder();
    }

    public int databases() {
        return databases;
    }

    public DbEngineFactory engineFactory() {
        return engineFactoryBinding == null ? null : engineFactoryBinding.engineFactory();
    }

    public EngineFactoryBinding engineFactoryBinding() {
        return engineFactoryBinding;
    }

    public YierdisChangeSink changeSink() {
        return changeSink;
    }

    public long maxmemoryBytes() {
        return maxmemoryBytes;
    }

    public MaxmemoryScope maxmemoryScope() {
        return maxmemoryScope;
    }

    public MaxmemoryPolicy maxmemoryPolicy() {
        return maxmemoryPolicy;
    }

    public int maxmemorySamples() {
        return maxmemorySamples;
    }

    public long evictionTimeLimitMillis() {
        return evictionTimeLimitMillis;
    }

    public long expireCleanupTimeLimitMillis() {
        return expireCleanupTimeLimitMillis;
    }

    public DbDefragConfig defrag() {
        return defrag;
    }

    public int commitStreamMaxEvents() {
        return commitStreamMaxEvents;
    }

    public long commitStreamMaxRetainedBytes() {
        return commitStreamMaxRetainedBytes;
    }

    public long commitStreamShutdownTimeoutMillis() {
        return commitStreamShutdownTimeoutMillis;
    }

    public static final class Builder {
        private int databases = 1;
        private EngineFactoryBinding engineFactoryBinding;
        private YierdisChangeSink changeSink = YierdisChangeSink.NOOP;

        private long maxmemoryBytes;
        private MaxmemoryScope maxmemoryScope = MaxmemoryScope.PER_DB;
        private MaxmemoryPolicy maxmemoryPolicy = MaxmemoryPolicy.NOEVICTION;
        private int maxmemorySamples = 5;
        private long evictionTimeLimitMillis = 5;
        private long expireCleanupTimeLimitMillis = 5;
        private DbDefragConfig defrag = new DbDefragConfig(false, 64L * 1024L, 64L, 1L);
        private int commitStreamMaxEvents = 8_192;
        private long commitStreamMaxRetainedBytes = 64L * 1024L * 1024L;
        private long commitStreamShutdownTimeoutMillis = 5_000L;

        private Builder() {
        }

        public Builder databases(int databases) {
            this.databases = databases;
            return this;
        }

        public Builder engineFactory(DbEngineFactory engineFactory) {
            this.engineFactoryBinding = engineFactory == null ? null : new EngineFactoryBinding(engineFactory);
            return this;
        }

        public Builder engineFactoryBinding(EngineFactoryBinding engineFactoryBinding) {
            this.engineFactoryBinding = engineFactoryBinding;
            return this;
        }

        public Builder changeSink(YierdisChangeSink changeSink) {
            this.changeSink = changeSink == null ? YierdisChangeSink.NOOP : changeSink;
            return this;
        }

        public Builder maxmemoryBytes(long maxmemoryBytes) {
            this.maxmemoryBytes = maxmemoryBytes;
            return this;
        }

        public Builder maxmemoryScope(MaxmemoryScope scope) {
            this.maxmemoryScope = scope;
            return this;
        }

        public Builder maxmemoryPolicy(MaxmemoryPolicy maxmemoryPolicy) {
            this.maxmemoryPolicy = maxmemoryPolicy;
            return this;
        }

        public Builder maxmemorySamples(int maxmemorySamples) {
            this.maxmemorySamples = maxmemorySamples;
            return this;
        }

        public Builder evictionTimeLimitMillis(long evictionTimeLimitMillis) {
            this.evictionTimeLimitMillis = evictionTimeLimitMillis;
            return this;
        }

        public Builder expireCleanupTimeLimitMillis(long expireCleanupTimeLimitMillis) {
            this.expireCleanupTimeLimitMillis = expireCleanupTimeLimitMillis;
            return this;
        }

        public Builder defrag(DbDefragConfig defrag) {
            this.defrag = Objects.requireNonNull(defrag, "defrag");
            return this;
        }

        public Builder commitStreamMaxEvents(int commitStreamMaxEvents) {
            this.commitStreamMaxEvents = commitStreamMaxEvents;
            return this;
        }

        public Builder commitStreamMaxRetainedBytes(long commitStreamMaxRetainedBytes) {
            this.commitStreamMaxRetainedBytes = commitStreamMaxRetainedBytes;
            return this;
        }

        public Builder commitStreamShutdownTimeoutMillis(long commitStreamShutdownTimeoutMillis) {
            this.commitStreamShutdownTimeoutMillis = commitStreamShutdownTimeoutMillis;
            return this;
        }

        public YierdisInstanceConfig build() {
            int dbs = Math.max(1, databases);
            if (maxmemoryBytes < 0) {
                throw new IllegalArgumentException("maxmemoryBytes must be >= 0");
            }
            if (maxmemorySamples <= 0) {
                throw new IllegalArgumentException("maxmemorySamples must be > 0");
            }
            if (evictionTimeLimitMillis <= 0) {
                throw new IllegalArgumentException("evictionTimeLimitMillis must be > 0");
            }
            if (expireCleanupTimeLimitMillis <= 0) {
                throw new IllegalArgumentException("expireCleanupTimeLimitMillis must be > 0");
            }
            if (commitStreamMaxEvents <= 0) {
                throw new IllegalArgumentException("commitStreamMaxEvents must be > 0");
            }
            if (commitStreamMaxRetainedBytes <= 0L) {
                throw new IllegalArgumentException("commitStreamMaxRetainedBytes must be > 0");
            }
            if (commitStreamShutdownTimeoutMillis <= 0L) {
                throw new IllegalArgumentException("commitStreamShutdownTimeoutMillis must be > 0");
            }
            MaxmemoryScope scope = maxmemoryScope == null ? MaxmemoryScope.PER_DB : maxmemoryScope;
            MaxmemoryPolicy policy = maxmemoryPolicy == null ? MaxmemoryPolicy.NOEVICTION : maxmemoryPolicy;
            return new YierdisInstanceConfig(this, dbs, scope, policy);
        }
    }
}
