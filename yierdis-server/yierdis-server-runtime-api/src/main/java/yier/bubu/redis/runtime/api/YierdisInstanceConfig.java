package yier.bubu.redis.runtime.api;

import yier.bubu.redis.storage.api.DbEngineFactory;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;

public final class YierdisInstanceConfig {
    public enum MaxmemoryScope {
        PER_DB,
        GLOBAL
    }

    private final int databases;
    private final DbEngineFactory engineFactory;
    private final AutoCloseable engineFactoryOwnedResource;
    private final YierdisChangeSink changeSink;

    private final long maxmemoryBytes;
    private final MaxmemoryScope maxmemoryScope;
    private final MaxmemoryPolicy maxmemoryPolicy;
    private final int maxmemorySamples;
    private final long evictionTimeLimitMillis;
    private final long expireCleanupTimeLimitMillis;
    private final boolean nativeDefragEnabled;
    private final long nativeDefragMaxMoveBytes;
    private final long nativeDefragMaxObjects;
    private final long nativeDefragTimeLimitMillis;

    private YierdisInstanceConfig(Builder b) {
        this.databases = b.databases;
        this.engineFactory = b.engineFactory;
        this.engineFactoryOwnedResource = b.engineFactoryOwnedResource;
        this.changeSink = b.changeSink == null ? YierdisChangeSink.NOOP : b.changeSink;
        this.maxmemoryBytes = b.maxmemoryBytes;
        this.maxmemoryScope = b.maxmemoryScope;
        this.maxmemoryPolicy = b.maxmemoryPolicy;
        this.maxmemorySamples = b.maxmemorySamples;
        this.evictionTimeLimitMillis = b.evictionTimeLimitMillis;
        this.expireCleanupTimeLimitMillis = b.expireCleanupTimeLimitMillis;
        this.nativeDefragEnabled = b.nativeDefragEnabled;
        this.nativeDefragMaxMoveBytes = b.nativeDefragMaxMoveBytes;
        this.nativeDefragMaxObjects = b.nativeDefragMaxObjects;
        this.nativeDefragTimeLimitMillis = b.nativeDefragTimeLimitMillis;
    }

    public static Builder builder() {
        return new Builder();
    }

    public int databases() {
        return databases;
    }

    public DbEngineFactory engineFactory() {
        return engineFactory;
    }

    public AutoCloseable engineFactoryOwnedResource() {
        return engineFactoryOwnedResource;
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

    public boolean nativeDefragEnabled() {
        return nativeDefragEnabled;
    }

    public long nativeDefragMaxMoveBytes() {
        return nativeDefragMaxMoveBytes;
    }

    public long nativeDefragMaxObjects() {
        return nativeDefragMaxObjects;
    }

    public long nativeDefragTimeLimitMillis() {
        return nativeDefragTimeLimitMillis;
    }

    public static final class Builder {
        private int databases = 1;
        private DbEngineFactory engineFactory;
        private AutoCloseable engineFactoryOwnedResource;
        private YierdisChangeSink changeSink = YierdisChangeSink.NOOP;

        private long maxmemoryBytes;
        private MaxmemoryScope maxmemoryScope = MaxmemoryScope.PER_DB;
        private MaxmemoryPolicy maxmemoryPolicy = MaxmemoryPolicy.NOEVICTION;
        private int maxmemorySamples = 5;
        private long evictionTimeLimitMillis = 5;
        private long expireCleanupTimeLimitMillis = 5;
        private boolean nativeDefragEnabled;
        private long nativeDefragMaxMoveBytes = 64L * 1024L;
        private long nativeDefragMaxObjects = 64L;
        private long nativeDefragTimeLimitMillis = 1L;

        private Builder() {
        }

        public Builder databases(int databases) {
            this.databases = databases;
            return this;
        }

        @Deprecated
        public Builder offHeapAllocator(Object ignored) {
            return this;
        }

        @Deprecated
        public Builder ownsOffHeapAllocator(boolean ignored) {
            return this;
        }

        @Deprecated
        public Builder offHeapKeysEnabled(boolean ignored) {
            return this;
        }

        public Builder engineFactory(DbEngineFactory engineFactory) {
            this.engineFactory = engineFactory;
            return this;
        }

        public Builder engineFactoryOwnedResource(AutoCloseable engineFactoryOwnedResource) {
            this.engineFactoryOwnedResource = engineFactoryOwnedResource;
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

        @Deprecated
        public Builder maxmemoryPolicy(String rawPolicy) {
            if (rawPolicy == null || rawPolicy.isBlank()) {
                this.maxmemoryPolicy = MaxmemoryPolicy.NOEVICTION;
            } else {
                this.maxmemoryPolicy = MaxmemoryPolicy.parse(rawPolicy);
            }
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

        public Builder nativeDefragEnabled(boolean nativeDefragEnabled) {
            this.nativeDefragEnabled = nativeDefragEnabled;
            return this;
        }

        public Builder nativeDefragMaxMoveBytes(long nativeDefragMaxMoveBytes) {
            this.nativeDefragMaxMoveBytes = nativeDefragMaxMoveBytes;
            return this;
        }

        public Builder nativeDefragMaxObjects(long nativeDefragMaxObjects) {
            this.nativeDefragMaxObjects = nativeDefragMaxObjects;
            return this;
        }

        public Builder nativeDefragTimeLimitMillis(long nativeDefragTimeLimitMillis) {
            this.nativeDefragTimeLimitMillis = nativeDefragTimeLimitMillis;
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
            if (nativeDefragMaxMoveBytes < 0) {
                throw new IllegalArgumentException("nativeDefragMaxMoveBytes must be >= 0");
            }
            if (nativeDefragMaxObjects < 0) {
                throw new IllegalArgumentException("nativeDefragMaxObjects must be >= 0");
            }
            if (nativeDefragTimeLimitMillis < 0) {
                throw new IllegalArgumentException("nativeDefragTimeLimitMillis must be >= 0");
            }
            MaxmemoryScope scope = maxmemoryScope == null ? MaxmemoryScope.PER_DB : maxmemoryScope;
            MaxmemoryPolicy policy = maxmemoryPolicy == null ? MaxmemoryPolicy.NOEVICTION : maxmemoryPolicy;

            Builder normalized = this;
            normalized.databases = dbs;
            normalized.maxmemoryScope = scope;
            normalized.maxmemoryPolicy = policy;
            return new YierdisInstanceConfig(normalized);
        }
    }
}
