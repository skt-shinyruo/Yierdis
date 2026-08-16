package yier.bubu.redis.runtime.api;

import yier.bubu.redis.storage.api.DbDefragConfig;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;

import java.util.Objects;

public final class YierdisInstanceConfig {
    public enum MaxmemoryScope {
        PER_DB,
        GLOBAL
    }

    private final int databases;
    private final int nativeSlotCapacity;

    private final long maxmemoryBytes;
    private final MaxmemoryScope maxmemoryScope;
    private final MaxmemoryPolicy maxmemoryPolicy;
    private final int maxmemorySamples;
    private final long evictionTimeLimitMillis;
    private final long expireCleanupTimeLimitMillis;
    private final DbDefragConfig defrag;

    private YierdisInstanceConfig(
            Builder b,
            int databases,
            MaxmemoryScope maxmemoryScope,
            MaxmemoryPolicy maxmemoryPolicy
    ) {
        this.databases = databases;
        this.nativeSlotCapacity = b.nativeSlotCapacity;
        this.maxmemoryBytes = b.maxmemoryBytes;
        this.maxmemoryScope = maxmemoryScope;
        this.maxmemoryPolicy = maxmemoryPolicy;
        this.maxmemorySamples = b.maxmemorySamples;
        this.evictionTimeLimitMillis = b.evictionTimeLimitMillis;
        this.expireCleanupTimeLimitMillis = b.expireCleanupTimeLimitMillis;
        this.defrag = b.defrag;
    }

    public static Builder builder() {
        return new Builder();
    }

    public int databases() {
        return databases;
    }

    public int nativeSlotCapacity() {
        return nativeSlotCapacity;
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

    public static final class Builder {
        private int databases = 1;
        private int nativeSlotCapacity;

        private long maxmemoryBytes;
        private MaxmemoryScope maxmemoryScope = MaxmemoryScope.PER_DB;
        private MaxmemoryPolicy maxmemoryPolicy = MaxmemoryPolicy.NOEVICTION;
        private int maxmemorySamples = 5;
        private long evictionTimeLimitMillis = 5;
        private long expireCleanupTimeLimitMillis = 5;
        private DbDefragConfig defrag = new DbDefragConfig(false, 64L * 1024L, 64L, 1L);

        private Builder() {
        }

        public Builder databases(int databases) {
            this.databases = databases;
            return this;
        }

        public Builder nativeSlotCapacity(int nativeSlotCapacity) {
            this.nativeSlotCapacity = nativeSlotCapacity;
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

        public YierdisInstanceConfig build() {
            int dbs = Math.max(1, databases);
            if (maxmemoryBytes < 0) {
                throw new IllegalArgumentException("maxmemoryBytes must be >= 0");
            }
            if (nativeSlotCapacity < 0) {
                throw new IllegalArgumentException("nativeSlotCapacity must be >= 0");
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
            MaxmemoryScope scope = maxmemoryScope == null ? MaxmemoryScope.PER_DB : maxmemoryScope;
            MaxmemoryPolicy policy = maxmemoryPolicy == null ? MaxmemoryPolicy.NOEVICTION : maxmemoryPolicy;
            return new YierdisInstanceConfig(this, dbs, scope, policy);
        }
    }
}
