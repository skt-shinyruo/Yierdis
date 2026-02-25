package yier.bubu.redis.runtime;

// YierdisInstanceConfig：定义可嵌入 instance 的装配参数（Netty-free），作为 core runtime 的稳定输入口径。

import yier.bubu.redis.db.offheap.api.YierdisOffHeapAllocator;

import java.util.Locale;

public final class YierdisInstanceConfig {
    public enum MaxmemoryScope {
        PER_DB,
        GLOBAL
    }

    private final int databases;
    private final YierdisOffHeapAllocator offHeapAllocator;
    private final boolean ownsOffHeapAllocator;
    private final boolean offHeapKeysEnabled;

    private final long maxmemoryBytes;
    private final MaxmemoryScope maxmemoryScope;
    private final String maxmemoryPolicy;
    private final int maxmemorySamples;
    private final long evictionTimeLimitMillis;
    private final long expireCleanupTimeLimitMillis;

    private YierdisInstanceConfig(Builder b) {
        this.databases = b.databases;
        this.offHeapAllocator = b.offHeapAllocator;
        this.ownsOffHeapAllocator = b.ownsOffHeapAllocator;
        this.offHeapKeysEnabled = b.offHeapKeysEnabled;
        this.maxmemoryBytes = b.maxmemoryBytes;
        this.maxmemoryScope = b.maxmemoryScope;
        this.maxmemoryPolicy = b.maxmemoryPolicy;
        this.maxmemorySamples = b.maxmemorySamples;
        this.evictionTimeLimitMillis = b.evictionTimeLimitMillis;
        this.expireCleanupTimeLimitMillis = b.expireCleanupTimeLimitMillis;
    }

    public static Builder builder() {
        return new Builder();
    }

    public int databases() {
        return databases;
    }

    public YierdisOffHeapAllocator offHeapAllocator() {
        return offHeapAllocator;
    }

    public boolean ownsOffHeapAllocator() {
        return ownsOffHeapAllocator;
    }

    public boolean offHeapKeysEnabled() {
        return offHeapKeysEnabled;
    }

    public long maxmemoryBytes() {
        return maxmemoryBytes;
    }

    public MaxmemoryScope maxmemoryScope() {
        return maxmemoryScope;
    }

    public String maxmemoryPolicy() {
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

    public static final class Builder {
        private int databases = 1;
        private YierdisOffHeapAllocator offHeapAllocator;
        private boolean ownsOffHeapAllocator;
        private boolean offHeapKeysEnabled;

        private long maxmemoryBytes;
        private MaxmemoryScope maxmemoryScope = MaxmemoryScope.PER_DB;
        private String maxmemoryPolicy = "noeviction";
        private int maxmemorySamples = 5;
        private long evictionTimeLimitMillis = 5;
        private long expireCleanupTimeLimitMillis = 5;

        private Builder() {
        }

        public Builder databases(int databases) {
            this.databases = databases;
            return this;
        }

        public Builder offHeapAllocator(YierdisOffHeapAllocator allocator) {
            this.offHeapAllocator = allocator;
            return this;
        }

        public Builder ownsOffHeapAllocator(boolean ownsOffHeapAllocator) {
            this.ownsOffHeapAllocator = ownsOffHeapAllocator;
            return this;
        }

        public Builder offHeapKeysEnabled(boolean offHeapKeysEnabled) {
            this.offHeapKeysEnabled = offHeapKeysEnabled;
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

        public Builder maxmemoryPolicy(String maxmemoryPolicy) {
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
            MaxmemoryScope scope = maxmemoryScope == null ? MaxmemoryScope.PER_DB : maxmemoryScope;

            String policy = maxmemoryPolicy == null ? "noeviction" : maxmemoryPolicy.trim();
            if (policy.isEmpty()) {
                policy = "noeviction";
            }
            policy = policy.toLowerCase(Locale.ROOT);

            Builder normalized = this;
            normalized.databases = dbs;
            normalized.maxmemoryScope = scope;
            normalized.maxmemoryPolicy = policy;
            return new YierdisInstanceConfig(normalized);
        }
    }
}

