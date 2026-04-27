package yier.bubu.redis.db;

final class YierdisDbMaxmemoryPolicies {
    private YierdisDbMaxmemoryPolicies() {
    }

    static YierdisDb.MaxmemoryPolicy parseOrDefault(String policy) {
        if (policy == null || policy.isBlank()) {
            return YierdisDb.MaxmemoryPolicy.NOEVICTION;
        }
        yier.bubu.redis.ops.MaxmemoryPolicy parsed;
        try {
            parsed = yier.bubu.redis.ops.MaxmemoryPolicy.parse(policy);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("unsupported maxmemoryPolicy: " + policy);
        }
        return switch (parsed) {
            case NOEVICTION -> YierdisDb.MaxmemoryPolicy.NOEVICTION;
            case ALLKEYS_RANDOM -> YierdisDb.MaxmemoryPolicy.ALLKEYS_RANDOM;
            case ALLKEYS_LRU -> YierdisDb.MaxmemoryPolicy.ALLKEYS_LRU;
        };
    }
}
