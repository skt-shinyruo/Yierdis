package yier.bubu.redis.ops;

import java.util.Locale;

/**
 * Redis-like maxmemory eviction policy.
 */
public enum MaxmemoryPolicy {
    NOEVICTION("noeviction"),
    ALLKEYS_RANDOM("allkeys-random"),
    ALLKEYS_LRU("allkeys-lru");

    private final String redisName;

    MaxmemoryPolicy(String redisName) {
        this.redisName = redisName;
    }

    public String redisName() {
        return redisName;
    }

    /**
     * Parse a Redis-style policy string.
     * <p>
     * Normalization:
     * <ul>
     *     <li>trim</li>
     *     <li>lower-case</li>
     *     <li>replace '_' with '-'</li>
     * </ul>
     *
     * @param s raw policy string (e.g. "allkeys-lru")
     * @return parsed policy
     * @throws IllegalArgumentException if unknown/blank
     */
    public static MaxmemoryPolicy parse(String s) {
        if (s == null) {
            throw new IllegalArgumentException("maxmemory policy is null");
        }
        String normalized = s.trim()
                .toLowerCase(Locale.ROOT)
                .replace('_', '-');
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException("maxmemory policy is blank");
        }

        return switch (normalized) {
            case "noeviction" -> NOEVICTION;
            case "allkeys-random" -> ALLKEYS_RANDOM;
            case "allkeys-lru" -> ALLKEYS_LRU;
            default -> throw new IllegalArgumentException("unknown maxmemory policy: " + s);
        };
    }
}
