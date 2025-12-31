package yier.bubu.redis.db;

/**
 * Redis-aligned encoding thresholds for packed vs “large” encodings.
 * <p>
 * This project intentionally keeps these as internal constants (not user-configurable) to remain minimal.
 * The names mirror Redis configuration knobs to make the mapping easy to understand.
 */
final class YierdisEncodingThresholds {
    // Hash (hash-max-listpack-entries / hash-max-listpack-value)
    static final int HASH_MAX_LISTPACK_ENTRIES = 512;
    static final int HASH_MAX_LISTPACK_VALUE_BYTES = 64;

    // ZSet (zset-max-listpack-entries / zset-max-listpack-value)
    static final int ZSET_MAX_LISTPACK_ENTRIES = 128;
    static final int ZSET_MAX_LISTPACK_VALUE_BYTES = 64;

    // Set (set-max-intset-entries)
    static final int SET_MAX_INTSET_ENTRIES = 512;

    // List (list-max-listpack-size = -2 => ~8KB nodes)
    static final int LIST_MAX_LISTPACK_BYTES = 8 * 1024;

    private YierdisEncodingThresholds() {
    }
}

