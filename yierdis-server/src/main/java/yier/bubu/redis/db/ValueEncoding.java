package yier.bubu.redis.db;

/**
 * Redis-style internal encodings per logical {@link ValueType}.
 * <p>
 * This is intentionally small and only covers the encodings implemented by this project.
 */
public enum ValueEncoding {
    // STRING
    STRING_INT,
    STRING_EMBSTR,
    STRING_RAW,

    // HASH
    HASH_PACKED,
    HASH_HT,

    // LIST
    LIST_PACKED,
    LIST_QUICKLIST,

    // SET
    SET_INTSET,
    SET_HT,

    // ZSET
    ZSET_PACKED,
    ZSET_SKIPLIST
}
