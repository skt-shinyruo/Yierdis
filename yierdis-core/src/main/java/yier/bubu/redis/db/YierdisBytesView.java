package yier.bubu.redis.db;

/**
 * A minimal read-only byte view (ptr+len style) intended for request-scoped lookups.
 * <p>
 * Implementations MUST be treated as ephemeral and MUST NOT be stored in the DB.
 */
public interface YierdisBytesView {
    int len();

    byte byteAt(int index);
}

