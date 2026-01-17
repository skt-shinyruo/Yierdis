package yier.bubu.redis.db.offheap.api;

/**
 * Compatibility alias for {@link yier.bubu.redis.bytes.BytesSink}.
 * <p>
 * This keeps existing package paths stable while allowing protocol code to depend on a neutral bytes module.
 */
@Deprecated
public interface YierdisBytesSink extends yier.bubu.redis.bytes.BytesSink {
}
