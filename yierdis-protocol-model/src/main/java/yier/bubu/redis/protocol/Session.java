package yier.bubu.redis.protocol;

/**
 * Marker interface for connection/session scoped state.
 * <p>
 * Concrete protocols may expose richer session objects (e.g. server-side {@link ServerSession}).
 */
public interface Session {
}
