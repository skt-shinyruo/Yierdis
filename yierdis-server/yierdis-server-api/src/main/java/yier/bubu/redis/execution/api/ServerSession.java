package yier.bubu.redis.execution.api;

/**
 * Server-side per-connection session state exposed to the command layer.
 * <p>
 * Compatibility aggregate for Redis-like connection state. New code should depend on the narrow capability interface it
 * needs instead of this full session surface.
 */
public interface ServerSession extends DbIndexSession, ClientMetadataSession, TransactionSession, ConnectionStatsSession, ProtocolNegotiationSession {
}
