package yier.bubu.redis.contract;

/**
 * Optional read-only connection observability exposed by server-side sessions.
 */
public interface ConnectionStatsProvider {
    ConnectionStatsView connectionStats();
}
