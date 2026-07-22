package yier.bubu.redis.execution.api;

/**
 * Read-only executor/transport counters exposed for server observability commands.
 */
public interface ConnectionStatsSession {
    ConnectionStatsView connectionStats();
}
