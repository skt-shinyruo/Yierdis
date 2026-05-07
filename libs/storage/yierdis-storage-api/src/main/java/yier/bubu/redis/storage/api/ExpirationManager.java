package yier.bubu.redis.storage.api;

// ExpirationManager：过期相关能力边界（清理/设置/移除等），便于与淘汰/记账解耦。

/**
 * Expiration management boundary.
 * <p>
 * 目前该接口仅收敛维护入口（cleanup）；后续可扩展为 AOF/RDB/replication 需要的快照/事件语义。
 */
public interface ExpirationManager {
    void cleanupExpired();
}

