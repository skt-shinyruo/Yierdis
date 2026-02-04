package yier.bubu.redis.ops;

// EvictionCoordinator：淘汰/预算相关的治理入口（用于将 maxmemory 策略从命令层收敛到单点）。

/**
 * Eviction coordination boundary.
 * <p>
 * 该接口用于将 maxmemory 淘汰/拒写点等逻辑从命令层抽离，便于后续替换实现（例如 ledger 驱动）。
 */
public interface EvictionCoordinator {
    void prepareWrite(long estimatedExtraBytes);

    void enforceMaxmemory();

    void rollbackWriteReservationIfAny();
}

