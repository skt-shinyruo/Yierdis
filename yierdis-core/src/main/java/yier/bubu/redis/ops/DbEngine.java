package yier.bubu.redis.ops;

// DbEngine：DB 核心编排入口（orchestrator 边界），对命令层提供稳定的“能力组合”视图。

/**
 * DB engine boundary (orchestrator-facing).
 * <p>
 * 该接口用于降低 command 层与具体 DB 实现（例如 {@code YierdisDb}）的直接耦合；
 * 通过 {@link #values()} / {@link #expiration()} / {@link #eviction()} 等子组件暴露能力边界。
 */
public interface DbEngine {
    ValueOps values();

    ExpirationManager expiration();

    EvictionCoordinator eviction();
}

