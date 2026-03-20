package yier.bubu.redis.ops;

// DbEngine：DB 核心编排入口（orchestrator 边界），对命令层提供稳定的“能力组合”视图。

/**
 * DB engine boundary (orchestrator-facing).
 * <p>
 * 该接口用于降低 command 层与具体 DB 实现（例如 {@code YierdisDb}）的直接耦合；
 * 新代码应优先通过 {@link #reads()} / {@link #writes()} 访问读写边界。
 * legacy mixed boundaries ({@link #values()} / {@link #eviction()} / {@link #keyspace()} / {@link #ttl()})
 * 仍暂时保留，供增量迁移使用。
 */
public interface DbEngine {
    DbReads reads();

    DbWrites writes();

    ValueOps values();

    ExpirationManager expiration();

    EvictionCoordinator eviction();

    KeyspaceOps keyspace();

    TtlOps ttl();

    MemoryOps memory();

    DbLifecycleOps lifecycle();
}
