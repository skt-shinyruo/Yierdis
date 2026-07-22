package yier.bubu.redis.storage.api;

/**
 * DB 的语义操作边界，提供读取、写入、过期、内存诊断、数据库管理与健康状态视图。
 */
public interface DbEngine {
    DbReads reads();

    DbWrites writes();

    ExpirationManager expiration();

    MemoryOps memory();

    DbLifecycleOps lifecycle();

    default DbHealthSnapshot health() {
        return DbHealthSnapshot.healthy();
    }
}
