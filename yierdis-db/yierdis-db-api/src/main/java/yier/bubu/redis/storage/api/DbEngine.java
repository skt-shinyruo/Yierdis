package yier.bubu.redis.storage.api;

/**
 * DB 的语义操作边界；运行期生命周期与可选维护能力由独立接口声明。
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
