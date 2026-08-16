package yier.bubu.redis.storage.api;

// DbLifecycleOps：DB 生命周期/管理能力边界（FLUSHDB 等）。

public interface DbLifecycleOps {
    MutationOutcome flushDb();

    /**
     * 立即分离当前逻辑 keyspace，并把旧数据的物理回收交给 DB owner 的后续维护工作。
     * 实现若不支持分离式回收，可以退化为同步 {@link #flushDb()}。
     */
    default MutationOutcome flushDbAsync() {
        return flushDb();
    }
}
