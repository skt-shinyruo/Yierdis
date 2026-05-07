package yier.bubu.redis.storage.api;

// DbLifecycleOps：DB 生命周期/管理能力边界（FLUSHDB 等）。

public interface DbLifecycleOps {
    MutationOutcome flushDb();
}
