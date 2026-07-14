package yier.bubu.redis.storage.api;

/**
 * 已提交变更的来源。
 */
public enum DbCommitKind {
    USER,
    EXPIRED,
    EVICTED
}
