package yier.bubu.redis.execution.api;

/**
 * prepared work 在真正修改状态前的有效性。
 */
public enum ValidationResult {
    VALID,
    STALE
}
