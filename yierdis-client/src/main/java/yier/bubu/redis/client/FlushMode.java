package yier.bubu.redis.client;

/**
 * FLUSHDB 的 SYNC 或 ASYNC。两者互斥，所以用一个枚举而不是两个可以同时打开的标志。
 */
public enum FlushMode {
    SYNC,
    ASYNC
}
