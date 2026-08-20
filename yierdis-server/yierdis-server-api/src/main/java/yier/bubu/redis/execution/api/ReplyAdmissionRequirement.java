package yier.bubu.redis.execution.api;

/**
 * 一个请求对同一连接后续 reply slot 注册的约束。
 */
public enum ReplyAdmissionRequirement {
    /** 后续请求可以继续注册自己的 reply slot。 */
    PIPELINED,

    /** 后续注册必须等待当前 reply slot 的资源清理完成。 */
    BARRIER_UNTIL_CLEANUP
}
