package yier.bubu.redis.protocol;

import java.util.List;

/**
 * 连接级事务状态（MULTI/EXEC/DISCARD）的最小抽象。
 * <p>
 * 该接口位于 protocol 模块中，用于让命令层访问“连接态”，同时避免 core 直接依赖 server/Netty。
 * <p>
 * 实现要求：
 * - 必须是 best-effort 且线程安全（至少能在连接关闭清理时安全 discard）
 * - 建议在 begin/discard/drain 时释放引用，避免长期驻留大请求数据
 */
public interface RespTransactionState {
    /**
     * 是否处于 MULTI 状态。
     */
    boolean active();

    /**
     * 进入 MULTI 状态，并清空历史队列。
     */
    void begin();

    /**
     * 退出 MULTI 状态并清空队列（DISCARD）。
     */
    void discard();

    /**
     * 入队一个命令（argv 形式，二进制安全）。
     */
    void enqueue(byte[][] argv);

    /**
     * 事务是否已进入“不可执行”状态（Redis 语义中的 EXECABORT）。
     * <p>
     * 典型触发条件：
     * - MULTI 队列超限（条数/bytes）
     * - 命令入队阶段发生可恢复错误（例如参数校验失败）并决定中止事务
     */
    default boolean aborted() {
        return false;
    }

    /**
     * 将事务标记为 aborted（后续 {@code EXEC} 返回 {@code EXECABORT} 并丢弃队列）。
     * <p>
     * 使用场景：
     * - 入队阶段发生错误（例如参数校验失败、队列超限）
     * - 禁止在 MULTI 中出现的“连接级/协议级”命令（例如会改变连接协议或 reply 容器语义的命令）
     * <p>
     * 默认实现为 no-op，用于兼容不支持 aborted 的实现；server 侧实现应覆盖该方法。
     */
    default void markAborted() {
        // no-op
    }

    /**
     * 尝试入队一个命令，并返回入队错误信息（如果有）。
     * <p>
     * 返回值约定：
     * - 返回 {@code null}：入队成功
     * - 返回非 {@code null}：入队失败，且实现方应在内部标记 {@link #aborted()}
     * <p>
     * 默认实现会直接调用 {@link #enqueue(byte[][])} 并视为成功，用于兼容没有超限保护的实现。
     */
    default String tryEnqueue(byte[][] argv) {
        enqueue(argv);
        return null;
    }

    /**
     * 当前队列长度（best-effort）。
     */
    int size();

    /**
     * 取出并清空队列，同时退出 MULTI 状态（EXEC）。
     */
    List<byte[][]> drain();
}
