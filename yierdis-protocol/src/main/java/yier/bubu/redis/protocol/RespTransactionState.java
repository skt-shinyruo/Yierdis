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
     * 当前队列长度（best-effort）。
     */
    int size();

    /**
     * 取出并清空队列，同时退出 MULTI 状态（EXEC）。
     */
    List<byte[][]> drain();
}

