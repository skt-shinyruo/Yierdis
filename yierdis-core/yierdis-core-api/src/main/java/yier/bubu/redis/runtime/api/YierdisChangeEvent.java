package yier.bubu.redis.runtime.api;

// YierdisChangeEvent：用于 AOF/RDB/replication 等能力的“变更事件”载体（默认按命令 argv 记录，可重放）。

import java.util.Objects;

/**
 * 变更事件（最小契约）：以“可重放”的命令 argv 表示一次写入事件。
 * <p>
 * 设计目标：
 * <ul>
 *   <li>不依赖 DB/keyspace 的内部实现细节</li>
 *   <li>可被 AOF/replication 直接复用（写入即记录 argv；消费端决定如何持久化/转发）</li>
 *   <li>保持载荷最小化：不携带 DB 内部对象引用或结构化 diff，仅记录 argv 快照（可重放）</li>
 * </ul>
 * <p>
 * 发射语义（命令层约定）：仅当命令执行成功，且本次命令造成 Keyspace/Value/TTL 元数据的真实变化时才应发射事件。
 * 该“真实变更”判定由核心内部追踪（例如 {@link YierdisChangeTracking}）提供事实信号，避免依赖“写命令名单”导致漂移。
 * <p>
 * 生命周期约束：{@link #argv()} 内的 byte[] 被视为不可变快照；创建方应保证其不会被后续修改。
 */
public record YierdisChangeEvent(int dbIndex, byte[][] argv) {
    public YierdisChangeEvent {
        if (dbIndex < 0) {
            dbIndex = 0;
        }
        Objects.requireNonNull(argv, "argv");
    }
}
