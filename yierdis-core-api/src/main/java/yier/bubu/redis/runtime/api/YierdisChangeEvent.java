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
 *   <li>保持实现最小化：不试图在核心层判定“是否真实发生变更”</li>
 * </ul>
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
