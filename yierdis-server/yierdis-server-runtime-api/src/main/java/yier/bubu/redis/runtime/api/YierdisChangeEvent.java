package yier.bubu.redis.runtime.api;

// YierdisChangeEvent：用于 AOF/RDB/replication 等能力的“变更事件”载体（按 ExecutionRecord 记录，可重放）。

import yier.bubu.redis.execution.api.ExecutionRecord;
import yier.bubu.redis.execution.api.ExecutionRequest;

import java.util.Objects;

/**
 * 变更事件（最小契约）：以“可重放”的 ExecutionRecord 表示一次写入事件。
 * <p>
 * 设计目标：
 * <ul>
 *   <li>不依赖 DB/keyspace 的内部实现细节</li>
 *   <li>可被 AOF/replication 直接复用（写入即记录 ExecutionRecord；消费端决定如何持久化/转发）</li>
 *   <li>保持载荷最小化：不携带 DB 内部对象引用或结构化 diff，仅记录请求快照（可重放）</li>
 * </ul>
 * <p>
 * 发射语义：用户命令仅当命令执行成功，且本次命令造成 Keyspace/Value/TTL 元数据的真实变化时才应发射事件；
 * DB 生命周期内部删除（过期/驱逐）应发射 synthetic 事件，并使用可重放的规范命令（例如 DEL key）表示结果。
 * <p>
 * 生命周期约束：{@link #request()} 内的参数字节被视为不可变快照；创建方应保证其不会被后续修改。
 */
public record YierdisChangeEvent(ExecutionRecord record, YierdisChangeKind kind, boolean synthetic) {
    public YierdisChangeEvent(ExecutionRecord record) {
        this(record, YierdisChangeKind.USER_COMMAND, false);
    }

    public YierdisChangeEvent {
        record = Objects.requireNonNull(record, "record");
        kind = kind == null ? YierdisChangeKind.USER_COMMAND : kind;
    }

    public int dbIndex() {
        return record.dbIndex();
    }

    public ExecutionRequest request() {
        return record.request();
    }
}
