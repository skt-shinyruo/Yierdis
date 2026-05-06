package yier.bubu.redis.runtime.api;

// YierdisChangeEvent：用于 AOF/RDB/replication 等能力的“变更事件”载体（按 ExecutionRecord 记录，可重放）。

import yier.bubu.redis.contract.ExecutionRecord;
import yier.bubu.redis.contract.ExecutionRequest;

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
 * 发射语义（命令层约定）：仅当命令执行成功，且本次命令造成 Keyspace/Value/TTL 元数据的真实变化时才应发射事件。
 * 该“真实变更”判定由 storage-api 写结果显式返回，避免依赖隐藏的线程本地状态。
 * <p>
 * 生命周期约束：{@link #request()} 内的参数字节被视为不可变快照；创建方应保证其不会被后续修改。
 */
public record YierdisChangeEvent(ExecutionRecord record) {
    public YierdisChangeEvent {
        record = Objects.requireNonNull(record, "record");
    }

    public int dbIndex() {
        return record.dbIndex();
    }

    public ExecutionRequest request() {
        return record.request();
    }
}
