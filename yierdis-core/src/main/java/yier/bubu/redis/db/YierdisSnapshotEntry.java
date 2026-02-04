package yier.bubu.redis.db;

// YierdisSnapshotEntry：快照输出条目（当前仅对 STRING 提供 valueBytes，其他类型可扩展编码策略）。

import java.util.Objects;

/**
 * Snapshot entry (consumer-facing).
 * <p>
 * 约束：该对象不暴露 DB 内部结构（keyspace/encoding/payload 等），只提供可持久化的最小信息。
 */
public record YierdisSnapshotEntry(byte[] keyBytes, ValueType type, byte[] stringValueBytes, Long expireAtMillis) {
    public YierdisSnapshotEntry {
        Objects.requireNonNull(keyBytes, "keyBytes");
        Objects.requireNonNull(type, "type");
    }
}

