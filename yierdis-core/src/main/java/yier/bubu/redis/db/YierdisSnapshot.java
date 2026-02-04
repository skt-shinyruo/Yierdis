package yier.bubu.redis.db;

// YierdisSnapshot：RDB/复制等需要的“快照读取”接口（基于 scan cursor v2，避免暴露 keyspace 实现细节）。

import java.util.List;

/**
 * DB 快照接口（最小契约）。
 * <p>
 * 设计目标：
 * <ul>
 *   <li>供 RDB/replication 等能力读取当前状态</li>
 *   <li>不侵入 keyspace 内部实现（仅依赖 {@link ScanCursorV2} 与公开的值类型信息）</li>
 *   <li>支持 time-slice（通过 count + cursor 分批推进，避免一次性全表扫描阻塞）</li>
 * </ul>
 */
public interface YierdisSnapshot {
    /**
     * Reads a batch of snapshot entries and returns the next cursor.
     *
     * @param cursor input cursor (0 means start)
     * @param count  max entries to return in this batch (must be > 0)
     * @param out    output container (append)
     * @return next cursor (0 means finished)
     */
    ScanCursorV2 snapshot(ScanCursorV2 cursor, int count, List<YierdisSnapshotEntry> out);
}

