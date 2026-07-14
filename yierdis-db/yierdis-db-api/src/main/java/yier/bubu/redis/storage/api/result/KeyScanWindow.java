package yier.bubu.redis.storage.api.result;

import yier.bubu.redis.storage.api.ScanCursorV2;

/**
 * 有界、可重放的 key 枚举窗口。
 *
 * <p>窗口只保留游标边界和元数据，不保留复制后的 key 数组；调用方在输出或丢弃响应后必须关闭它。</p>
 */
public interface KeyScanWindow extends MeasuredBulkStringSequence {
    ScanCursorV2 nextCursor();

    long encodedElementBytes();

    long inspectedSlots();

    long tableGeneration();

    long expiryEvaluationMillis();

    boolean current();

    @Override
    void close();
}
