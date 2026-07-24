package yier.bubu.redis.storage.api.result;

import yier.bubu.redis.storage.api.ScanCursorV2;

/**
 * 集合类 SCAN 命令的一次有界结果窗口。
 *
 * <p>窗口中的元素在创建时已经稳定，调用方在输出完成或放弃输出后必须关闭它。</p>
 */
public interface CollectionScanWindow extends ByteSequenceSource {
    ScanCursorV2 nextCursor();

    @Override
    void close();
}
