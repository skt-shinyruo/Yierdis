package yier.bubu.redis.storage.api;

// StringReadOps：string 只读能力边界。

import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.storage.api.result.BulkStringValue;

public interface StringReadOps {
    byte[] getStringBytes(byte[] keyBytes);

    BulkStringValue getStringValue(BytesView keyView);

    /**
     * 返回仅用于回复容量预检的值视图；该读取不会更新访问时钟或回收过期键，调用方必须关闭结果。
     */
    BulkStringValue previewStringValue(BytesView keyView);

    long strlen(BytesView keyView);

    int getBit(BytesView keyView, long offset);

    long bitcount(BytesView keyView);

    long bitcount(BytesView keyView, long start, long end);
}
