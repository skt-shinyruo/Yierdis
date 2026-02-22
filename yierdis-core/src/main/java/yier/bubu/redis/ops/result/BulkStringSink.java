package yier.bubu.redis.ops.result;

// BulkStringSink：用于流式输出 bulk string 值的中立端口（domain 层），避免 db/value/offheap 依赖协议层接口。

import yier.bubu.redis.bytes.BytesSlice;

/**
 * 用于“协议无关”地流式输出 bulk string 值的输出端口。
 * <p>
 * 该接口用于让 storage/value/off-heap 层与协议输出端口解耦
 * （例如 {@code ReplyWriter}/{@code ReplySink}），同时仍允许低分配的流式输出。
 * <p>
 * 语义约定：
 * <ul>
 *   <li>传入 {@code null} 表示 “null bulk string”。</li>
 *   <li>实现必须同步消费输入，且不得保留任何引用。</li>
 * </ul>
 */
public interface BulkStringSink {
    void bulkString(byte[] data);

    void bulkString(byte[] data, int off, int len);

    void bulkString(BytesSlice slice);

    void bulkStringLongAscii(long value);

    default void bulkStringNull() {
        bulkString((byte[]) null);
    }
}
