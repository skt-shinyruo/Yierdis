package yier.bubu.redis.storage.api.result;

/**
 * 可在写入前提供精确 RESP 元素字节数的 bulk string 序列。
 *
 * <p>调用方必须在输出完成或放弃输出后关闭该来源。</p>
 */
public interface MeasuredBulkStringSequence extends BulkStringSequence, AutoCloseable {
    long encodedElementBytes();

    default long retainedMemoryBytes() {
        return 0L;
    }

    @Override
    void close();
}
