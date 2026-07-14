package yier.bubu.redis.storage.api.result;

/**
 * 可在写入前提供精确 RESP 元素字节数的 field/value 回复来源。
 *
 * <p>调用方必须在输出完成或放弃输出后关闭该来源。</p>
 */
public interface BulkStringMapMetrics extends BulkStringMapPairs, AutoCloseable {
    long encodedElementBytes();

    long retainedMemoryBytes();

    @Override
    void close();
}
