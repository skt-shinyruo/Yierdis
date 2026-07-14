package yier.bubu.redis.storage.memory.internal.expire;

import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;

/**
 * {@link YierdisDb} 内部使用的 TTL index 抽象。
 *
 * <p>实现故意保持最小边界且不保证线程安全；调用方必须在 DB owner thread 内串行访问。</p>
 */
public interface YierdisExpireIndex {
    int size();

    Long get(byte[] keyBytes);

    Long get(BytesView keyView);

    /**
     * 返回稳定 {@link KeyHandle} identity 对应的 TTL timestamp；不存在时返回 {@code null}。
     *
     * <p>约束：实现不得为查找而隐式生成 canonical heap key copy。</p>
     */
    Long get(KeyHandle keyHandle);

    /**
     * 不推进增量 rehash 等实现维护工作的 TTL 查询。
     *
     * <p>枚举必须走此路径，确保 discovery 和 replay 都不会改变表结构。</p>
     */
    default Long getForScan(KeyHandle keyHandle) {
        return get(keyHandle);
    }

    byte[] randomKey();

    /**
     * 返回一个随机稳定 key identity handle；index 为空时返回 {@code null}。
     *
     * <p>约束：实现不得为返回 handle 而隐式生成 canonical heap key copy。</p>
     */
    KeyHandle randomKeyHandle();

    void clear();

    /**
     * 为稳定 {@link KeyHandle} identity 设置 TTL timestamp。
     *
     * <p>约束：实现不得为 set 而隐式生成 canonical heap key copy。</p>
     */
    void setExpireAtMillis(KeyHandle keyHandle, long expireAtMillis);

    default PreparedTtlMutation prepareSetExpireAtMillis(KeyHandle keyHandle, long expireAtMillis) {
        return new PreparedTtlMutation() {
            @Override
            public long stagedNonNativeGrowthBytes() {
                return 0L;
            }

            @Override
            public void commit() {
                setExpireAtMillis(keyHandle, expireAtMillis);
            }

            @Override
            public void releaseSuperseded() {
            }

            @Override
            public void abort() {
            }
        };
    }

    void removeExpire(byte[] keyBytes);

    /**
     * 移除稳定 {@link KeyHandle} identity 对应的 TTL entry。
     *
     * <p>约束：实现不得为 remove 而隐式生成 canonical heap key copy。</p>
     */
    void removeExpire(KeyHandle keyHandle);

    default PreparedTtlMutation prepareRemoveExpire(KeyHandle keyHandle) {
        return new PreparedTtlMutation() {
            @Override
            public long stagedNonNativeGrowthBytes() {
                return 0L;
            }

            @Override
            public void commit() {
                removeExpire(keyHandle);
            }

            @Override
            public void releaseSuperseded() {
            }

            @Override
            public void abort() {
            }
        };
    }
}
