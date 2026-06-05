package yier.bubu.redis.storage.memory.internal.expire;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

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

    void removeExpire(byte[] keyBytes);

    /**
     * 移除稳定 {@link KeyHandle} identity 对应的 TTL entry。
     *
     * <p>约束：实现不得为 remove 而隐式生成 canonical heap key copy。</p>
     */
    void removeExpire(KeyHandle keyHandle);
}
