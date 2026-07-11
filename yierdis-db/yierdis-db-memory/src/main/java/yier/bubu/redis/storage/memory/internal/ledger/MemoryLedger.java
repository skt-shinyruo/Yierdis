package yier.bubu.redis.storage.memory.internal.ledger;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

// MemoryLedger：maxmemory/预算判定的 SSOT，提供 reserve → commit/rollback 的两阶段写入语义。

/**
 * Memory accounting SSOT（Single Source Of Truth）。
 * <p>
 * 该接口用于将“预算判定/拒写点/淘汰触发/可观测口径”收敛到单点，避免 {@code usedBytes} 分散维护导致漂移。
 * <p>
 * 线程语义：实现默认假设由单线程（DB owner thread）调用，不要求并发安全。
 */
public interface MemoryLedger {
    long limitBytes();

    long usedBytes();

    long reservedBytes();

    public default long effectiveUsedBytes() {
        return usedBytes() + reservedBytes();
    }

    MemoryReservation reserve(long estimatedExtraBytes);

    void reconcile(MemoryReservation reservation, long requiredBytes);

    MemoryReservation beginReclamation();

    public default void commit(MemoryReservation reservation) {
        commit(reservation, reservation == null ? 0 : reservation.reservedBytes());
    }

    /**
     * 结束一次 reservation，并用真实内存增量修正账本；真实增量可以为负数。
     * <p>
     * 契约：
     * - {@code actualDeltaBytes} 可以为负，例如覆盖为更小值、删除 key 或过期清理。
     * - {@code reservation.reservedBytes()} 已由 reconcile 收敛为提交前确认的物理增长预算。
     */
    void commit(MemoryReservation reservation, long actualDeltaBytes);

    void rollback(MemoryReservation reservation);
}
