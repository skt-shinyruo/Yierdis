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

    public default void commit(MemoryReservation reservation) {
        commit(reservation, reservation == null ? 0 : reservation.reservedBytes());
    }

    /**
     * Finishes a reservation and applies the actual delta bytes (can be negative).
     * <p>
     * Contract:
     * - {@code actualDeltaBytes} may be negative (e.g. overwrite smaller value, delete, expire cleanup).
     * - {@code reservation.reservedBytes()} is a best-effort upper bound; implementations may choose to validate strictly or accept drift.
     */
    void commit(MemoryReservation reservation, long actualDeltaBytes);

    void rollback(MemoryReservation reservation);
}
