package yier.bubu.redis.db.memory;

// MemoryReservation：一次 reserve 的返回 token，用于 commit/rollback 收敛异常路径与不变量。

public interface MemoryReservation {
    long reservedBytes();
}

