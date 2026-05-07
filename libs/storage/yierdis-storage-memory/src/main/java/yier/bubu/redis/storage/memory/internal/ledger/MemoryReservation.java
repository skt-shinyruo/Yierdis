package yier.bubu.redis.storage.memory.internal.ledger;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

// MemoryReservation：一次 reserve 的返回 token，用于 commit/rollback 收敛异常路径与不变量。

public interface MemoryReservation {
    long reservedBytes();
}

