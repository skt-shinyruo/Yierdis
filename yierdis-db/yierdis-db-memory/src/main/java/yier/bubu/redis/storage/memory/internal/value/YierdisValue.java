package yier.bubu.redis.storage.memory.internal.value;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;


import yier.bubu.redis.storage.api.ValueType;
public interface YierdisValue extends AutoCloseable {
    ValueType type();

    ValueEncoding encoding();

    @Override
    void close();
}
