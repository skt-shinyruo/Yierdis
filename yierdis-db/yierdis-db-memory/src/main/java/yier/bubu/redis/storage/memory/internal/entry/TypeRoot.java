package yier.bubu.redis.storage.memory.internal.entry;

import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

public interface TypeRoot extends AutoCloseable {
    ValueType type();

    ValueEncoding encoding();

    long estimatedBytes(ValueHandle handle);

    void release(ValueHandle handle);

    @Override
    default void close() {
    }
}
