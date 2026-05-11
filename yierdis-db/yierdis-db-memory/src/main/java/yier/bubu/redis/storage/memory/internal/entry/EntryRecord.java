package yier.bubu.redis.storage.memory.internal.entry;

import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

public record EntryRecord(
        long keyHandle,
        ValueHandle valueHandle,
        int keyHash,
        ValueType type,
        ValueEncoding encoding,
        int flags,
        long expireAtMillis,
        long version,
        long lruOrLfu
) {
    public EntryRecord {
        if (valueHandle == null) {
            throw new NullPointerException("valueHandle");
        }
        if (type == null) {
            throw new NullPointerException("type");
        }
        if (encoding == null) {
            throw new NullPointerException("encoding");
        }
    }
}
