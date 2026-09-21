package yier.bubu.redis.storage.memory.internal.entry;

import java.util.Objects;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.memory.internal.value.ValueEncoding;

public record EntryRecord(
        NativeHandle keyHandle,
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
        keyHandle = Objects.requireNonNull(keyHandle, "keyHandle");
        valueHandle = Objects.requireNonNull(valueHandle, "valueHandle");
        type = Objects.requireNonNull(type, "type");
        encoding = Objects.requireNonNull(encoding, "encoding");
    }
}
