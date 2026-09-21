package yier.bubu.redis.storage.memory.internal.entry;

import java.util.Objects;
import yier.bubu.redis.memory.api.NativeHandle;

public record EntryHandle(NativeHandle nativeHandle) {
    public EntryHandle {
        Objects.requireNonNull(nativeHandle, "nativeHandle");
        if (nativeHandle.isNull()) {
            throw new IllegalArgumentException("entry handle must not be null");
        }
    }
}
