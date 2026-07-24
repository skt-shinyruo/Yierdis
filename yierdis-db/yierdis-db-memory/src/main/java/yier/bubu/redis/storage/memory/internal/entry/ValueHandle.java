package yier.bubu.redis.storage.memory.internal.entry;

import java.util.Objects;
import yier.bubu.redis.memory.api.NativeHandle;

public record ValueHandle(NativeHandle nativeHandle) {
    public static final ValueHandle NULL = new ValueHandle(NativeHandle.NULL);

    public ValueHandle {
        Objects.requireNonNull(nativeHandle, "nativeHandle");
    }

    public boolean isNull() {
        return nativeHandle.isNull();
    }
}
