package yier.bubu.redis.storage.memory.internal.entry;

import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeHandleDomain;

public record ValueHandle(long raw) {
    public static final ValueHandle NULL = new ValueHandle(0L);

    public ValueHandle {
        NativeHandle.requireValidRaw(raw);
    }

    public static ValueHandle fromNativeHandle(NativeHandle handle) {
        if (handle == null) {
            throw new NullPointerException("handle");
        }
        return new ValueHandle(handle.raw());
    }

    public static ValueHandle fromRaw(long raw) {
        return new ValueHandle(raw);
    }

    public NativeHandle nativeHandle() {
        return NativeHandle.fromRaw(raw);
    }

    public boolean isNull() {
        return raw == 0L;
    }

    public void requireDomain(NativeHandleDomain domain) {
        if (domain == null || NativeHandle.domainCode(raw) != domain.code()) {
            throw new IllegalArgumentException("value handle domain mismatch: expected " + domain);
        }
    }
}
