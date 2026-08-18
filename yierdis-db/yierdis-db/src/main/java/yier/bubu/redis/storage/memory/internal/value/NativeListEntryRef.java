package yier.bubu.redis.storage.memory.internal.value;

import yier.bubu.redis.memory.api.NativeHandle;

import java.util.Objects;

public record NativeListEntryRef(NativeHandle handle, int payloadOffset, int payloadLength, int retainedBytes) {
    public NativeListEntryRef {
        if (payloadOffset < 0) {
            throw new IllegalArgumentException("payloadOffset must be >= 0");
        }
        if (payloadLength < -1) {
            throw new IllegalArgumentException("payloadLength must be >= -1");
        }
        if (retainedBytes < 0) {
            throw new IllegalArgumentException("retainedBytes must be >= 0");
        }
    }

    public static NativeListEntryRef nullValue() {
        return new NativeListEntryRef(null, 0, -1, 0);
    }

    public static NativeListEntryRef handle(NativeHandle handle, int payloadLength, int retainedBytes) {
        return handle(handle, 0, payloadLength, retainedBytes);
    }

    public static NativeListEntryRef handle(
            NativeHandle handle,
            int payloadOffset,
            int payloadLength,
            int retainedBytes
    ) {
        return new NativeListEntryRef(
                Objects.requireNonNull(handle, "handle"),
                payloadOffset,
                payloadLength,
                retainedBytes
        );
    }
}
