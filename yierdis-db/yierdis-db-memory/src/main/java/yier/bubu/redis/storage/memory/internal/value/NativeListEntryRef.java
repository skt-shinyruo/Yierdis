package yier.bubu.redis.storage.memory.internal.value;

import yier.bubu.redis.memory.api.NativeHandle;

import java.util.Objects;

public final class NativeListEntryRef {
    private final NativeHandle handle;
    private final int payloadLength;
    private final int retainedBytes;

    private NativeListEntryRef(NativeHandle handle, int payloadLength, int retainedBytes) {
        if (payloadLength < -1) {
            throw new IllegalArgumentException("payloadLength must be >= -1");
        }
        if (retainedBytes < 0) {
            throw new IllegalArgumentException("retainedBytes must be >= 0");
        }
        this.handle = handle;
        this.payloadLength = payloadLength;
        this.retainedBytes = retainedBytes;
    }

    public static NativeListEntryRef nullValue() {
        return new NativeListEntryRef(null, -1, 0);
    }

    public static NativeListEntryRef handle(NativeHandle handle, int payloadLength, int retainedBytes) {
        return new NativeListEntryRef(Objects.requireNonNull(handle, "handle"), payloadLength, retainedBytes);
    }

    public NativeHandle handle() {
        return handle;
    }

    public int payloadLength() {
        return payloadLength;
    }

    public int retainedBytes() {
        return retainedBytes;
    }

    public long encodedElementBytes() {
        if (payloadLength < 0) {
            return 5L;
        }
        return 1L + decimalDigits(payloadLength) + 2L + payloadLength + 2L;
    }

    private static int decimalDigits(int value) {
        int digits = 1;
        int v = value;
        while (v >= 10) {
            v /= 10;
            digits++;
        }
        return digits;
    }
}
