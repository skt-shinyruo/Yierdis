package yier.bubu.redis.storage.api.result;

import java.util.Objects;
import yier.bubu.redis.bytes.BytesSlice;

/** 将 bulk string 访问转换为精确 RESP2 元素字节数，且不复制 payload。 */
public final class BulkStringMetrics implements BulkStringSink {
    private int count;
    private long encodedElementBytes;

    public int count() {
        return count;
    }

    public long encodedElementBytes() {
        return encodedElementBytes;
    }

    @Override
    public void bulkString(byte[] data) {
        if (data == null) {
            recordNull();
            return;
        }
        recordPayload(data.length);
    }

    @Override
    public void bulkString(byte[] data, int off, int len) {
        if (data == null) {
            recordNull();
            return;
        }
        Objects.checkFromIndexSize(off, len, data.length);
        recordPayload(len);
    }

    @Override
    public void bulkString(BytesSlice slice) {
        if (slice == null) {
            recordNull();
            return;
        }
        recordPayload(slice.length());
    }

    @Override
    public void bulkStringLongAscii(long value) {
        recordPayload(decimalDigits(value));
    }

    private void recordNull() {
        recordEncoded(5L);
    }

    private void recordPayload(int payloadLength) {
        if (payloadLength < 0) {
            throw new IllegalArgumentException("bulk string payload length must be >= 0");
        }
        long encoded = 1L + decimalDigits(payloadLength) + 2L;
        encoded = saturatedAdd(encoded, payloadLength);
        recordEncoded(saturatedAdd(encoded, 2L));
    }

    private void recordEncoded(long encodedBytes) {
        if (count == Integer.MAX_VALUE) {
            throw new IllegalStateException("bulk string result count exceeds Integer.MAX_VALUE");
        }
        count++;
        encodedElementBytes = saturatedAdd(encodedElementBytes, encodedBytes);
    }

    private static int decimalDigits(int value) {
        int digits = 1;
        int remaining = value;
        while (remaining >= 10) {
            remaining /= 10;
            digits++;
        }
        return digits;
    }

    private static int decimalDigits(long value) {
        if (value == Long.MIN_VALUE) {
            return 20;
        }
        long remaining = value < 0L ? -value : value;
        int digits = value < 0L ? 2 : 1;
        while (remaining >= 10L) {
            remaining /= 10L;
            digits++;
        }
        return digits;
    }

    private static long saturatedAdd(long left, long right) {
        if (left < 0L || right < 0L || Long.MAX_VALUE - left < right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }
}
