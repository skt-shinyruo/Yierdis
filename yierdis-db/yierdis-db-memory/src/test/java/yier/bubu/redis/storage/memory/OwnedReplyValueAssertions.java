package yier.bubu.redis.storage.memory;

import java.nio.charset.StandardCharsets;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.storage.api.result.BulkStringSink;
import yier.bubu.redis.storage.api.result.BulkStringValue;

final class OwnedReplyValueAssertions {
    private OwnedReplyValueAssertions() {
    }

    static boolean isNull(BulkStringValue value) {
        try (value) {
            return value.isNull();
        }
    }

    static byte[] bytes(BulkStringValue value) {
        try (value) {
            byte[][] captured = new byte[1][];
            value.writeTo(new BulkStringSink() {
                @Override
                public void bulkString(byte[] data) {
                    captured[0] = data == null ? null : data.clone();
                }

                @Override
                public void bulkString(byte[] data, int off, int len) {
                    byte[] copy = new byte[len];
                    if (len > 0) {
                        System.arraycopy(data, off, copy, 0, len);
                    }
                    captured[0] = copy;
                }

                @Override
                public void bulkString(BytesSlice slice) {
                    byte[] copy = new byte[slice.length()];
                    if (copy.length > 0) {
                        slice.getBytes(0, copy, 0, copy.length);
                    }
                    captured[0] = copy;
                }

                @Override
                public void bulkStringLongAscii(long value) {
                    captured[0] = Long.toString(value).getBytes(StandardCharsets.US_ASCII);
                }
            });
            return captured[0];
        }
    }
}
