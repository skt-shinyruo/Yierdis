package yier.bubu.redis.storage.memory;

import java.nio.charset.StandardCharsets;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.storage.api.result.ByteValueSink;
import yier.bubu.redis.storage.api.result.ByteValue;

final class OwnedReplyValueAssertions {
    private OwnedReplyValueAssertions() {
    }

    static boolean isNull(ByteValue value) {
        try (value) {
            return value.isNull();
        }
    }

    static byte[] bytes(ByteValue value) {
        try (value) {
            byte[][] captured = new byte[1][];
            value.emitTo(new ByteValueSink() {
                @Override
                public void value(byte[] data) {
                    captured[0] = data == null ? null : data.clone();
                }

                @Override
                public void value(byte[] data, int off, int len) {
                    byte[] copy = new byte[len];
                    if (len > 0) {
                        System.arraycopy(data, off, copy, 0, len);
                    }
                    captured[0] = copy;
                }

                @Override
                public void value(BytesSlice slice) {
                    byte[] copy = new byte[slice.length()];
                    if (copy.length > 0) {
                        slice.getBytes(0, copy, 0, copy.length);
                    }
                    captured[0] = copy;
                }

                @Override
                public void longAscii(long value) {
                    captured[0] = Long.toString(value).getBytes(StandardCharsets.US_ASCII);
                }

                @Override
                public void nullValue() {
                    captured[0] = null;
                }
            });
            return captured[0];
        }
    }
}
