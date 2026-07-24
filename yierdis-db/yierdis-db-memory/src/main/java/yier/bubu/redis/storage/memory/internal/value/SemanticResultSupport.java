package yier.bubu.redis.storage.memory.internal.value;

import java.util.Objects;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.storage.api.result.ByteValueSink;
import yier.bubu.redis.storage.api.result.PayloadLengthSink;

public final class SemanticResultSupport {
    private SemanticResultSupport() {
    }

    public static ByteValueSink lengthSink(PayloadLengthSink out) {
        return new LengthSink(Objects.requireNonNull(out, "out"));
    }

    public static int signedLongAsciiLength(long value) {
        if (value == Long.MIN_VALUE) {
            return 20;
        }
        long remaining = value < 0L ? -value : value;
        int length = value < 0L ? 2 : 1;
        while (remaining >= 10L) {
            remaining /= 10L;
            length++;
        }
        return length;
    }

    private static final class LengthSink implements ByteValueSink {
        private final PayloadLengthSink out;

        private LengthSink(PayloadLengthSink out) {
            this.out = out;
        }

        @Override
        public void value(byte[] data) {
            out.payloadLength(data == null ? -1 : data.length);
        }

        @Override
        public void value(byte[] data, int offset, int length) {
            if (data == null) {
                out.payloadLength(-1);
                return;
            }
            Objects.checkFromIndexSize(offset, length, data.length);
            out.payloadLength(length);
        }

        @Override
        public void value(BytesSlice slice) {
            out.payloadLength(slice == null ? -1 : slice.length());
        }

        @Override
        public void longAscii(long value) {
            out.payloadLength(signedLongAsciiLength(value));
        }

        @Override
        public void nullValue() {
            out.payloadLength(-1);
        }
    }
}
