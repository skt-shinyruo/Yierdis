package yier.bubu.redis.storage.memory.internal.value;

import java.util.function.IntConsumer;

import java.util.Objects;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.storage.api.result.ByteValueSink;

public final class SemanticResultSupport {
    private SemanticResultSupport() {
    }

    public static ByteValueSink lengthSink(IntConsumer out) {
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
        private final IntConsumer out;

        private LengthSink(IntConsumer out) {
            this.out = out;
        }

        @Override
        public void value(byte[] data) {
            out.accept(data == null ? -1 : data.length);
        }

        @Override
        public void value(byte[] data, int offset, int length) {
            if (data == null) {
                out.accept(-1);
                return;
            }
            Objects.checkFromIndexSize(offset, length, data.length);
            out.accept(length);
        }

        @Override
        public void value(BytesSlice slice) {
            out.accept(slice == null ? -1 : slice.length());
        }

        @Override
        public void longAscii(long value) {
            out.accept(signedLongAsciiLength(value));
        }

        @Override
        public void nullValue() {
            out.accept(-1);
        }
    }
}
