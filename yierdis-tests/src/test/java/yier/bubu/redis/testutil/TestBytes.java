package yier.bubu.redis.testutil;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.storage.api.StringOps;
import yier.bubu.redis.storage.api.result.ByteValue;
import yier.bubu.redis.storage.api.result.ByteValueSink;

public final class TestBytes {
    private TestBytes() {
    }

    public static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    public static List<byte[]> cmd(String... parts) {
        List<byte[]> out = new ArrayList<>(parts.length);
        for (String p : parts) {
            out.add(b(p));
        }
        return out;
    }

    public static byte[] stringValue(StringOps operations, byte[] key) {
        return byteValue(operations.getStringValue(new ArrayBytesView(key)));
    }

    public static byte[] byteValue(ByteValue value) {
        try (value) {
            CopySink sink = new CopySink();
            value.emitTo(sink);
            return sink.value;
        }
    }

    private record ArrayBytesView(byte[] bytes) implements BytesView {
        @Override
        public int length() {
            return bytes.length;
        }

        @Override
        public byte getByte(int index) {
            return bytes[index];
        }
    }

    private static final class CopySink implements ByteValueSink {
        private byte[] value;

        @Override
        public void value(byte[] data) {
            value = data == null ? null : data.clone();
        }

        @Override
        public void value(byte[] data, int offset, int length) {
            value = Arrays.copyOfRange(data, offset, offset + length);
        }

        @Override
        public void value(BytesSlice slice) {
            value = new byte[slice.length()];
            slice.getBytes(0, value, 0, value.length);
        }

        @Override
        public void longAscii(long number) {
            value = Long.toString(number).getBytes(StandardCharsets.US_ASCII);
        }

        @Override
        public void nullValue() {
            value = null;
        }
    }
}
