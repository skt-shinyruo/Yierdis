package yier.bubu.redis.storage.testkit;

import java.nio.charset.StandardCharsets;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.bytes.BytesView;

public final class TestBytes {
    private TestBytes() {
    }

    public static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    public static BytesView view(byte[] bytes) {
        return slice(bytes);
    }

    public static BytesSlice slice(byte[] bytes) {
        return new ArraySlice(bytes);
    }

    private record ArraySlice(byte[] bytes) implements BytesSlice {
        @Override
        public int length() {
            return bytes.length;
        }

        @Override
        public byte getByte(int index) {
            return bytes[index];
        }

        @Override
        public void writeTo(BytesSink out) {
            out.writeBytes(bytes, 0, bytes.length);
        }
    }
}
