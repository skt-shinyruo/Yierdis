package yier.bubu.redis.protocol;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class RespObjectParserLimitsTest {
    @Test
    public void defaultArrayLenLimitMatchesRespLimits() {
        int tooMany = RespLimits.DEFAULT_MAX_ARRAY_LEN + 1;
        StringBuilder sb = new StringBuilder(tooMany * 4 + 32);
        sb.append('*').append(tooMany).append("\r\n");
        for (int i = 0; i < tooMany; i++) {
            sb.append(":0\r\n");
        }

        byte[] bytes = sb.toString().getBytes(StandardCharsets.US_ASCII);
        try (RespFrame frame = new HeapRespFrame(bytes)) {
            try {
                RespObjectParser.parse(frame);
                Assert.fail("Expected IllegalArgumentException");
            } catch (IllegalArgumentException e) {
                Assert.assertTrue(e.getMessage().toLowerCase().contains("array length"));
            }
        }
    }

    private static final class HeapRespFrame implements RespFrame {
        private final byte[] data;

        private HeapRespFrame(byte[] data) {
            this.data = data;
        }

        @Override
        public int length() {
            return data.length;
        }

        @Override
        public byte getByte(int index) {
            return data[index];
        }

        @Override
        public void getBytes(int index, byte[] dst, int dstOff, int len) {
            System.arraycopy(data, index, dst, dstOff, len);
        }

        @Override
        public void close() {
            // heap frame: no-op
        }
    }
}

