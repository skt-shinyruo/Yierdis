package yier.bubu.redis.protocol.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.db.offheap.api.YierdisOffHeapBuf;
import yier.bubu.redis.db.offheap.api.YierdisBytesSink;
import yier.bubu.redis.db.offheap.unsafe.YierdisUnsafeOffHeapAllocator;
import yier.bubu.redis.protocol.RespWriter;

import java.nio.charset.StandardCharsets;

public class RespWriterSliceTest {
    @Test
    public void bulkStringSupportsNullEmptyAndSlices() {
        ByteBuf buf = Unpooled.buffer();
        try {
            YierdisBytesSink sink = (src, srcIndex, len) -> buf.writeBytes(src, srcIndex, len);
            RespWriter w = new RespWriter(sink);

            w.bulkString((byte[]) null);
            w.bulkString(new byte[0]);

            byte[] data = new byte[]{'a', 'b', 'c', 'X', 'Y'};
            w.bulkString(data, 0, 3);
            w.bulkString(data, 1, 1);

            byte[] expected = concat(
                    ascii("$-1\r\n"),
                    ascii("$0\r\n\r\n"),
                    ascii("$3\r\nabc\r\n"),
                    ascii("$1\r\nb\r\n")
            );
            Assert.assertArrayEquals(expected, readAll(buf));
        } finally {
            buf.release();
        }
    }

    @Test
    public void bulkStringLongAsciiWritesCorrectFrame() {
        ByteBuf buf = Unpooled.buffer();
        try {
            YierdisBytesSink sink = (src, srcIndex, len) -> buf.writeBytes(src, srcIndex, len);
            RespWriter w = new RespWriter(sink);
            w.bulkStringLongAscii(0);
            w.bulkStringLongAscii(-123);
            w.bulkStringLongAscii(Long.MIN_VALUE);

            byte[] expected = concat(
                    ascii("$1\r\n0\r\n"),
                    ascii("$4\r\n-123\r\n"),
                    ascii("$20\r\n-9223372036854775808\r\n")
            );
            Assert.assertArrayEquals(expected, readAll(buf));
        } finally {
            buf.release();
        }
    }

    @Test
    public void bulkStringSupportsOffHeapSlices() {
        YierdisUnsafeOffHeapAllocator alloc = new YierdisUnsafeOffHeapAllocator(0);
        try {
            YierdisOffHeapBuf buf = alloc.allocate(6);
            try {
                buf.setBytes(0, new byte[]{0, 1, (byte) 0xFF, 'a', 'b', 'c'}, 0, 6);

                ByteBuf out = Unpooled.buffer();
                try {
                    YierdisBytesSink sink = (src, srcIndex, len) -> out.writeBytes(src, srcIndex, len);
                    RespWriter w = new RespWriter(sink);
                    w.bulkString(buf.slice(1, 3)); // [1, 0xFF, 'a']

                    byte[] expected = new byte[]{
                            '$', '3', '\r', '\n',
                            1, (byte) 0xFF, 'a', '\r', '\n'
                    };
                    Assert.assertArrayEquals(expected, readAll(out));
                } finally {
                    out.release();
                }
            } finally {
                buf.close();
            }
        } finally {
            alloc.close();
        }
    }

    @Test
    public void errorSanitizesCrlfAndLimitsLength() {
        ByteBuf buf = Unpooled.buffer();
        try {
            YierdisBytesSink sink = (src, srcIndex, len) -> buf.writeBytes(src, srcIndex, len);
            RespWriter w = new RespWriter(sink);
            w.error("ERR oops\r\n+PONG\r\n");

            byte[] out = readAll(buf);
            Assert.assertEquals('\r', (char) out[out.length - 2]);
            Assert.assertEquals('\n', (char) out[out.length - 1]);
            for (int i = 0; i < out.length - 2; i++) {
                Assert.assertNotEquals("should not contain CR before final CRLF", '\r', (char) out[i]);
                Assert.assertNotEquals("should not contain LF before final CRLF", '\n', (char) out[i]);
            }

            buf.clear();
            char[] big = new char[300];
            for (int i = 0; i < big.length; i++) {
                big[i] = 'x';
            }
            w.error("ERR " + new String(big));
            byte[] limited = readAll(buf);
            Assert.assertEquals("dash + 256 chars + CRLF", 1 + 256 + 2, limited.length);
        } finally {
            buf.release();
        }
    }

    private static byte[] readAll(ByteBuf buf) {
        byte[] out = new byte[buf.readableBytes()];
        buf.readBytes(out);
        return out;
    }

    private static byte[] ascii(String s) {
        return s.getBytes(StandardCharsets.US_ASCII);
    }

    private static byte[] concat(byte[]... parts) {
        int total = 0;
        for (byte[] p : parts) {
            total += p.length;
        }
        byte[] out = new byte[total];
        int pos = 0;
        for (byte[] p : parts) {
            System.arraycopy(p, 0, out, pos, p.length);
            pos += p.length;
        }
        return out;
    }
}
