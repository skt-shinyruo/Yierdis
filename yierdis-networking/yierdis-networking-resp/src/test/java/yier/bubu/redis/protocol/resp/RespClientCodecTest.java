package yier.bubu.redis.protocol.resp;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class RespClientCodecTest {
    @Test
    public void encodesCommand() throws Exception {
        Assert.assertEquals("*2\r\n$4\r\nPING\r\n$3\r\nhey\r\n",
                ascii(RespClientCodec.encodeCommand(List.of(bytes("PING"), bytes("hey")))));
    }

    @Test
    public void readsSimpleBulkIntegerNullAndArrayReplies() throws Exception {
        Assert.assertTrue(RespClientCodec.readReply(in("+OK\r\n"), 1024).isSimpleString("OK"));
        Assert.assertArrayEquals(bytes("abc"), RespClientCodec.readReply(in("$3\r\nabc\r\n"), 1024).bytes());
        Assert.assertEquals(Long.valueOf(7), RespClientCodec.readReply(in(":7\r\n"), 1024).integer());
        Assert.assertTrue(RespClientCodec.readReply(in("$-1\r\n"), 1024).isNull());
        Assert.assertEquals(2, RespClientCodec.readReply(in("*2\r\n+OK\r\n:1\r\n"), 1024).values().size());
    }

    private static ByteArrayInputStream in(String s) {
        return new ByteArrayInputStream(s.getBytes(StandardCharsets.US_ASCII));
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.US_ASCII);
    }

    private static String ascii(byte[] bytes) {
        return new String(bytes, StandardCharsets.US_ASCII);
    }
}
