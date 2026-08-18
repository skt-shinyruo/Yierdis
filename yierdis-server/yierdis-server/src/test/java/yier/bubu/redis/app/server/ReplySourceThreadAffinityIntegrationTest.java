package yier.bubu.redis.app.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.protocol.resp.RespClientCodec;
import yier.bubu.redis.protocol.resp.RespClientCodec.RespReply;
import yier.bubu.redis.protocol.resp.RespProtocolLimits;

public class ReplySourceThreadAffinityIntegrationTest {
    @Test
    public void getReplyCleanupReturnsTheNativePinToTheCommandOwnerBeforeShutdown() throws Exception {
        YierdisServerBootstrap server = YierdisServerBootstrap.start(
                "--port", "0",
                "--maxmemoryBytes", "0",
                "--noCleanup"
        );
        try {
            try (Socket socket = new Socket("127.0.0.1", server.port())) {
                socket.setSoTimeout(3_000);
                OutputStream out = socket.getOutputStream();
                InputStream in = socket.getInputStream();

                writeCommand(out, "SET", "key", "value");
                assertSimpleString(readReply(in), "OK");

                writeCommand(out, "GET", "key");
                assertBulkString(readReply(in), "value");

                writeCommand(out, "RPUSH", "list", "a", "bb");
                assertInteger(readReply(in), 2L);
                writeCommand(out, "LRANGE", "list", "0", "-1");
                assertBulkArray(readReply(in), "a", "bb");

                writeCommand(out, "HSET", "hash", "field", "value");
                assertInteger(readReply(in), 1L);
                writeCommand(out, "HGETALL", "hash");
                assertBulkArray(readReply(in), "field", "value");

                writeCommand(out, "SADD", "set", "a", "bb");
                assertInteger(readReply(in), 2L);
                writeCommand(out, "SMEMBERS", "set");
                RespReply members = readReply(in);
                Assert.assertEquals(RespReply.Kind.ARRAY, members.kind());
                Assert.assertEquals(2, members.values().size());

                writeCommand(out, "HSCAN", "hash", "0");
                assertScanReply(readReply(in), 2);
                writeCommand(out, "SSCAN", "set", "0");
                assertScanReply(readReply(in), 2);

                writeCommand(out, "QUIT");
                assertSimpleString(readReply(in), "OK");
                Assert.assertEquals(-1, in.read());
            }
        } finally {
            server.close();
        }
    }

    private static void writeCommand(OutputStream out, String... args) throws IOException {
        RespClientCodec.writeCommand(
                out,
                Arrays.stream(args).map(ReplySourceThreadAffinityIntegrationTest::bytes).toList()
        );
        out.flush();
    }

    private static RespReply readReply(InputStream in) throws IOException {
        return RespClientCodec.readReply(in, RespProtocolLimits.DEFAULT_MAX_BULK_BYTES);
    }

    private static void assertSimpleString(RespReply reply, String expected) {
        Assert.assertEquals(RespReply.Kind.SIMPLE_STRING, reply.kind());
        Assert.assertEquals(expected, reply.text());
    }

    private static void assertInteger(RespReply reply, long expected) {
        Assert.assertEquals(RespReply.Kind.INTEGER, reply.kind());
        Assert.assertEquals(Long.valueOf(expected), reply.integer());
    }

    private static void assertBulkString(RespReply reply, String expected) {
        Assert.assertEquals(RespReply.Kind.BULK_STRING, reply.kind());
        Assert.assertArrayEquals(bytes(expected), reply.bytes());
    }

    private static void assertBulkArray(RespReply reply, String... expected) {
        Assert.assertEquals(RespReply.Kind.ARRAY, reply.kind());
        List<RespReply> values = reply.values();
        Assert.assertEquals(expected.length, values.size());
        for (int index = 0; index < expected.length; index++) {
            assertBulkString(values.get(index), expected[index]);
        }
    }

    private static void assertScanReply(RespReply reply, int expectedElements) {
        Assert.assertEquals(RespReply.Kind.ARRAY, reply.kind());
        List<RespReply> outer = reply.values();
        Assert.assertEquals(2, outer.size());
        assertBulkString(outer.get(0), "0");
        Assert.assertEquals(RespReply.Kind.ARRAY, outer.get(1).kind());
        Assert.assertEquals(expectedElements, outer.get(1).values().size());
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }
}
