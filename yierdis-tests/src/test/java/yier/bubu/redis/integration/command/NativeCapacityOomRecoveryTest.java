package yier.bubu.redis.integration.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.app.server.YierdisServerBootstrap;
import yier.bubu.redis.protocol.resp.RespClientCodec;
import yier.bubu.redis.protocol.resp.RespClientCodec.RespReply;
import yier.bubu.redis.protocol.resp.RespProtocolLimits;
import yier.bubu.redis.storage.api.MaxmemoryErrors;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class NativeCapacityOomRecoveryTest {
    @Test
    public void nativeSlotExhaustionLeavesConnectionUsableAfterExactOomReply() throws Exception {
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(
                "--port", "0",
                "--maxmemoryBytes", "0",
                "--databases", "1",
                "--noCleanup",
                "--nativeSlotCapacity", "5"
        );
             Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("127.0.0.1", server.port()), 2000);
            socket.setSoTimeout(3000);

            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            String committedKey = null;
            String committedValue = null;
            String failedKey = null;
            for (int i = 0; i < 32; i++) {
                String key = "native-slot-oom:" + i;
                String value = "value-" + i;
                writeCommand(out, "SET", key, value);
                out.flush();

                RespReply reply = readReply(in);
                if (reply.kind() == RespReply.Kind.SIMPLE_STRING && "OK".equals(reply.text())) {
                    committedKey = key;
                    committedValue = value;
                    continue;
                }

                Assert.assertEquals(RespReply.Kind.ERROR, reply.kind());
                Assert.assertEquals(MaxmemoryErrors.OOM_ERR, reply.text());
                failedKey = key;
                break;
            }

            Assert.assertNotNull("expected at least one successful write before OOM", committedKey);
            Assert.assertNotNull("expected one write to exhaust native slots", failedKey);

            writeCommand(out, "GET", committedKey);
            out.flush();
            assertBulkString(readReply(in), committedValue);

            writeCommand(out, "PING");
            out.flush();
            assertSimpleString(readReply(in), "PONG");

            writeCommand(out, "GET", failedKey);
            out.flush();
            Assert.assertEquals(RespReply.Kind.NULL, readReply(in).kind());
        }
    }

    private static void writeCommand(OutputStream out, String... parts) throws IOException {
        RespClientCodec.writeCommand(
                out,
                Arrays.stream(parts).map(NativeCapacityOomRecoveryTest::bytes).toList()
        );
    }

    private static RespReply readReply(InputStream in) throws IOException {
        return RespClientCodec.readReply(in, RespProtocolLimits.DEFAULT_MAX_BULK_BYTES);
    }

    private static void assertSimpleString(RespReply reply, String expected) {
        Assert.assertEquals(RespReply.Kind.SIMPLE_STRING, reply.kind());
        Assert.assertEquals(expected, reply.text());
    }

    private static void assertBulkString(RespReply reply, String expected) {
        Assert.assertEquals(RespReply.Kind.BULK_STRING, reply.kind());
        Assert.assertArrayEquals(bytes(expected), reply.bytes());
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }
}
