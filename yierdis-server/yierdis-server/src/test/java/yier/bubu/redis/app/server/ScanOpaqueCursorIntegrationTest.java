package yier.bubu.redis.app.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.protocol.resp.RespClientCodec;
import yier.bubu.redis.protocol.resp.RespClientCodec.RespReply;
import yier.bubu.redis.protocol.resp.RespProtocolLimits;

/**
 * 入站/执行器缝：SCAN 家族把 cursor 当不透明整数。任意非负 cursor（包括内部 phase 位
 * 「看起来非法」的值）都必须得到两元素 scan 回复，而不是 {@code ERR internal error}，
 * 且连接不得因此关闭。
 */
public class ScanOpaqueCursorIntegrationTest {
    private static final String[] OPAQUE_CURSORS = {
            "8589934592",
            "12884901888",
            "9223372036854775807"
    };

    @Test
    public void scanFamilyAcceptsOpaqueCursorsAndKeepsTheConnection() throws Exception {
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

                for (int i = 0; i < 5; i++) {
                    writeCommand(out, "SET", "k" + i, "v");
                    assertSimpleString(readReply(in), "OK");
                }
                // key SCAN：不透明 cursor 按重启迭代处理（允许重复），结束仍回 0，且覆盖全部 key。
                for (String cursor : OPAQUE_CURSORS) {
                    assertScanRestartsAndCovers(out, in, cursor, 5);
                }

                writeCommand(out, "HSET", "hash", "f", "v");
                assertInteger(readReply(in), 1L);
                writeCommand(out, "SADD", "set", "m");
                assertInteger(readReply(in), 1L);
                writeCommand(out, "ZADD", "zset", "1", "m");
                assertInteger(readReply(in), 1L);

                // 集合 scan 与 key SCAN 一致：合法两元素 scan 窗口，而不是错误回复。
                for (String[] command : new String[][]{
                        {"HSCAN", "hash"}, {"SSCAN", "set"}, {"ZSCAN", "zset"}
                }) {
                    for (String cursor : OPAQUE_CURSORS) {
                        writeCommand(out, command[0], command[1], cursor);
                        assertScanWindow(readReply(in));
                    }
                }

                // 上述输入不得干掉连接：后续命令必须照常得到回答。
                writeCommand(out, "PING");
                assertSimpleString(readReply(in), "PONG");

                writeCommand(out, "QUIT");
                assertSimpleString(readReply(in), "OK");
                Assert.assertEquals(-1, in.read());
            }
        } finally {
            server.close();
        }
    }

    private static void assertScanRestartsAndCovers(
            OutputStream out,
            InputStream in,
            String initialCursor,
            int expectedKeys
    ) throws IOException {
        Set<String> seen = new HashSet<>();
        String cursor = initialCursor;
        for (int round = 0; round < 100; round++) {
            writeCommand(out, "SCAN", cursor, "COUNT", "3");
            List<RespReply> outer = assertScanWindow(readReply(in));
            cursor = new String(outer.get(0).bytes(), StandardCharsets.US_ASCII);
            for (RespReply element : outer.get(1).values()) {
                Assert.assertEquals(RespReply.Kind.BULK_STRING, element.kind());
                seen.add(new String(element.bytes(), StandardCharsets.US_ASCII));
            }
            if ("0".equals(cursor)) {
                Assert.assertEquals(
                        "SCAN from opaque cursor " + initialCursor + " must restart and cover every key",
                        expectedKeys,
                        seen.size()
                );
                return;
            }
        }
        Assert.fail("SCAN from opaque cursor " + initialCursor + " did not terminate at 0");
    }

    private static List<RespReply> assertScanWindow(RespReply reply) {
        Assert.assertNotEquals(
                "opaque cursor must not produce an error reply: " + reply.text(),
                RespReply.Kind.ERROR,
                reply.kind()
        );
        Assert.assertEquals(RespReply.Kind.ARRAY, reply.kind());
        List<RespReply> outer = reply.values();
        Assert.assertEquals(2, outer.size());
        Assert.assertEquals(RespReply.Kind.BULK_STRING, outer.get(0).kind());
        Assert.assertEquals(RespReply.Kind.ARRAY, outer.get(1).kind());
        return outer;
    }

    private static void writeCommand(OutputStream out, String... args) throws IOException {
        RespClientCodec.writeCommand(
                out,
                Arrays.stream(args).map(ScanOpaqueCursorIntegrationTest::bytes).toList()
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

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }
}
