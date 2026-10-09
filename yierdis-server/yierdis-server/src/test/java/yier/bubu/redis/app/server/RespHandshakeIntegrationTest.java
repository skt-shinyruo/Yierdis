package yier.bubu.redis.app.server;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.app.server.args.YierdisServerRuntimeConfig;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import yier.bubu.redis.protocol.resp.RespClientCodec;
import yier.bubu.redis.protocol.resp.RespProtocolLimits;

public class RespHandshakeIntegrationTest {
    private static final String CLIENT_NAME_ERROR_LINE =
            "-ERR Client names cannot contain spaces, newlines or special characters.\r\n";
    private static final String WRONGPASS_LINE =
            "-WRONGPASS invalid username-password pair or user is disabled.\r\n";

    @Test
    public void hello3SwitchesConnectionToResp3() throws Exception {
        YierdisServerRuntimeConfig config = serverConfig();
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(config);
             Socket socket = new Socket("127.0.0.1", server.port())) {
            socket.setSoTimeout(3000);
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            out.write("*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            RespClientCodec.RespReply hello = RespClientCodec.readReply(
                    in, RespProtocolLimits.DEFAULT_MAX_BULK_BYTES);
            Assert.assertEquals(RespClientCodec.RespReply.Kind.MAP, hello.kind());
            Assert.assertEquals(10, hello.values().size());
            Assert.assertArrayEquals(bytes("proto"), hello.values().get(4).bytes());
            Assert.assertEquals(Long.valueOf(3L), hello.values().get(5).integer());

            out.write("*1\r\n$4\r\nPING\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            Assert.assertEquals("+PONG\r\n", readAscii(in, 7));
        }
    }

    @Test
    public void helloCanSwitchFromResp3BackToResp2WithoutClosingTheConnection() throws Exception {
        YierdisServerRuntimeConfig config = serverConfig();
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(config);
             Socket socket = new Socket("127.0.0.1", server.port())) {
            socket.setSoTimeout(3000);
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            out.write("*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            RespClientCodec.RespReply hello3 = RespClientCodec.readReply(
                    in, RespProtocolLimits.DEFAULT_MAX_BULK_BYTES);
            Assert.assertEquals(RespClientCodec.RespReply.Kind.MAP, hello3.kind());

            out.write("*2\r\n$5\r\nHELLO\r\n$1\r\n2\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            RespClientCodec.RespReply hello2 = RespClientCodec.readReply(
                    in, RespProtocolLimits.DEFAULT_MAX_BULK_BYTES);
            Assert.assertEquals(RespClientCodec.RespReply.Kind.ARRAY, hello2.kind());
            Assert.assertEquals(10, hello2.values().size());
            Assert.assertArrayEquals(bytes("proto"), hello2.values().get(4).bytes());
            Assert.assertEquals(Long.valueOf(2L), hello2.values().get(5).integer());

            out.write("*1\r\n$4\r\nPING\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            RespClientCodec.RespReply pong = RespClientCodec.readReply(
                    in, RespProtocolLimits.DEFAULT_MAX_BULK_BYTES);
            Assert.assertEquals(RespClientCodec.RespReply.Kind.SIMPLE_STRING, pong.kind());
            Assert.assertEquals("PONG", pong.text());
        }
    }

    @Test
    public void builtInClientCodecReadsHelloMapAndSmembersSetInResp3() throws Exception {
        YierdisServerRuntimeConfig config = serverConfig();
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(config);
             Socket socket = new Socket("127.0.0.1", server.port())) {
            socket.setSoTimeout(3000);
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            out.write(RespClientCodec.encodeCommand(List.of(bytes("HELLO"), bytes("3"))));
            out.flush();
            RespClientCodec.RespReply hello = RespClientCodec.readReply(
                    in, RespProtocolLimits.DEFAULT_MAX_BULK_BYTES);
            Assert.assertEquals(RespClientCodec.RespReply.Kind.MAP, hello.kind());
            Assert.assertEquals(10, hello.values().size());

            out.write(RespClientCodec.encodeCommand(List.of(
                    bytes("SADD"), bytes("members"), bytes("alpha"), bytes("beta"))));
            out.flush();
            Assert.assertEquals(Long.valueOf(2), RespClientCodec.readReply(
                    in, RespProtocolLimits.DEFAULT_MAX_BULK_BYTES).integer());

            out.write(RespClientCodec.encodeCommand(List.of(bytes("SMEMBERS"), bytes("members"))));
            out.flush();
            RespClientCodec.RespReply members = RespClientCodec.readReply(
                    in, RespProtocolLimits.DEFAULT_MAX_BULK_BYTES);
            Assert.assertEquals(RespClientCodec.RespReply.Kind.SET, members.kind());
            Set<String> values = members.values().stream()
                    .map(value -> new String(value.bytes(), StandardCharsets.UTF_8))
                    .collect(Collectors.toSet());
            Assert.assertEquals(Set.of("alpha", "beta"), values);
        }
    }

    @Test
    public void hello2SetnameUnsupportedProtoAndAuthAreHandled() throws Exception {
        YierdisServerRuntimeConfig config = serverConfig();
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(config);
             Socket socket = new Socket("127.0.0.1", server.port())) {
            socket.setSoTimeout(3000);
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            out.write("*4\r\n$5\r\nHELLO\r\n$1\r\n2\r\n$7\r\nSETNAME\r\n$5\r\nalpha\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            RespClientCodec.RespReply hello2 = RespClientCodec.readReply(
                    in, RespProtocolLimits.DEFAULT_MAX_BULK_BYTES);
            Assert.assertEquals(RespClientCodec.RespReply.Kind.ARRAY, hello2.kind());
            Assert.assertEquals(10, hello2.values().size());
            Assert.assertArrayEquals(bytes("proto"), hello2.values().get(4).bytes());
            Assert.assertEquals(Long.valueOf(2L), hello2.values().get(5).integer());

            out.write("*2\r\n$6\r\nCLIENT\r\n$7\r\nGETNAME\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            Assert.assertEquals("$5\r\nalpha\r\n", readAscii(in, 11));

            out.write("*2\r\n$5\r\nHELLO\r\n$1\r\n4\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            Assert.assertEquals("-NOPROTO unsupported protocol version\r\n", readAscii(in, 39));

            out.write(("*5\r\n$5\r\nHELLO\r\n$1\r\n3\r\n$4\r\nAUTH\r\n$7\r\ndefault\r\n$2\r\npw\r\n")
                    .getBytes(StandardCharsets.US_ASCII));
            out.flush();
            RespClientCodec.RespReply authHello = RespClientCodec.readReply(
                    in, RespProtocolLimits.DEFAULT_MAX_BULK_BYTES);
            Assert.assertEquals(RespClientCodec.RespReply.Kind.MAP, authHello.kind());
            Assert.assertEquals(Long.valueOf(3L), authHello.values().get(5).integer());
        }
    }

    // 以下文案和顺序对照 Redis 8.9.241（未配置 requirepass）：SETNAME 校验在认证之前，
    // 两者任一失败都不切协议、不改名字。
    @Test
    public void helloAuthAndSetnameFailuresKeepProtocolAndName() throws Exception {
        YierdisServerRuntimeConfig config = serverConfig();
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(config);
             Socket socket = new Socket("127.0.0.1", server.port())) {
            socket.setSoTimeout(3000);
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            Assert.assertEquals("+OK\r\n", sendAscii(out, in, 5, "CLIENT", "SETNAME", "orig"));
            Assert.assertEquals(
                    WRONGPASS_LINE,
                    sendAscii(out, in, WRONGPASS_LINE.length(), "HELLO", "3", "AUTH", "bob", "x", "SETNAME", "next"));
            Assert.assertEquals(
                    CLIENT_NAME_ERROR_LINE,
                    sendAscii(out, in, CLIENT_NAME_ERROR_LINE.length(), "HELLO", "3", "SETNAME", "a b"));
            Assert.assertEquals(
                    CLIENT_NAME_ERROR_LINE,
                    sendAscii(out, in, CLIENT_NAME_ERROR_LINE.length(),
                            "HELLO", "3", "AUTH", "bob", "x", "SETNAME", "a\nb"));
            Assert.assertEquals("$4\r\norig\r\n", sendAscii(out, in, 10, "CLIENT", "GETNAME"));
            Assert.assertEquals("$-1\r\n", sendAscii(out, in, 5, "GET", "missing"));

            RespClientCodec.RespReply cleared = send(out, in, "HELLO", "2", "SETNAME", "");
            Assert.assertEquals(RespClientCodec.RespReply.Kind.ARRAY, cleared.kind());
            Assert.assertEquals("$-1\r\n", sendAscii(out, in, 5, "CLIENT", "GETNAME"));
        }
    }

    @Test
    public void helloOptionAndVersionErrorsUseRedisText() throws Exception {
        YierdisServerRuntimeConfig config = serverConfig();
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(config);
             Socket socket = new Socket("127.0.0.1", server.port())) {
            socket.setSoTimeout(3000);
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            assertErrorReply("ERR Syntax error in HELLO option 'AUTH'", send(out, in, "HELLO", "3", "AUTH", "default"));
            assertErrorReply("ERR Syntax error in HELLO option 'SETNAME'", send(out, in, "HELLO", "3", "SETNAME"));
            assertErrorReply("ERR Syntax error in HELLO option 'foo'", send(out, in, "HELLO", "3", "foo"));
            assertErrorReply(
                    "ERR Protocol version is not an integer or out of range", send(out, in, "HELLO", "x"));
            assertErrorReply(
                    "ERR Protocol version is not an integer or out of range", send(out, in, "HELLO", "02"));
            assertErrorReply("NOPROTO unsupported protocol version", send(out, in, "HELLO", "4"));
            Assert.assertEquals("$-1\r\n", sendAscii(out, in, 5, "GET", "missing"));
        }
    }

    // Redis 把 HELLO 排进事务：EXEC 数组里 HELLO 之前的回复按旧协议、HELLO 及之后的按新协议编码。
    @Test
    public void helloInsideMultiIsQueuedAndSwitchesProtocolMidExec() throws Exception {
        YierdisServerRuntimeConfig config = serverConfig();
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(config);
             Socket socket = new Socket("127.0.0.1", server.port())) {
            socket.setSoTimeout(3000);
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            Assert.assertEquals("+OK\r\n", sendAscii(out, in, 5, "MULTI"));
            Assert.assertEquals("+QUEUED\r\n", sendAscii(out, in, 9, "GET", "missing"));
            Assert.assertEquals("+QUEUED\r\n", sendAscii(out, in, 9, "HELLO", "3"));
            Assert.assertEquals("+QUEUED\r\n", sendAscii(out, in, 9, "GET", "missing"));
            RespClientCodec.RespReply toResp3 = send(out, in, "EXEC");
            Assert.assertEquals(RespClientCodec.RespReply.Kind.ARRAY, toResp3.kind());
            Assert.assertEquals(3, toResp3.values().size());
            Assert.assertEquals(RespClientCodec.RespReply.Kind.NULL, toResp3.values().get(0).kind());
            Assert.assertEquals(RespClientCodec.RespReply.Kind.MAP, toResp3.values().get(1).kind());
            Assert.assertEquals(Long.valueOf(3L), toResp3.values().get(1).values().get(5).integer());
            Assert.assertEquals(RespClientCodec.RespReply.Kind.NULL_TYPE, toResp3.values().get(2).kind());
            Assert.assertEquals("_\r\n", sendAscii(out, in, 3, "GET", "missing"));

            Assert.assertEquals("+OK\r\n", sendAscii(out, in, 5, "MULTI"));
            Assert.assertEquals("+QUEUED\r\n", sendAscii(out, in, 9, "GET", "missing"));
            Assert.assertEquals("+QUEUED\r\n", sendAscii(out, in, 9, "HELLO", "2"));
            Assert.assertEquals("+QUEUED\r\n", sendAscii(out, in, 9, "GET", "missing"));
            RespClientCodec.RespReply toResp2 = send(out, in, "EXEC");
            Assert.assertEquals(RespClientCodec.RespReply.Kind.ARRAY, toResp2.kind());
            Assert.assertEquals(RespClientCodec.RespReply.Kind.NULL_TYPE, toResp2.values().get(0).kind());
            Assert.assertEquals(RespClientCodec.RespReply.Kind.ARRAY, toResp2.values().get(1).kind());
            Assert.assertEquals(10, toResp2.values().get(1).values().size());
            Assert.assertEquals(RespClientCodec.RespReply.Kind.NULL, toResp2.values().get(2).kind());
            Assert.assertEquals("$-1\r\n", sendAscii(out, in, 5, "GET", "missing"));
        }
    }

    @Test
    public void helloSetnameErrorInsideMultiFailsOnlyThatCommand() throws Exception {
        YierdisServerRuntimeConfig config = serverConfig();
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(config);
             Socket socket = new Socket("127.0.0.1", server.port())) {
            socket.setSoTimeout(3000);
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            Assert.assertEquals("+OK\r\n", sendAscii(out, in, 5, "MULTI"));
            Assert.assertEquals("+QUEUED\r\n", sendAscii(out, in, 9, "HELLO", "3", "SETNAME", "a b"));
            Assert.assertEquals("+QUEUED\r\n", sendAscii(out, in, 9, "GET", "missing"));
            String expected = "*2\r\n" + CLIENT_NAME_ERROR_LINE + "$-1\r\n";
            Assert.assertEquals(expected, sendAscii(out, in, expected.length(), "EXEC"));
            Assert.assertEquals("$-1\r\n", sendAscii(out, in, 5, "CLIENT", "GETNAME"));
        }
    }

    @Test
    public void clientSetinfoSetnameAndGetnameAreAccepted() throws Exception {
        YierdisServerRuntimeConfig config = serverConfig();
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(config);
             Socket socket = new Socket("127.0.0.1", server.port())) {
            socket.setSoTimeout(3000);
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            out.write("*4\r\n$6\r\nCLIENT\r\n$7\r\nSETINFO\r\n$8\r\nLIB-NAME\r\n$8\r\ngo-redis\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            Assert.assertEquals("+OK\r\n", readAscii(in, 5));

            out.write("*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n$4\r\ntest\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            Assert.assertEquals("+OK\r\n", readAscii(in, 5));

            out.write("*2\r\n$6\r\nCLIENT\r\n$7\r\nGETNAME\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            Assert.assertEquals("$4\r\ntest\r\n", readAscii(in, 10));
        }
    }

    @Test
    public void maxClientsRejectionDeliversErrorBeforeClosing() throws Exception {
        YierdisServerRuntimeConfig config = TestServerConfigs.config(
                "--maxClients", "1"
        );
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(config);
             Socket first = new Socket("127.0.0.1", server.port());
             Socket second = new Socket("127.0.0.1", server.port())) {
            first.setSoTimeout(3000);
            second.setSoTimeout(3000);

            // 让第一条连接完成准入后再连第二条，避免两条连接竞争同一个 max-clients 名额。
            OutputStream firstOut = first.getOutputStream();
            firstOut.write("*1\r\n$4\r\nPING\r\n".getBytes(StandardCharsets.US_ASCII));
            firstOut.flush();
            Assert.assertEquals("+PONG\r\n", readAscii(first.getInputStream(), 7));

            InputStream secondIn = second.getInputStream();
            Assert.assertEquals("-ERR max number of clients reached\r\n", readAscii(secondIn, 36));
            Assert.assertEquals(-1, secondIn.read());
        }
    }

    private static RespClientCodec.RespReply send(OutputStream out, InputStream in, String... args) throws Exception {
        write(out, args);
        return RespClientCodec.readReply(in, RespProtocolLimits.DEFAULT_MAX_BULK_BYTES);
    }

    private static String sendAscii(OutputStream out, InputStream in, int len, String... args) throws Exception {
        write(out, args);
        return readAscii(in, len);
    }

    private static void write(OutputStream out, String... args) throws Exception {
        out.write(RespClientCodec.encodeCommand(Arrays.stream(args).map(RespHandshakeIntegrationTest::bytes).toList()));
        out.flush();
    }

    private static void assertErrorReply(String expected, RespClientCodec.RespReply reply) {
        Assert.assertEquals(RespClientCodec.RespReply.Kind.ERROR, reply.kind());
        Assert.assertEquals(expected, reply.text());
    }

    private static String readAscii(InputStream in, int len) throws Exception {
        return new String(in.readNBytes(len), StandardCharsets.US_ASCII);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static YierdisServerRuntimeConfig serverConfig() {
        return TestServerConfigs.config();
    }
}
