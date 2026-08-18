package yier.bubu.redis.app.server;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.app.server.args.YierdisServerRuntimeConfig;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import yier.bubu.redis.protocol.resp.RespClientCodec;

public class RespHandshakeIntegrationTest {
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
            String hello = readSome(in);
            Assert.assertTrue(hello.startsWith("%5\r\n"));
            Assert.assertTrue(hello.contains("$5\r\nproto\r\n:3\r\n"));

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
            RespClientCodec.RespReply hello3 = RespClientCodec.readReply(in, 1_024);
            Assert.assertEquals(RespClientCodec.RespReply.Kind.MAP, hello3.kind());

            out.write("*2\r\n$5\r\nHELLO\r\n$1\r\n2\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            RespClientCodec.RespReply hello2 = RespClientCodec.readReply(in, 1_024);
            Assert.assertEquals(RespClientCodec.RespReply.Kind.ARRAY, hello2.kind());
            Assert.assertEquals(10, hello2.values().size());
            Assert.assertArrayEquals(bytes("proto"), hello2.values().get(4).bytes());
            Assert.assertEquals(Long.valueOf(2L), hello2.values().get(5).integer());

            out.write("*1\r\n$4\r\nPING\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            RespClientCodec.RespReply pong = RespClientCodec.readReply(in, 1_024);
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
            RespClientCodec.RespReply hello = RespClientCodec.readReply(in, 1024);
            Assert.assertEquals(RespClientCodec.RespReply.Kind.MAP, hello.kind());
            Assert.assertEquals(10, hello.values().size());

            out.write(RespClientCodec.encodeCommand(List.of(
                    bytes("SADD"), bytes("members"), bytes("alpha"), bytes("beta"))));
            out.flush();
            Assert.assertEquals(Long.valueOf(2), RespClientCodec.readReply(in, 1024).integer());

            out.write(RespClientCodec.encodeCommand(List.of(bytes("SMEMBERS"), bytes("members"))));
            out.flush();
            RespClientCodec.RespReply members = RespClientCodec.readReply(in, 1024);
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
            String hello2 = readSome(in);
            Assert.assertTrue(hello2.startsWith("*10\r\n"));
            Assert.assertTrue(hello2.contains("$5\r\nproto\r\n:2\r\n"));

            out.write("*2\r\n$6\r\nCLIENT\r\n$7\r\nGETNAME\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            Assert.assertEquals("$5\r\nalpha\r\n", readAscii(in, 11));

            out.write("*2\r\n$5\r\nHELLO\r\n$1\r\n4\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            Assert.assertEquals("-NOPROTO unsupported protocol version\r\n", readAscii(in, 39));

            out.write(("*5\r\n$5\r\nHELLO\r\n$1\r\n3\r\n$4\r\nAUTH\r\n$7\r\ndefault\r\n$2\r\npw\r\n")
                    .getBytes(StandardCharsets.US_ASCII));
            out.flush();
            String authError = readSome(in);
            Assert.assertTrue(authError.contains("called without any password configured"));
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

    private static String readAscii(InputStream in, int len) throws Exception {
        return new String(in.readNBytes(len), StandardCharsets.US_ASCII);
    }

    private static String readSome(InputStream in) throws Exception {
        byte[] buf = new byte[256];
        int n = in.read(buf);
        return new String(buf, 0, n, StandardCharsets.US_ASCII);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static YierdisServerRuntimeConfig serverConfig() {
        return ServerConfig.fromArgs(new String[]{
                "--port", "0",
                "--maxmemoryBytes", "0"
        });
    }
}
