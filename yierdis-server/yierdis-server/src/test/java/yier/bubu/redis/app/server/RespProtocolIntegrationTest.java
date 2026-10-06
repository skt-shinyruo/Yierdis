package yier.bubu.redis.app.server;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.app.server.args.YierdisServerRuntimeConfig;
import yier.bubu.redis.protocol.resp.RespClientCodec;
import yier.bubu.redis.protocol.resp.RespProtocolLimits;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class RespProtocolIntegrationTest {
    @Test
    public void serverAcceptsRedisCliStyleResp2Commands() throws Exception {
        YierdisServerRuntimeConfig config = TestServerConfigs.config();
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(config);
             Socket socket = new Socket("127.0.0.1", server.port())) {
            socket.setSoTimeout(3000);
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            out.write("*1\r\n$4\r\nPING\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            Assert.assertEquals("+PONG\r\n", readAscii(in, 7));

            out.write("*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            Assert.assertEquals("+OK\r\n", readAscii(in, 5));

            out.write("*2\r\n$3\r\nGET\r\n$1\r\na\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            Assert.assertEquals("$1\r\n1\r\n", readAscii(in, 7));
        }
    }

    @Test
    public void zeroLengthTokensAndInlineCommandsStayValid() throws Exception {
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(TestServerConfigs.config());
             Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("127.0.0.1", server.port()), 2000);
            socket.setSoTimeout(2000);
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            out.write("PING\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            Assert.assertEquals("+PONG\r\n", readAscii(in, 7));

            out.write("*0\r\n*2\r\n$4\r\nECHO\r\n$0\r\n\r\n*1\r\n$4\r\nPING\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            RespClientCodec.RespReply emptyArray = readReply(in);
            Assert.assertEquals(RespClientCodec.RespReply.Kind.ERROR, emptyArray.kind());
            Assert.assertEquals("ERR empty command", emptyArray.text());
            Assert.assertEquals("$0\r\n\r\n", readAscii(in, 6));
            Assert.assertEquals("+PONG\r\n", readAscii(in, 7));
        }
    }

    private static RespClientCodec.RespReply readReply(InputStream in) throws IOException {
        return RespClientCodec.readReply(in, RespProtocolLimits.DEFAULT_MAX_BULK_BYTES);
    }

    private static String readAscii(InputStream in, int len) throws Exception {
        byte[] bytes = in.readNBytes(len);
        return new String(bytes, StandardCharsets.US_ASCII);
    }
}
