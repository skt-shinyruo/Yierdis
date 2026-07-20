package yier.bubu.redis.app.server;

import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class RespProtocolIntegrationTest {
    @Test
    public void serverAcceptsRedisCliStyleResp2Commands() throws Exception {
        ServerConfig config = ServerConfig.fromArgs(new String[]{
                "--port", "0",
                "--maxmemoryBytes", "0"
        });
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

    private static String readAscii(InputStream in, int len) throws Exception {
        byte[] bytes = in.readNBytes(len);
        return new String(bytes, StandardCharsets.US_ASCII);
    }
}
