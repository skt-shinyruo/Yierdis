package yier.bubu.redis.app.server;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class RespProtocolErrorIntegrationTest {
    @Test
    public void malformedRespReturnsProtocolErrorAndClosesConnection() throws Exception {
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start("--port", "0");
             Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("127.0.0.1", server.port()), 2000);
            socket.setSoTimeout(2000);

            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            out.write("*1\r\nPING\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();

            String error = readLine(in);
            Assert.assertTrue(error, error.startsWith("-ERR Protocol error"));
            Assert.assertEquals("malformed RESP should close the connection after the error reply", -1, in.read());
        }
    }

    private static String readLine(InputStream in) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        for (; ; ) {
            int b = in.read();
            if (b < 0) {
                throw new IOException("unexpected EOF before RESP line");
            }
            if (b == '\n') {
                return buf.toString(StandardCharsets.US_ASCII);
            }
            buf.write(b);
        }
    }
}
