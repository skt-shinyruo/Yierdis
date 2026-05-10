package yier.bubu.redis.app.server;

import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class RespHandshakeIntegrationTest {
    @Test
    public void hello3SwitchesConnectionToResp3() throws Exception {
        ServerConfig config = ServerConfig.fromArgs(new String[]{"--port", "0"});
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
    public void clientSetinfoSetnameAndGetnameAreAccepted() throws Exception {
        ServerConfig config = ServerConfig.fromArgs(new String[]{"--port", "0"});
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
}
