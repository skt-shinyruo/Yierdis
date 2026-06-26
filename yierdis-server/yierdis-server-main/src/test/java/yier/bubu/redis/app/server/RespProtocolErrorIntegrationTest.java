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

    @Test
    public void protocolErrorDropsPipelinedWriteInSamePacket() throws Exception {
        String key = "pepk";
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(
                "--port", "0",
                "--protocolMaxBulkBytes", "4"
        );
             Socket bad = new Socket()) {
            bad.connect(new InetSocketAddress("127.0.0.1", server.port()), 2000);
            bad.setSoTimeout(2000);

            OutputStream out = bad.getOutputStream();
            InputStream in = bad.getInputStream();

            out.write((
                    "*1\r\n$5\r\nabcde\r\n" +
                            "*3\r\n$3\r\nSET\r\n$" + key.length() + "\r\n" + key + "\r\n$1\r\n1\r\n"
            ).getBytes(StandardCharsets.US_ASCII));
            out.flush();

            String error = readLine(in);
            Assert.assertTrue(error, error.startsWith("-ERR Protocol error"));
            Assert.assertEquals("protocol error should close the first connection", -1, in.read());

            try (Socket verify = new Socket()) {
                verify.connect(new InetSocketAddress("127.0.0.1", server.port()), 2000);
                verify.setSoTimeout(2000);
                verify.getOutputStream().write(("*2\r\n$3\r\nGET\r\n$" + key.length() + "\r\n" + key + "\r\n")
                        .getBytes(StandardCharsets.US_ASCII));
                verify.getOutputStream().flush();

                Assert.assertEquals("$-1\r", readLine(verify.getInputStream()));
            }
        }
    }

    @Test
    public void oversizedTotalCommandBytesReturnsProtocolErrorAndClosesConnection() throws Exception {
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(
                "--port", "0",
                "--protocolMaxCommandBytes", "4"
        );
             Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("127.0.0.1", server.port()), 2000);
            socket.setSoTimeout(2000);

            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            out.write("*2\r\n$3\r\nGET\r\n$2\r\nab\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();

            String error = readLine(in);
            Assert.assertEquals("-ERR Protocol error: command is too large\r", error);
            Assert.assertEquals(-1, in.read());
        }
    }

    @Test
    public void malformedRespAfterHello3ReturnsProtocolErrorThenCloses() throws Exception {
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start("--port", "0");
             Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("127.0.0.1", server.port()), 2000);
            socket.setSoTimeout(2000);

            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            out.write("*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            readRespFrame(in);

            out.write("*1\r\nPING\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();

            String error = readLine(in);
            Assert.assertTrue(error, error.startsWith("-ERR Protocol error"));
            Assert.assertEquals(-1, in.read());
        }
    }

    private static void readRespFrame(InputStream in) throws IOException {
        int type = in.read();
        if (type == '%') {
            int pairs = Integer.parseInt(respLineValue(readLine(in)));
            for (int i = 0; i < pairs * 2; i++) {
                int entryType = in.read();
                if (entryType == '$') {
                    int len = Integer.parseInt(respLineValue(readLine(in)));
                    in.readNBytes(len);
                    if (in.read() != '\r' || in.read() != '\n') {
                        throw new IOException("expected CRLF after HELLO bulk string");
                    }
                    continue;
                }
                if (entryType == ':') {
                    readLine(in);
                    continue;
                }
                throw new IOException("unexpected HELLO map entry type: " + entryType);
            }
            return;
        }
        throw new IOException("unexpected RESP frame type: " + type);
    }

    private static String respLineValue(String line) {
        return line.endsWith("\r") ? line.substring(0, line.length() - 1) : line;
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
