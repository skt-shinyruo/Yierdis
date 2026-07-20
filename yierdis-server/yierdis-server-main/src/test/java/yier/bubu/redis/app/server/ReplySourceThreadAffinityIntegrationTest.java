package yier.bubu.redis.app.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import org.junit.Assert;
import org.junit.Test;

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
                Assert.assertEquals("+OK\r\n", readLine(in));

                writeCommand(out, "GET", "key");
                Assert.assertEquals("$5\r\n", readLine(in));
                Assert.assertEquals("value", new String(in.readNBytes(7), 0, 5, StandardCharsets.US_ASCII));

                writeCommand(out, "QUIT");
                Assert.assertEquals("+OK\r\n", readLine(in));
                Assert.assertEquals(-1, in.read());
            }
        } finally {
            server.close();
        }
    }

    private static void writeCommand(OutputStream out, String... args) throws IOException {
        StringBuilder command = new StringBuilder("*").append(args.length).append("\r\n");
        for (String arg : args) {
            byte[] bytes = arg.getBytes(StandardCharsets.US_ASCII);
            command.append('$').append(bytes.length).append("\r\n").append(arg).append("\r\n");
        }
        out.write(command.toString().getBytes(StandardCharsets.US_ASCII));
        out.flush();
    }

    private static String readLine(InputStream in) throws IOException {
        StringBuilder line = new StringBuilder();
        while (true) {
            int next = in.read();
            if (next < 0) {
                throw new IOException("unexpected EOF while reading RESP line");
            }
            line.append((char) next);
            if (next == '\n') {
                return line.toString();
            }
        }
    }
}
