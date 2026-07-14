package yier.bubu.redis.integration.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.app.server.YierdisServerBootstrap;
import yier.bubu.redis.storage.api.MaxmemoryErrors;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class NativeCapacityOomRecoveryTest {
    private static final String OOM_REPLY = "-" + MaxmemoryErrors.OOM_ERR + "\r";

    @Test
    public void nativeSlotExhaustionLeavesConnectionUsableAfterExactOomReply() throws Exception {
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(
                "--port", "0",
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

                String reply = readLine(in);
                if ("+OK\r".equals(reply)) {
                    committedKey = key;
                    committedValue = value;
                    continue;
                }

                Assert.assertEquals(OOM_REPLY, reply);
                failedKey = key;
                break;
            }

            Assert.assertNotNull("expected at least one successful write before OOM", committedKey);
            Assert.assertNotNull("expected one write to exhaust native slots", failedKey);

            writeCommand(out, "GET", committedKey);
            out.flush();
            Assert.assertEquals(committedValue, readBulkString(in));

            writeCommand(out, "PING");
            out.flush();
            Assert.assertEquals("+PONG\r", readLine(in));

            writeCommand(out, "GET", failedKey);
            out.flush();
            Assert.assertNull(readBulkString(in));
        }
    }

    private static void writeCommand(OutputStream out, String... parts) throws IOException {
        StringBuilder frame = new StringBuilder();
        frame.append('*').append(parts.length).append("\r\n");
        for (String part : parts) {
            byte[] bytes = part.getBytes(StandardCharsets.US_ASCII);
            frame.append('$').append(bytes.length).append("\r\n");
            frame.append(part).append("\r\n");
        }
        out.write(frame.toString().getBytes(StandardCharsets.US_ASCII));
    }

    private static String readBulkString(InputStream in) throws IOException {
        String header = readLine(in);
        if ("$-1\r".equals(header)) {
            return null;
        }
        Assert.assertTrue(header, header.startsWith("$"));
        int len = Integer.parseInt(header.substring(1, header.length() - 1));
        byte[] payload = in.readNBytes(len);
        Assert.assertEquals(len, payload.length);
        Assert.assertEquals('\r', in.read());
        Assert.assertEquals('\n', in.read());
        return new String(payload, StandardCharsets.US_ASCII);
    }

    private static String readLine(InputStream in) throws IOException {
        byte[] buf = new byte[256];
        int n = 0;
        for (; ; ) {
            int b = in.read();
            if (b < 0) {
                throw new IOException("unexpected EOF before RESP line");
            }
            if (b == '\n') {
                return new String(buf, 0, n, StandardCharsets.US_ASCII);
            }
            if (n == buf.length) {
                byte[] grown = new byte[buf.length * 2];
                System.arraycopy(buf, 0, grown, 0, buf.length);
                buf = grown;
            }
            buf[n++] = (byte) b;
        }
    }
}
