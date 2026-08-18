package yier.bubu.redis.app.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
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

                writeCommand(out, "RPUSH", "list", "a", "bb");
                Assert.assertEquals(2L, readReply(in));
                writeCommand(out, "LRANGE", "list", "0", "-1");
                assertBulkArray(readReply(in), "a", "bb");

                writeCommand(out, "HSET", "hash", "field", "value");
                Assert.assertEquals(1L, readReply(in));
                writeCommand(out, "HGETALL", "hash");
                assertBulkArray(readReply(in), "field", "value");

                writeCommand(out, "SADD", "set", "a", "bb");
                Assert.assertEquals(2L, readReply(in));
                writeCommand(out, "SMEMBERS", "set");
                Assert.assertEquals(2, ((List<?>) readReply(in)).size());

                writeCommand(out, "HSCAN", "hash", "0");
                assertScanReply(readReply(in), 2);
                writeCommand(out, "SSCAN", "set", "0");
                assertScanReply(readReply(in), 2);

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

    private static Object readReply(InputStream in) throws IOException {
        int marker = in.read();
        if (marker < 0) {
            throw new IOException("unexpected EOF while reading RESP reply");
        }
        String line = readLine(in);
        String value = line.substring(0, line.length() - 2);
        return switch (marker) {
            case '+' -> value;
            case ':' -> Long.parseLong(value);
            case '$' -> readBulk(in, Integer.parseInt(value));
            case '*' -> readArray(in, Integer.parseInt(value));
            case '-' -> throw new AssertionError(value);
            default -> throw new IOException("unsupported RESP marker: " + (char) marker);
        };
    }

    private static byte[] readBulk(InputStream in, int length) throws IOException {
        if (length < 0) {
            return null;
        }
        byte[] data = in.readNBytes(length);
        Assert.assertEquals(length, data.length);
        Assert.assertEquals('\r', in.read());
        Assert.assertEquals('\n', in.read());
        return data;
    }

    private static List<Object> readArray(InputStream in, int length) throws IOException {
        if (length < 0) {
            return null;
        }
        List<Object> values = new ArrayList<>(length);
        for (int index = 0; index < length; index++) {
            values.add(readReply(in));
        }
        return values;
    }

    private static void assertBulkArray(Object reply, String... expected) {
        Assert.assertTrue(reply instanceof List<?>);
        List<?> values = (List<?>) reply;
        Assert.assertEquals(expected.length, values.size());
        for (int index = 0; index < expected.length; index++) {
            Assert.assertArrayEquals(expected[index].getBytes(StandardCharsets.US_ASCII), (byte[]) values.get(index));
        }
    }

    private static void assertScanReply(Object reply, int expectedElements) {
        Assert.assertTrue(reply instanceof List<?>);
        List<?> outer = (List<?>) reply;
        Assert.assertEquals(2, outer.size());
        Assert.assertArrayEquals("0".getBytes(StandardCharsets.US_ASCII), (byte[]) outer.get(0));
        Assert.assertEquals(expectedElements, ((List<?>) outer.get(1)).size());
    }
}
