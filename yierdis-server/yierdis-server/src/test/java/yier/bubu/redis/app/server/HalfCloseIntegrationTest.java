package yier.bubu.redis.app.server;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.protocol.resp.RespClientCodec;
import yier.bubu.redis.protocol.resp.RespProtocolLimits;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * 客户端半关闭（shutdownOutput）后，EOF 前已完整收到的命令必须全部执行并按序回写，
 * 再关闭连接。覆盖 issue #186。
 */
public class HalfCloseIntegrationTest {
    @Test
    public void pipelineThenImmediateHalfCloseReturnsEveryReplyInOrderAndAppliesWrites() throws Exception {
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(TestServerConfigs.config());
             Socket socket = connect(server)) {
            writeRaw(socket, join(
                    command("PING"),
                    command("SET", "hc", "1"),
                    command("GET", "hc")
            ));
            socket.shutdownOutput();

            Assert.assertEquals("+PONG\r\n", readAsciiFrame(socket));
            Assert.assertEquals("+OK\r\n", readAsciiFrame(socket));
            Assert.assertEquals("$1\r\n1\r\n", readAsciiFrame(socket));
            assertEof(socket);
        }
    }

    @Test
    public void largePipelineThenHalfCloseReturnsOneReplyPerCommand() throws Exception {
        int commandCount = 4_000;
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(TestServerConfigs.config());
             Socket socket = connect(server)) {
            ByteArrayOutputStream pipeline = new ByteArrayOutputStream();
            for (int index = 0; index < commandCount; index++) {
                pipeline.writeBytes(command("SET", "k" + index, "v" + index));
            }
            writeRaw(socket, pipeline.toByteArray());
            socket.shutdownOutput();

            for (int index = 0; index < commandCount; index++) {
                Assert.assertEquals("+OK\r\n", readAsciiFrame(socket));
            }
            assertEof(socket);

            try (Socket verify = connect(server)) {
                writeRaw(verify, command("GET", "k0"));
                Assert.assertEquals("$2\r\nv0\r\n", readAsciiFrame(verify));
                String lastValue = "v" + (commandCount - 1);
                writeRaw(verify, command("GET", "k" + (commandCount - 1)));
                Assert.assertEquals("$" + lastValue.length() + "\r\n" + lastValue + "\r\n", readAsciiFrame(verify));
            }
        }
    }

    @Test
    public void multiExecThenHalfCloseReturnsExecArrayReply() throws Exception {
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(TestServerConfigs.config());
             Socket socket = connect(server)) {
            writeRaw(socket, join(
                    command("MULTI"),
                    command("SET", "tx", "1"),
                    command("GET", "tx"),
                    command("EXEC")
            ));
            socket.shutdownOutput();

            Assert.assertEquals("+OK\r\n", readAsciiFrame(socket));
            Assert.assertEquals("+QUEUED\r\n", readAsciiFrame(socket));
            Assert.assertEquals("+QUEUED\r\n", readAsciiFrame(socket));
            RespClientCodec.RespReply exec = readReply(socket);
            Assert.assertEquals(RespClientCodec.RespReply.Kind.ARRAY, exec.kind());
            Assert.assertEquals(2, exec.values().size());
            Assert.assertEquals(RespClientCodec.RespReply.Kind.SIMPLE_STRING, exec.values().get(0).kind());
            Assert.assertEquals("OK", exec.values().get(0).text());
            Assert.assertEquals(RespClientCodec.RespReply.Kind.BULK_STRING, exec.values().get(1).kind());
            Assert.assertArrayEquals("1".getBytes(StandardCharsets.UTF_8), exec.values().get(1).bytes());
            assertEof(socket);
        }
    }

    @Test
    public void multiWithoutExecThenHalfCloseDoesNotApplyQueuedWrites() throws Exception {
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(TestServerConfigs.config());
             Socket socket = connect(server)) {
            writeRaw(socket, join(
                    command("MULTI"),
                    command("SET", "abandoned", "1")
            ));
            socket.shutdownOutput();

            Assert.assertEquals("+OK\r\n", readAsciiFrame(socket));
            Assert.assertEquals("+QUEUED\r\n", readAsciiFrame(socket));
            assertEof(socket);

            try (Socket verify = connect(server)) {
                writeRaw(verify, command("GET", "abandoned"));
                Assert.assertTrue(readReply(verify).isNull());
            }
        }
    }

    @Test
    public void completeCommandPlusTrailingIncompleteFrameThenHalfCloseDropsTheFragmentQuietly() throws Exception {
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(TestServerConfigs.config());
             Socket socket = connect(server)) {
            writeRaw(socket, join(
                    command("PING"),
                    "*2\r\n$3\r\nSET\r\n".getBytes(StandardCharsets.US_ASCII)
            ));
            socket.shutdownOutput();

            Assert.assertEquals("+PONG\r\n", readAsciiFrame(socket));
            assertEof(socket);
        }
    }

    @Test
    public void fullCloseReleasesClientSlotPromptly() throws Exception {
        try (YierdisServerBootstrap server = YierdisServerBootstrap.start(TestServerConfigs.config("--maxClients", "2"))) {
            ChildChannelRegistry registry = server.childChannelRegistryForTests();
            try (Socket socket = connect(server)) {
                writeRaw(socket, command("PING"));
                Assert.assertEquals("+PONG\r\n", readAsciiFrame(socket));
                awaitActiveClients(registry, 1);
            }
            awaitActiveClients(registry, 0);
        }
    }

    private static Socket connect(YierdisServerBootstrap server) throws IOException {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress("127.0.0.1", server.port()), 2_000);
        socket.setSoTimeout(5_000);
        return socket;
    }

    private static void writeRaw(Socket socket, byte[] bytes) throws IOException {
        OutputStream out = socket.getOutputStream();
        out.write(bytes);
        out.flush();
    }

    private static byte[] command(String... arguments) {
        ByteArrayOutputStream frame = new ByteArrayOutputStream();
        writeAscii(frame, "*" + arguments.length + "\r\n");
        for (String argument : arguments) {
            byte[] bytes = argument.getBytes(StandardCharsets.UTF_8);
            writeAscii(frame, "$" + bytes.length + "\r\n");
            frame.writeBytes(bytes);
            writeAscii(frame, "\r\n");
        }
        return frame.toByteArray();
    }

    private static byte[] join(byte[]... parts) {
        ByteArrayOutputStream joined = new ByteArrayOutputStream();
        for (byte[] part : parts) {
            joined.writeBytes(part);
        }
        return joined.toByteArray();
    }

    private static void writeAscii(ByteArrayOutputStream target, String value) {
        target.writeBytes(value.getBytes(StandardCharsets.US_ASCII));
    }

    private static String readAsciiFrame(Socket socket) throws IOException {
        return readFrame(socket.getInputStream());
    }

    private static String readFrame(InputStream in) throws IOException {
        ByteArrayOutputStream frame = new ByteArrayOutputStream();
        int marker = in.read();
        if (marker < 0) {
            throw new IOException("unexpected EOF before RESP frame");
        }
        frame.write(marker);
        switch (marker) {
            case '+', '-', ':':
                frame.writeBytes(readLineIncludingCrlf(in));
                break;
            case '$':
                readBulk(in, frame);
                break;
            case '*':
                readAggregate(in, frame);
                break;
            default:
                throw new IOException("unexpected RESP frame marker: " + (char) marker);
        }
        return frame.toString(StandardCharsets.UTF_8);
    }

    private static void readBulk(InputStream in, ByteArrayOutputStream frame) throws IOException {
        byte[] header = readLineIncludingCrlf(in);
        frame.writeBytes(header);
        int length = parseLineInt(header);
        if (length < 0) {
            return;
        }
        byte[] payload = in.readNBytes(length);
        if (payload.length != length) {
            throw new IOException("unexpected EOF in RESP bulk payload");
        }
        frame.writeBytes(payload);
        expectCrlf(in, frame);
    }

    private static void readAggregate(InputStream in, ByteArrayOutputStream frame) throws IOException {
        byte[] header = readLineIncludingCrlf(in);
        frame.writeBytes(header);
        int entries = parseLineInt(header);
        if (entries < 0) {
            return;
        }
        for (int index = 0; index < entries; index++) {
            frame.writeBytes(readFrame(in).getBytes(StandardCharsets.UTF_8));
        }
    }

    private static byte[] readLineIncludingCrlf(InputStream in) throws IOException {
        ByteArrayOutputStream line = new ByteArrayOutputStream();
        int previous = -1;
        for (; ; ) {
            int next = in.read();
            if (next < 0) {
                throw new IOException("unexpected EOF before RESP line terminator");
            }
            line.write(next);
            if (previous == '\r' && next == '\n') {
                return line.toByteArray();
            }
            previous = next;
        }
    }

    private static int parseLineInt(byte[] line) throws IOException {
        if (line.length < 2 || line[line.length - 2] != '\r' || line[line.length - 1] != '\n') {
            throw new IOException("invalid RESP numeric line");
        }
        return Integer.parseInt(new String(line, 0, line.length - 2, StandardCharsets.US_ASCII));
    }

    private static void expectCrlf(InputStream in, ByteArrayOutputStream frame) throws IOException {
        int cr = in.read();
        int lf = in.read();
        if (cr != '\r' || lf != '\n') {
            throw new IOException("expected RESP CRLF terminator");
        }
        frame.write(cr);
        frame.write(lf);
    }

    private static void assertEof(Socket socket) throws IOException {
        Assert.assertEquals("expected server to close after flushing half-close replies", -1, socket.getInputStream().read());
    }

    private static RespClientCodec.RespReply readReply(Socket socket) throws IOException {
        return RespClientCodec.readReply(socket.getInputStream(), RespProtocolLimits.DEFAULT_MAX_BULK_BYTES);
    }

    private static void awaitActiveClients(ChildChannelRegistry registry, int expected) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2L);
        while (registry.activeChannelCount() != expected && System.nanoTime() < deadline) {
            Thread.sleep(10L);
        }
        Assert.assertEquals(expected, registry.activeChannelCount());
    }
}
