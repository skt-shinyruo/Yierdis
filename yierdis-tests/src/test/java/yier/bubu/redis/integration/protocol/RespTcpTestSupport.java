package yier.bubu.redis.integration.protocol;

import org.junit.Assert;
import yier.bubu.redis.app.server.YierdisServerBootstrap;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public final class RespTcpTestSupport {
    private RespTcpTestSupport() {
    }

    public static Socket connect(YierdisServerBootstrap server) throws IOException {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress("127.0.0.1", server.port()), 2_000);
        socket.setSoTimeout(5_000);
        return socket;
    }

    public static void writeCommand(Socket socket, String... arguments) throws IOException {
        writeRaw(socket, command(arguments));
    }

    public static void writePipeline(Socket socket, String[]... commands) throws IOException {
        OutputStream out = socket.getOutputStream();
        for (String[] command : commands) {
            out.write(command(command));
        }
        out.flush();
    }

    public static void writeRaw(Socket socket, byte[] bytes) throws IOException {
        OutputStream out = socket.getOutputStream();
        out.write(bytes);
        out.flush();
    }

    public static byte[] command(String... arguments) {
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

    public static byte[] join(byte[]... parts) {
        ByteArrayOutputStream joined = new ByteArrayOutputStream();
        for (byte[] part : parts) {
            joined.writeBytes(part);
        }
        return joined.toByteArray();
    }

    public static String readFrame(Socket socket) throws IOException {
        return readFrame(socket.getInputStream());
    }

    public static String readFrame(InputStream in) throws IOException {
        ByteArrayOutputStream frame = new ByteArrayOutputStream();
        int marker = in.read();
        if (marker < 0) {
            throw new IOException("unexpected EOF before RESP frame");
        }
        frame.write(marker);
        switch (marker) {
            case '+', '-', ':', ',', '(', '#':
                frame.writeBytes(readLineIncludingCrlf(in));
                break;
            case '_':
                expectCrlf(in, frame);
                break;
            case '$', '!', '=':
                readBulkLike(in, frame);
                break;
            case '*', '~', '>':
                readAggregate(in, frame, 1);
                break;
            case '%', '|':
                readAggregate(in, frame, 2);
                break;
            default:
                throw new IOException("unexpected RESP frame marker: " + (char) marker);
        }
        return frame.toString(StandardCharsets.UTF_8);
    }

    public static String bulkPayload(String frame) {
        Assert.assertTrue("expected RESP bulk string: " + frame, frame.startsWith("$"));
        int lineEnd = frame.indexOf("\r\n");
        Assert.assertTrue("missing RESP bulk header terminator", lineEnd >= 0);
        int length = Integer.parseInt(frame.substring(1, lineEnd));
        Assert.assertTrue("expected non-null RESP bulk string", length >= 0);
        int payloadStart = lineEnd + 2;
        Assert.assertEquals("bulk reply length", payloadStart + length + 2, frame.length());
        return frame.substring(payloadStart, payloadStart + length);
    }

    public static void assertEof(Socket socket) throws IOException {
        Assert.assertEquals("expected server to close the connection", -1, socket.getInputStream().read());
    }

    public static String asciiRepeat(char value, int count) {
        char[] chars = new char[count];
        java.util.Arrays.fill(chars, value);
        return new String(chars);
    }

    private static void readBulkLike(InputStream in, ByteArrayOutputStream frame) throws IOException {
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

    private static void readAggregate(InputStream in, ByteArrayOutputStream frame, int multiplier) throws IOException {
        byte[] header = readLineIncludingCrlf(in);
        frame.writeBytes(header);
        int entries = parseLineInt(header);
        if (entries < 0) {
            return;
        }
        for (int index = 0; index < entries * multiplier; index++) {
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

    private static void writeAscii(ByteArrayOutputStream target, String value) {
        target.writeBytes(value.getBytes(StandardCharsets.US_ASCII));
    }
}
