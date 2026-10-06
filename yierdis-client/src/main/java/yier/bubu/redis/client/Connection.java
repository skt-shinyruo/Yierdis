package yier.bubu.redis.client;

import yier.bubu.redis.protocol.resp.RespClientCodec;
import yier.bubu.redis.protocol.resp.RespProtocolLimits;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PushbackInputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * 指向一台 standalone Yierdis 的阻塞连接。同一时刻只由一个调用方使用。
 * <p>
 * 原始命令返回 {@code String}、{@code Long}、{@code null} 或嵌套 {@code List}。
 * 服务端错误和 UTF-8 解码失败都读完当前回复，连接可以继续用。
 * 读超时、读写失败、超过 512 MiB 的 bulk，以及顶层 {@code %}、{@code ~}、{@code _} 会关掉连接，并且不自动重试。
 */
public final class Connection implements AutoCloseable {
    private enum Mode {
        NORMAL
    }

    private final Socket socket;
    private final PushbackInputStream in;
    private final OutputStream out;
    private final long commandTimeoutMillis;
    // 这条连接目前只有普通模式。之后的管道和事务仍是这一条通道上的模式，不是第二条连接。
    private final Mode mode = Mode.NORMAL;
    private int database;
    private boolean closed;

    private Connection(Socket socket, long commandTimeoutMillis) throws IOException {
        this.socket = socket;
        this.commandTimeoutMillis = commandTimeoutMillis;
        this.in = new PushbackInputStream(socket.getInputStream(), 1);
        this.out = socket.getOutputStream();
    }

    public static Connection connect() {
        return connect(ConnectionSettings.defaults());
    }

    public static Connection connect(String host, int port) {
        return connect(ConnectionSettings.defaults().withHost(host).withPort(port));
    }

    public static Connection connect(ConnectionSettings settings) {
        Objects.requireNonNull(settings, "settings");
        Socket socket = new Socket();
        try {
            socket.setTcpNoDelay(true);
            socket.connect(
                    new InetSocketAddress(settings.host(), settings.port()),
                    toSocketTimeoutMillis(settings.connectTimeoutMillis())
            );
            Connection connection = new Connection(socket, settings.commandTimeoutMillis());
            try {
                if (settings.database() != 0) {
                    // 非 0 的 DB 要在把连接交给调用方之前 SELECT 成功。失败时关掉 socket，调用方拿不到这条连接。
                    connection.command("SELECT", Integer.toString(settings.database()));
                }
                return connection;
            } catch (Throwable failure) {
                connection.close();
                throw failure;
            }
        } catch (IOException e) {
            closeQuietly(socket);
            throw new ConnectionException(
                    "failed to connect to " + settings.host() + ":" + settings.port(), e);
        }
    }

    public Object command(String... args) {
        return command(commandTimeoutMillis, args);
    }

    public Object command(long commandTimeoutMillis, String... args) {
        if (closed) {
            throw new IllegalStateException("connection is closed");
        }
        if (mode != Mode.NORMAL) {
            throw new IllegalStateException("connection is not in normal mode");
        }
        if (commandTimeoutMillis <= 0) {
            throw new IllegalArgumentException("commandTimeoutMillis must be > 0");
        }
        validateArgs(args);
        List<byte[]> encoded = encodeArgs(args);
        try {
            socket.setSoTimeout(toSocketTimeoutMillis(commandTimeoutMillis));
            RespClientCodec.writeCommand(out, encoded);
            out.flush();
        } catch (IOException e) {
            close();
            throw new ConnectionException("connection closed after a write failure", e);
        }
        try {
            Object value = readValue();
            noteSuccessfulCommand(args);
            return value;
        } catch (SocketTimeoutException e) {
            close();
            throw new CommandTimeoutException("timed out waiting for a reply", e);
        } catch (IOException e) {
            close();
            throw new ConnectionException("connection closed after a read failure", e);
        }
    }

    /**
     * 这条连接记录的 DB 下标。打开时的非 0 下标要等 SELECT 成功才记上；
     * 之后原始 {@code SELECT} 成功才更新，服务端错误不改变它。
     */
    public int database() {
        return database;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        closeQuietly(socket);
    }

    private Object readValue() throws IOException {
        int type = in.read();
        if (type < 0) {
            throw new IOException("unexpected EOF before RESP reply");
        }
        // 只看顶层回复的第一个字节。`%`、`~`、`_` 表示这条连接已经离开 RESP2，关掉它，不能交给 readReply 当成成功结果。
        if (type == '%' || type == '~' || type == '_') {
            throw new IOException("RESP3 reply marker: " + (char) type);
        }
        if (type == '+' || type == '-') {
            // codec 读 simple string 时会用替换字符吞掉非法 UTF-8。顶层 `+` / `-` 在帧读完后按 REPORT 解码。
            String text = decodeReplyText(readLineBody(in));
            if (type == '-') {
                throw new ServerException(text);
            }
            return text;
        }
        // `$` 和 `*` 仍交给 codec，这样 null bulk 和 null array 保持 null，连接继续可用。
        in.unread(type);
        return convert(RespClientCodec.readReply(in, RespProtocolLimits.DEFAULT_MAX_BULK_BYTES));
    }

    private static byte[] readLineBody(InputStream in) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        int previous = -1;
        while (true) {
            int current = in.read();
            if (current < 0) {
                throw new IOException("unexpected EOF before RESP line terminator");
            }
            if (previous == '\r' && current == '\n') {
                byte[] raw = buf.toByteArray();
                return Arrays.copyOf(raw, raw.length - 1);
            }
            buf.write(current);
            // 与 codec 一样，把尚未配对的 CR 算进上限。超限当读失败关掉连接，不在半行上重同步。
            if (buf.size() > RespProtocolLimits.DEFAULT_MAX_BULK_BYTES + 1) {
                throw new IOException("RESP line exceeds limit");
            }
            previous = current;
        }
    }

    private static Object convert(RespClientCodec.RespReply reply) {
        return switch (reply.kind()) {
            case SIMPLE_STRING -> reply.text();
            case BULK_STRING -> {
                byte[] bytes = reply.bytes();
                yield bytes == null ? null : decodeReplyText(bytes);
            }
            case INTEGER -> {
                Long integer = reply.integer();
                if (integer == null) {
                    throw new DecodeException("integer reply has no value", null);
                }
                yield integer;
            }
            case NULL -> null;
            // 顶层 `-` 已在 readValue 里抛出。数组元素里的错误没有单独的返回位置，保留服务端文本。
            case ERROR -> reply.text();
            case ARRAY, MAP, SET -> convertAggregate(reply.values());
        };
    }

    private static List<Object> convertAggregate(List<RespClientCodec.RespReply> values) {
        if (values == null) {
            throw new DecodeException("aggregate reply has no elements", null);
        }
        List<Object> converted = new ArrayList<>(values.size());
        for (int i = 0; i < values.size(); i++) {
            converted.add(convert(values.get(i)));
        }
        return converted;
    }

    private void noteSuccessfulCommand(String[] args) {
        if (isCommand(args[0], "QUIT")) {
            // QUIT 的回复先返回给调用方，本地再关掉。服务端错误在读回复时已经抛出，不会走到这里。
            close();
            return;
        }
        if (isCommand(args[0], "SELECT") && args.length >= 2) {
            Integer index = parseDbIndex(args[1]);
            if (index != null) {
                database = index;
            }
        }
    }

    private static void validateArgs(String[] args) {
        if (args == null || args.length == 0 || args[0] == null || args[0].isEmpty()) {
            throw new IllegalArgumentException("command is required");
        }
        for (int i = 1; i < args.length; i++) {
            if (args[i] == null) {
                throw new IllegalArgumentException("command argument is null");
            }
        }
    }

    private static List<byte[]> encodeArgs(String[] args) {
        // 全部参数先按 UTF-8 REPORT 编完，再写第一个字节。某个参数编不成时，连接上还没有任何半条命令。
        CharsetEncoder encoder = StandardCharsets.UTF_8.newEncoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        List<byte[]> encoded = new ArrayList<>(args.length);
        for (String arg : args) {
            ByteBuffer buffer;
            try {
                buffer = encoder.encode(CharBuffer.wrap(arg));
            } catch (CharacterCodingException e) {
                throw new DecodeException("command argument cannot be encoded as UTF-8", e);
            }
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            encoded.add(bytes);
        }
        return encoded;
    }

    private static String decodeReplyText(byte[] bytes) {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes))
                    .toString();
        } catch (CharacterCodingException e) {
            throw new DecodeException("reply text is not valid UTF-8", e);
        }
    }

    private static boolean isCommand(String name, String expected) {
        return name.equalsIgnoreCase(expected);
    }

    private static Integer parseDbIndex(String text) {
        try {
            long value = Long.parseLong(text);
            if (value < 0 || value > Integer.MAX_VALUE) {
                return null;
            }
            return (int) value;
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private static int toSocketTimeoutMillis(long timeoutMillis) {
        if (timeoutMillis <= 0) {
            throw new IllegalArgumentException("timeout must be > 0");
        }
        return timeoutMillis > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) timeoutMillis;
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
        }
    }
}
