package yier.bubu.redis.command;

import yier.bubu.redis.protocol.RespCommand;
import yier.bubu.redis.protocol.RespProtocol;
import yier.bubu.redis.protocol.RespWriter;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Properties;

final class ServerCommands {
    private static final byte[] HELLO_SERVER_KEY = "server".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HELLO_SERVER_VALUE = "yierdis".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HELLO_VERSION_KEY = "version".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HELLO_VERSION_VALUE = loadVersionBytes();
    private static final byte[] HELLO_PROTO_KEY = "proto".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HELLO_PROTO_VALUE = "2".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HELLO_PROTO_VALUE_RESP3 = "3".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HELLO_MODE_KEY = "mode".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HELLO_MODE_VALUE = "standalone".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HELLO_ROLE_KEY = "role".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HELLO_ROLE_VALUE = "master".getBytes(StandardCharsets.US_ASCII);

    private final CommandSupport support;

    ServerCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    void register(CommandRegistry registry) {
        Objects.requireNonNull(registry, "registry");
        registry.register("PING", this::ping);
        registry.register("ECHO", this::echo);
        registry.register("HELLO", this::hello);
        registry.register("COMMAND", (cmd, out) -> out.emptyArray());
        registry.register("SELECT", this::select);
        registry.register("FLUSHDB", this::flushdb);
    }

    private void ping(RespCommand cmd, RespWriter out) {
        if (cmd.argc() == 1) {
            out.simpleString("PONG");
            return;
        }
        if (cmd.argc() == 2) {
            out.bulkString(cmd.toByteArray(1));
            return;
        }
        CommandSupport.wrongArity(out, "ping");
    }

    private void echo(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 2) {
            CommandSupport.wrongArity(out, "echo");
            return;
        }
        out.bulkString(cmd.toByteArray(1));
    }

    private void hello(RespCommand cmd, RespWriter out) {
        // Minimal HELLO implementation (RESP2 + RESP3 handshake).
        String version = cmd.argc() >= 2 ? CommandSupport.utf8(cmd, 1) : "2";
        if ("3".equals(version)) {
            // Switch the connection to RESP3 and return a map as required by RESP3 clients.
            out.setProtocol(RespProtocol.RESP3);
            out.mapHeader(5);
            out.bulkString(HELLO_SERVER_KEY);
            out.bulkString(HELLO_SERVER_VALUE);
            out.bulkString(HELLO_VERSION_KEY);
            out.bulkString(HELLO_VERSION_VALUE);
            out.bulkString(HELLO_PROTO_KEY);
            out.bulkString(HELLO_PROTO_VALUE_RESP3);
            out.bulkString(HELLO_MODE_KEY);
            out.bulkString(HELLO_MODE_VALUE);
            out.bulkString(HELLO_ROLE_KEY);
            out.bulkString(HELLO_ROLE_VALUE);
            return;
        }
        if (!"2".equals(version)) {
            out.error("ERR unsupported protocol version");
            return;
        }

        // Switch back to RESP2 when explicitly requested.
        out.setProtocol(RespProtocol.RESP2);
        out.arrayHeader(10);
        out.bulkString(HELLO_SERVER_KEY);
        out.bulkString(HELLO_SERVER_VALUE);
        out.bulkString(HELLO_VERSION_KEY);
        out.bulkString(HELLO_VERSION_VALUE);
        out.bulkString(HELLO_PROTO_KEY);
        out.bulkString(HELLO_PROTO_VALUE);
        out.bulkString(HELLO_MODE_KEY);
        out.bulkString(HELLO_MODE_VALUE);
        out.bulkString(HELLO_ROLE_KEY);
        out.bulkString(HELLO_ROLE_VALUE);
    }

    private void select(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 2) {
            CommandSupport.wrongArity(out, "select");
            return;
        }
        if (!cmd.isNull(1) && cmd.len(1) == 1 && cmd.byteAt(1, 0) == '0') {
            out.simpleString("OK");
            return;
        }
        out.error("ERR only DB 0 is supported");
    }

    private void flushdb(RespCommand cmd, RespWriter out) {
        support.db().flushDb();
        out.simpleString("OK");
    }

    private static byte[] loadVersionBytes() {
        String version = "unknown";
        try (InputStream in = ServerCommands.class.getResourceAsStream("/yierdis-version.properties")) {
            if (in != null) {
                Properties props = new Properties();
                props.load(in);
                String v = props.getProperty("version");
                if (v != null && !v.isBlank()) {
                    version = v.trim();
                }
            }
        } catch (IOException ignored) {
            // ignore
        }
        return version.getBytes(StandardCharsets.US_ASCII);
    }
}

