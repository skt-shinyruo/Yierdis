package yier.bubu.redis.command;

// server 侧通用命令实现：包含 PING/ECHO/HELLO/INFO/QUIT 等基础命令，并通过 RespWriter 直接写回响应。

import yier.bubu.redis.protocol.RespCommand;
import yier.bubu.redis.protocol.RespProtocol;
import yier.bubu.redis.protocol.RespWriter;
import yier.bubu.redis.protocol.YierdisBuildInfo;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

final class ServerCommands {
    private static final byte[] HELLO_SERVER_KEY = "server".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HELLO_SERVER_VALUE = "yierdis".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HELLO_VERSION_KEY = "version".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HELLO_VERSION_VALUE = YierdisBuildInfo.versionAsciiBytes();
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
        registry.register("COMMAND", (cmd, out) -> command(cmd, out, registry));
        registry.register("INFO", this::info);
        registry.register("STATS", this::stats);
        registry.register("SELECT", this::select);
        registry.register("QUIT", this::quit);
        registry.register("FLUSHDB", this::flushdb);
    }

    private void info(RespCommand cmd, RespWriter out) {
        ServerInfoProvider provider = support.infoProvider();
        if (provider == null) {
            out.error("ERR INFO not supported");
            return;
        }
        provider.info(cmd, out);
    }

    private void stats(RespCommand cmd, RespWriter out) {
        ServerInfoProvider provider = support.infoProvider();
        if (provider == null) {
            out.error("ERR STATS not supported");
            return;
        }
        provider.stats(cmd, out);
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

    private void quit(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 1) {
            CommandSupport.wrongArity(out, "quit");
            return;
        }
        // Redis-compatible: reply OK, then close the connection.
        out.simpleString("OK");
        out.requestCloseAfterReply();
    }

    private void flushdb(RespCommand cmd, RespWriter out) {
        support.db().flushDb();
        out.simpleString("OK");
    }

    private static void command(RespCommand cmd, RespWriter out, CommandRegistry registry) {
        if (cmd.argc() == 1) {
            // Redis-compatible shape: array of arrays.
            String[] names = registry.upperNamesSorted();
            out.arrayHeader(names.length);
            for (String upper : names) {
                writeCommandInfo(out, upper);
            }
            return;
        }

        if (cmd.argc() == 2 && CommandSupport.asciiEqualsIgnoreCase(cmd, 1, "COUNT")) {
            out.integer(registry.commandCount());
            return;
        }

        if (cmd.argc() >= 2 && CommandSupport.asciiEqualsIgnoreCase(cmd, 1, "INFO")) {
            if (cmd.argc() == 2) {
                CommandSupport.wrongArity(out, "command");
                return;
            }
            int n = cmd.argc() - 2;
            out.arrayHeader(n);
            for (int i = 2; i < cmd.argc(); i++) {
                if (cmd.isNull(i) || cmd.len(i) <= 0) {
                    out.nullArray();
                    continue;
                }
                String upper = CommandSupport.utf8(cmd, i);
                if (upper == null || upper.isBlank()) {
                    out.nullArray();
                    continue;
                }
                upper = upper.trim().toUpperCase(java.util.Locale.ROOT);
                if (!registry.containsUpperName(upper)) {
                    out.nullArray();
                    continue;
                }
                writeCommandInfo(out, upper);
            }
            return;
        }

        out.error("ERR syntax error");
    }

    /**
     * Writes a minimal Redis-like COMMAND INFO entry.
     * <p>
     * Format: [name, arity, flags, firstKey, lastKey, step]
     */
    private static void writeCommandInfo(RespWriter out, String nameUpper) {
        // We keep this minimal but compatible: flags are empty; key specs are best-effort.
        int arity = commandArity(nameUpper);
        int firstKey = firstKeyIndex(nameUpper);
        int lastKey = lastKeyIndex(nameUpper);
        int step = keyStep(nameUpper);

        out.arrayHeader(6);
        out.bulkString(nameUpper.toLowerCase(java.util.Locale.ROOT).getBytes(java.nio.charset.StandardCharsets.US_ASCII));
        out.integer(arity);
        out.arrayHeader(0); // flags
        out.integer(firstKey);
        out.integer(lastKey);
        out.integer(step);
    }

    private static int commandArity(String nameUpper) {
        if (nameUpper == null) {
            return -1;
        }
        switch (nameUpper) {
            // server / connection
            case "PING":
                return -1;
            case "ECHO":
                return 2;
            case "HELLO":
                return -1;
            case "COMMAND":
                return -1;
            case "INFO":
                return -1;
            case "STATS":
                return 1;
            case "SELECT":
                return 2;
            case "QUIT":
            case "FLUSHDB":
                return 1;

            // key space
            case "TYPE":
            case "KEYS":
            case "TTL":
            case "GET":
            case "STRLEN":
            case "INCR":
            case "DECR":
            case "SMEMBERS":
            case "SCARD":
            case "HGETALL":
            case "HLEN":
                return 2;
            case "EXPIRE":
            case "APPEND":
            case "HGET":
            case "SISMEMBER":
            case "GETBIT":
                return 3;
            case "SETBIT":
            case "LRANGE":
            case "ZREMRANGEBYRANK":
            case "ZREMRANGEBYSCORE":
                return 4;

            // subcommands / variable arity
            case "DEL":
            case "EXISTS":
            case "MEMORY":
            case "OBJECT":
            case "BITCOUNT":
            case "LPOP":
            case "RPOP":
            case "PFCOUNT":
                return -2;
            case "SET":
            case "LPUSH":
            case "RPUSH":
            case "SADD":
            case "SREM":
            case "HDEL":
            case "ZREM":
            case "PFADD":
            case "PFMERGE":
                return -3;
            case "HSET":
            case "ZADD":
            case "ZRANGE":
            case "ZREVRANGE":
            case "ZRANGEBYSCORE":
            case "ZREVRANGEBYSCORE":
                return -4;

            default:
                return -1;
        }
    }

    private static int firstKeyIndex(String nameUpper) {
        if (nameUpper == null) {
            return 0;
        }
        switch (nameUpper) {
            // no keys / non-key arguments / movable keys
            case "PING":
            case "ECHO":
            case "HELLO":
            case "COMMAND":
            case "INFO":
            case "STATS":
            case "QUIT":
            case "FLUSHDB":
            case "SELECT":
            case "KEYS":
            case "MEMORY":
            case "OBJECT":
                return 0;
            default:
                return 1;
        }
    }

    private static int lastKeyIndex(String nameUpper) {
        if (nameUpper == null) {
            return 0;
        }
        switch (nameUpper) {
            // multi-key commands
            case "DEL":
            case "EXISTS":
            case "PFCOUNT":
            case "PFMERGE":
                return -1;

            // no keys / non-key arguments / movable keys
            case "PING":
            case "ECHO":
            case "HELLO":
            case "COMMAND":
            case "INFO":
            case "STATS":
            case "QUIT":
            case "FLUSHDB":
            case "SELECT":
            case "KEYS":
            case "MEMORY":
            case "OBJECT":
                return 0;

            default:
                return 1;
        }
    }

    private static int keyStep(String nameUpper) {
        if (nameUpper == null) {
            return 0;
        }
        switch (nameUpper) {
            case "PING":
            case "ECHO":
            case "HELLO":
            case "COMMAND":
            case "INFO":
            case "STATS":
            case "QUIT":
            case "FLUSHDB":
            case "SELECT":
            case "KEYS":
            case "MEMORY":
            case "OBJECT":
                return 0;
            default:
                return 1;
        }
    }
}
