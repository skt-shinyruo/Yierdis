package yier.bubu.redis.command;

// server 侧通用命令实现：包含 PING/ECHO/HELLO/INFO/QUIT 等基础命令，并通过 ReplyWriter 直接写回响应。

import yier.bubu.redis.protocol.Command;
import yier.bubu.redis.protocol.ReplyWriter;
import yier.bubu.redis.protocol.ServerSession;
import yier.bubu.redis.protocol.YierdisBuildInfo;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

final class ServerCommands {
    private static final byte[] HELLO_SERVER_KEY = "server".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HELLO_SERVER_VALUE = "yierdis".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HELLO_VERSION_KEY = "version".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HELLO_VERSION_VALUE = YierdisBuildInfo.versionAsciiBytes();
    private static final byte[] HELLO_PROTO_KEY = "proto".getBytes(StandardCharsets.US_ASCII);
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

    private void info(Command cmd, ReplyWriter out) {
        ServerInfoProvider provider = support.infoProvider();
        if (provider == null) {
            out.error("ERR INFO not supported");
            return;
        }
        if (cmd.argc() != 1 && cmd.argc() != 2) {
            CommandSupport.wrongArity(out, "info");
            return;
        }
        provider.info(cmd, out);
    }

    private void stats(Command cmd, ReplyWriter out) {
        ServerInfoProvider provider = support.infoProvider();
        if (provider == null) {
            out.error("ERR STATS not supported");
            return;
        }
        provider.stats(cmd, out);
    }

    private void ping(Command cmd, ReplyWriter out) {
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

    private void echo(Command cmd, ReplyWriter out) {
        if (cmd.argc() != 2) {
            CommandSupport.wrongArity(out, "echo");
            return;
        }
        out.bulkString(cmd.toByteArray(1));
    }

    private void hello(Command cmd, ReplyWriter out) {
        // Custom protocol: HELLO is informational only (best-effort compatibility for existing clients/tests).
        // Optional argv[1] is accepted but does not negotiate a wire protocol version.
        if (cmd.argc() != 1 && cmd.argc() != 2) {
            CommandSupport.wrongArity(out, "hello");
            return;
        }
        out.mapHeader(5);
        out.bulkString(HELLO_SERVER_KEY);
        out.bulkString(HELLO_SERVER_VALUE);
        out.bulkString(HELLO_VERSION_KEY);
        out.bulkString(HELLO_VERSION_VALUE);
        out.bulkString(HELLO_PROTO_KEY);
        out.integer(1);
        out.bulkString(HELLO_MODE_KEY);
        out.bulkString(HELLO_MODE_VALUE);
        out.bulkString(HELLO_ROLE_KEY);
        out.bulkString(HELLO_ROLE_VALUE);
    }

    private void select(Command cmd, ReplyWriter out) {
        if (cmd.argc() != 2) {
            CommandSupport.wrongArity(out, "select");
            return;
        }
        long idx;
        try {
            idx = CommandSupport.parseLong(cmd, 1, "index");
        } catch (IllegalArgumentException e) {
            out.error("ERR value is not an integer or out of range");
            return;
        }
        int dbIndex;
        if (idx < Integer.MIN_VALUE) {
            dbIndex = Integer.MIN_VALUE;
        } else if (idx > Integer.MAX_VALUE) {
            dbIndex = Integer.MAX_VALUE;
        } else {
            dbIndex = (int) idx;
        }

        int databases = support.databases();
        if (dbIndex < 0 || dbIndex >= databases) {
            out.error("ERR DB index is out of range");
            return;
        }

        if (out.session() instanceof ServerSession s) {
            s.setDbIndex(dbIndex);
        } else if (dbIndex != 0) {
            // 在没有连接态的场景（例如部分单元测试）下，仅允许 DB0。
            out.error("ERR DB index is out of range");
            return;
        }
        out.simpleString("OK");
    }

    private void quit(Command cmd, ReplyWriter out) {
        if (cmd.argc() != 1) {
            CommandSupport.wrongArity(out, "quit");
            return;
        }
        // Redis-compatible: reply OK, then close the connection.
        out.simpleString("OK");
        out.requestCloseAfterReply();
    }

    private void flushdb(Command cmd, ReplyWriter out) {
        if (cmd.argc() != 1 && cmd.argc() != 2) {
            CommandSupport.wrongArity(out, "flushdb");
            return;
        }
        if (cmd.argc() == 2) {
            // Redis 生态兼容：接受 SYNC/ASYNC（本实现为单线程执行器，二者语义等价）。
            if (!CommandSupport.asciiEqualsIgnoreCase(cmd, 1, "SYNC")
                    && !CommandSupport.asciiEqualsIgnoreCase(cmd, 1, "ASYNC")) {
                out.error("ERR syntax error");
                return;
            }
        }
        support.db(out).lifecycle().flushDb();
        out.simpleString("OK");
    }

    private static void command(Command cmd, ReplyWriter out, CommandRegistry registry) {
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
    private static void writeCommandInfo(ReplyWriter out, String nameUpper) {
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
