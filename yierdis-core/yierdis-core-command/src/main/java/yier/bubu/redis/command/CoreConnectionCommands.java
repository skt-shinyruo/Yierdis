package yier.bubu.redis.command;

import yier.bubu.redis.contract.Command;
import yier.bubu.redis.contract.CommandContext;
import yier.bubu.redis.contract.ReplyWriter;
import yier.bubu.redis.contract.ServerSession;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Objects;

/**
 * Transport-agnostic connection and DB-lifecycle commands.
 * <p>
 * These commands remain in core because they do not depend on protocol-model
 * metadata or server runtime observability assembly.
 */
final class CoreConnectionCommands {
    private final CommandSupport support;

    CoreConnectionCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    void register(CommandRegistry registry) {
        Objects.requireNonNull(registry, "registry");
        registry.register("PING", this::ping);
        registry.register("ECHO", this::echo);
        registry.register("COMMAND", (cmd, ctx) -> command(cmd, ctx.out(), registry));
        registry.register("SELECT", this::select);
        registry.register("QUIT", this::quit);
        registry.register("FLUSHDB", this::flushdb);
    }

    private void ping(Command cmd, CommandContext ctx) {
        ReplyWriter out = ctx.out();
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

    private void echo(Command cmd, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (cmd.argc() != 2) {
            CommandSupport.wrongArity(out, "echo");
            return;
        }
        out.bulkString(cmd.toByteArray(1));
    }

    private void select(Command cmd, CommandContext ctx) {
        ReplyWriter out = ctx.out();
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

        ServerSession s = ctx.serverSessionOrNull();
        if (s != null) {
            s.setDbIndex(dbIndex);
        } else if (dbIndex != 0) {
            out.error("ERR DB index is out of range");
            return;
        }
        out.simpleString("OK");
    }

    private void quit(Command cmd, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (cmd.argc() != 1) {
            CommandSupport.wrongArity(out, "quit");
            return;
        }
        out.simpleString("OK");
        out.requestCloseAfterReply();
    }

    private void flushdb(Command cmd, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (cmd.argc() != 1 && cmd.argc() != 2) {
            CommandSupport.wrongArity(out, "flushdb");
            return;
        }
        if (cmd.argc() == 2) {
            if (!CommandSupport.asciiEqualsIgnoreCase(cmd, 1, "SYNC")
                    && !CommandSupport.asciiEqualsIgnoreCase(cmd, 1, "ASYNC")) {
                out.error("ERR syntax error");
                return;
            }
        }
        support.db(ctx).lifecycle().flushDb();
        out.simpleString("OK");
    }

    private static void command(Command cmd, ReplyWriter out, CommandRegistry registry) {
        if (cmd.argc() == 1) {
            String[] names = registry.upperNamesSorted();
            out.arrayHeader(names.length);
            for (String upper : names) {
                writeCommandInfo(out, upper, registry.descriptorByUpperName(upper));
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
                upper = upper.trim().toUpperCase(Locale.ROOT);
                CommandDescriptor descriptor = registry.descriptorByUpperName(upper);
                if (descriptor == null) {
                    out.nullArray();
                    continue;
                }
                writeCommandInfo(out, upper, descriptor);
            }
            return;
        }

        out.error("ERR syntax error");
    }

    private static void writeCommandInfo(ReplyWriter out, String nameUpper, CommandDescriptor descriptor) {
        CommandDescriptor effective = descriptor == null
                ? CommandDescriptor.defaultForNameUpper(nameUpper)
                : descriptor;
        out.arrayHeader(6);
        out.bulkString(nameUpper.toLowerCase(Locale.ROOT).getBytes(StandardCharsets.US_ASCII));
        out.integer(effective.arity());
        out.arrayHeader(0);
        out.integer(effective.firstKeyIndex());
        out.integer(effective.lastKeyIndex());
        out.integer(effective.keyStep());
    }
}
