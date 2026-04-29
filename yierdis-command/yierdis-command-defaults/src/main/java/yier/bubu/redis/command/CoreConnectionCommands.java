package yier.bubu.redis.command;

import yier.bubu.redis.contract.CommandContext;
import yier.bubu.redis.contract.ExecutionRequest;
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

    void register(CommandModule.Registration registration) {
        Objects.requireNonNull(registration, "registration");
        registration.register("PING", CommandDescriptor.of(-1, 0, 0, 0), CommandParsers.oneOfRequest("ping", 1, 2), this::ping);
        registration.register("ECHO", CommandDescriptor.of(2, 0, 0, 0), CommandParsers.exactRequest(2, "echo"), this::echo);
        registration.register(
                "COMMAND",
                CommandDescriptor.of(-1, 0, 0, 0),
                CommandParsers.minRequest(1, "command"),
                (cmd, ctx) -> command(cmd, ctx.out(), registration)
        );
        registration.register("SELECT", CommandDescriptor.of(2, 0, 0, 0), CommandParsers.exactRequest(2, "select"), this::select);
        registration.register("QUIT", CommandDescriptor.of(1, 0, 0, 0), CommandParsers.exactRequest(1, "quit"), this::quit);
        registration.register("FLUSHDB", CommandDescriptor.of(-1, 0, 0, 0), CommandParsers.oneOfRequest("flushdb", 1, 2), this::flushdb);
    }

    private void ping(ExecutionRequest request, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (request.argc() == 1) {
            out.simpleString("PONG");
            return;
        }
        if (request.argc() == 2) {
            out.bulkString(request.readOnlyByteArray(1));
            return;
        }
        CommandSupport.wrongArity(out, "ping");
    }

    private void echo(ExecutionRequest request, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (request.argc() != 2) {
            CommandSupport.wrongArity(out, "echo");
            return;
        }
        out.bulkString(request.readOnlyByteArray(1));
    }

    private void select(ExecutionRequest request, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (request.argc() != 2) {
            CommandSupport.wrongArity(out, "select");
            return;
        }
        long idx;
        try {
            idx = CommandSupport.parseLong(request, 1, "index");
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

    private void quit(ExecutionRequest request, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (request.argc() != 1) {
            CommandSupport.wrongArity(out, "quit");
            return;
        }
        out.simpleString("OK");
        out.requestCloseAfterReply();
    }

    private void flushdb(ExecutionRequest request, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (request.argc() != 1 && request.argc() != 2) {
            CommandSupport.wrongArity(out, "flushdb");
            return;
        }
        if (request.argc() == 2) {
            if (!CommandSupport.asciiEqualsIgnoreCase(request, 1, "SYNC")
                    && !CommandSupport.asciiEqualsIgnoreCase(request, 1, "ASYNC")) {
                out.error("ERR syntax error");
                return;
            }
        }
        support.db(ctx).lifecycle().flushDb();
        out.simpleString("OK");
    }

    private static void command(ExecutionRequest request, ReplyWriter out, CommandModule.Registration registration) {
        if (request.argc() == 1) {
            String[] names = registration.upperNamesSorted();
            out.arrayHeader(names.length);
            for (String upper : names) {
                CommandDescriptor descriptor = registration.descriptorByUpperName(upper);
                if (descriptor == null) {
                    out.nullArray();
                    continue;
                }
                writeCommandInfo(out, upper, descriptor);
            }
            return;
        }

        if (request.argc() == 2 && CommandSupport.asciiEqualsIgnoreCase(request, 1, "COUNT")) {
            out.integer(registration.commandCount());
            return;
        }

        if (request.argc() >= 2 && CommandSupport.asciiEqualsIgnoreCase(request, 1, "INFO")) {
            if (request.argc() == 2) {
                CommandSupport.wrongArity(out, "command");
                return;
            }
            int n = request.argc() - 2;
            out.arrayHeader(n);
            for (int i = 2; i < request.argc(); i++) {
                if (request.isNull(i) || request.len(i) <= 0) {
                    out.nullArray();
                    continue;
                }
                String upper = CommandSupport.utf8(request, i);
                if (upper == null || upper.isBlank()) {
                    out.nullArray();
                    continue;
                }
                upper = upper.trim().toUpperCase(Locale.ROOT);
                CommandDescriptor descriptor = registration.descriptorByUpperName(upper);
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
        Objects.requireNonNull(nameUpper, "nameUpper");
        Objects.requireNonNull(descriptor, "descriptor");
        out.arrayHeader(6);
        out.bulkString(nameUpper.toLowerCase(Locale.ROOT).getBytes(StandardCharsets.US_ASCII));
        out.integer(descriptor.arity());
        out.arrayHeader(0);
        out.integer(descriptor.firstKeyIndex());
        out.integer(descriptor.lastKeyIndex());
        out.integer(descriptor.keyStep());
    }
}
