package yier.bubu.redis.command.defaults.connection;

import yier.bubu.redis.command.api.ArgReader;
import yier.bubu.redis.command.api.CommandArity;
import yier.bubu.redis.command.api.CommandDescriptor;
import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.CommandParseError;
import yier.bubu.redis.command.api.CommandParseResult;
import yier.bubu.redis.command.api.CommandParsers;
import yier.bubu.redis.command.api.ServerInfoProvider;
import yier.bubu.redis.command.api.SlowCommandGovernor;
import yier.bubu.redis.command.defaults.BulkStringReplyAdapter;
import yier.bubu.redis.command.defaults.CommandSupport;

import yier.bubu.redis.execution.api.CommandContext;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.ReplyPlan;
import yier.bubu.redis.execution.api.ReplyPlans;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Objects;

/**
 * Transport-agnostic connection and DB-lifecycle commands.
 * <p>
 * These commands remain in core because they do not depend on protocol-model
 * metadata or server runtime observability assembly.
 */
public final class CoreConnectionCommands {
    private final CommandSupport support;

    public CoreConnectionCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    public void register(CommandModule.Registration registration) {
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
        registration.register("CLIENT", CommandDescriptor.of(-2, 0, 0, 0), CommandParsers.minRequest(2, "client"), this::client);
        registration.register("AUTH", CommandDescriptor.of(-2, 0, 0, 0), CommandParsers.minRequest(1, "auth"), this::auth);
        registration.register("FLUSHDB", CommandDescriptor.of(-1, 0, 0, 0), CommandParsers.oneOfRequest("flushdb", 1, 2), this::flushdb);
    }

    private void ping(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        if (request.argc() == 1) {
            out.simpleString("PONG");
            return;
        }
        if (request.argc() == 2) {
            writeRetainedArgument(request, 1, out);
            return;
        }
        CommandSupport.wrongArity(out, "ping");
    }

    private void echo(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        if (request.argc() != 2) {
            CommandSupport.wrongArity(out, "echo");
            return;
        }
        writeRetainedArgument(request, 1, out);
    }

    private void select(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
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

        ctx.dbIndexSession().setDbIndex(dbIndex);
        out.simpleString("OK");
    }

    private void quit(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        if (request.argc() != 1) {
            CommandSupport.wrongArity(out, "quit");
            return;
        }
        out.simpleString("OK");
        out.requestCloseAfterReply();
    }

    private void client(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        if (CommandSupport.asciiEqualsIgnoreCase(request, 1, "SETINFO")) {
            out.simpleString("OK");
            return;
        }
        if (CommandSupport.asciiEqualsIgnoreCase(request, 1, "SETNAME")) {
            if (request.argc() != 3) {
                CommandSupport.wrongArity(out, "client|setname");
                return;
            }
            ctx.clientMetadataSession().setClientName(CommandSupport.utf8(request, 2));
            out.simpleString("OK");
            return;
        }
        if (CommandSupport.asciiEqualsIgnoreCase(request, 1, "GETNAME")) {
            if (request.argc() != 2) {
                CommandSupport.wrongArity(out, "client|getname");
                return;
            }
            String name = ctx.clientMetadataSession().clientName();
            if (name == null) {
                out.nullValue();
            } else {
                byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
                out.requireReply(ReplyPlans.bulkString(nameBytes.length, 0L));
                out.bulkString(nameBytes);
            }
            return;
        }
        out.error("ERR unknown subcommand '" + CommandSupport.utf8(request, 1) + "'. Try CLIENT HELP.");
    }

    private void auth(ExecutionRequest request, CommandContext ctx) {
        ctx.out().error("ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?");
    }

    private void flushdb(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
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
        support.commandDb(ctx).lifecycle().flushDb();
        out.simpleString("OK");
    }

    private static void writeRetainedArgument(ExecutionRequest request, int index, RedisReplyWriter out) {
        ExecutionRequest retained = request.retain();
        boolean ownershipTransferred = false;
        try {
            out.requireReply(ReplyPlans.bulkString(request.len(index), retained.admittedMemoryBytes()));
            out.bulkString(request.readOnlyByteArray(index));
            out.transferReplyOwnership(retained);
            ownershipTransferred = true;
        } finally {
            if (!ownershipTransferred) {
                retained.close();
            }
        }
    }

    private static void command(ExecutionRequest request, RedisReplyWriter out, CommandModule.Registration registration) {
        if (request.argc() == 1) {
            String[] names = registration.upperNamesSorted();
            out.requireReply(commandListReplyPlan(names, registration));
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
            out.requireReply(commandInfoReplyPlan(request, registration));
            out.arrayHeader(n);
            for (int i = 2; i < request.argc(); i++) {
                String upper = commandInfoName(request, i);
                if (upper == null) {
                    out.nullArray();
                    continue;
                }
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

    private static ReplyPlan commandListReplyPlan(String[] names, CommandModule.Registration registration) {
        long encodedElementBytes = 0L;
        for (String upper : names) {
            CommandDescriptor descriptor = registration.descriptorByUpperName(upper);
            encodedElementBytes = saturatingAdd(
                    encodedElementBytes,
                    descriptor == null ? 5L : commandInfoEncodedBytes(upper, descriptor)
            );
        }
        return ReplyPlans.bulkStringArray(names.length, encodedElementBytes, 0L);
    }

    private static ReplyPlan commandInfoReplyPlan(ExecutionRequest request, CommandModule.Registration registration) {
        int count = request.argc() - 2;
        long encodedElementBytes = 0L;
        for (int index = 2; index < request.argc(); index++) {
            String upper = commandInfoName(request, index);
            CommandDescriptor descriptor = upper == null ? null : registration.descriptorByUpperName(upper);
            encodedElementBytes = saturatingAdd(
                    encodedElementBytes,
                    descriptor == null ? 5L : commandInfoEncodedBytes(upper, descriptor)
            );
        }
        return ReplyPlans.bulkStringArray(count, encodedElementBytes, 0L);
    }

    private static String commandInfoName(ExecutionRequest request, int index) {
        if (request.isNull(index) || request.len(index) <= 0) {
            return null;
        }
        String upper = CommandSupport.utf8(request, index);
        if (upper == null || upper.isBlank()) {
            return null;
        }
        return upper.trim().toUpperCase(Locale.ROOT);
    }

    private static long commandInfoEncodedBytes(String nameUpper, CommandDescriptor descriptor) {
        byte[] name = nameUpper.toLowerCase(Locale.ROOT).getBytes(StandardCharsets.US_ASCII);
        long encodedElementBytes = ReplyPlans.bulkString(name.length, 0L).encodedUpperBoundBytes();
        encodedElementBytes = saturatingAdd(encodedElementBytes, integerEncodedBytes(descriptor.arity()));
        encodedElementBytes = saturatingAdd(
                encodedElementBytes,
                ReplyPlans.bulkStringArray(0, 0L, 0L).encodedUpperBoundBytes()
        );
        encodedElementBytes = saturatingAdd(encodedElementBytes, integerEncodedBytes(descriptor.firstKeyIndex()));
        encodedElementBytes = saturatingAdd(encodedElementBytes, integerEncodedBytes(descriptor.lastKeyIndex()));
        encodedElementBytes = saturatingAdd(encodedElementBytes, integerEncodedBytes(descriptor.keyStep()));
        return ReplyPlans.bulkStringArray(6, encodedElementBytes, 0L).encodedUpperBoundBytes();
    }

    private static long integerEncodedBytes(long value) {
        return 3L + Long.toString(value).length();
    }

    private static long saturatingAdd(long left, long right) {
        if (left < 0L || right < 0L || left > Long.MAX_VALUE - right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    private static void writeCommandInfo(RedisReplyWriter out, String nameUpper, CommandDescriptor descriptor) {
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
