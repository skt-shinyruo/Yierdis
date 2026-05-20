package yier.bubu.redis.command.defaults.list;

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

import yier.bubu.redis.storage.api.result.BulkStringSequence;
import yier.bubu.redis.execution.api.CommandContext;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.ReplyWriter;

import java.util.List;
import java.util.Objects;

public final class ListCommands implements CommandModule {
    private final CommandSupport support;

    public ListCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    @Override
    public void register(CommandModule.Registration registration) {
        Objects.requireNonNull(registration, "registration");
        registration.register("LPUSH", CommandDescriptor.of(-3, 1, 1, 1), CommandParsers.minRequest(3, "lpush"), this::lpush);
        registration.register("RPUSH", CommandDescriptor.of(-3, 1, 1, 1), CommandParsers.minRequest(3, "rpush"), this::rpush);
        registration.register("LRANGE", CommandDescriptor.of(4, 1, 1, 1), CommandParsers.exactRequest(4, "lrange"), this::lrange);
        registration.register("LPOP", CommandDescriptor.of(-2, 1, 1, 1), CommandParsers.oneOfRequest("lpop", 2, 3), this::lpop);
        registration.register("RPOP", CommandDescriptor.of(-2, 1, 1, 1), CommandParsers.oneOfRequest("rpop", 2, 3), this::rpop);
    }

    private void lpush(ExecutionRequest request, CommandContext ctx) {
        push(request, ctx, true);
    }

    private void rpush(ExecutionRequest request, CommandContext ctx) {
        push(request, ctx, false);
    }

    private void lpop(ExecutionRequest request, CommandContext ctx) {
        pop(request, ctx, true);
    }

    private void rpop(ExecutionRequest request, CommandContext ctx) {
        pop(request, ctx, false);
    }

    private void push(ExecutionRequest request, CommandContext ctx, boolean left) {
        ReplyWriter out = ctx.out();
        if (request.argc() < 3) {
            CommandSupport.wrongArity(out, left ? "lpush" : "rpush");
            return;
        }
        int valuesLen = request.argc() - 2;
        support.sliceResetFromRequest(request, 2, valuesLen);
        try {
            long length = support.recordWriteValue(
                    ctx,
                    left
                            ? support.commandDb(ctx).writes().lists().lpush(request.readOnlyByteArray(1), support.slice())
                            : support.commandDb(ctx).writes().lists().rpush(request.readOnlyByteArray(1), support.slice())
            );
            out.integer(length);
        } finally {
            support.clearScratch(valuesLen);
        }
    }

    private void lrange(ExecutionRequest request, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (request.argc() != 4) {
            CommandSupport.wrongArity(out, "lrange");
            return;
        }
        int start = CommandSupport.parseIntClamped(request, 2, "start");
        int stop = CommandSupport.parseIntClamped(request, 3, "stop");

        byte[] key = request.readOnlyByteArray(1);
        BulkStringSequence seq = support.commandDb(ctx).reads().lists().lrange(key, start, stop);
        int count = seq.count();
        out.arrayHeader(count);
        if (count == 0) {
            return;
        }
        seq.emitTo(new BulkStringReplyAdapter(out));
    }

    private void pop(ExecutionRequest request, CommandContext ctx, boolean left) {
        ReplyWriter out = ctx.out();
        if (request.argc() != 2 && request.argc() != 3) {
            CommandSupport.wrongArity(out, left ? "lpop" : "rpop");
            return;
        }
        int count = 1;
        boolean hasCount = request.argc() == 3;
        if (hasCount) {
            long v = CommandSupport.parseLong(request, 2, "count");
            if (v < 0) {
                throw new IllegalArgumentException("value is not an integer or out of range");
            }
            if (v > Integer.MAX_VALUE) {
                count = Integer.MAX_VALUE;
            } else {
                count = (int) v;
            }
        }

        List<byte[]> popped = support.recordWriteValue(
                ctx,
                left
                        ? support.commandDb(ctx).writes().lists().lpop(request.readOnlyByteArray(1), count)
                        : support.commandDb(ctx).writes().lists().rpop(request.readOnlyByteArray(1), count)
        );
        popResponse(out, popped, hasCount);
    }

    private static void popResponse(ReplyWriter out, List<byte[]> popped, boolean hasCount) {
        if (!hasCount) {
            if (popped == null || popped.isEmpty()) {
                out.bulkString((byte[]) null);
                return;
            }
            out.bulkString(popped.get(0));
            return;
        }
        if (popped == null) {
            out.nullArray();
            return;
        }
        out.bulkStringArray(popped);
    }
}
