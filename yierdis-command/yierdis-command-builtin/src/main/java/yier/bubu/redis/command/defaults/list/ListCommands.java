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

import yier.bubu.redis.storage.api.result.MeasuredBulkStringSequence;
import yier.bubu.redis.storage.api.result.PoppedValueSequence;
import yier.bubu.redis.execution.api.CommandContext;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.ReplyPlan;
import yier.bubu.redis.execution.api.ReplyPlans;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.common.command.ResultUnknownException;

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
        RedisReplyWriter out = ctx.out();
        if (request.argc() < 3) {
            CommandSupport.wrongArity(out, left ? "lpush" : "rpush");
            return;
        }
        int valuesLen = request.argc() - 2;
        support.sliceResetFromRequest(request, 2, valuesLen);
        try {
            long length = (left
                    ? support.commandDb(ctx).writes().lists().lpush(request.readOnlyByteArray(1), support.slice())
                    : support.commandDb(ctx).writes().lists().rpush(request.readOnlyByteArray(1), support.slice()))
                    .value();
            out.integer(length);
        } finally {
            support.clearScratch(valuesLen);
        }
    }

    private void lrange(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        if (request.argc() != 4) {
            CommandSupport.wrongArity(out, "lrange");
            return;
        }
        int start = CommandSupport.parseIntClamped(request, 2, "start");
        int stop = CommandSupport.parseIntClamped(request, 3, "stop");

        byte[] key = request.readOnlyByteArray(1);
        MeasuredBulkStringSequence seq = support.commandDb(ctx).reads().lists().lrange(key, start, stop);
        CommandSupport.writeMeasuredBulkStringArray(out, seq);
    }

    private void pop(ExecutionRequest request, CommandContext ctx, boolean left) {
        RedisReplyWriter out = ctx.out();
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

        ReplyPlan preflight;
        try (PoppedValueSequence preview = support.commandDb(ctx).reads().lists()
                .previewPop(request.readOnlyByteArray(1), count, left)) {
            preflight = popReplyPlan(preview, hasCount);
            out.requireReply(preflight);
        }

        PoppedValueSequence popped = (left
                ? support.commandDb(ctx).writes().lists().lpop(request.readOnlyByteArray(1), count)
                : support.commandDb(ctx).writes().lists().rpop(request.readOnlyByteArray(1), count))
                .value();
        boolean ownershipTransferred = false;
        try {
            if (!matchesPreflight(preflight, popped, hasCount)) {
                throw new ResultUnknownException("pop reply source changed after mutation");
            }
            popResponse(out, popped, hasCount);
            if (popped != null) {
                out.transferReplyOwnership(popped);
                ownershipTransferred = true;
            }
        } finally {
            if (popped != null && !ownershipTransferred) {
                popped.close();
            }
        }
    }

    private static ReplyPlan popReplyPlan(PoppedValueSequence popped, boolean hasCount) {
        if (!hasCount) {
            if (popped == null || popped.isNull() || popped.count() == 0) {
                return ReplyPlans.bulkString(-1, 0L);
            }
            return ReplyPlans.raw(popped.encodedElementBytes(), popped.retainedMemoryBytes());
        }
        if (popped == null || popped.isNull()) {
            return ReplyPlans.raw(5L, 0L);
        }
        return ReplyPlans.bulkStringArray(popped.count(), popped.encodedElementBytes(), popped.retainedMemoryBytes());
    }

    private static boolean matchesPreflight(ReplyPlan plan, PoppedValueSequence popped, boolean hasCount) {
        return popReplyPlan(popped, hasCount).equals(plan);
    }

    private static void popResponse(RedisReplyWriter out, PoppedValueSequence popped, boolean hasCount) {
        if (!hasCount) {
            if (popped == null || popped.isNull() || popped.count() == 0) {
                out.bulkString((byte[]) null);
                return;
            }
            popped.emitTo(new BulkStringReplyAdapter(out));
            return;
        }
        if (popped == null || popped.isNull()) {
            out.nullArray();
            return;
        }
        out.arrayHeader(popped.count());
        if (popped.count() > 0) {
            popped.emitTo(new BulkStringReplyAdapter(out));
        }
    }
}
