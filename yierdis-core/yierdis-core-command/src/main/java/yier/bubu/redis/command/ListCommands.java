package yier.bubu.redis.command;

import yier.bubu.redis.ops.result.BulkStringSequence;
import yier.bubu.redis.contract.CommandContext;
import yier.bubu.redis.contract.ExecutionRequest;
import yier.bubu.redis.contract.ReplyWriter;

import java.util.List;
import java.util.Objects;

final class ListCommands implements CommandModule {
    private final CommandSupport support;

    ListCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    @Override
    public void register(CommandModule.Registration registration) {
        Objects.requireNonNull(registration, "registration");
        registration.register("LPUSH", this::lpush, CommandDescriptor.of(-3, 1, 1, 1));
        registration.register("RPUSH", this::rpush, CommandDescriptor.of(-3, 1, 1, 1));
        registration.register("LRANGE", this::lrange, CommandDescriptor.of(4, 1, 1, 1));
        registration.register("LPOP", this::lpop, CommandDescriptor.of(-2, 1, 1, 1));
        registration.register("RPOP", this::rpop, CommandDescriptor.of(-2, 1, 1, 1));
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
            long len = left
                    ? support.dbWrites(ctx).lists().lpush(request.readOnlyByteArray(1), support.slice())
                    : support.dbWrites(ctx).lists().rpush(request.readOnlyByteArray(1), support.slice());
            out.integer(len);
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
        BulkStringSequence seq = support.dbReads(ctx).lists().lrange(key, start, stop);
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

        List<byte[]> popped = left
                ? support.dbWrites(ctx).lists().lpop(request.readOnlyByteArray(1), count)
                : support.dbWrites(ctx).lists().rpop(request.readOnlyByteArray(1), count);
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
