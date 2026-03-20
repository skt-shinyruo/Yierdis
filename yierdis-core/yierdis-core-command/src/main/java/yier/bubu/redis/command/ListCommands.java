package yier.bubu.redis.command;

import yier.bubu.redis.ops.result.BulkStringSequence;
import yier.bubu.redis.contract.Command;
import yier.bubu.redis.contract.CommandContext;
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
        registration.register("LPUSH", this::lpush);
        registration.register("RPUSH", this::rpush);
        registration.register("LRANGE", this::lrange);
        registration.register("LPOP", this::lpop);
        registration.register("RPOP", this::rpop);
    }

    private void lpush(Command cmd, CommandContext ctx) {
        push(cmd, ctx, true);
    }

    private void rpush(Command cmd, CommandContext ctx) {
        push(cmd, ctx, false);
    }

    private void lpop(Command cmd, CommandContext ctx) {
        pop(cmd, ctx, true);
    }

    private void rpop(Command cmd, CommandContext ctx) {
        pop(cmd, ctx, false);
    }

    private void push(Command cmd, CommandContext ctx, boolean left) {
        ReplyWriter out = ctx.out();
        if (cmd.argc() < 3) {
            CommandSupport.wrongArity(out, left ? "lpush" : "rpush");
            return;
        }
        int valuesLen = cmd.argc() - 2;
        support.sliceResetFromCommand(cmd, 2, valuesLen);
        try {
            long len = left
                    ? support.dbWrites(ctx).lists().lpush(cmd.toByteArray(1), support.slice())
                    : support.dbWrites(ctx).lists().rpush(cmd.toByteArray(1), support.slice());
            out.integer(len);
        } finally {
            support.clearScratch(valuesLen);
        }
    }

    private void lrange(Command cmd, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (cmd.argc() != 4) {
            CommandSupport.wrongArity(out, "lrange");
            return;
        }
        int start = CommandSupport.parseIntClamped(cmd, 2, "start");
        int stop = CommandSupport.parseIntClamped(cmd, 3, "stop");

        byte[] key = cmd.toByteArray(1);
        BulkStringSequence seq = support.dbReads(ctx).lists().lrange(key, start, stop);
        int count = seq.count();
        out.arrayHeader(count);
        if (count == 0) {
            return;
        }
        seq.emitTo(new BulkStringReplyAdapter(out));
    }

    private void pop(Command cmd, CommandContext ctx, boolean left) {
        ReplyWriter out = ctx.out();
        if (cmd.argc() != 2 && cmd.argc() != 3) {
            CommandSupport.wrongArity(out, left ? "lpop" : "rpop");
            return;
        }
        int count = 1;
        boolean hasCount = cmd.argc() == 3;
        if (hasCount) {
            long v = CommandSupport.parseLong(cmd, 2, "count");
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
                ? support.dbWrites(ctx).lists().lpop(cmd.toByteArray(1), count)
                : support.dbWrites(ctx).lists().rpop(cmd.toByteArray(1), count);
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
