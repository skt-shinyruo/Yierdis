package yier.bubu.redis.command;

import yier.bubu.redis.ops.DbMemoryConstants;
import yier.bubu.redis.ops.DbEngine;
import yier.bubu.redis.ops.result.BulkStringSequence;
import yier.bubu.redis.protocol.Command;
import yier.bubu.redis.protocol.CommandContext;
import yier.bubu.redis.protocol.ReplyWriter;

import java.util.List;
import java.util.Objects;

final class ListCommands {
    private final CommandSupport support;

    ListCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    void register(CommandRegistry registry) {
        Objects.requireNonNull(registry, "registry");
        registry.register("LPUSH", this::lpush);
        registry.register("RPUSH", this::rpush);
        registry.register("LRANGE", this::lrange);
        registry.register("LPOP", this::lpop);
        registry.register("RPOP", this::rpop);
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
        DbEngine engine = support.db(ctx);
        long extra = (long) Math.max(0, cmd.len(1)) + DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE;
        for (int i = 2; i < cmd.argc(); i++) {
            extra += Math.max(0, cmd.len(i));
        }
        engine.eviction().prepareWrite(extra);
        int valuesLen = cmd.argc() - 2;
        support.sliceResetFromCommand(cmd, 2, valuesLen);
        try {
            long len = left
                    ? engine.values().lists().lpush(cmd.toByteArray(1), support.slice())
                    : engine.values().lists().rpush(cmd.toByteArray(1), support.slice());
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
        DbEngine engine = support.db(ctx);
        BulkStringSequence seq = engine.values().lists().lrange(key, start, stop);
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

        DbEngine engine = support.db(ctx);
        List<byte[]> popped = left
                ? engine.values().lists().lpop(cmd.toByteArray(1), count)
                : engine.values().lists().rpop(cmd.toByteArray(1), count);
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
