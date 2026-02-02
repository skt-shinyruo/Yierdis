package yier.bubu.redis.command;

import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.db.DbMemoryConstants;
import yier.bubu.redis.protocol.RespCommand;
import yier.bubu.redis.protocol.RespWriter;

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

    private void lpush(RespCommand cmd, RespWriter out) {
        push(cmd, out, true);
    }

    private void rpush(RespCommand cmd, RespWriter out) {
        push(cmd, out, false);
    }

    private void lpop(RespCommand cmd, RespWriter out) {
        pop(cmd, out, true);
    }

    private void rpop(RespCommand cmd, RespWriter out) {
        pop(cmd, out, false);
    }

    private void push(RespCommand cmd, RespWriter out, boolean left) {
        if (cmd.argc() < 3) {
            CommandSupport.wrongArity(out, left ? "lpush" : "rpush");
            return;
        }
        YierdisDb db = support.db(out);
        long extra = (long) Math.max(0, cmd.len(1)) + DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE;
        for (int i = 2; i < cmd.argc(); i++) {
            extra += Math.max(0, cmd.len(i));
        }
        db.prepareWrite(extra);
        int valuesLen = cmd.argc() - 2;
        support.sliceResetFromCommand(cmd, 2, valuesLen);
        try {
            int len = left
                    ? db.lpush(cmd.toByteArray(1), support.slice())
                    : db.rpush(cmd.toByteArray(1), support.slice());
            db.enforceMaxmemory();
            out.integer(len);
        } finally {
            support.clearScratch(valuesLen);
        }
    }

    private void lrange(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 4) {
            CommandSupport.wrongArity(out, "lrange");
            return;
        }
        int start = CommandSupport.parseIntClamped(cmd, 2, "start");
        int stop = CommandSupport.parseIntClamped(cmd, 3, "stop");

        byte[] key = cmd.toByteArray(1);
        int count = support.db(out).lrangeReplyCount(key, start, stop);
        out.arrayHeader(count);
        if (count == 0) {
            return;
        }
        support.db(out).lrangeReplyInto(key, start, stop, support.bulkOut(out));
    }

    private void pop(RespCommand cmd, RespWriter out, boolean left) {
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
                ? support.db(out).lpop(cmd.toByteArray(1), count)
                : support.db(out).rpop(cmd.toByteArray(1), count);
        popResponse(out, popped, hasCount);
    }

    private static void popResponse(RespWriter out, List<byte[]> popped, boolean hasCount) {
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
