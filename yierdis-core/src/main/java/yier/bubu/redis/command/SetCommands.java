package yier.bubu.redis.command;

import yier.bubu.redis.db.DbMemoryConstants;
import yier.bubu.redis.ops.DbEngine;
import yier.bubu.redis.ops.result.BulkStringSequence;
import yier.bubu.redis.protocol.Command;
import yier.bubu.redis.protocol.CommandContext;
import yier.bubu.redis.protocol.ReplyWriter;

import java.util.Objects;

final class SetCommands {
    private final CommandSupport support;

    SetCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    void register(CommandRegistry registry) {
        Objects.requireNonNull(registry, "registry");
        registry.register("SADD", this::sadd);
        registry.register("SREM", this::srem);
        registry.register("SMEMBERS", this::smembers);
        registry.register("SISMEMBER", this::sismember);
        registry.register("SCARD", this::scard);
    }

    private void sadd(Command cmd, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (cmd.argc() < 3) {
            CommandSupport.wrongArity(out, "sadd");
            return;
        }
        DbEngine engine = support.db(ctx);
        long extra = (long) Math.max(0, cmd.len(1)) + DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE;
        for (int i = 2; i < cmd.argc(); i++) {
            extra += Math.max(0, cmd.len(i));
        }
        engine.eviction().prepareWrite(extra);
        int membersLen = cmd.argc() - 2;
        support.sliceResetFromCommand(cmd, 2, membersLen);
        try {
            long added = engine.values().sets().sadd(cmd.toByteArray(1), support.slice());
            out.integer(added);
        } finally {
            support.clearScratch(membersLen);
        }
    }

    private void srem(Command cmd, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (cmd.argc() < 3) {
            CommandSupport.wrongArity(out, "srem");
            return;
        }
        int membersLen = cmd.argc() - 2;
        support.sliceResetFromCommand(cmd, 2, membersLen);
        try {
            DbEngine engine = support.db(ctx);
            out.integer(engine.values().sets().srem(cmd.toByteArray(1), support.slice()));
        } finally {
            support.clearScratch(membersLen);
        }
    }

    private void smembers(Command cmd, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (cmd.argc() != 2) {
            CommandSupport.wrongArity(out, "smembers");
            return;
        }

        byte[] key = cmd.toByteArray(1);
        DbEngine engine = support.db(ctx);
        BulkStringSequence seq = engine.values().sets().smembers(key);
        int count = seq.count();
        out.arrayHeader(count);
        if (count == 0) {
            return;
        }
        seq.emitTo(new BulkStringReplyAdapter(out));
    }

    private void sismember(Command cmd, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (cmd.argc() != 3) {
            CommandSupport.wrongArity(out, "sismember");
            return;
        }
        DbEngine engine = support.db(ctx);
        out.integer(engine.values().sets().sismember(cmd.toByteArray(1), cmd.toByteArray(2)) ? 1 : 0);
    }

    private void scard(Command cmd, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (cmd.argc() != 2) {
            CommandSupport.wrongArity(out, "scard");
            return;
        }
        DbEngine engine = support.db(ctx);
        out.integer(engine.values().sets().scard(cmd.toByteArray(1)));
    }
}
